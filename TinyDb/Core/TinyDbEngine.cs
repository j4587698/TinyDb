using System;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using TinyDb.Bson;
using TinyDb.Security;
using TinyDb.Collections;
using TinyDb.Index;
using TinyDb.Serialization;
using TinyDb.Storage;
using TinyDb.Attributes;
using TinyDb.Utils;
using Microsoft.IO;

namespace TinyDb.Core;

/// <summary>
/// TinyDb 核心存储引擎。
/// 该引擎管理数据库的完整生命周期，包括文件 I/O、页面管理、写前日志 (WAL)、
/// 事务协调、索引管理以及文档的 CRUD 操作。
/// </summary>
/// <remarks>
/// 架构层次：
/// 1. 存储层：IDiskStream -> PageManager -> WriteAheadLog (WAL)
/// 2. 逻辑层：CollectionMetaStore -> CollectionState -> IDocumentCollection
/// 3. 操作层：TransactionManager -> IndexManager -> QueryExecutor
/// 
/// 引擎确保在崩溃后通过 WAL 自动恢复数据一致性。
/// </remarks>
public sealed class TinyDbEngine : IDisposable
{
    private readonly string _filePath;
    private readonly TinyDbOptions _options;
    private IDiskStream _diskStream = null!;
    private PageManager _pageManager = null!;
    private WriteAheadLog _writeAheadLog = null!;
    private FlushScheduler _flushScheduler = null!;
    private TinyDb.Metadata.MetadataManager _metadataManager = null!;
    private readonly ConcurrentDictionary<string, IDocumentCollection> _collections;
    internal CollectionMetaStore _collectionMetaStore = null!;
    private readonly ConcurrentDictionary<string, IndexManager> _indexManagers;
    private readonly TransactionManager _transactionManager;
    private readonly ConcurrentDictionary<string, CollectionState> _collectionStates;
    private LargeDocumentStorage _largeDocumentStorage = null!;
    private DataPageAccess _dataPageAccess = null!;
    private readonly object _lock = new();
    private readonly AsyncLocal<ITransaction?> _currentTransaction = new();
    private DatabaseHeader _header;
    private bool _disposed;
    private bool _isInitialized;

    private const int DocumentLengthPrefixSize = sizeof(int);
    private const int MinimumFreeSpaceThreshold = DocumentLengthPrefixSize + 64;

    /// <summary>
    /// 获取数据库文件路径。
    /// </summary>
    public string FilePath => _filePath;

    /// <summary>
    /// 获取此数据库实例使用的选项。
    /// </summary>
    public TinyDbOptions Options => _options;

    public TinyDb.Metadata.MetadataManager MetadataManager => _metadataManager;

    /// <summary>
    /// 获取数据库头部信息。
    /// </summary>
    public DatabaseHeader Header => _header;

    /// <summary>
    /// 获取数据库是否已初始化的值。
    /// </summary>
    public bool IsInitialized => _isInitialized;

    /// <summary>
    /// 获取数据库中集合的数量。
    /// </summary>
    public int CollectionCount => GetCollectionNames().Count();

    /// <summary>
    /// 初始化 <see cref="TinyDbEngine"/> 类的新实例。
    /// </summary>
    /// <param name="f">数据库文件路径。</param>
    /// <param name="o">可选的配置选项。</param>
    [DynamicDependency(DynamicallyAccessedMemberTypes.All, typeof(TinyDbEngine))]
    public TinyDbEngine(string f, TinyDbOptions? o = null) : this(f, o, null) { }

    internal TinyDbEngine(string f, TinyDbOptions? o, IDiskStream? ds)
    {
        _filePath = f ?? throw new ArgumentNullException();
        _options = o ?? new TinyDbOptions();
        _options.Validate();

        _collections = new ConcurrentDictionary<string, IDocumentCollection>(StringComparer.Ordinal);
        _indexManagers = new ConcurrentDictionary<string, IndexManager>(StringComparer.Ordinal);
        _transactionManager = new TransactionManager(this, _options.MaxTransactions, _options.TransactionTimeout);
        _collectionStates = new ConcurrentDictionary<string, CollectionState>(StringComparer.Ordinal);

        InitializeComponents(ds);
    }

    private void InitializeComponents(IDiskStream? ds)
    {
        if (ds != null)
        {
            _diskStream = ds;
        }
        else
        {
            var dir = Path.GetDirectoryName(_filePath);
            if (!string.IsNullOrEmpty(dir) && !Directory.Exists(dir)) Directory.CreateDirectory(dir);
            _diskStream = new DiskStream(_filePath, FileAccess.ReadWrite, FileShare.ReadWrite);
        }

        _pageManager = new PageManager(_diskStream, _options.PageSize, _options.CacheSize);
        _writeAheadLog = new WriteAheadLog(_filePath, (int)_options.PageSize, _options.EnableJournaling, _options.WalFileNameFormat);
        
        _pageManager.RegisterWAL(lsn => _writeAheadLog.FlushToLSNAsync(lsn));

        _flushScheduler = new FlushScheduler(_pageManager, _writeAheadLog, NormalizeInterval(_options.BackgroundFlushInterval));
        _largeDocumentStorage = new LargeDocumentStorage(_pageManager, (int)_options.PageSize);
        _dataPageAccess = new DataPageAccess(_pageManager, _largeDocumentStorage, _writeAheadLog);
        _metadataManager = new TinyDb.Metadata.MetadataManager(this);

        InitializeDatabase();
    }

    private void DisposeComponents()
    {
        _flushScheduler.Dispose();
        _writeAheadLog.Dispose();
        _pageManager.Dispose();
        _diskStream.Dispose();
    }

    /// <summary>
    /// 压缩数据库（碎片整理）
    /// 注意：此操作会阻塞所有其他操作，并重建数据库文件。
    /// </summary>
    public void CompactDatabase()
    {
        ThrowIfDisposed();
        lock (_lock)
        {
            EnsureInitialized();
            
            // 1. 刷新当前状态
            Flush();
            
            var tempFile = _filePath + ".compact";
            if (File.Exists(tempFile)) File.Delete(tempFile);
            
            // 使用临时引擎写入新文件
            using (var tempEngine = new TinyDbEngine(tempFile, _options))
            {
                // 2. 迁移数据
                foreach (var colName in GetCollectionNames(includeSystemCollections: true))
                {
                    // 获取源数据
                    var docs = FindAll(colName).ToList();

                    // 写入目标：避免 GetCollection<T> 的泛型缓存冲突（同名集合可能同时以不同实体类型访问）
                    tempEngine.RegisterCollection(colName);

                    // 3. 重建索引（先建索引，再写入，确保索引得到填充）
                    var idxMgr = GetIndexManager(colName);
                    var tempIdxMgr = tempEngine.GetIndexManager(colName);
                    foreach (var stat in idxMgr.GetAllStatistics())
                    {
                        tempIdxMgr.CreateIndex(stat.Name, stat.Fields, stat.IsUnique);
                    }

                    if (docs.Count > 0)
                    {
                        tempEngine.InsertDocuments(colName, docs.ToArray());
                    }
                }
            }
            
            // 4. 释放当前组件以解除文件锁定
            DisposeComponents();
            
            // 5. 替换文件
            // 先备份? 可选，这里直接覆盖
            try 
            {
                File.Move(tempFile, _filePath, overwrite: true);
            }
            catch
            {
                // 移动失败，尝试恢复组件
                InitializeComponents(null);
                throw;
            }
            
            // 6. 清理缓存状态
            _collectionStates.Clear();
            DisposeIndexManagers(); 
            _collectionMetaStore = null!;
            _isInitialized = false;
            
            // 7. 重新初始化
            InitializeComponents(null);
        }
    }

    /// <summary>
    /// 初始化数据库。
    /// 此方法是引擎启动的核心，负责：
    /// 1. 执行 WAL 重放以实现崩溃恢复。
    /// 2. 检查数据库文件完整性或初始化新文件。
    /// 3. 同步内存中的页面管理器状态与磁盘 Header。
    /// 4. 加载系统页和集合元数据。
    /// </summary>
    private void InitializeDatabase()
    {
        lock (_lock)
        {
            if (_isInitialized) return;
            try
            {
                // 步骤 1: 崩溃恢复。
                // 只有当 WAL 中的 LSN 大于磁盘页面的 LSN 时，才应用恢复逻辑（幂等恢复）。
                if (_writeAheadLog.IsEnabled)
                {
                    _writeAheadLog.ReplayAsync(async (id, data) =>
                    {
                        var pageOffset = (id - 1) * _options.PageSize;
                        if (_diskStream.Size >= pageOffset + _options.PageSize)
                        {
                            var diskData = await _diskStream.ReadPageAsync(pageOffset, (int)_options.PageSize).ConfigureAwait(false);
                            var diskHeader = PageHeader.FromByteArray(diskData);
                            var walHeader = PageHeader.FromByteArray(data);

                            if (walHeader.LSN <= diskHeader.LSN)
                            {
                                // 磁盘版本已经是最新的，跳过此条日志
                                return;
                            }
                        }
                        
                        _pageManager.RestorePage(id, data);
                    }).GetAwaiter().GetResult();
                }

                // 步骤 2: 文件结构初始化。
                if (_diskStream.Size == 0)
                {
                    // 空文件：初始化 Header 页。
                    _header = new DatabaseHeader();
                    _header.Initialize(_options.PageSize, _options.DatabaseName, _options.EnableJournaling);
                    var p1 = _pageManager.NewPage(PageType.Header);
                    _pageManager.SavePage(p1, true);
                    // 初始化 PageManager (新数据库)
                    _pageManager.Initialize(1, 0);
                }
                else
                {
                    // 已有文件：加载 Header 并验证。
                    ReadHeader();
                    if (!_header.IsValid()) throw new InvalidOperationException("Invalid database header");
                    
                    // 初始化 PageManager (现有数据库)
                    _pageManager.Initialize(_header.TotalPages, _header.FirstFreePage);
                    
                    // 步骤 3: 状态一致性同步。
                    // 关键修复：如果在崩溃前分配了页面但 Header 未更新，
                    // WAL 重放后 PageManager 知道最新状态，需反向同步回 Header
                    if (_pageManager.TotalPages > _header.TotalPages || _pageManager.FirstFreePageID != _header.FirstFreePage)
                    {
                        WriteHeader();
                    }
                }

                // 步骤 4: 加载核心系统组件。
                _header.EnableJournaling = _options.EnableJournaling;
                InitializeSystemPages();
                _collectionMetaStore = new CollectionMetaStore(_pageManager, () => _header.CollectionInfoPage, id => _header.CollectionInfoPage = id);
                _collectionMetaStore.LoadCollections();
                _isInitialized = true;
                
                // 步骤 5: 安全检查。
                EnsureDatabaseSecurity();
            }
            catch
            {
                _isInitialized = false;
                Dispose();
                throw;
            }
        }
    }

    private void ReadHeader()
    {
        var p = _pageManager.GetPage(1);
        _header = DatabaseHeader.FromByteArray(p.ReadBytes(0, DatabaseHeader.Size));
    }

    private void WriteHeader()
    {
        _header.TotalPages = _pageManager.TotalPages;
        _header.FirstFreePage = _pageManager.FirstFreePageID;
        var p = _pageManager.GetPage(1);
        p.WriteData(0, _header.ToByteArray());
        _pageManager.SavePage(p, false);
    }

    private void InitializeSystemPages()
    {
        if (_header.CollectionInfoPage == 0) _header.CollectionInfoPage = AllocateSystemPage(PageType.Collection, "Cols");
        if (_header.IndexInfoPage == 0) _header.IndexInfoPage = AllocateSystemPage(PageType.Index, "Idxs");
    }

    private uint AllocateSystemPage(PageType t, string n)
    {
        var p = _pageManager.NewPage(t);
        _pageManager.SavePage(p, true);
        return p.PageID;
    }

    /// <summary>
    /// 执行检查点。
    /// 将所有脏页面刷新到磁盘，并截断 WAL 日志。
    /// </summary>
    public async Task CheckpointAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        EnsureInitialized();

        // 1. 刷新所有脏页面
        // 由于 SavePage 已包含 WAL 检查，这一步会确保相关日志先刷新
        await _pageManager.FlushDirtyPagesAsync(cancellationToken).ConfigureAwait(false);

        // 2. 截断 WAL
        await _writeAheadLog.TruncateAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// 开始一个新事务。
    /// </summary>
    /// <returns>新创建的事务。</returns>
    public ITransaction BeginTransaction()
    {
        ThrowIfDisposed();
        EnsureInitialized();
        var t = _transactionManager.BeginTransaction();
        _currentTransaction.Value = t;
        return t;
    }

    /// <summary>
    /// 获取指定集合的元数据。
    /// </summary>
    internal BsonDocument GetCollectionMetadata(string collectionName)
    {
        return _collectionMetaStore.GetMetadata(collectionName);
    }

    /// <summary>
    /// 确保当前事务的所有变更已持久化。
    /// </summary>
    internal void CommitTransactionDurability()
    {
        // 强制刷新 WAL 缓冲区到磁盘。
        // 对于最高安全级别 (WriteConcern.Journaled)，这将等待磁盘确认。
        _writeAheadLog.FlushLogAsync().GetAwaiter().GetResult();
    }

    internal Transaction? GetCurrentTransaction() => _currentTransaction.Value as Transaction;

    /// <summary>
    /// 获取关于事务的统计信息。
    /// </summary>
    /// <returns>事务管理器统计信息。</returns>
    public TransactionManagerStatistics GetTransactionStatistics()
    {
        ThrowIfDisposed();
        return _transactionManager.GetStatistics();
    }

    /// <summary>
    /// 检索或为指定类型创建集合。
    /// 集合名称按以下优先级确定：Entity特性的Name属性 > 类名
    /// </summary>
    /// <typeparam name="T">集合中文档的类型。</typeparam>
    /// <returns>集合实例。</returns>
    public ITinyCollection<T> GetCollection<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>() where T : class, new()
    {
        var n = GetCollectionNameFromEntityAttribute<T>() ?? typeof(T).Name;
        return (ITinyCollection<T>)_collections.GetOrAdd(n, name =>
        {
            _metadataManager.EnsureSchema(name, typeof(T));
            RegisterCollection(name);
            return new DocumentCollection<T>(this, name);
        });
    }

    /// <summary>
    /// 检索或使用特定名称创建集合。
    /// 集合名称按以下优先级确定：name参数 > Entity特性的Name属性 > 类名
    /// </summary>
    /// <typeparam name="T">集合中文档的类型。</typeparam>
    /// <param name="name">集合的名称（可选）。如果为null或空，则使用Entity特性的Name属性或类名。</param>
    /// <returns>集合实例。</returns>
    public ITinyCollection<T> GetCollection<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>(string? name) where T : class, new()
    {
        var n = !string.IsNullOrEmpty(name) ? name : (GetCollectionNameFromEntityAttribute<T>() ?? typeof(T).Name);
        return (ITinyCollection<T>)_collections.GetOrAdd(n, actualName =>
        {
            _metadataManager.EnsureSchema(actualName, typeof(T));
            RegisterCollection(actualName);
            return new DocumentCollection<T>(this, actualName);
        });
    }



    /// <summary>
    /// 获取数据库中所有集合的名称。
    /// </summary>
    /// <returns>集合名称列表。</returns>
    public IEnumerable<string> GetCollectionNames()
        => GetCollectionNames(includeSystemCollections: false);

    public IEnumerable<string> GetCollectionNames(bool includeSystemCollections)
    {
        ThrowIfDisposed();
        EnsureInitialized();
        var all = new HashSet<string>(_collectionMetaStore.GetCollectionNames());
        foreach (var n in _collections.Keys) all.Add(n);

        if (!includeSystemCollections)
        {
            all.RemoveWhere(n => n.StartsWith("__", StringComparison.Ordinal));
        }

        return all.ToList();
    }

    /// <summary>
    /// 检查集合是否存在。
    /// </summary>
    /// <param name="n">集合名称。</param>
    /// <returns>如果集合存在则为 true；否则为 false。</returns>
    public bool CollectionExists(string n) => _collections.ContainsKey(n) || _collectionMetaStore.IsKnown(n);

    /// <summary>
    /// 删除（丢弃）集合及其所有数据。
    /// </summary>
    /// <param name="n">集合名称。</param>
    /// <returns>如果集合被删除则为 true；如果集合不存在则为 false。</returns>
    public bool DropCollection(string n)
    {
        ThrowIfDisposed();
        EnsureInitialized();
        bool r = _collections.TryRemove(n, out var col);
        if (r)
        {
            col.DeleteAll();
            col.Dispose();
        }
        if (_collectionMetaStore.IsKnown(n))
        {
            _collectionMetaStore.RemoveCollection(n, true);
            _collectionStates.TryRemove(n, out _);
            return true;
        }
        return r;
    }

    /// <summary>
    /// 将所有挂起的更改刷新到磁盘。
    /// </summary>
    public void Flush()
    {
        ThrowIfDisposed();
        if (!_isInitialized) return;
        _flushScheduler.FlushAsync().GetAwaiter().GetResult();
        _collectionMetaStore.SaveCollections(true);
        WriteHeader();
        _diskStream.Flush();
    }

    /// <summary>
    /// 获取数据库统计信息。
    /// </summary>
    /// <returns>统计信息对象。</returns>
    public DatabaseStatistics GetStatistics()
    {
        ThrowIfDisposed();
        EnsureInitialized();
        var pmStats = _pageManager.GetStatistics();
        return new DatabaseStatistics
        {
            FilePath = _filePath,
            DatabaseName = _header.DatabaseName,
            PageSize = _header.PageSize,
            TotalPages = _header.TotalPages,
            UsedPages = _header.UsedPages,
            CollectionCount = GetCollectionNames().Count(),
            EnableJournaling = _options.EnableJournaling,
            FileSize = _diskStream.Size,
            FreePages = pmStats.FreePages
        };
    }

    /// <summary>
    /// 检查是否启用了写前日志 (WAL)。
    /// </summary>
    /// <returns>如果启用了 WAL 则为 true。</returns>
    public bool GetWalEnabled() => _writeAheadLog.IsEnabled;

    /// <summary>
    /// 确保在特定字段上存在索引。
    /// </summary>
    /// <param name="collectionName">集合名称。</param>
    /// <param name="fieldName">要索引的字段。</param>
    /// <param name="indexName">索引的名称。</param>
    /// <param name="unique">索引是否应该是唯一的。</param>
    /// <returns>如果成功则为 true。</returns>
    public bool EnsureIndex(string collectionName, string fieldName, string indexName, bool unique = false)
    {
        return GetIndexManager(collectionName).CreateIndex(indexName, new[] { fieldName }, unique);
    }

    public IndexManager GetIndexManager(string c) => _indexManagers.GetOrAdd(c, n => new IndexManager(n, _pageManager));

    internal void ClearCurrentTransaction() => _currentTransaction.Value = null;

    internal bool TryGetSecurityMetadata(out DatabaseSecurityMetadata m) => _header.TryGetSecurityMetadata(out m);

    internal void SetSecurityMetadata(DatabaseSecurityMetadata m)
    {
        lock (_lock)
        {
            _header.SetSecurityMetadata(m);
            WriteHeader();
        }
    }

    internal void ClearSecurityMetadata()
    {
        lock (_lock)
        {
            _header.ClearSecurityMetadata();
            WriteHeader();
        }
    }

    private void RegisterCollection(string n) => _collectionMetaStore.RegisterCollection(n, _options.WriteConcern == WriteConcern.Synced);

    private void EnsureDatabaseSecurity()
    {
        var p = _options.Password;
        var isS = DatabaseSecurity.IsDatabaseSecure(this);
        if (string.IsNullOrEmpty(p))
        {
            if (isS) throw new UnauthorizedAccessException();
            return;
        }
        if (p.Length < 4) throw new ArgumentException();
        if (isS)
        {
            if (!DatabaseSecurity.AuthenticateDatabase(this, p)) throw new UnauthorizedAccessException();
            return;
        }
        DatabaseSecurity.CreateSecureDatabase(this, p);
    }

    private CollectionState GetCollectionState(string col)
    {
        return _collectionStates.GetOrAdd(col, n =>
        {
            var s = new CollectionState { Index = new MemoryDocumentIndex() };
            BuildDocumentLocationCache(n, s);
            s.MarkCacheInitialized();
            return s;
        });
    }

    internal BsonValue InsertDocument(string col, BsonDocument doc)
    {
        var pr = PrepareDocumentForInsert(col, doc, out var id);
        _metadataManager.ValidateDocumentForWrite(col, pr, _options.SchemaValidationMode);
        var res = InsertPreparedDocument(col, pr, id, GetCollectionState(col), GetIndexManager(col), true);
        EnsureWriteDurability();
        return res;
    }

    /// <summary>
    /// 异步插入文档
    /// </summary>
    /// <param name="col">集合名称</param>
    /// <param name="doc">要插入的文档</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>插入文档的ID</returns>
    internal async Task<BsonValue> InsertDocumentAsync(string col, BsonDocument doc, CancellationToken cancellationToken = default)
    {
        var pr = PrepareDocumentForInsert(col, doc, out var id);
        _metadataManager.ValidateDocumentForWrite(col, pr, _options.SchemaValidationMode);
        var res = InsertPreparedDocument(col, pr, id, GetCollectionState(col), GetIndexManager(col), true);
        await EnsureWriteDurabilityAsync(cancellationToken).ConfigureAwait(false);
        return res;
    }

    internal int UpdateDocument(string col, BsonDocument doc)
    {
        if (!doc.TryGetValue("_id", out var id)) return 0;

        // Ensure _collection field matches the target collection name
        // This is critical for AOT-generated documents where _collection might be derived from [Entity] attribute
        // but stored in a differently named collection (e.g. metadata tables)
        if (!doc.TryGetValue("_collection", out var docCol) || docCol.ToString() != col)
        {
            doc = doc.Set("_collection", col);
        }

        _metadataManager.ValidateDocumentForWrite(col, doc, _options.SchemaValidationMode);

        var st = GetCollectionState(col);
        PageDocumentEntry old = default; // Declare outside lock

        lock (st.PageState.SyncRoot)
        {
            if (!TryResolveDocumentLocation(col, st, id, out var p, out var e, out var i)) return 0;
            if (i >= e.Count) return 0; // Guard against index mismatch
            old = e[i];

            // Handle Large Doc
            var bs = BsonSerializer.SerializeDocument(doc);
            if (LargeDocumentStorage.RequiresLargeDocumentStorage(bs.Length, _dataPageAccess.GetMaxDocumentSize()))
            {
                var lId = _largeDocumentStorage.StoreLargeDocument(bs, col);
                doc = CreateLargeDocumentIndexDocument(id, col, lId, bs.Length);
                bs = BsonSerializer.SerializeDocument(doc);
            }

            e[i] = new PageDocumentEntry(doc, bs);

            // Check if it fits
            if (!_dataPageAccess.CanFitInPage(p, e))
            {
                // Overflow! Fallback: Delete + Insert
                e[i] = old; // Revert in memory list to old state to delete cleanly
                // Delete logic inline
                e.RemoveAt(i);
                st.Index.Remove(id);

                _dataPageAccess.RewritePageWithDocuments(col, st, p, e, (key, pId, idx) => st.Index.Set(key, new DocumentLocation(pId, idx)));

                // Now Insert new
                var pr = PrepareDocumentForInsert(col, doc, out _);
                InsertPreparedDocument(col, pr, id, st, GetIndexManager(col), false); // false = don't update index manager yet
            }
            else
            {
                _dataPageAccess.RewritePageWithDocuments(col, st, p, e, (key, pId, idx) => st.Index.Set(key, new DocumentLocation(pId, idx)));
            }

            if (old.IsLargeDocument) try { _largeDocumentStorage.DeleteLargeDocument(old.LargeDocumentIndexPageId); } catch { }
        }
        GetIndexManager(col).UpdateDocument(old.Document, doc, id);

        EnsureWriteDurability();
        return 1;
    }

    /// <summary>
    /// 异步更新文档
    /// </summary>
    /// <param name="col">集合名称</param>
    /// <param name="doc">要更新的文档</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>更新的文档数量</returns>
    internal async Task<int> UpdateDocumentAsync(string col, BsonDocument doc, CancellationToken cancellationToken = default)
    {
        if (!doc.TryGetValue("_id", out var id)) return 0;

        // Ensure _collection field matches the target collection name
        if (!doc.TryGetValue("_collection", out var docCol) || docCol.ToString() != col)
        {
            doc = doc.Set("_collection", col);
        }

        _metadataManager.ValidateDocumentForWrite(col, doc, _options.SchemaValidationMode);

        var st = GetCollectionState(col);
        PageDocumentEntry old = default;

        lock (st.PageState.SyncRoot)
        {
            if (!TryResolveDocumentLocation(col, st, id, out var p, out var e, out var i)) return 0;
            if (i >= e.Count) return 0;
            old = e[i];

            var bs = BsonSerializer.SerializeDocument(doc);
            if (LargeDocumentStorage.RequiresLargeDocumentStorage(bs.Length, _dataPageAccess.GetMaxDocumentSize()))
            {
                var lId = _largeDocumentStorage.StoreLargeDocument(bs, col);
                doc = CreateLargeDocumentIndexDocument(id, col, lId, bs.Length);
                bs = BsonSerializer.SerializeDocument(doc);
            }

            e[i] = new PageDocumentEntry(doc, bs);

            if (!_dataPageAccess.CanFitInPage(p, e))
            {
                e[i] = old;
                e.RemoveAt(i);
                st.Index.Remove(id);

                _dataPageAccess.RewritePageWithDocuments(col, st, p, e, (key, pId, idx) => st.Index.Set(key, new DocumentLocation(pId, idx)));

                var pr = PrepareDocumentForInsert(col, doc, out _);
                InsertPreparedDocument(col, pr, id, st, GetIndexManager(col), false);
            }
            else
            {
                _dataPageAccess.RewritePageWithDocuments(col, st, p, e, (key, pId, idx) => st.Index.Set(key, new DocumentLocation(pId, idx)));
            }

            if (old.IsLargeDocument) try { _largeDocumentStorage.DeleteLargeDocument(old.LargeDocumentIndexPageId); } catch { }
        }
        GetIndexManager(col).UpdateDocument(old.Document, doc, id);

        await EnsureWriteDurabilityAsync(cancellationToken).ConfigureAwait(false);
        return 1;
    }

    internal int DeleteDocument(string col, BsonValue id)
    {
        var st = GetCollectionState(col);
        lock (st.PageState.SyncRoot)
        {
            if (!TryResolveDocumentLocation(col, st, id, out var p, out var e, out var i)) return 0;
            if (i >= e.Count) return 0; // Guard against index mismatch
            var entry = e[i];
            e.RemoveAt(i);
            st.Index.Remove(id);
            if (e.Count == 0)
            {
                st.OwnedPages.TryRemove(p.PageID, out _); // Remove from OwnedPages
                // Also need to reset st.PageState.PageId if it points to freed page (Bug Fix reinforcement)
                if (st.PageState.PageId == p.PageID) st.PageState.PageId = 0;

                _pageManager.FreePage(p.PageID);
                lock (_lock) { _header.UsedPages--; WriteHeader(); }
            }
            else
            {
                _dataPageAccess.RewritePageWithDocuments(col, st, p, e, (key, pId, idx) => st.Index.Set(key, new DocumentLocation(pId, idx)));
            }
            if (entry.IsLargeDocument) try { _largeDocumentStorage.DeleteLargeDocument(entry.LargeDocumentIndexPageId); } catch { }
            GetIndexManager(col).DeleteDocument(entry.Document, id);
        }
        EnsureWriteDurability();
        return 1;
    }

    /// <summary>
    /// 异步删除文档
    /// </summary>
    /// <param name="col">集合名称</param>
    /// <param name="id">要删除的文档ID</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>删除的文档数量</returns>
    internal async Task<int> DeleteDocumentAsync(string col, BsonValue id, CancellationToken cancellationToken = default)
    {
        var st = GetCollectionState(col);
        lock (st.PageState.SyncRoot)
        {
            if (!TryResolveDocumentLocation(col, st, id, out var p, out var e, out var i)) return 0;
            if (i >= e.Count) return 0;
            var entry = e[i];
            e.RemoveAt(i);
            st.Index.Remove(id);
            if (e.Count == 0)
            {
                st.OwnedPages.TryRemove(p.PageID, out _);
                if (st.PageState.PageId == p.PageID) st.PageState.PageId = 0;
                _pageManager.FreePage(p.PageID);
                lock (_lock) { _header.UsedPages--; WriteHeader(); }
            }
            else
            {
                _dataPageAccess.RewritePageWithDocuments(col, st, p, e, (key, pId, idx) => st.Index.Set(key, new DocumentLocation(pId, idx)));
            }
            if (entry.IsLargeDocument) try { _largeDocumentStorage.DeleteLargeDocument(entry.LargeDocumentIndexPageId); } catch { }
            GetIndexManager(col).DeleteDocument(entry.Document, id);
        }
        await EnsureWriteDurabilityAsync(cancellationToken).ConfigureAwait(false);
        return 1;
    }

    internal int InsertDocuments(string col, BsonDocument[] docs)
    {
        ThrowIfDisposed();
        EnsureInitialized();
        if (docs == null) throw new ArgumentNullException(nameof(docs));
        if (docs.Length == 0) return 0;

        var st = GetCollectionState(col);
        var idx = GetIndexManager(col);
        var docsToUpdateIndex = new List<(BsonDocument Doc, BsonValue Id)>(docs.Length);
        int insertedCount = 0;

        using var buffer = new PooledBufferWriter(1024);

        var exceptions = new List<Exception>();

        var pagesToPersist = new HashSet<Page>();

        lock (st.PageState.SyncRoot)
        {
            Page? currentPage = null;
            if (st.PageState.PageId != 0)
            {
                try { currentPage = _pageManager.GetPage(st.PageState.PageId); } catch { }
            }

            if (currentPage == null || currentPage.Header.PageType != PageType.Data)
            {
                 var (p, isN) = _dataPageAccess.GetWritableDataPageLocked(st.PageState, 1024); 
                 currentPage = p;
                 st.OwnedPages.TryAdd(p.PageID, 0);
                 if (isN) lock (_lock) { _header.UsedPages++; }
             }

            foreach (var d in docs)
            {
                if (d == null) continue;

                try
                {
                    var doc = PrepareDocumentForInsert(col, d, out var id);
                    _metadataManager.ValidateDocumentForWrite(col, doc, _options.SchemaValidationMode);

                    buffer.Reset();
                    BsonSerializer.SerializeDocumentToBuffer(doc, buffer);

                    if (LargeDocumentStorage.RequiresLargeDocumentStorage(buffer.WrittenCount, _dataPageAccess.GetMaxDocumentSize()))
                    {
                        var bytes = buffer.WrittenSpan.ToArray();
                        var lId = _largeDocumentStorage.StoreLargeDocument(bytes, col);
                        doc = CreateLargeDocumentIndexDocument(id, col, lId, bytes.Length);
                        
                        buffer.Reset();
                        BsonSerializer.SerializeDocumentToBuffer(doc, buffer);
                    }

                    int len = buffer.WrittenCount;
                    int reqSize = DataPageAccess.GetEntrySize(len);

                    if (currentPage!.Header.FreeBytes < reqSize)
                    {
                         pagesToPersist.Add(currentPage);
                         var (p, isN) = _dataPageAccess.GetWritableDataPageLocked(st.PageState, reqSize);
                         currentPage = p;
                         st.OwnedPages.TryAdd(p.PageID, 0);
                         if (isN) lock (_lock) { _header.UsedPages++; }
                     }

                    ushort i = currentPage.Header.ItemCount;
                    _dataPageAccess.AppendDocumentToPage(currentPage, buffer.WrittenSpan);
                    st.Index.Set(id, new DocumentLocation(currentPage.PageID, i));
                    
                    docsToUpdateIndex.Add((doc, id));
                    insertedCount++;
                }
                catch (Exception ex)
                {
                    exceptions.Add(ex);
                }
            }

            if (currentPage != null)
            {
                pagesToPersist.Add(currentPage);
            }

            lock (_lock) { WriteHeader(); }
        }

        // 在锁外执行持久化
        foreach (var page in pagesToPersist)
        {
            _dataPageAccess.PersistPage(page);
        }

        foreach (var (doc, id) in docsToUpdateIndex)
        {
            try
            {
                idx.InsertDocument(doc, id);
            }
            catch (Exception ex)
            {
                exceptions.Add(ex);
            }
        }

        EnsureWriteDurability();

        if (exceptions.Count > 0)
        {
            throw new AggregateException("One or more errors occurred during batch insert", exceptions);
        }

        return insertedCount;
    }

    /// <summary>
    /// 异步批量插入文档
    /// </summary>
    /// <param name="col">集合名称</param>
    /// <param name="docs">要插入的文档数组</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>插入的文档数量</returns>
    internal async Task<int> InsertDocumentsAsync(string col, BsonDocument[] docs, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        EnsureInitialized();
        if (docs == null) throw new ArgumentNullException(nameof(docs));
        if (docs.Length == 0) return 0;

        var st = GetCollectionState(col);
        var idx = GetIndexManager(col);
        var docsToUpdateIndex = new List<(BsonDocument Doc, BsonValue Id)>(docs.Length);
        int insertedCount = 0;

        using var buffer = new PooledBufferWriter(1024);

        var exceptions = new List<Exception>();

        var pagesToPersist = new HashSet<Page>();

        lock (st.PageState.SyncRoot)
        {
            Page? currentPage = null;
            if (st.PageState.PageId != 0)
            {
                try { currentPage = _pageManager.GetPage(st.PageState.PageId); } catch { }
            }

            if (currentPage == null || currentPage.Header.PageType != PageType.Data)
            {
                 var (p, isN) = _dataPageAccess.GetWritableDataPageLocked(st.PageState, 1024); 
                 currentPage = p;
                 st.OwnedPages.TryAdd(p.PageID, 0);
                 if (isN) lock (_lock) { _header.UsedPages++; }
             }

            foreach (var d in docs)
            {
                if (d == null) continue;
                cancellationToken.ThrowIfCancellationRequested();

                try
                {
                    var doc = PrepareDocumentForInsert(col, d, out var id);
                    _metadataManager.ValidateDocumentForWrite(col, doc, _options.SchemaValidationMode);

                    buffer.Reset();
                    BsonSerializer.SerializeDocumentToBuffer(doc, buffer);

                    if (LargeDocumentStorage.RequiresLargeDocumentStorage(buffer.WrittenCount, _dataPageAccess.GetMaxDocumentSize()))
                    {
                        var bytes = buffer.WrittenSpan.ToArray();
                        var lId = _largeDocumentStorage.StoreLargeDocument(bytes, col);
                        doc = CreateLargeDocumentIndexDocument(id, col, lId, bytes.Length);

                        buffer.Reset();
                        BsonSerializer.SerializeDocumentToBuffer(doc, buffer);
                    }

                    int len = buffer.WrittenCount;
                    int reqSize = DataPageAccess.GetEntrySize(len);

                    if (currentPage!.Header.FreeBytes < reqSize)
                    {
                         pagesToPersist.Add(currentPage);
                         var (p, isN) = _dataPageAccess.GetWritableDataPageLocked(st.PageState, reqSize);
                         currentPage = p;
                         st.OwnedPages.TryAdd(p.PageID, 0);
                         if (isN) lock (_lock) { _header.UsedPages++; }
                     }

                    ushort i = currentPage.Header.ItemCount;
                    _dataPageAccess.AppendDocumentToPage(currentPage, buffer.WrittenSpan);
                    st.Index.Set(id, new DocumentLocation(currentPage.PageID, i));
                    
                    docsToUpdateIndex.Add((doc, id));
                    insertedCount++;
                }
                catch (Exception ex)
                {
                    exceptions.Add(ex);
                }
            }

            if (currentPage != null)
            {
                pagesToPersist.Add(currentPage);
            }

            lock (_lock) { WriteHeader(); }
        }

        // 持久化页面
        foreach (var page in pagesToPersist)
        {
            _dataPageAccess.PersistPage(page);
        }

        foreach (var (doc, id) in docsToUpdateIndex)
        {
            try
            {
                idx.InsertDocument(doc, id);
            }
            catch (Exception ex)
            {
                exceptions.Add(ex);
            }
        }

        await EnsureWriteDurabilityAsync(cancellationToken).ConfigureAwait(false);

        if (exceptions.Count > 0)
        {
            throw new AggregateException("One or more errors occurred during batch insert", exceptions);
        }

        return insertedCount;
    }

    internal IEnumerable<BsonDocument> FindAll(string col)
    {
        var st = GetCollectionState(col);
        var ds = new List<BsonDocument>();
        
        // 获取所有属于该集合的页面ID并排序，以确保存储顺序读取
        var pages = st.OwnedPages.Keys.ToList();
        pages.Sort();
        
        foreach (var pageId in pages)
        {
            try
            {
                var p = _pageManager.GetPage(pageId);
                // 验证页面类型和归属（双重检查）
                if (p.PageType != PageType.Data || p.Header.ItemCount == 0) continue;

                foreach (var doc in _dataPageAccess.ScanDocumentsFromPage(p))
                {
                    // 再次检查集合标签，防止页面跨集合共享时的泄露
                    if (doc.TryGetValue("_collection", out var c) && c.ToString() != col) continue;
                    ds.Add(ResolveLargeDocument(doc));
                }
            }
            catch { }
        }

        var tx = GetCurrentTransaction();
        // 关键修复：即使 ds 为空，也应该进行事务合并，以包含事务中新增但尚未落盘的文档
        return tx != null ? MergeTransactionOperations(col, ds, tx) : ds;
    }

    internal async Task<List<BsonDocument>> FindAllAsync(string col, CancellationToken cancellationToken = default)
    {
        var st = GetCollectionState(col);
        var ds = new List<BsonDocument>();

        var pages = st.OwnedPages.Keys.ToList();
        pages.Sort();

        foreach (var pageId in pages)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                var p = await _pageManager.GetPageAsync(pageId, cancellationToken: cancellationToken).ConfigureAwait(false);
                if (p.PageType != PageType.Data || p.Header.ItemCount == 0) continue;

                foreach (var doc in _dataPageAccess.ScanDocumentsFromPage(p))
                {
                    if (doc.TryGetValue("_collection", out var c) && c.ToString() != col) continue;
                    ds.Add(await ResolveLargeDocumentAsync(doc, cancellationToken).ConfigureAwait(false));
                }
            }
            catch
            {
            }
        }

        var tx = GetCurrentTransaction();
        return tx != null ? MergeTransactionOperations(col, ds, tx).ToList() : ds;
    }

    /// <summary>
    /// 获取集合中所有文档的原始数据（零拷贝）
    /// 用于高性能查询引擎，支持并行解析
    /// </summary>
    /// <param name="col">集合名称</param>
    /// <returns>文档原始数据的枚举</returns>
    internal IEnumerable<ReadOnlyMemory<byte>> FindAllRaw(string col)
    {
        var st = GetCollectionState(col);
        var pages = st.OwnedPages.Keys.ToList();
        pages.Sort();

        foreach (var pageId in pages)
        {
            Page? p = null;
            try { p = _pageManager.GetPage(pageId); } catch { continue; }
            if (p == null || p.PageType != PageType.Data || p.Header.ItemCount == 0) continue;

            foreach (var slice in _dataPageAccess.ScanRawDocumentsFromPage(p))
            {
                // 注意：这里返回的是原始字节，未校验 _collection 字段
                // 调用者必须在解析后进行校验
                yield return slice;
            }
        }
    }

    internal BsonDocument? FindById(string col, BsonValue id)
    {
        var st = GetCollectionState(col);
        if (st.Index.TryGet(id, out var loc))
        {
            var p = _pageManager.GetPage(loc.PageId);
            var entry = _dataPageAccess.ReadDocumentAt(p, loc.EntryIndex);
            if (entry != null) return ResolveLargeDocument(entry.Value.Document);
        }
        return FindByIdFullScan(col, id, st);
    }

    internal async Task<BsonDocument?> FindByIdAsync(string col, BsonValue id, CancellationToken cancellationToken = default)
    {
        var st = GetCollectionState(col);
        if (st.Index.TryGet(id, out var loc))
        {
            var p = await _pageManager.GetPageAsync(loc.PageId, cancellationToken: cancellationToken).ConfigureAwait(false);
            var entry = _dataPageAccess.ReadDocumentAt(p, loc.EntryIndex);
            if (entry != null) return await ResolveLargeDocumentAsync(entry.Value.Document, cancellationToken).ConfigureAwait(false);
        }

        return await FindByIdFullScanAsync(col, id, cancellationToken).ConfigureAwait(false);
    }

    private async Task<BsonDocument?> FindByIdFullScanAsync(string col, BsonValue id, CancellationToken cancellationToken = default)
    {
        var all = await FindAllAsync(col, cancellationToken).ConfigureAwait(false);
        return all.FirstOrDefault(d => d["_id"].Equals(id));
    }

    internal BsonDocument ResolveLargeDocument(BsonDocument doc)
    {
        if (doc.TryGetValue("_isLargeDocument", out var v) && v.ToBoolean(null))
        {
            uint lId = (uint)doc["_largeDocumentIndex"].ToInt64(null);
            var data = _largeDocumentStorage.ReadLargeDocument(lId);
            return BsonSerializer.DeserializeDocument(data);
        }
        return doc;
    }

    internal async Task<BsonDocument> ResolveLargeDocumentAsync(BsonDocument doc, CancellationToken cancellationToken = default)
    {
        if (doc.TryGetValue("_isLargeDocument", out var v) && v.ToBoolean(null))
        {
            uint lId = (uint)doc["_largeDocumentIndex"].ToInt64(null);
            var data = await _largeDocumentStorage.ReadLargeDocumentAsync(lId, cancellationToken).ConfigureAwait(false);
            return BsonSerializer.DeserializeDocument(data);
        }
        return doc;
    }

    internal BsonValue InsertDocumentInternal(string col, BsonDocument d)
    {
        var pr = PrepareDocumentForInsert(col, d, out var id);
        return InsertPreparedDocument(col, pr, id, GetCollectionState(col), GetIndexManager(col), true);
    }

    internal int UpdateDocumentInternal(string col, BsonDocument d) => UpdateDocument(col, d);
    internal int DeleteDocumentInternal(string col, BsonValue id) => DeleteDocument(col, id);
    public int GetCachedDocumentCount(string col) => GetCollectionState(col).Index.Count;

    private BsonValue InsertPreparedDocument(string col, BsonDocument doc, BsonValue id, CollectionState st, IndexManager idx, bool u)
    {
        var buffer = new PooledBufferWriter(BsonSerializer.CalculateDocumentSize(doc));
        try
        {
            BsonSerializer.SerializeDocumentToBuffer(doc, buffer);

            if (LargeDocumentStorage.RequiresLargeDocumentStorage(buffer.WrittenCount, _dataPageAccess.GetMaxDocumentSize()))
            {
                var bytes = buffer.WrittenSpan.ToArray();
                var lId = _largeDocumentStorage.StoreLargeDocument(bytes, col);
                doc = CreateLargeDocumentIndexDocument(id, col, lId, bytes.Length);

                buffer.Reset();
                BsonSerializer.SerializeDocumentToBuffer(doc, buffer);
            }

            var span = buffer.WrittenSpan;

            Page p;
            bool isNewPage = false;
            ushort entryIndex;

            lock (st.PageState.SyncRoot)
            {
                var (page, isN) = _dataPageAccess.GetWritableDataPageLocked(st.PageState, DataPageAccess.GetEntrySize(span.Length));
                p = page;
                isNewPage = isN;
                st.OwnedPages.TryAdd(p.PageID, 0);
                entryIndex = p.Header.ItemCount;
                _dataPageAccess.AppendDocumentToPage(p, span);
                st.Index.Set(id, new DocumentLocation(p.PageID, entryIndex));
            }

            // 在锁外处理全局状态和持久化
            if (isNewPage)
            {
                lock (_lock) { _header.UsedPages++; WriteHeader(); }
            }
        
            _dataPageAccess.PersistPage(p);

            if (u) idx.InsertDocument(doc, id);
            return id;
        }
        finally
        {
            buffer.Dispose();
        }
    }

    private BsonDocument PrepareDocumentForInsert(string col, BsonDocument doc, out BsonValue id)
    {
        bool hasId = doc.TryGetValue("_id", out var exId) && exId != null && !exId.IsNull;
        // 检查 _collection 是否已经存在且值正确（避免不必要的重建）
        bool hasCorrectCol = doc.TryGetValue("_collection", out var existingCol) && existingCol?.ToString() == col;

        // 快速路径：如果已有 _id 且 _collection 值正确，直接返回原文档
        if (hasId && hasCorrectCol)
        {
            id = exId!;
            return doc;
        }

        // 优化：直接创建新的 Builder，一次性添加所有字段，避免 ToBuilder() 的额外转换
        var builder = ImmutableDictionary.CreateBuilder<string, BsonValue>();

        // 复制原文档的所有字段
        foreach (var kvp in doc._elements)
        {
            builder[kvp.Key] = kvp.Value;
        }

        // 添加 _id（如果缺失）
        if (!hasId)
        {
            id = ObjectId.NewObjectId();
            builder["_id"] = id;
        }
        else
        {
            id = exId!;
        }

        // 强制设置 _collection 为实际使用的集合名称（覆盖 AOT 生成器设置的值）
        builder["_collection"] = col;

        return new BsonDocument(builder.ToImmutable());
    }

    private static BsonDocument CreateLargeDocumentIndexDocument(BsonValue id, string col, uint pId, int s)
    {
        var dict = new Dictionary<string, BsonValue>(5)
        {
            { "_id", id },
            { "_collection", col },
            { "_largeDocumentIndex", (long)pId },
            { "_largeDocumentSize", s },
            { "_isLargeDocument", true }
        };
        return new BsonDocument(dict);
    }

    private BsonDocument? FindByIdFullScan(string col, BsonValue id, CollectionState st) => FindAll(col).FirstOrDefault(d => d["_id"].Equals(id));

    private bool TryResolveDocumentLocation(string col, CollectionState st, BsonValue id, out Page p, out List<PageDocumentEntry> e, out ushort i)
    {
        p = null!; e = null!; i = 0;
        if (!st.Index.TryGet(id, out var loc)) return false;
        p = _pageManager.GetPage(loc.PageId);
        e = _dataPageAccess.ReadDocumentsFromPage(p);
        i = loc.EntryIndex;
        return true;
    }

    private void BuildDocumentLocationCache(string col, CollectionState st)
    {
        st.Index.Clear();
        st.OwnedPages.Clear();
        uint total = _pageManager.TotalPages;
        for (uint pId = 1; pId <= total; pId++)
        {
            Page p;
            try { p = _pageManager.GetPage(pId); } catch { continue; }
            if (p.PageType != PageType.Data || p.Header.ItemCount == 0) continue;
            
            ushort idx = 0;
            bool pageOwned = false;

            foreach (var doc in _dataPageAccess.ScanDocumentsFromPage(p))
            {
                if (idx == 0)
                {
                    if (doc.TryGetValue("_collection", out var c) && c.ToString() == col)
                    {
                        st.OwnedPages.TryAdd(pId, 0);
                        pageOwned = true;
                    }
                    else
                    {
                        break;
                    }
                }

                if (pageOwned)
                {
                    if (doc.TryGetValue("_id", out var id))
                    {
                        st.Index.Set(id, new DocumentLocation(p.PageID, idx));
                    }
                }
                idx++;
            }
        }
    }

    private void EnsureWriteDurability() { _flushScheduler.EnsureDurabilityAsync(_options.WriteConcern).GetAwaiter().GetResult(); }

    private Task EnsureWriteDurabilityAsync(CancellationToken cancellationToken = default) 
    { 
        return _flushScheduler.EnsureDurabilityAsync(_options.WriteConcern, cancellationToken); 
    }

    private IEnumerable<BsonDocument> MergeTransactionOperations(string col, IEnumerable<BsonDocument> ds, Transaction tx)
    {
        var dict = ds.ToDictionary(d => d["_id"].ToString(), d => d);
        foreach (var op in tx.Operations.Where(o => o.CollectionName == col))
        {
            var k = op.DocumentId?.ToString() ?? "";
            if (op.OperationType == TransactionOperationType.Delete) dict.Remove(k);
            else if (op.NewDocument != null) dict[k] = op.NewDocument;
        }
        return dict.Values;
    }

    private void SafeFlush() { try { Flush(); } catch { } }

    private void DisposeIndexManagers() { foreach (var m in _indexManagers.Values) m.Dispose(); _indexManagers.Clear(); }

    private static TimeSpan NormalizeInterval(TimeSpan i) => i == Timeout.InfiniteTimeSpan ? TimeSpan.Zero : i;

    private void EnsureInitialized() { if (!_isInitialized) throw new InvalidOperationException(); }

    private void ThrowIfDisposed() { if (_disposed) throw new ObjectDisposedException(nameof(TinyDbEngine)); }

    private static string? GetCollectionNameFromEntityAttribute<T>() where T : class => typeof(T).GetCustomAttribute<EntityAttribute>()?.Name;

    public void Dispose()
    {
        if (!_disposed)
        {
            try
            {
                if (_isInitialized) _collectionMetaStore.SaveCollections(false);
                SafeFlush();
                foreach (var c in _collections.Values) c.Dispose();
                _collections.Clear();
                _collectionStates.Clear();
                _transactionManager.Dispose();
                DisposeIndexManagers();
            }
            catch { }
            finally
            {
                DisposeComponents();
                _disposed = true;
                _isInitialized = false;
            }
        }
    }
}
