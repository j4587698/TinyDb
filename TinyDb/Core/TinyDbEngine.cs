using System;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using TinyDb.Bson;
using TinyDb.Security;
using TinyDb.Collections;
using TinyDb.Index;
using TinyDb.Query;
using TinyDb.Serialization;
using TinyDb.Storage;
using TinyDb.Attributes;
using TinyDb.Utils;

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
    private readonly object _collectionRegistryLock = new();
    private readonly object _collectionStateInitLock = new();
    private readonly object _identitySequenceLock = new();
    private readonly ConcurrentDictionary<string, object> _indexCreationLocks;
    private readonly ConcurrentDictionary<string, IdentitySequenceState> _identitySequences;
    private LargeDocumentStorage _largeDocumentStorage = null!;
    private DataPageAccess _dataPageAccess = null!;
    private readonly object _lock = new();
    private readonly AsyncLocal<ITransaction?> _currentTransaction = new();
    private readonly Action<TinyDbLogLevel, string, Exception?> _log;
    private DatabaseHeader _header;
    private EncryptionContext? _encryptionContext;
    private int _disposed;
    private Exception? _corruptionException;
    private bool _isInitialized;
    private long _findByIdFullScanCount;
    private long _findByIdFullScanHitCount;
    private long _findByIdStaleIndexHitCount;

    private const int DocumentLengthPrefixSize = sizeof(int);
    private const int MinimumFreeSpaceThreshold = DocumentLengthPrefixSize + 64;
    private const string IndexMetadataKey = "__indexes";
    private const string IndexNameKey = "n";
    private const string IndexFieldsKey = "f";
    private const string IndexUniqueKey = "u";
    private const string IndexSparseKey = "s";
    private const string IndexRootPageKey = "r";
    private const string IndexMaxKeysKey = "m";
    private const int IdentitySequenceReservationSize = 1024;

    /// <summary>
    /// 获取数据库文件路径。
    /// </summary>
    public string FilePath => _filePath;

    /// <summary>
    /// 获取此数据库实例使用的选项。
    /// </summary>
    public TinyDbOptions Options => _options;

    /// <summary>
    /// 获取 FindById 查询回退到全集合扫描的次数。
    /// </summary>
    public long FindByIdFullScanCount => Interlocked.Read(ref _findByIdFullScanCount);

    /// <summary>
    /// 获取 FindById 全扫描回退实际找回已存在文档的次数。
    /// </summary>
    public long FindByIdFullScanHitCount => Interlocked.Read(ref _findByIdFullScanHitCount);

    /// <summary>
    /// 获取 FindById 主键索引命中但指向记录失效或不匹配的次数。
    /// </summary>
    public long FindByIdStaleIndexHitCount => Interlocked.Read(ref _findByIdStaleIndexHitCount);

    public TinyDb.Metadata.MetadataManager MetadataManager => _metadataManager;

    /// <summary>
    /// 获取数据库头部信息。
    /// </summary>
    public DatabaseHeader Header => _header;

    /// <summary>
    /// 获取数据库是否已初始化的值。
    /// </summary>
    public bool IsInitialized => _isInitialized;

    public bool IsCorrupted => Volatile.Read(ref _corruptionException) != null;

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
        _log = _options.Logger ?? TinyDbLogging.NoopLogger;

        _collections = new ConcurrentDictionary<string, IDocumentCollection>(StringComparer.Ordinal);
        _indexManagers = new ConcurrentDictionary<string, IndexManager>(StringComparer.Ordinal);
        _transactionManager = new TransactionManager(this, _options.MaxTransactions, _options.TransactionTimeout);
        _collectionStates = new ConcurrentDictionary<string, CollectionState>(StringComparer.Ordinal);
        _indexCreationLocks = new ConcurrentDictionary<string, object>(StringComparer.Ordinal);
        _identitySequences = new ConcurrentDictionary<string, IdentitySequenceState>(StringComparer.Ordinal);

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

        var isNewDatabase = _diskStream.Size == 0;
        var hasBootstrapHeader = TryReadBootstrapHeader(_diskStream, out var bootstrapHeader);
        if (hasBootstrapHeader && bootstrapHeader.Magic == DatabaseHeader.MagicNumber && IsSupportedPageSize(bootstrapHeader.PageSize))
        {
            _options.PageSize = bootstrapHeader.PageSize;
        }

        _encryptionContext = ResolveEncryptionContext(isNewDatabase, hasBootstrapHeader, bootstrapHeader);
        var pageCodec = _encryptionContext?.PageCodec ?? new NoOpPageCodec(_options.PageSize);
        var walCodec = _encryptionContext?.WalCodec ?? new NoOpWalCodec();

        _pageManager = new PageManager(_diskStream, _options.PageSize, _options.CacheSize, _log, pageCodec);
        _writeAheadLog = new WriteAheadLog(_filePath, (int)_options.PageSize, _options.EnableJournaling, _options.WalFileNameFormat, _log, walCodec);
        
        _pageManager.RegisterWAL(
            (page, beforeImage) => _writeAheadLog.AppendPage(page, beforeImage),
            lsn => _writeAheadLog.FlushToLSN(lsn),
            () => _writeAheadLog.RequiresBeforeImage);
        _pageManager.RegisterDeferredWAL((page, beforeImage) => _writeAheadLog.AppendPageDeferred(page, beforeImage));
        _writeAheadLog.DeferredTransactionPageLogged = _pageManager.MarkDeferredWalPageLogged;
        _pageManager.RegisterWAL((page, beforeImage, ct) => _writeAheadLog.AppendPageAsync(page, beforeImage, ct));
        _pageManager.RegisterWAL((lsn, ct) => _writeAheadLog.FlushToLSNAsync(lsn, ct));

        _flushScheduler = new FlushScheduler(_pageManager, _writeAheadLog, NormalizeInterval(_options.BackgroundFlushInterval), _log);
        _largeDocumentStorage = new LargeDocumentStorage(_pageManager, (int)_options.PageSize, _log);
        _dataPageAccess = new DataPageAccess(_pageManager, _largeDocumentStorage, _writeAheadLog);
        _metadataManager = new TinyDb.Metadata.MetadataManager(this);

        InitializeDatabase();
    }

    private static bool TryReadBootstrapHeader(IDiskStream diskStream, out DatabaseHeader header)
    {
        header = default;
        if (diskStream.Size < Page.DataStartOffset + DatabaseHeader.Size)
        {
            return false;
        }

        try
        {
            var headerBytes = diskStream.ReadPage(Page.DataStartOffset, DatabaseHeader.Size);
            header = DatabaseHeader.FromByteArray(headerBytes);
            return true;
        }
        catch
        {
            return false;
        }
    }

    private EncryptionContext? ResolveEncryptionContext(bool isNewDatabase, bool hasBootstrapHeader, DatabaseHeader bootstrapHeader)
    {
        if (hasBootstrapHeader &&
            bootstrapHeader.Magic == DatabaseHeader.MagicNumber &&
            IsSupportedPageSize(bootstrapHeader.PageSize) &&
            EncryptionMetadataStore.TryReadFromDisk(_diskStream, bootstrapHeader.PageSize, out var metadata) &&
            metadata != null)
        {
            if (metadata.LogicalPageSize != bootstrapHeader.PageSize)
            {
                throw new SecurityCorruptedException("Encryption metadata page size does not match database header.");
            }

            _options.PageSize = metadata.LogicalPageSize;
            _options.EnableEncryption = true;
            var context = EncryptionContext.OpenExisting(metadata, _options);
            try
            {
                EncryptionMetadataStore.WriteToDisk(_diskStream, metadata.LogicalPageSize, context.Metadata);
                return context;
            }
            catch
            {
                context.Dispose();
                throw;
            }
        }

        if (isNewDatabase)
        {
            return _options.EnableEncryption ? EncryptionContext.CreateNew(_options) : null;
        }

        if (_options.EnableEncryption)
        {
            throw new InvalidOperationException("Existing unencrypted databases are not encrypted implicitly. Use an explicit migration or compact-to-encrypted workflow.");
        }

        return null;
    }

    private static bool IsSupportedPageSize(uint pageSize)
    {
        return pageSize >= 4096 && pageSize <= int.MaxValue && (pageSize & (pageSize - 1)) == 0;
    }

    private sealed class IdentitySequenceState
    {
        public IdentitySequenceState(string collectionName, string metadataKey, long current)
        {
            CollectionName = collectionName;
            MetadataKey = metadataKey;
            Current = current;
            ReservedUntil = current;
        }

        public string CollectionName { get; }
        public string MetadataKey { get; }
        public long Current { get; set; }
        public long ReservedUntil { get; set; }
        public bool HasUnflushedExactValue { get; set; }
    }

    private void DisposeComponents()
    {
        _flushScheduler.Dispose();
        _writeAheadLog.Dispose();
        _pageManager.Dispose();
        _diskStream.Dispose();
        _encryptionContext?.Dispose();
        _encryptionContext = null;
    }

    private void ResetRuntimeStateForReinitialize()
    {
        _collectionStates.Clear();
        DisposeIndexManagers();
        _collectionMetaStore = null!;
        _isInitialized = false;
    }

    /// <summary>
    /// 压缩数据库（碎片整理）
    /// 注意：此操作会阻塞所有其他操作，并重建数据库文件。
    /// </summary>
    public void CompactDatabase()
    {
        ThrowIfDisposed();

        EnsureInitialized();
        var tempFile = _filePath + ".compact";
        var collectionNames = GetCollectionNames(includeSystemCollections: true)
            .Distinct(StringComparer.Ordinal)
            .OrderBy(static name => name, StringComparer.Ordinal)
            .ToArray();

        using var collectionLocks = EnterCollectionCommitGates(collectionNames);

        lock (_lock)
        {
            FlushCore();
        }

        if (File.Exists(tempFile)) File.Delete(tempFile);

        using (var tempEngine = new TinyDbEngine(tempFile, _options))
        {
            CopyCollectionsTo(tempEngine, collectionNames);
        }

        // 释放当前组件以解除文件锁定，并替换文件。
        lock (_lock)
        {
            DisposeComponents();

            try 
            {
                File.Move(tempFile, _filePath, overwrite: true);
            }
            catch (Exception ex) when (ex is not OutOfMemoryException)
            {
                // 移动失败，尝试恢复组件
                ResetRuntimeStateForReinitialize();
                InitializeComponents(null);
                throw;
            }

            ResetRuntimeStateForReinitialize();
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
                var isNewDatabase = _diskStream.Size == 0;

                // 步骤 1: 崩溃恢复。
                // 只有当 WAL 中的 LSN 大于磁盘页面的 LSN 时，才应用恢复逻辑（幂等恢复）。
                if (_writeAheadLog.IsEnabled)
                {
                    if (isNewDatabase)
                    {
                        // If the main database file was deleted but a stale WAL remains, replaying that WAL
                        // would resurrect pages from a different database generation and can corrupt the new header.
                        _writeAheadLog.Truncate();
                    }
                    else
                    {
                        _writeAheadLog.Replay((id, data) =>
                        {
                            if (_pageManager.TryReadLogicalPageSnapshot(id, out var diskData))
                            {
                                var diskHeader = PageHeader.FromByteArray(diskData);
                                var walHeader = PageHeader.FromByteArray(data);

                                if (walHeader.LSN <= diskHeader.LSN && diskHeader.IsValid() && diskHeader.PageID == id && diskHeader.VerifyChecksum(diskData))
                                {
                                    // 磁盘版本已经是最新的，跳过此条日志
                                    return;
                                }
                            }

                            _pageManager.RestorePage(id, data);
                        }, (id, data) => _pageManager.RestorePage(id, data));
                    }
                }

                // 步骤 2: 文件结构初始化。
                if (isNewDatabase)
                {
                    // 空文件：初始化 Header 页。
                    _header = new DatabaseHeader();
                    _header.Initialize(_options.PageSize, _options.DatabaseName, _options.EnableJournaling);
                    var p1 = _pageManager.NewPage(PageType.Header);
                    p1.WriteData(0, _header.ToByteArray());
                    if (_encryptionContext != null)
                    {
                        EncryptionMetadataStore.WriteToPage(p1, _encryptionContext.Metadata);
                    }
                    _pageManager.SavePage(p1, true);
                    // 初始化 PageManager (新数据库)
                    _pageManager.Initialize(1, 0);
                }
                else
                {
                    // 已有文件：加载 Header 并验证。
                    ReadHeader();
                    if (!_header.IsValid()) throw new InvalidOperationException(CreateInvalidHeaderMessage(_header));
                    
                    // 初始化 PageManager (现有数据库)
                    _pageManager.Initialize(
                        _header.TotalPages,
                        _header.FirstFreePage,
                        _header.FreePageCount,
                        _header.HasFreePageCount);
                    
                    // 步骤 3: 状态一致性同步。
                    // 关键修复：如果在崩溃前分配了页面但 Header 未更新，
                    // WAL 重放后 PageManager 知道最新状态，需反向同步回 Header
                    if (_pageManager.TotalPages > _header.TotalPages ||
                        _pageManager.FirstFreePageID != _header.FirstFreePage ||
                        !_header.HasFreePageCount ||
                        _pageManager.FreePageCount != _header.FreePageCount)
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
            catch (Exception ex) when (ex is not OutOfMemoryException)
            {
                _isInitialized = false;
                Dispose();
                throw;
            }
        }
    }

    private static string CreateInvalidHeaderMessage(DatabaseHeader header)
    {
        return "Invalid database header " +
               $"(magic=0x{header.Magic:X8}, version=0x{header.DatabaseVersion:X8}, pageSize={header.PageSize}, " +
               $"totalPages={header.TotalPages}, usedPages={header.UsedPages}, " +
               $"createdAt={header.CreatedAt}, modifiedAt={header.ModifiedAt}).";
    }

    private void ReadHeader()
    {
        var p = _pageManager.GetPage(1);
        _header = DatabaseHeader.FromByteArray(p.ReadBytes(0, DatabaseHeader.Size));
        if (!_header.IsValid())
        {
            throw new InvalidOperationException(CreateInvalidHeaderMessage(_header));
        }

        if (_header.Checksum != 0 && !_header.VerifyChecksum())
        {
            throw new InvalidDataException("Database header checksum verification failed.");
        }
    }

    private void WriteHeader(bool forceFlush = false)
    {
        lock (_lock)
        {
            _header.TotalPages = _pageManager.TotalPages;
            _header.FirstFreePage = _pageManager.FirstFreePageID;
            _header.FreePageCount = _pageManager.FreePageCount;
            _header.UpdateModification();
            var p = _pageManager.GetPage(1);
            p.WriteData(0, _header.ToByteArray());
            _pageManager.SavePage(p, forceFlush);
        }
    }

    private void DecrementUsedPagesAndWriteHeader()
    {
        lock (_lock)
        {
            if (_header.UsedPages <= 1)
            {
                throw new InvalidOperationException("Database header used-page count cannot be decremented below the header page.");
            }

            _header.UsedPages--;
            WriteHeader();
        }
    }

    private void InitializeSystemPages()
    {
        var headerChanged = false;
        if (_header.CollectionInfoPage == 0)
        {
            _header.CollectionInfoPage = AllocateSystemPage(PageType.Collection, "Cols");
            headerChanged = true;
        }

        if (_header.IndexInfoPage == 0)
        {
            _header.IndexInfoPage = AllocateSystemPage(PageType.Index, "Idxs");
            headerChanged = true;
        }

        if (headerChanged)
        {
            WriteHeader(forceFlush: true);
        }
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
        _writeAheadLog.FlushLog();
    }

    internal WriteAheadLog.WalTransactionScope BeginTransactionDurabilityScope(Guid transactionId)
    {
        return _writeAheadLog.BeginTransaction(transactionId);
    }

    internal void RollbackTransactionDurabilityScope(WriteAheadLog.WalTransactionScope durabilityScope)
    {
        if (durabilityScope == null) throw new ArgumentNullException(nameof(durabilityScope));

        durabilityScope.Rollback((pageId, beforeImage) => _pageManager.RestorePage(pageId, beforeImage));
        ResetRuntimeStateAfterDurabilityRollback();
    }

    private void ResetRuntimeStateAfterDurabilityRollback()
    {
        lock (_lock)
        {
            _pageManager.ClearCache();
            ReadHeader();
            _pageManager.Initialize(
                _header.TotalPages,
                _header.FirstFreePage,
                _header.FreePageCount,
                _header.HasFreePageCount);

            _collectionStates.Clear();
            DisposeIndexManagers();

            _collectionMetaStore = new CollectionMetaStore(
                _pageManager,
                () => _header.CollectionInfoPage,
                id => _header.CollectionInfoPage = id);
            _collectionMetaStore.LoadCollections();

            _metadataManager = new TinyDb.Metadata.MetadataManager(this);
            _identitySequences.Clear();
            _transactionManager.ClearForeignKeyCache();
        }
    }

    private WriteAheadLog.WalTransactionScope? BeginImplicitWalTransaction()
    {
        if (!_writeAheadLog.IsEnabled || _writeAheadLog.IsInTransactionScope)
        {
            return null;
        }

        return _writeAheadLog.BeginTransaction(Guid.NewGuid(), flushOnCommit: false);
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
        return GetOrCreateCollection<T>(n);
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
        return GetOrCreateCollection<T>(n);
    }

    public IEnumerable<T> QuerySql<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>(
        string sql,
        IReadOnlyDictionary<string, object?>? parameters = null)
        where T : class, new()
    {
        ThrowIfDisposed();
        if (sql == null) throw new ArgumentNullException(nameof(sql));

        var query = SqlQueryParser.ParseSelect(sql, parameters);
        var collection = GetCollection<T>(query.CollectionName);
        return collection is DocumentCollection<T> documentCollection
            ? documentCollection.Execute<T>(query).Rows
            : collection.FindSql(sql, parameters);
    }

    public IEnumerable<BsonDocument> QuerySqlDocuments<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] TSource>(
        string sql,
        IReadOnlyDictionary<string, object?>? parameters = null)
        where TSource : class, new()
    {
        ThrowIfDisposed();
        if (sql == null) throw new ArgumentNullException(nameof(sql));

        var query = SqlQueryParser.ParseSelect(sql, parameters);
        var collection = GetCollection<TSource>(query.CollectionName);
        return collection is DocumentCollection<TSource> documentCollection
            ? documentCollection.Execute(query).Documents
            : collection.FindSqlDocuments(sql, parameters);
    }

    public IEnumerable<TProjection> QuerySql<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] TSource,
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] TProjection>(
        string sql,
        IReadOnlyDictionary<string, object?>? parameters = null)
        where TSource : class, new()
        where TProjection : class, new()
    {
        ThrowIfDisposed();
        if (sql == null) throw new ArgumentNullException(nameof(sql));

        var query = SqlQueryParser.ParseSelect(sql, parameters);
        var collection = GetCollection<TSource>(query.CollectionName);
        return collection is DocumentCollection<TSource> documentCollection
            ? documentCollection.Execute<TProjection>(query).Rows
            : collection.FindSql<TProjection>(sql, parameters);
    }

    public SqlExecutionResult Execute<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] TSource>(
        string sql,
        IReadOnlyDictionary<string, object?>? parameters = null)
        where TSource : class, new()
    {
        ThrowIfDisposed();
        if (sql == null) throw new ArgumentNullException(nameof(sql));

        var statement = SqlQueryParser.Parse(sql, parameters);
        var collection = GetCollection<TSource>(statement.CollectionName);
        return collection is DocumentCollection<TSource> documentCollection
            ? documentCollection.Execute(statement)
            : collection.Execute(sql, parameters);
    }

    public SqlExecutionResult<TProjection> Execute<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] TSource,
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] TProjection>(
        string sql,
        IReadOnlyDictionary<string, object?>? parameters = null)
        where TSource : class, new()
        where TProjection : class, new()
    {
        ThrowIfDisposed();
        if (sql == null) throw new ArgumentNullException(nameof(sql));

        var statement = SqlQueryParser.Parse(sql, parameters);
        var collection = GetCollection<TSource>(statement.CollectionName);
        return collection is DocumentCollection<TSource> documentCollection
            ? documentCollection.Execute<TProjection>(statement)
            : collection.Execute<TProjection>(sql, parameters);
    }

    private ITinyCollection<T> GetOrCreateCollection<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>(string name) where T : class, new()
    {
        if (_collections.TryGetValue(name, out var existing))
        {
            return (ITinyCollection<T>)existing;
        }

        lock (_collectionRegistryLock)
        {
            if (_collections.TryGetValue(name, out existing))
            {
                return (ITinyCollection<T>)existing;
            }

            var isKnownCollection = _collectionMetaStore.IsKnown(name);
            _metadataManager.EnsureSchema(name, typeof(T));
            RegisterCollection(name);
            if (!isKnownCollection)
            {
                var state = CreateEmptyCollectionState();
                state.MarkCacheInitialized();
                _collectionStates.TryAdd(name, state);
            }

            var collection = new DocumentCollection<T>(this, name);
            _collections[name] = collection;
            return collection;
        }
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

        using var collectionLock = EnterCollectionCommitGates(new[] { n });
        bool r = _collections.TryRemove(n, out var col);
        if (r)
        {
            col.DeleteAll();
            col.Dispose();
        }
        if (_collectionMetaStore.IsKnown(n))
        {
            _collectionMetaStore.RemoveCollection(n, true);
            if (_indexManagers.TryRemove(n, out var indexManager)) indexManager.Dispose();
            ClearCollectionRuntimeCaches(n);
            return true;
        }

        if (_indexManagers.TryRemove(n, out var removedIndexManager)) removedIndexManager.Dispose();
        ClearCollectionRuntimeCaches(n);
        return r;
    }

    private void ClearCollectionRuntimeCaches(string collectionName)
    {
        _collectionStates.TryRemove(collectionName, out _);

        var identityPrefix = collectionName + "\0";
        foreach (var key in _identitySequences.Keys)
        {
            if (key.StartsWith(identityPrefix, StringComparison.Ordinal))
            {
                _identitySequences.TryRemove(key, out _);
            }
        }

        var indexLockPrefix = collectionName + "\u001F";
        foreach (var key in _indexCreationLocks.Keys)
        {
            if (key.StartsWith(indexLockPrefix, StringComparison.Ordinal))
            {
                _indexCreationLocks.TryRemove(key, out _);
            }
        }

        _transactionManager.ClearForeignKeyCache();
        _metadataManager.InvalidateMetadata(collectionName);
    }

    /// <summary>
    /// 将所有挂起的更改刷新到磁盘。
    /// </summary>
    public void Flush()
    {
        ThrowIfDisposed();
        FlushCore();
    }

    private void FlushCore()
    {
        if (!_isInitialized) return;
        _flushScheduler.Flush();
        FlushIdentitySequenceExactValues();
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
    public bool EnsureIndex(string collectionName, string fieldName, string indexName, bool unique = false, bool sparse = false)
    {
        return EnsureIndex(collectionName, new[] { fieldName }, indexName, unique, sparse);
    }

    internal bool EnsureIndex(string collectionName, string[] fields, string indexName, bool unique = false, bool sparse = false)
    {
        var lockKey = collectionName + "\u001F" + indexName;
        var indexLock = _indexCreationLocks.GetOrAdd(lockKey, _ => new object());
        lock (indexLock)
        {
            var indexManager = GetIndexManager(collectionName);
            var created = indexManager.CreateIndexForBackfill(indexName, fields, unique, sparse);
            if (!created) return false;

            try
            {
                BackfillIndex(collectionName, indexManager, indexName);
                indexManager.PersistCurrentDefinitions(_options.WriteConcern == WriteConcern.Synced);
                return true;
            }
            catch
            {
                indexManager.DropIndex(indexName);
                throw;
            }
        }
    }

    private void BackfillIndex(string collectionName, IndexManager indexManager, string indexName)
    {
        var state = GetCollectionState(collectionName);
        var existingDocuments = ReadAllDocumentsSnapshotFromPageSnapshots(collectionName, state);
        indexManager.RebuildIndex(indexName, existingDocuments);
    }

    public IndexManager GetIndexManager(string c)
    {
        if (_indexManagers.TryGetValue(c, out var existing))
        {
            return existing;
        }

        lock (_collectionRegistryLock)
        {
            if (_indexManagers.TryGetValue(c, out existing))
            {
                return existing;
            }

            var created = CreateIndexManager(c);
            if (_indexManagers.TryAdd(c, created))
            {
                return created;
            }

            created.Dispose();
            return _indexManagers[c];
        }
    }

    private IndexManager CreateIndexManager(string collectionName)
    {
        var persistedDefinitions = LoadPersistedIndexDefinitions(collectionName);
        return new IndexManager(
            collectionName,
            _pageManager,
            persistedDefinitions,
            (definitions, forceFlush) => SavePersistedIndexDefinitions(
                collectionName,
                definitions,
                forceFlush || _options.WriteConcern == WriteConcern.Synced));
    }

    private IReadOnlyList<PersistedIndexDefinition> LoadPersistedIndexDefinitions(string collectionName)
    {
        var metadata = _collectionMetaStore.GetMetadata(collectionName, includeInternal: true);
        if (!metadata.TryGetValue(IndexMetadataKey, out var indexDefinitionsValue) || indexDefinitionsValue is not BsonArray indexDefinitions)
        {
            return Array.Empty<PersistedIndexDefinition>();
        }

        var definitions = new List<PersistedIndexDefinition>(indexDefinitions.Count);
        foreach (var indexDefinitionValue in indexDefinitions)
        {
            if (!TryGetDocument(indexDefinitionValue, out var indexDefinition)) continue;
            if (!TryGetString(indexDefinition, IndexNameKey, out var name)) continue;
            if (!TryGetStringArray(indexDefinition, IndexFieldsKey, out var fields) || fields.Length == 0) continue;
            if (!TryGetUInt32(indexDefinition, IndexRootPageKey, out var rootPageId) || rootPageId == 0) continue;

            var unique = TryGetBoolean(indexDefinition, IndexUniqueKey, out var uniqueValue) && uniqueValue;
            var sparse = TryGetBoolean(indexDefinition, IndexSparseKey, out var sparseValue) && sparseValue;
            var maxKeys = TryGetInt32(indexDefinition, IndexMaxKeysKey, out var maxKeysValue) && maxKeysValue > 0
                ? maxKeysValue
                : 200;

            definitions.Add(new PersistedIndexDefinition(name, fields, unique, sparse, rootPageId, maxKeys));
        }

        return definitions;
    }

    private void SavePersistedIndexDefinitions(string collectionName, IReadOnlyList<PersistedIndexDefinition> definitions, bool forceFlush)
    {
        var metadata = _collectionMetaStore.GetMetadata(collectionName, includeInternal: true);
        if (definitions.Count == 0)
        {
            metadata = metadata.RemoveKey(IndexMetadataKey);
        }
        else
        {
            var indexDefinitions = new BsonArray();
            foreach (var definition in definitions)
            {
                var fields = new BsonArray();
                foreach (var field in definition.Fields)
                {
                    fields = fields.AddValue(new BsonString(field));
                }

                var document = new BsonDocument()
                    .Set(IndexNameKey, new BsonString(definition.Name))
                    .Set(IndexFieldsKey, fields)
                    .Set(IndexUniqueKey, BsonBoolean.FromValue(definition.IsUnique))
                    .Set(IndexSparseKey, BsonBoolean.FromValue(definition.IsSparse))
                    .Set(IndexRootPageKey, new BsonInt64(definition.RootPageId))
                    .Set(IndexMaxKeysKey, BsonInt32.FromValue(definition.MaxKeys));

                indexDefinitions = indexDefinitions.AddValue(document);
            }

            metadata = metadata.Set(IndexMetadataKey, indexDefinitions);
        }

        _collectionMetaStore.UpdateMetadata(collectionName, metadata, forceFlush);
    }

    private static bool TryGetDocument(BsonValue value, [NotNullWhen(true)] out BsonDocument? document)
    {
        if (value is BsonDocument bsonDocument)
        {
            document = bsonDocument;
            return true;
        }

        if (value.IsDocument && value.RawValue is BsonDocument rawDocument)
        {
            document = rawDocument;
            return true;
        }

        document = null;
        return false;
    }

    private static bool TryGetString(BsonDocument document, string key, [NotNullWhen(true)] out string? value)
    {
        if (document.TryGetValue(key, out var bsonValue) && bsonValue is BsonString bsonString)
        {
            value = bsonString.Value;
            return !string.IsNullOrEmpty(value);
        }

        value = null;
        return false;
    }

    private static bool TryGetStringArray(BsonDocument document, string key, out string[] values)
    {
        if (!document.TryGetValue(key, out var bsonValue) || bsonValue is not BsonArray array)
        {
            values = Array.Empty<string>();
            return false;
        }

        var result = new List<string>(array.Count);
        foreach (var item in array)
        {
            if (item is BsonString bsonString && !string.IsNullOrEmpty(bsonString.Value))
            {
                result.Add(bsonString.Value);
            }
        }

        values = result.ToArray();
        return values.Length > 0;
    }

    private static bool TryGetBoolean(BsonDocument document, string key, out bool value)
    {
        if (document.TryGetValue(key, out var bsonValue) && bsonValue is BsonBoolean bsonBoolean)
        {
            value = bsonBoolean.Value;
            return true;
        }

        value = false;
        return false;
    }

    private static bool TryGetUInt32(BsonDocument document, string key, out uint value)
    {
        if (TryGetInt64(document, key, out var longValue) && longValue >= 0 && longValue <= uint.MaxValue)
        {
            value = (uint)longValue;
            return true;
        }

        value = 0;
        return false;
    }

    private static bool TryGetInt32(BsonDocument document, string key, out int value)
    {
        if (TryGetInt64(document, key, out var longValue) && longValue >= int.MinValue && longValue <= int.MaxValue)
        {
            value = (int)longValue;
            return true;
        }

        value = 0;
        return false;
    }

    private static bool TryGetInt64(BsonDocument document, string key, out long value)
    {
        if (document.TryGetValue(key, out var bsonValue))
        {
            switch (bsonValue)
            {
                case BsonInt32 bsonInt32:
                    value = bsonInt32.Value;
                    return true;
                case BsonInt64 bsonInt64:
                    value = bsonInt64.Value;
                    return true;
                case BsonString bsonString when long.TryParse(bsonString.Value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed):
                    value = parsed;
                    return true;
            }
        }

        value = 0;
        return false;
    }

    internal void ClearCurrentTransaction() => _currentTransaction.Value = null;

    internal bool TryGetSecurityMetadata(out DatabaseSecurityMetadata m) => _header.TryGetSecurityMetadata(out m);

    internal bool IsEncrypted => _encryptionContext != null;

    internal bool VerifyEncryptionPassword(string password)
    {
        if (_encryptionContext == null)
        {
            throw new InvalidOperationException("Database is not encrypted.");
        }

        return _encryptionContext.VerifyPassword(password);
    }

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

    internal void RewrapEncryptionPassword(string newPassword)
    {
        if (_encryptionContext == null)
        {
            return;
        }

        if (_encryptionContext.Metadata.CredentialKind != EncryptionCredentialKind.Password)
        {
            throw new InvalidOperationException("Password changes are only supported for password-encrypted databases.");
        }

        lock (_lock)
        {
            _encryptionContext.RewrapWithPassword(newPassword);
            var p = _pageManager.GetPage(1);
            EncryptionMetadataStore.WriteToPage(p, _encryptionContext.Metadata);
            _pageManager.SavePage(p, true);
        }
    }

    internal void RegisterCollection(string n) => _collectionMetaStore.RegisterCollection(n, _options.WriteConcern == WriteConcern.Synced);

    internal void CopyCollectionsTo(TinyDbEngine target, IReadOnlyList<string>? collectionNames = null)
    {
        if (target == null) throw new ArgumentNullException(nameof(target));

        const int CopyBatchSize = 1000;
        var names = collectionNames ?? GetCollectionNames(includeSystemCollections: true)
            .Distinct(StringComparer.Ordinal)
            .OrderBy(static name => name, StringComparer.Ordinal)
            .ToArray();

        foreach (var collectionName in names)
        {
            target.RegisterCollection(collectionName);

            var sourceIndexManager = GetIndexManager(collectionName);
            var targetIndexManager = target.GetIndexManager(collectionName);
            foreach (var stat in sourceIndexManager.GetAllStatistics())
            {
                targetIndexManager.CreateIndex(stat.Name, stat.Fields, stat.IsUnique, stat.IsSparse);
            }

            var documentBatch = new List<BsonDocument>(CopyBatchSize);
            foreach (var document in StreamCollectionDocumentsForCopy(collectionName))
            {
                documentBatch.Add(document);
                if (documentBatch.Count < CopyBatchSize)
                {
                    continue;
                }

                target.InsertDocuments(collectionName, documentBatch);
                documentBatch.Clear();
            }

            if (documentBatch.Count > 0)
            {
                target.InsertDocuments(collectionName, documentBatch);
            }
        }
    }

    private IEnumerable<BsonDocument> StreamCollectionDocumentsForCopy(string collectionName)
    {
        var state = GetCollectionState(collectionName);
        foreach (var result in StreamRawScanResultPages(collectionName, state, null))
        {
            var document = DeserializeDocumentOrThrow(result.Slice);
            if (document.TryGetValue("_collection", out var collectionValue) &&
                collectionValue.ToString() != collectionName)
            {
                continue;
            }

            yield return ResolveLargeDocument(document);
        }
    }

    internal BsonValue AllocateIdentityId(string collectionName, string idFieldName, Type idType)
    {
        if (string.IsNullOrWhiteSpace(collectionName)) throw new ArgumentException("Collection name cannot be empty.", nameof(collectionName));
        if (string.IsNullOrWhiteSpace(idFieldName)) throw new ArgumentException("ID field name cannot be empty.", nameof(idFieldName));

        bool isInt32 = idType == typeof(int);
        bool isInt64 = idType == typeof(long);
        if (!isInt32 && !isInt64)
        {
            throw new NotSupportedException($"Identity ID type '{idType.FullName}' is not supported.");
        }

        var metadataKey = isInt32 ? $"__identity_int32_{idFieldName}" : $"__identity_int64_{idFieldName}";
        var cacheKey = $"{collectionName}\0{metadataKey}";

        lock (_identitySequenceLock)
        {
            if (!_identitySequences.TryGetValue(cacheKey, out var sequence))
            {
                var current = Math.Max(ReadPersistedIdentityValue(collectionName, metadataKey), ScanMaxIdentityValue(collectionName));
                sequence = new IdentitySequenceState(collectionName, metadataKey, current);
                _identitySequences[cacheKey] = sequence;
            }

            var next = checked(sequence.Current + 1);
            var maxValue = isInt32 ? int.MaxValue : long.MaxValue;
            if (next > maxValue)
            {
                throw new InvalidOperationException($"Identity sequence for '{collectionName}.{idFieldName}' exceeded {idType.Name}.MaxValue.");
            }

            if (next > sequence.ReservedUntil)
            {
                var reservedUntil = ReserveIdentityRangeEnd(next, maxValue);
                PersistIdentityValue(collectionName, metadataKey, reservedUntil);
                sequence.ReservedUntil = reservedUntil;
            }

            sequence.Current = next;
            sequence.HasUnflushedExactValue = true;
            return isInt32 ? new BsonInt32((int)next) : new BsonInt64(next);
        }
    }

    private static long ReserveIdentityRangeEnd(long next, long maxValue)
    {
        var remaining = maxValue - next;
        if (remaining < IdentitySequenceReservationSize - 1)
        {
            return maxValue;
        }

        return next + IdentitySequenceReservationSize - 1;
    }

    private long ReadPersistedIdentityValue(string collectionName, string metadataKey)
    {
        var metadata = _collectionMetaStore.GetMetadata(collectionName, includeInternal: true);
        return TryGetInt64(metadata, metadataKey, out var value) ? value : 0;
    }

    private void PersistIdentityValue(string collectionName, string metadataKey, long value)
    {
        var metadata = _collectionMetaStore.GetMetadata(collectionName, includeInternal: true);
        metadata = metadata.Set(metadataKey, new BsonInt64(value));
        _collectionMetaStore.UpdateMetadata(collectionName, metadata, _options.WriteConcern == WriteConcern.Synced);
    }

    private void FlushIdentitySequenceExactValues()
    {
        lock (_identitySequenceLock)
        {
            foreach (var sequence in _identitySequences.Values)
            {
                if (!sequence.HasUnflushedExactValue)
                {
                    continue;
                }

                if (sequence.Current >= sequence.ReservedUntil)
                {
                    sequence.HasUnflushedExactValue = false;
                    continue;
                }

                PersistIdentityValue(sequence.CollectionName, sequence.MetadataKey, sequence.Current);
                sequence.ReservedUntil = sequence.Current;
                sequence.HasUnflushedExactValue = false;
            }
        }
    }

    private long ScanMaxIdentityValue(string collectionName)
    {
        var state = GetCollectionState(collectionName);
        long max = 0;
        foreach (var document in ReadAllDocumentsSnapshotFromPageSnapshots(collectionName, state))
        {
            if (document.TryGetValue("_id", out var id) && TryConvertIdentityValue(id, out var value) && value > max)
            {
                max = value;
            }
        }

        return max;
    }

    private static bool TryConvertIdentityValue(BsonValue? value, out long result)
    {
        result = 0;
        switch (value)
        {
            case BsonInt32 int32:
                result = int32.Value;
                return result > 0;
            case BsonInt64 int64:
                result = int64.Value;
                return result > 0;
            default:
                return false;
        }
    }

    private void EnsureDatabaseSecurity()
    {
        if (IsEncrypted)
        {
            return;
        }

        var p = _options.Password;
        var hasSecurityMetadata = TryGetSecurityMetadata(out _);
        if (string.IsNullOrEmpty(p))
        {
            if (hasSecurityMetadata) throw new UnauthorizedAccessException();
            return;
        }
        if (p.Length < 8) throw new ArgumentException();
        if (hasSecurityMetadata)
        {
            if (!DatabaseSecurity.AuthenticateDatabase(this, p)) throw new UnauthorizedAccessException();
            return;
        }
        DatabaseSecurity.CreateSecureDatabase(this, p);
    }

    private CollectionState GetCollectionState(string col)
    {
        if (_collectionStates.TryGetValue(col, out var existing))
        {
            return existing;
        }

        lock (_collectionStateInitLock)
        {
            if (_collectionStates.TryGetValue(col, out existing))
            {
                return existing;
            }

            var state = CreateEmptyCollectionState();
            BuildDocumentLocationCache(col, state);
            state.MarkCacheInitialized();
            _collectionStates[col] = state;
            return state;
        }
    }

    private static CollectionState CreateEmptyCollectionState()
    {
        return new CollectionState { Index = new MemoryDocumentIndex() };
    }

    internal IDisposable EnterCollectionCommitGates(IEnumerable<string> collectionNames)
    {
        if (collectionNames == null) throw new ArgumentNullException(nameof(collectionNames));

        var gates = collectionNames
            .Where(static name => !string.IsNullOrWhiteSpace(name))
            .Distinct(StringComparer.Ordinal)
            .OrderBy(static name => name, StringComparer.Ordinal)
            .Select(GetCollectionState)
            .Select(static state => state.CommitGate)
            .ToArray();

        return new CollectionState.MonitorLockScope(gates);
    }

    internal IDisposable EnterCollectionDocumentLocks(IEnumerable<CollectionDocumentLockKey> lockKeys)
    {
        if (lockKeys == null) throw new ArgumentNullException(nameof(lockKeys));

        var scopes = lockKeys
            .Where(static key => !string.IsNullOrWhiteSpace(key.CollectionName) &&
                                 key.DocumentId != null &&
                                 !key.DocumentId.IsNull)
            .GroupBy(static key => key.CollectionName, StringComparer.Ordinal)
            .OrderBy(static group => group.Key, StringComparer.Ordinal)
            .Select(group => GetCollectionState(group.Key).EnterDocumentLocks(group.Select(static key => key.DocumentId)))
            .ToArray();

        return new DisposableListScope(scopes);
    }

    private sealed class DisposableListScope : IDisposable
    {
        private readonly IDisposable[] _scopes;
        private bool _disposed;

        public DisposableListScope(IDisposable[] scopes)
        {
            _scopes = scopes ?? throw new ArgumentNullException(nameof(scopes));
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;

            for (var i = _scopes.Length - 1; i >= 0; i--)
            {
                _scopes[i].Dispose();
            }
        }
    }

    private sealed class PrefetchedDocumentLockScope : IDisposable
    {
        private readonly IDisposable _documentLocks;
        private readonly Page[] _pinnedPages;
        private bool _disposed;

        public PrefetchedDocumentLockScope(IDisposable documentLocks, Page[] pinnedPages)
        {
            _documentLocks = documentLocks ?? throw new ArgumentNullException(nameof(documentLocks));
            _pinnedPages = pinnedPages ?? throw new ArgumentNullException(nameof(pinnedPages));
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;

            _documentLocks.Dispose();
            foreach (var page in _pinnedPages)
            {
                page.Unpin();
            }
        }
    }

    private sealed class PrefetchedSingleDocumentLockScope : IDisposable
    {
        private readonly IDisposable _documentLock;
        private readonly Page? _pinnedPage;
        private bool _disposed;

        public PrefetchedSingleDocumentLockScope(IDisposable documentLock, Page? pinnedPage)
        {
            _documentLock = documentLock ?? throw new ArgumentNullException(nameof(documentLock));
            _pinnedPage = pinnedPage;
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;

            _documentLock.Dispose();
            _pinnedPage?.Unpin();
        }
    }

    private async Task<IDisposable> EnterDocumentLocksWithPrefetchedPagesAsync(
        CollectionState st,
        IEnumerable<BsonValue> ids,
        CancellationToken cancellationToken)
    {
        if (st == null) throw new ArgumentNullException(nameof(st));
        if (ids == null) throw new ArgumentNullException(nameof(ids));

        var documentIds = CollectDistinctDocumentIds(ids, static id => id);

        return await EnterDocumentLocksWithPrefetchedPagesAsync(st, documentIds, cancellationToken).ConfigureAwait(false);
    }

    private async Task<IDisposable> EnterDocumentLocksWithPrefetchedPagesAsync<T>(
        CollectionState st,
        IReadOnlyList<T> items,
        Func<T, BsonValue> idSelector,
        CancellationToken cancellationToken)
    {
        if (st == null) throw new ArgumentNullException(nameof(st));
        if (items == null) throw new ArgumentNullException(nameof(items));
        if (idSelector == null) throw new ArgumentNullException(nameof(idSelector));

        var documentIds = CollectDistinctDocumentIds(items, idSelector);

        return await EnterDocumentLocksWithPrefetchedPagesAsync(st, documentIds, cancellationToken).ConfigureAwait(false);
    }

    private async Task<IDisposable> EnterDocumentLockWithPrefetchedPageAsync(
        CollectionState st,
        BsonValue documentId,
        CancellationToken cancellationToken)
    {
        if (st == null) throw new ArgumentNullException(nameof(st));
        if (documentId == null)
        {
            return await st.EnterDocumentLocksAsync(Array.Empty<BsonValue>(), cancellationToken).ConfigureAwait(false);
        }

        if (documentId.IsNull)
        {
            return await st.EnterDocumentLockAsync(documentId, cancellationToken).ConfigureAwait(false);
        }

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            Page? pinnedPage = null;
            IDisposable? documentLock = null;
            var transferOwnership = false;

            try
            {
                var prefetchedPageId = GetCurrentDocumentPageId(st, documentId);
                if (prefetchedPageId != 0)
                {
                    pinnedPage = await _pageManager.GetPagePinnedAsync(
                        prefetchedPageId,
                        cancellationToken: cancellationToken).ConfigureAwait(false);
                }

                documentLock = await st.EnterDocumentLockAsync(documentId, cancellationToken).ConfigureAwait(false);

                var currentPageId = GetCurrentDocumentPageId(st, documentId);
                if (currentPageId == 0 || currentPageId == prefetchedPageId)
                {
                    transferOwnership = true;
                    return new PrefetchedSingleDocumentLockScope(documentLock, pinnedPage);
                }
            }
            finally
            {
                if (!transferOwnership)
                {
                    documentLock?.Dispose();
                    pinnedPage?.Unpin();
                }
            }
        }
    }

    private async Task<IDisposable> EnterDocumentLocksWithPrefetchedPagesAsync(
        CollectionState st,
        BsonValue[] documentIds,
        CancellationToken cancellationToken)
    {
        if (st == null) throw new ArgumentNullException(nameof(st));
        if (documentIds == null) throw new ArgumentNullException(nameof(documentIds));

        if (documentIds.Length == 0)
        {
            return await st.EnterDocumentLocksAsync(documentIds, cancellationToken).ConfigureAwait(false);
        }

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var pinnedPages = new Dictionary<uint, Page>();
            IDisposable? documentLocks = null;
            var transferOwnership = false;

            try
            {
                foreach (var pageId in GetCurrentDocumentPageIds(st, documentIds))
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    if (pinnedPages.ContainsKey(pageId))
                    {
                        continue;
                    }

                    var page = await _pageManager.GetPagePinnedAsync(pageId, cancellationToken: cancellationToken).ConfigureAwait(false);
                    pinnedPages.Add(pageId, page);
                }

                documentLocks = await st.EnterDocumentLocksAsync(documentIds, cancellationToken).ConfigureAwait(false);

                var currentPageIds = GetCurrentDocumentPageIds(st, documentIds);
                if (currentPageIds.All(pinnedPages.ContainsKey))
                {
                    transferOwnership = true;
                    return new PrefetchedDocumentLockScope(documentLocks, ToPageArray(pinnedPages.Values));
                }
            }
            finally
            {
                if (!transferOwnership)
                {
                    documentLocks?.Dispose();
                    foreach (var page in pinnedPages.Values)
                    {
                        page.Unpin();
                    }
                }
            }
        }
    }

    private static BsonValue[] CollectDistinctDocumentIds<T>(IEnumerable<T> items, Func<T, BsonValue> idSelector)
    {
        List<BsonValue>? ids = null;
        HashSet<BsonValue>? seen = null;
        BsonValue? singleId = null;

        foreach (var item in items)
        {
            var id = idSelector(item);
            if (id == null || id.IsNull)
            {
                continue;
            }

            if (singleId == null)
            {
                singleId = id;
                continue;
            }

            if (seen == null)
            {
                seen = new HashSet<BsonValue>(BsonValueComparer.EqualityComparer) { singleId };
                ids = items is IReadOnlyCollection<T> collection
                    ? new List<BsonValue>(collection.Count)
                    : new List<BsonValue>();
                ids.Add(singleId);
            }

            if (seen.Add(id))
            {
                ids!.Add(id);
            }
        }

        if (ids == null)
        {
            return singleId == null ? Array.Empty<BsonValue>() : new[] { singleId };
        }

        return ids.ToArray();
    }

    private static uint GetCurrentDocumentPageId(CollectionState st, BsonValue id)
    {
        return st.Index.TryGet(id, out var location) ? location.PageId : 0;
    }

    private static uint[] GetCurrentDocumentPageIds(CollectionState st, IReadOnlyList<BsonValue> ids)
    {
        List<uint>? pageIds = null;
        HashSet<uint>? seen = null;
        uint singlePageId = 0;

        foreach (var id in ids)
        {
            if (st.Index.TryGet(id, out var location) && location.PageId != 0)
            {
                if (singlePageId == 0)
                {
                    singlePageId = location.PageId;
                    continue;
                }

                if (seen == null)
                {
                    seen = new HashSet<uint> { singlePageId };
                    pageIds = new List<uint>(ids.Count) { singlePageId };
                }

                if (seen.Add(location.PageId))
                {
                    pageIds!.Add(location.PageId);
                }
            }
        }

        if (pageIds == null)
        {
            return singlePageId == 0 ? Array.Empty<uint>() : new[] { singlePageId };
        }

        pageIds.Sort();
        return pageIds.ToArray();
    }

    private static Page[] ToPageArray(Dictionary<uint, Page>.ValueCollection pages)
    {
        var result = new Page[pages.Count];
        var index = 0;
        foreach (var page in pages)
        {
            result[index++] = page;
        }

        return result;
    }

    private sealed class PreparedInsertPayload : IDisposable
    {
        private PooledBufferWriter? _buffer;

        private PreparedInsertPayload(BsonDocument document, BsonValue id, PooledBufferWriter buffer)
        {
            Document = document;
            Id = id;
            _buffer = buffer;
        }

        public BsonDocument Document { get; private set; }
        public BsonValue Id { get; }
        public int SerializedLength => _buffer?.WrittenCount ?? 0;
        public ReadOnlySpan<byte> SerializedSpan => _buffer != null ? _buffer.WrittenSpan : ReadOnlySpan<byte>.Empty;

        public static PreparedInsertPayload Create(BsonDocument document, BsonValue id)
        {
            var buffer = new PooledBufferWriter();
            try
            {
                BsonSerializer.SerializeDocumentToBuffer(document, buffer);
                return new PreparedInsertPayload(document, id, buffer);
            }
            catch
            {
                buffer.Dispose();
                throw;
            }
        }

        public void ReplaceDocument(BsonDocument document)
        {
            var nextBuffer = new PooledBufferWriter();
            try
            {
                BsonSerializer.SerializeDocumentToBuffer(document, nextBuffer);
            }
            catch
            {
                nextBuffer.Dispose();
                throw;
            }

            _buffer?.Dispose();
            _buffer = nextBuffer;
            Document = document;
        }

        public void Dispose()
        {
            _buffer?.Dispose();
            _buffer = null;
        }
    }

    private sealed class WritableDataPageSelectionException : InvalidOperationException
    {
        public WritableDataPageSelectionException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }

    internal BsonValue InsertDocument(string col, BsonDocument doc)
    {
        var st = GetCollectionState(col);
        var idx = GetIndexManager(col);
        BsonValue res;
        using (var pr = PrepareSerializedInsertPayload(col, doc, out _))
        {
            _metadataManager.ValidateDocumentForWrite(col, pr.Document, _options.SchemaValidationMode);
            using var documentLock = st.EnterDocumentLock(pr.Id);
            using var durabilityScope = BeginImplicitWalTransaction();
            res = InsertPreparedDocument(col, pr, st, idx, true);
            durabilityScope?.Commit();
        }
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
        var st = GetCollectionState(col);
        var idx = GetIndexManager(col);
        BsonValue res;
        using (var pr = PrepareSerializedInsertPayload(col, doc, out _))
        {
            _metadataManager.ValidateDocumentForWrite(col, pr.Document, _options.SchemaValidationMode);
            using var documentLock = await st.EnterDocumentLockAsync(pr.Id, cancellationToken).ConfigureAwait(false);
            using var durabilityScope = BeginImplicitWalTransaction();
            res = InsertPreparedDocument(col, pr, st, idx, true);
            durabilityScope?.Commit();
        }
        await EnsureWriteDurabilityAsync(cancellationToken).ConfigureAwait(false);
        return res;
    }

    internal int UpdateDocument(string col, BsonDocument doc)
    {
        doc = PrepareDocumentForUpdate(col, doc, out var id);
        if (id == null || id.IsNull) return 0;

        _metadataManager.ValidateDocumentForWrite(col, doc, _options.SchemaValidationMode);

        var st = GetCollectionState(col);
        var idxMgr = GetIndexManager(col);
        bool updated;

        using (st.EnterDocumentLock(id))
        {
            using var durabilityScope = BeginImplicitWalTransaction();
            updated = TryUpdatePreparedDocument(col, doc, id, st, idxMgr);
            if (updated) durabilityScope?.Commit();
        }

        if (!updated) return 0;

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
        cancellationToken.ThrowIfCancellationRequested();
        doc = PrepareDocumentForUpdate(col, doc, out var id);
        if (id == null || id.IsNull) return 0;

        _metadataManager.ValidateDocumentForWrite(col, doc, _options.SchemaValidationMode);

        var st = GetCollectionState(col);
        var idxMgr = GetIndexManager(col);
        bool updated;

        using (await EnterDocumentLockWithPrefetchedPageAsync(st, id, cancellationToken).ConfigureAwait(false))
        {
            using var durabilityScope = BeginImplicitWalTransaction();
            updated = TryUpdatePreparedDocument(col, doc, id, st, idxMgr);
            if (updated) durabilityScope?.Commit();
        }

        if (!updated) return 0;

        await EnsureWriteDurabilityAsync(cancellationToken).ConfigureAwait(false);
        return 1;
    }

    internal int UpdateDocuments(string col, IReadOnlyList<BsonDocument> docs)
    {
        ThrowIfDisposed();
        EnsureInitialized();
        if (docs == null) throw new ArgumentNullException(nameof(docs));
        if (docs.Count == 0) return 0;

        var prepared = new List<(BsonDocument Doc, BsonValue Id)>(docs.Count);
        foreach (var d in docs)
        {
            if (d == null) continue;

            var doc = PrepareDocumentForUpdate(col, d, out var id);
            if (id == null || id.IsNull) continue;

            _metadataManager.ValidateDocumentForWrite(col, doc, _options.SchemaValidationMode);
            prepared.Add((doc, id));
        }

        if (prepared.Count == 0) return 0;

        var st = GetCollectionState(col);
        var idxMgr = GetIndexManager(col);
        var updatedCount = 0;

        using (st.EnterDocumentLocks(prepared, static item => item.Id))
        {
            using var durabilityScope = BeginImplicitWalTransaction();
            foreach (var (doc, id) in prepared)
            {
                if (TryUpdatePreparedDocument(col, doc, id, st, idxMgr))
                {
                    updatedCount++;
                }
            }
            if (updatedCount > 0) durabilityScope?.Commit();
        }

        if (updatedCount > 0)
        {
            EnsureWriteDurability();
        }

        return updatedCount;
    }

    internal async Task<int> UpdateDocumentsAsync(string col, IReadOnlyList<BsonDocument> docs, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        EnsureInitialized();
        if (docs == null) throw new ArgumentNullException(nameof(docs));
        if (docs.Count == 0) return 0;

        var prepared = new List<(BsonDocument Doc, BsonValue Id)>(docs.Count);
        foreach (var d in docs)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (d == null) continue;

            var doc = PrepareDocumentForUpdate(col, d, out var id);
            if (id == null || id.IsNull) continue;

            _metadataManager.ValidateDocumentForWrite(col, doc, _options.SchemaValidationMode);
            prepared.Add((doc, id));
        }

        if (prepared.Count == 0) return 0;

        var st = GetCollectionState(col);
        var idxMgr = GetIndexManager(col);
        var updatedCount = 0;

        using (await EnterDocumentLocksWithPrefetchedPagesAsync(st, prepared, static item => item.Id, cancellationToken).ConfigureAwait(false))
        {
            using var durabilityScope = BeginImplicitWalTransaction();
            foreach (var (doc, id) in prepared)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (TryUpdatePreparedDocument(col, doc, id, st, idxMgr))
                {
                    updatedCount++;
                }
            }
            if (updatedCount > 0) durabilityScope?.Commit();
        }

        if (updatedCount > 0)
        {
            await EnsureWriteDurabilityAsync(cancellationToken).ConfigureAwait(false);
        }

        return updatedCount;
    }

    internal (UpdateType UpdateType, int Count) UpsertDocument(string col, BsonDocument doc)
    {
        var result = UpsertDocuments(col, new[] { doc });
        return result.InsertedCount > 0
            ? (UpdateType.Insert, result.InsertedCount)
            : (UpdateType.Update, result.UpdatedCount);
    }

    internal async Task<(UpdateType UpdateType, int Count)> UpsertDocumentAsync(string col, BsonDocument doc, CancellationToken cancellationToken = default)
    {
        var result = await UpsertDocumentsAsync(col, new[] { doc }, cancellationToken).ConfigureAwait(false);
        return result.InsertedCount > 0
            ? (UpdateType.Insert, result.InsertedCount)
            : (UpdateType.Update, result.UpdatedCount);
    }

    internal (int InsertedCount, int UpdatedCount) UpsertDocuments(string col, IReadOnlyList<BsonDocument> docs)
    {
        ThrowIfDisposed();
        EnsureInitialized();
        if (docs == null) throw new ArgumentNullException(nameof(docs));
        if (docs.Count == 0) return (0, 0);

        var prepared = new List<(BsonDocument Doc, BsonValue Id)>(docs.Count);
        foreach (var d in docs)
        {
            if (d == null) continue;

            var doc = PrepareDocumentForInsert(col, d, out var id);
            _metadataManager.ValidateDocumentForWrite(col, doc, _options.SchemaValidationMode);
            prepared.Add((doc, id));
        }

        if (prepared.Count == 0) return (0, 0);

        var st = GetCollectionState(col);
        var idxMgr = GetIndexManager(col);
        var insertedCount = 0;
        var updatedCount = 0;

        using (st.EnterDocumentLocks(prepared, static item => item.Id))
        {
            using var durabilityScope = BeginImplicitWalTransaction();
            foreach (var (doc, id) in prepared)
            {
                if (TryUpdatePreparedDocument(col, doc, id, st, idxMgr))
                {
                    updatedCount++;
                }
                else
                {
                    InsertPreparedDocument(col, doc, id, st, idxMgr, true);
                    insertedCount++;
                }
            }
            if (insertedCount + updatedCount > 0) durabilityScope?.Commit();
        }

        if (insertedCount + updatedCount > 0)
        {
            EnsureWriteDurability();
        }

        return (insertedCount, updatedCount);
    }

    internal async Task<(int InsertedCount, int UpdatedCount)> UpsertDocumentsAsync(string col, IReadOnlyList<BsonDocument> docs, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        EnsureInitialized();
        if (docs == null) throw new ArgumentNullException(nameof(docs));
        if (docs.Count == 0) return (0, 0);

        var prepared = new List<(BsonDocument Doc, BsonValue Id)>(docs.Count);
        foreach (var d in docs)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (d == null) continue;

            var doc = PrepareDocumentForInsert(col, d, out var id);
            _metadataManager.ValidateDocumentForWrite(col, doc, _options.SchemaValidationMode);
            prepared.Add((doc, id));
        }

        if (prepared.Count == 0) return (0, 0);

        var st = GetCollectionState(col);
        var idxMgr = GetIndexManager(col);
        var insertedCount = 0;
        var updatedCount = 0;

        using (await EnterDocumentLocksWithPrefetchedPagesAsync(st, prepared, static item => item.Id, cancellationToken).ConfigureAwait(false))
        {
            using var durabilityScope = BeginImplicitWalTransaction();
            foreach (var (doc, id) in prepared)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (TryUpdatePreparedDocument(col, doc, id, st, idxMgr))
                {
                    updatedCount++;
                }
                else
                {
                    InsertPreparedDocument(col, doc, id, st, idxMgr, true);
                    insertedCount++;
                }
            }
            if (insertedCount + updatedCount > 0) durabilityScope?.Commit();
        }

        if (insertedCount + updatedCount > 0)
        {
            await EnsureWriteDurabilityAsync(cancellationToken).ConfigureAwait(false);
        }

        return (insertedCount, updatedCount);
    }

    internal int DeleteDocument(string col, BsonValue id)
    {
        if (id == null || id.IsNull) return 0;

        var st = GetCollectionState(col);
        var idxMgr = GetIndexManager(col);
        int deleted;
        using (st.EnterDocumentLock(id))
        {
            deleted = DeleteDocumentCore(col, id, st, idxMgr);
        }

        if (deleted > 0)
        {
            EnsureWriteDurability();
        }

        return deleted;
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
        if (id == null || id.IsNull) return 0;

        var st = GetCollectionState(col);
        var idxMgr = GetIndexManager(col);
        using (await EnterDocumentLockWithPrefetchedPageAsync(st, id, cancellationToken).ConfigureAwait(false))
        {
            var deleted = DeleteDocumentCore(col, id, st, idxMgr);
            if (deleted == 0) return 0;
        }
        await EnsureWriteDurabilityAsync(cancellationToken).ConfigureAwait(false);
        return 1;
    }

    internal int InsertDocuments(string col, IReadOnlyList<BsonDocument> docs)
    {
        ThrowIfDisposed();
        EnsureInitialized();
        if (docs == null) throw new ArgumentNullException(nameof(docs));
        if (docs.Count == 0) return 0;

        var st = GetCollectionState(col);
        var idx = GetIndexManager(col);
        var exceptions = new List<Exception>();
        var preparedPayloads = PrepareSerializedInsertPayloads(col, docs, exceptions);
        if (preparedPayloads.Count == 0)
        {
            if (exceptions.Count > 0)
            {
                throw new AggregateException("One or more errors occurred during batch insert", exceptions);
            }

            return 0;
        }

        var insertedPayloads = new List<PreparedInsertPayload>(preparedPayloads.Count);
        int insertedCount = 0;

        try
        {
            using (st.EnterDocumentLocks(preparedPayloads, static payload => payload.Id))
            {
                using var durabilityScope = BeginImplicitWalTransaction();

                foreach (var payload in preparedPayloads)
                {
                    try
                    {
                        InsertPreparedDocument(col, payload, st, idx, true);
                        insertedPayloads.Add(payload);
                        insertedCount++;
                    }
                    catch (Exception ex)
                    {
                        exceptions.Add(ex);
                    }
                }

                if (exceptions.Count > 0)
                {
                    RollbackInsertedDocuments(col, insertedPayloads, st, idx, exceptions);
                    if (exceptions.FirstOrDefault(static ex => ex is WritableDataPageSelectionException) is { } pageSelectionException)
                    {
                        throw pageSelectionException;
                    }

                    throw new AggregateException("One or more errors occurred during batch insert", exceptions);
                }

                durabilityScope?.Commit();
            }

            EnsureWriteDurability();
            return insertedCount;
        }
        finally
        {
            foreach (var payload in preparedPayloads)
            {
                payload.Dispose();
            }
        }
    }

    /// <summary>
    /// 异步批量插入文档
    /// </summary>
    /// <param name="col">集合名称</param>
    /// <param name="docs">要插入的文档数组</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>插入的文档数量</returns>
    internal async Task<int> InsertDocumentsAsync(string col, IReadOnlyList<BsonDocument> docs, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        EnsureInitialized();
        if (docs == null) throw new ArgumentNullException(nameof(docs));
        if (docs.Count == 0) return 0;
        cancellationToken.ThrowIfCancellationRequested();

        var st = GetCollectionState(col);
        var idx = GetIndexManager(col);
        var exceptions = new List<Exception>();
        var preparedPayloads = PrepareSerializedInsertPayloads(col, docs, exceptions, cancellationToken);
        if (preparedPayloads.Count == 0)
        {
            if (exceptions.Count > 0)
            {
                var cancellationException = exceptions.Count == 1
                    ? exceptions[0] as OperationCanceledException
                    : null;
                if (cancellationException != null)
                {
                    throw cancellationException;
                }

                throw new AggregateException("One or more errors occurred during batch insert", exceptions);
            }

            return 0;
        }

        var insertedPayloads = new List<PreparedInsertPayload>(preparedPayloads.Count);
        int insertedCount = 0;

        try
        {
            using (await st.EnterDocumentLocksAsync(
                       preparedPayloads,
                       static payload => payload.Id,
                       cancellationToken).ConfigureAwait(false))
            {
                using var durabilityScope = BeginImplicitWalTransaction();

                foreach (var payload in preparedPayloads)
                {
                    if (cancellationToken.IsCancellationRequested)
                    {
                        exceptions.Add(new OperationCanceledException(cancellationToken));
                        break;
                    }

                    try
                    {
                        InsertPreparedDocument(col, payload, st, idx, true);
                        insertedPayloads.Add(payload);
                        insertedCount++;
                    }
                    catch (Exception ex)
                    {
                        exceptions.Add(ex);
                    }
                }

                if (exceptions.Count > 0)
                {
                    var cancellationException = exceptions.Count == 1
                        ? exceptions[0] as OperationCanceledException
                        : null;
                    RollbackInsertedDocuments(col, insertedPayloads, st, idx, exceptions);
                    if (cancellationException != null && exceptions.Count == 1)
                    {
                        throw cancellationException;
                    }

                    if (exceptions.FirstOrDefault(static ex => ex is WritableDataPageSelectionException) is { } pageSelectionException)
                    {
                        throw pageSelectionException;
                    }

                    throw new AggregateException("One or more errors occurred during batch insert", exceptions);
                }

                durabilityScope?.Commit();
            }

            await EnsureWriteDurabilityAsync(cancellationToken).ConfigureAwait(false);
            return insertedCount;
        }
        finally
        {
            foreach (var payload in preparedPayloads)
            {
                payload.Dispose();
            }
        }
    }

    internal IEnumerable<BsonDocument> FindAll(string col)
    {
        var st = GetCollectionState(col);
        var ds = ReadAllDocumentsSnapshotFromPageSnapshots(col, st);
        var tx = GetCurrentTransaction();
        // 即使 ds 为空，也需要合并事务挂起操作
        return tx != null ? MergeTransactionOperations(col, ds, tx) : ds;
    }

    internal async Task<List<BsonDocument>> FindAllAsync(string col, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var st = GetCollectionState(col);
        var ds = await ReadAllDocumentsSnapshotAsync(col, st, cancellationToken).ConfigureAwait(false);
        var tx = GetCurrentTransaction();
        var result = tx != null ? MergeTransactionOperations(col, ds, tx).ToList() : ds;
        return result;
    }

    /// <summary>
    /// 获取集合中所有文档的原始数据快照，支持原地谓词下推。
    /// </summary>
    /// <param name="col">集合名称</param>
    /// <param name="predicates">扫描谓词（可选）</param>
    /// <returns>文档原始数据的枚举</returns>
    internal IEnumerable<ReadOnlyMemory<byte>> FindAllRaw(string col, ScanPredicate[]? predicates = null)
    {
        var st = GetCollectionState(col);
        return ReadRawDocumentSnapshot(col, st, predicates);
    }

    internal IEnumerable<RawScanResult> FindAllRawWithPredicateInfo(string col, ScanPredicate[]? predicates = null)
    {
        var st = GetCollectionState(col);
        return ReadRawScanResultSnapshot(col, st, predicates);
    }

    internal async IAsyncEnumerable<ReadOnlyMemory<byte>> FindAllRawAsync(
        string col,
        ScanPredicate[]? predicates = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var result in FindAllRawWithPredicateInfoAsync(col, predicates, cancellationToken).ConfigureAwait(false))
        {
            yield return result.Slice;
        }
    }

    internal async IAsyncEnumerable<RawScanResult> FindAllRawWithPredicateInfoAsync(
        string col,
        ScanPredicate[]? predicates = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var st = GetCollectionState(col);
        await foreach (var result in StreamRawScanResultPagesAsync(col, st, predicates, cancellationToken).ConfigureAwait(false))
        {
            yield return result;
        }
    }

    internal BsonDocument? FindById(string col, BsonValue id)
    {
        var tx = GetCurrentTransaction();
        if (tx != null && TryGetTransactionDocument(tx, col, id, out var transactionDocument))
        {
            return transactionDocument;
        }

        return FindCommittedById(col, id);
    }

    internal List<BsonDocument?> FindByIds(string col, IReadOnlyList<BsonValue> ids)
    {
        if (ids == null) throw new ArgumentNullException(nameof(ids));
        if (ids.Count == 0) return new List<BsonDocument?>();

        var tx = GetCurrentTransaction();
        if (tx == null)
        {
            return FindCommittedByIds(col, ids);
        }

        var results = new BsonDocument?[ids.Count];
        var committedIds = new List<BsonValue>(ids.Count);
        var committedOrdinals = new List<int>(ids.Count);

        var operations = tx.GetOperationsSnapshot();
        for (int i = 0; i < ids.Count; i++)
        {
            if (TryGetTransactionDocument(operations, col, ids[i], out var transactionDocument))
            {
                results[i] = transactionDocument;
                continue;
            }

            committedOrdinals.Add(i);
            committedIds.Add(ids[i]);
        }

        if (committedIds.Count > 0)
        {
            var committedDocuments = FindCommittedByIds(col, committedIds);
            for (int i = 0; i < committedDocuments.Count; i++)
            {
                results[committedOrdinals[i]] = committedDocuments[i];
            }
        }

        return results.ToList();
    }

    internal BsonDocument? FindCommittedById(string col, BsonValue id)
    {
        var st = GetCollectionState(col);
        DocumentLocation? indexedLocation = null;
        if (st.Index.TryGet(id, out var loc))
        {
            indexedLocation = loc;
        }

        if (indexedLocation is { } location)
        {
            var p = _pageManager.GetPage(location.PageId);
            BsonDocument? document = null;

            using (st.EnterPageMutationLock(location.PageId))
            {
                if (p.PageType == PageType.Data && location.EntryIndex < p.Header.ItemCount)
                {
                    var entry = _dataPageAccess.ReadDocumentAt(p, location.EntryIndex);
                    document = entry?.Document;
                }
            }

            if (document != null &&
                document.TryGetValue("_id", out var documentId) &&
                BsonValuesEqual(documentId, id) &&
                (!document.TryGetValue("_collection", out var collectionValue) || collectionValue.ToString() == col))
            {
                return ResolveLargeDocument(document);
            }

            RecordFindByIdStaleIndexHit(col, id);
        }

        return FindByIdFullScan(col, id, st);
    }

    internal List<BsonDocument?> FindCommittedByIds(string col, IReadOnlyList<BsonValue> ids)
    {
        if (ids == null) throw new ArgumentNullException(nameof(ids));

        var st = GetCollectionState(col);
        var results = new BsonDocument?[ids.Count];
        var indexHits = new bool[ids.Count];
        var pageLookups = new Dictionary<uint, List<(BsonValue Id, int Ordinal, DocumentLocation Location)>>();

        for (int i = 0; i < ids.Count; i++)
        {
            if (st.Index.TryGet(ids[i], out var location))
            {
                indexHits[i] = true;
                if (!pageLookups.TryGetValue(location.PageId, out var lookups))
                {
                    lookups = new List<(BsonValue Id, int Ordinal, DocumentLocation Location)>();
                    pageLookups.Add(location.PageId, lookups);
                }

                lookups.Add((ids[i], i, location));
            }
        }

        foreach (var (pageId, lookups) in pageLookups)
        {
            var page = _pageManager.GetPage(pageId);

            using (st.EnterPageMutationLock(pageId))
            {
                ReadCommittedPageLookups(col, page, lookups, results);
            }
        }

        for (int i = 0; i < results.Length; i++)
        {
            if (results[i] == null && indexHits[i])
            {
                RecordFindByIdStaleIndexHit(col, ids[i]);
            }

            results[i] ??= FindByIdFullScan(col, ids[i], st);
            if (results[i] != null)
            {
                results[i] = ResolveLargeDocument(results[i]!);
            }
        }

        return results.ToList();
    }

    internal async Task<BsonDocument?> FindByIdAsync(string col, BsonValue id, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var tx = GetCurrentTransaction();
        if (tx != null && TryGetTransactionDocument(tx, col, id, out var transactionDocument))
        {
            return transactionDocument;
        }

        var st = GetCollectionState(col);
        DocumentLocation? indexedLocation = null;
        if (st.Index.TryGet(id, out var loc))
        {
            indexedLocation = loc;
        }

        if (indexedLocation is { } location)
        {
            var indexedDocument = await TryReadCommittedByLocationAsync(col, id, st, location, cancellationToken).ConfigureAwait(false);
            if (indexedDocument != null) return indexedDocument;
            RecordFindByIdStaleIndexHit(col, id);
        }

        return await FindByIdFullScanAsync(col, id, cancellationToken).ConfigureAwait(false);
    }

    internal async Task<List<BsonDocument?>> FindByIdsAsync(
        string col,
        IReadOnlyList<BsonValue> ids,
        CancellationToken cancellationToken = default)
    {
        if (ids == null) throw new ArgumentNullException(nameof(ids));
        cancellationToken.ThrowIfCancellationRequested();
        if (ids.Count == 0) return new List<BsonDocument?>();

        var tx = GetCurrentTransaction();
        if (tx == null)
        {
            return await FindCommittedByIdsAsync(col, ids, cancellationToken).ConfigureAwait(false);
        }

        var results = new BsonDocument?[ids.Count];
        var committedIds = new List<BsonValue>(ids.Count);
        var committedOrdinals = new List<int>(ids.Count);

        var operations = tx.GetOperationsSnapshot();
        for (int i = 0; i < ids.Count; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (TryGetTransactionDocument(operations, col, ids[i], out var transactionDocument))
            {
                results[i] = transactionDocument;
                continue;
            }

            committedOrdinals.Add(i);
            committedIds.Add(ids[i]);
        }

        if (committedIds.Count > 0)
        {
            var committedDocuments = await FindCommittedByIdsAsync(col, committedIds, cancellationToken).ConfigureAwait(false);
            for (int i = 0; i < committedDocuments.Count; i++)
            {
                results[committedOrdinals[i]] = committedDocuments[i];
            }
        }

        return results.ToList();
    }

    internal async Task<List<BsonDocument?>> FindCommittedByIdsAsync(
        string col,
        IReadOnlyList<BsonValue> ids,
        CancellationToken cancellationToken = default)
    {
        if (ids == null) throw new ArgumentNullException(nameof(ids));
        cancellationToken.ThrowIfCancellationRequested();

        var st = GetCollectionState(col);
        var results = new BsonDocument?[ids.Count];
        var indexHits = new bool[ids.Count];
        var pageLookups = new Dictionary<uint, List<(BsonValue Id, int Ordinal, DocumentLocation Location)>>();

        for (int i = 0; i < ids.Count; i++)
        {
            if (st.Index.TryGet(ids[i], out var location))
            {
                indexHits[i] = true;
                if (!pageLookups.TryGetValue(location.PageId, out var lookups))
                {
                    lookups = new List<(BsonValue Id, int Ordinal, DocumentLocation Location)>();
                    pageLookups.Add(location.PageId, lookups);
                }

                lookups.Add((ids[i], i, location));
            }
        }

        foreach (var (pageId, lookups) in pageLookups)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var page = await _pageManager.GetPageAsync(pageId, cancellationToken: cancellationToken).ConfigureAwait(false);

            using (st.EnterPageMutationLock(pageId))
            {
                ReadCommittedPageLookups(col, page, lookups, results);
            }
        }

        for (int i = 0; i < results.Length; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (results[i] == null && indexHits[i])
            {
                RecordFindByIdStaleIndexHit(col, ids[i]);
            }

            results[i] ??= await FindByIdFullScanAsync(col, ids[i], cancellationToken).ConfigureAwait(false);
            if (results[i] != null)
            {
                results[i] = await ResolveLargeDocumentAsync(results[i]!, cancellationToken).ConfigureAwait(false);
            }
        }

        return results.ToList();
    }

    private async Task<BsonDocument?> FindByIdFullScanAsync(string col, BsonValue id, CancellationToken cancellationToken = default)
    {
        RecordFindByIdFullScan();

        var idPredicate = new[]
        {
            new ScanPredicate(
                Encoding.UTF8.GetBytes("_id"),
                Encoding.UTF8.GetBytes("id"),
                Encoding.UTF8.GetBytes("Id"),
                id,
                ExpressionType.Equal)
        };

        await foreach (var result in FindAllRawWithPredicateInfoAsync(col, idPredicate, cancellationToken).ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();

            var doc = DeserializeDocumentOrThrow(result.Slice);
            if (doc.TryGetValue("_collection", out var c) && c.ToString() != col)
            {
                continue;
            }

            doc = await ResolveLargeDocumentAsync(doc, cancellationToken).ConfigureAwait(false);
            if (doc.TryGetValue("_id", out var documentId) && BsonValuesEqual(documentId, id))
            {
                RecordFindByIdFullScanHit(col, id);
                return doc;
            }
        }

        return null;
    }

    private List<BsonDocument> ReadAllDocumentsSnapshot(string col, CollectionState st, CancellationToken cancellationToken = default)
    {
        return ReadAllDocumentsSnapshotFromPageSnapshots(col, st, cancellationToken);
    }

    private List<BsonDocument> ReadAllDocumentsSnapshotFromPageSnapshots(string col, CollectionState st, CancellationToken cancellationToken = default)
    {
        var ds = new List<BsonDocument>();

        foreach (var result in StreamRawScanResultPages(col, st, null))
        {
            cancellationToken.ThrowIfCancellationRequested();

            var doc = DeserializeDocumentOrThrow(result.Slice);
            if (doc.TryGetValue("_collection", out var c) && c.ToString() != col) continue;
            ds.Add(ResolveLargeDocument(doc));
        }

        return ds;
    }

    private async Task<List<BsonDocument>> ReadAllDocumentsSnapshotAsync(string col, CollectionState st, CancellationToken cancellationToken = default)
    {
        var ds = new List<BsonDocument>();

        await foreach (var result in StreamRawScanResultPagesAsync(col, st, null, cancellationToken).ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();

            var doc = DeserializeDocumentOrThrow(result.Slice);
            if (doc.TryGetValue("_collection", out var c) && c.ToString() != col)
            {
                continue;
            }

            ds.Add(await ResolveLargeDocumentAsync(doc, cancellationToken).ConfigureAwait(false));
        }

        return ds;
    }

    private IEnumerable<ReadOnlyMemory<byte>> ReadRawDocumentSnapshot(string col, CollectionState st, ScanPredicate[]? predicates)
    {
        foreach (var result in StreamRawScanResultPages(col, st, predicates))
        {
            yield return result.Slice;
        }
    }

    private IEnumerable<RawScanResult> ReadRawScanResultSnapshot(string col, CollectionState st, ScanPredicate[]? predicates)
    {
        foreach (var result in StreamRawScanResultPages(col, st, predicates))
        {
            yield return result;
        }
    }

    private IEnumerable<RawScanResult> StreamRawScanResultPages(string col, CollectionState st, ScanPredicate[]? predicates)
    {
        List<uint> pages;
        pages = st.OwnedPages.Keys.ToList();

        pages.Sort();

        foreach (var pageId in pages)
        {
            byte[]? pageSnapshot = null;
            int itemCount = 0;
            int endOffset = 0;
            Exception? lastError = null;

            for (int attempt = 0; attempt < 2; attempt++)
            {
                try
                {
                    var p = _pageManager.GetPage(pageId);

                    using (st.EnterPageMutationLock(pageId))
                    {
                        if (p.PageType != PageType.Data || p.Header.ItemCount == 0)
                        {
                            pageSnapshot = null;
                            itemCount = 0;
                            endOffset = 0;
                            break;
                        }

                        // 为当前页创建稳定快照：每页只复制一次，避免逐文档 ToArray 带来的分配放大。
                        p.Pin();
                        try
                        {
                            itemCount = p.Header.ItemCount;
                            pageSnapshot = p.Snapshot(includeUnusedTail: false);
                            endOffset = pageSnapshot.Length;
                        }
                        finally
                        {
                            p.Unpin();
                        }
                    }

                    lastError = null;
                    break;
                }
                catch (ObjectDisposedException ex) when (attempt == 0)
                {
                    // 页面可能在极窄窗口内被缓存淘汰，重试一次即可。
                    lastError = ex;
                }
                catch (Exception ex)
                {
                    lastError = ex;
                    break;
                }
            }

            if (lastError != null)
            {
                throw new InvalidOperationException(
                    $"Failed to read page {pageId} in collection '{col}'.", lastError);
            }

            if (pageSnapshot == null || itemCount == 0) continue;

            foreach (var result in _dataPageAccess.ScanRawDocumentsFromPageSnapshotWithPredicateInfo(pageSnapshot, itemCount, endOffset, predicates))
            {
                yield return result;
            }
        }
    }

    private async IAsyncEnumerable<RawScanResult> StreamRawScanResultPagesAsync(
        string col,
        CollectionState st,
        ScanPredicate[]? predicates,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        List<uint> pages;
        pages = st.OwnedPages.Keys.ToList();

        pages.Sort();

        foreach (var pageId in pages)
        {
            cancellationToken.ThrowIfCancellationRequested();

            byte[]? pageSnapshot = null;
            int itemCount = 0;
            int endOffset = 0;
            Exception? lastError = null;

            for (int attempt = 0; attempt < 2; attempt++)
            {
                try
                {
                    var p = await _pageManager.GetPageAsync(pageId, cancellationToken: cancellationToken).ConfigureAwait(false);

                    using (st.EnterPageMutationLock(pageId))
                    {
                        if (p.PageType != PageType.Data || p.Header.ItemCount == 0)
                        {
                            pageSnapshot = null;
                            itemCount = 0;
                            endOffset = 0;
                            break;
                        }

                        p.Pin();
                        try
                        {
                            itemCount = p.Header.ItemCount;
                            pageSnapshot = p.Snapshot(includeUnusedTail: false);
                            endOffset = pageSnapshot.Length;
                        }
                        finally
                        {
                            p.Unpin();
                        }
                    }

                    lastError = null;
                    break;
                }
                catch (ObjectDisposedException ex) when (attempt == 0)
                {
                    lastError = ex;
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    lastError = ex;
                    break;
                }
            }

            if (lastError != null)
            {
                throw new InvalidOperationException(
                    $"Failed to read page {pageId} in collection '{col}'.", lastError);
            }

            if (pageSnapshot == null || itemCount == 0) continue;

            foreach (var result in _dataPageAccess.ScanRawDocumentsFromPageSnapshotWithPredicateInfo(pageSnapshot, itemCount, endOffset, predicates))
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return result;
            }
        }
    }

    private async Task<BsonDocument?> TryReadCommittedByLocationAsync(
        string col,
        BsonValue id,
        CollectionState st,
        DocumentLocation location,
        CancellationToken cancellationToken)
    {
        var page = await _pageManager.GetPageAsync(location.PageId, cancellationToken: cancellationToken).ConfigureAwait(false);
        BsonDocument? document = null;

        using (st.EnterPageMutationLock(location.PageId))
        {
            if (page.PageType != PageType.Data || location.EntryIndex >= page.Header.ItemCount)
            {
                return null;
            }

            var entry = _dataPageAccess.ReadDocumentAt(page, location.EntryIndex);
            if (entry == null)
            {
                return null;
            }

            document = entry.Value.Document;
        }

        if (!document.TryGetValue("_id", out var documentId) || !BsonValuesEqual(documentId, id))
        {
            return null;
        }

        if (document.TryGetValue("_collection", out var collectionValue) && collectionValue.ToString() != col)
        {
            return null;
        }

        return await ResolveLargeDocumentAsync(document, cancellationToken).ConfigureAwait(false);
    }

    private static BsonDocument DeserializeDocumentOrThrow(ReadOnlyMemory<byte> slice)
    {
        try
        {
            return BsonSerializer.DeserializeDocument(slice);
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException("Failed to deserialize BSON document from storage slice.", ex);
        }
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
        var st = GetCollectionState(col);
        var idx = GetIndexManager(col);
        using var pr = PrepareSerializedInsertPayload(col, d, out _);
        BsonValue result;
        using (st.EnterDocumentLock(pr.Id))
        {
            using var durabilityScope = BeginImplicitWalTransaction();
            result = InsertPreparedDocument(col, pr, st, idx, true);
            durabilityScope?.Commit();
        }

        return result;
    }

    internal int UpdateDocumentInternal(string col, BsonDocument d) => UpdateDocument(col, d);
    internal int DeleteDocumentInternal(string col, BsonValue id) => DeleteDocument(col, id);
    public int GetCachedDocumentCount(string col) => GetCollectionState(col).Index.Count;

    internal long GetTransactionalDocumentCount(string col, Transaction transaction)
    {
        if (transaction == null) throw new ArgumentNullException(nameof(transaction));

        var operations = transaction.GetOperationsSnapshot()
            .Where(op => op.CollectionName == col &&
                         op.DocumentId != null &&
                         !op.DocumentId.IsNull &&
                         op.OperationType is TransactionOperationType.Insert or TransactionOperationType.Update or TransactionOperationType.Delete)
            .ToArray();

        if (operations.Length == 0)
        {
            return GetCachedDocumentCount(col);
        }

        var initialExistsById = new Dictionary<BsonValue, bool>(BsonValueComparer.EqualityComparer);
        var currentExistsById = new Dictionary<BsonValue, bool>(BsonValueComparer.EqualityComparer);

        foreach (var operation in operations)
        {
            var id = operation.DocumentId!;
            if (!currentExistsById.TryGetValue(id, out var exists))
            {
                exists = FindCommittedById(col, id) != null;
                initialExistsById[id] = exists;
            }

            currentExistsById[id] = operation.OperationType switch
            {
                TransactionOperationType.Insert => true,
                TransactionOperationType.Delete => false,
                _ => exists
            };
        }

        long delta = 0;
        foreach (var (id, currentExists) in currentExistsById)
        {
            var initiallyExists = initialExistsById[id];
            if (currentExists && !initiallyExists) delta++;
            else if (!currentExists && initiallyExists) delta--;
        }

        return GetCachedDocumentCount(col) + delta;
    }

    private BsonValue InsertPreparedDocument(string col, BsonDocument doc, BsonValue id, CollectionState st, IndexManager idx, bool u)
    {
        using var prepared = PreparedInsertPayload.Create(doc, id);
        return InsertPreparedDocument(col, prepared, st, idx, u);
    }

    private BsonValue InsertPreparedDocument(string col, PreparedInsertPayload prepared, CollectionState st, IndexManager idx, bool u)
    {
        var doc = prepared.Document;
        var indexDocument = doc;

        ThrowIfPrimaryKeyExists(st, prepared.Id);

        if (LargeDocumentStorage.RequiresLargeDocumentStorage(prepared.SerializedLength, _dataPageAccess.GetMaxDocumentSize()))
        {
            var originalLength = prepared.SerializedLength;
            var lId = _largeDocumentStorage.StoreLargeDocument(prepared.SerializedSpan, col);
            doc = CreateLargeDocumentIndexDocument(prepared.Id, col, lId, originalLength);
            prepared.ReplaceDocument(doc);
        }

        AppendDocumentBytesToWritableDataPage(st, prepared.Id, prepared.SerializedSpan);
        if (u)
        {
            try
            {
                idx.InsertDocument(indexDocument, prepared.Id);
            }
            catch
            {
                DeleteDocumentCore(col, prepared.Id, st, idx);
                throw;
            }
        }

        return prepared.Id;
    }

    private void ThrowIfPrimaryKeyExists(CollectionState st, BsonValue id)
    {
        if (st.Index.TryGet(id, out _))
        {
            throw new InvalidOperationException($"Duplicate document id '{id}'.");
        }
    }

    private (Page Page, bool IsNew) SelectWritableDataPage(CollectionState st, int requiredSize)
    {
        try
        {
            lock (st.PageState.SyncRoot)
            {
                var selected = _dataPageAccess.GetWritableDataPageLocked(st.PageState, requiredSize);
                st.OwnedPages.TryAdd(selected.Page.PageID, 0);
                if (selected.IsNew)
                {
                    IncrementUsedPagesAndWriteHeader();
                }

                return selected;
            }
        }
        catch (Exception ex) when (ex is not WritableDataPageSelectionException)
        {
            throw new WritableDataPageSelectionException("Failed to select a writable data page.", ex);
        }
    }

    private void MarkWritablePageUnavailable(CollectionState st, uint pageId)
    {
        Interlocked.CompareExchange(ref st.PageState.PageId, 0, pageId);
    }

    private void IncrementUsedPagesAndWriteHeader()
    {
        lock (_lock)
        {
            _header.UsedPages++;
            WriteHeader();
        }
    }

    private DocumentLocation AppendDocumentBytesToWritableDataPage(
        CollectionState st,
        BsonValue id,
        ReadOnlySpan<byte> bytes)
    {
        var requiredSize = DataPageAccess.GetEntrySize(bytes.Length);

        while (true)
        {
            var (page, _) = SelectWritableDataPage(st, requiredSize);
            try
            {
                using (st.EnterPageMutationLock(page.PageID))
                {
                    if (page.Header.PageType != PageType.Data || page.Header.FreeBytes < requiredSize)
                    {
                        MarkWritablePageUnavailable(st, page.PageID);
                        continue;
                    }

                    var entryIndex = page.Header.ItemCount;
                    var beforeImage = _dataPageAccess.CaptureBeforeImageForWal(page);
                    _dataPageAccess.AppendDocumentToPage(page, bytes);
                    var location = new DocumentLocation(page.PageID, entryIndex);
                    st.Index.Set(id, location);
                    _dataPageAccess.PersistPageDeferred(page, beforeImage);
                    return location;
                }
            }
            finally
            {
                page.Unpin();
            }
        }
    }

    private bool TryUpdatePreparedDocument(string col, BsonDocument doc, BsonValue id, CollectionState st, IndexManager idxMgr)
    {
        PageDocumentEntry old;
        var updatedDocument = doc;
        var newIndexDocument = doc;
        uint newLargeDocumentPageId = 0;
        byte[]? relocationBytes = null;

        if (!st.Index.TryGet(id, out var initialLocation)) return false;

        using (st.EnterPageMutationLock(initialLocation.PageId))
        {
            if (!TryResolveDocumentLocation(col, st, id, out var p, out var e, out var i)) return false;
            try
            {
                if (i >= e.Count) return false;
                old = e[i];

                var bs = BsonSerializer.SerializeDocument(updatedDocument);
                if (LargeDocumentStorage.RequiresLargeDocumentStorage(bs.Length, _dataPageAccess.GetMaxDocumentSize()))
                {
                    var largeDocumentPageId = _largeDocumentStorage.StoreLargeDocument(bs, col);
                    newLargeDocumentPageId = largeDocumentPageId;
                    updatedDocument = CreateLargeDocumentIndexDocument(id, col, largeDocumentPageId, bs.Length);
                    bs = BsonSerializer.SerializeDocument(updatedDocument);
                }

                e[i] = new PageDocumentEntry(updatedDocument, bs);

                if (!_dataPageAccess.CanFitInPage(p, e))
                {
                    e[i] = old;
                    relocationBytes = bs;
                }
                else
                {
                    _dataPageAccess.RewritePageWithDocuments(col, st, p, e, (key, pId, idx) => st.Index.Set(key, new DocumentLocation(pId, idx)));
                }
            }
            finally
            {
                p.Unpin();
            }
        }

        if (relocationBytes != null)
        {
            MoveUpdatedDocumentToWritablePage(col, st, id, updatedDocument, relocationBytes);
        }

        try
        {
            var oldIndexDocument = old.IsLargeDocument ? ResolveLargeDocument(old.Document) : old.Document;
            idxMgr.UpdateDocument(oldIndexDocument, newIndexDocument, id);
        }
        catch
        {
            RestoreDocumentDataWithoutIndexUpdate(col, st, id, old);
            if (newLargeDocumentPageId != 0)
            {
                DeleteLargeDocumentOrThrow(newLargeDocumentPageId);
            }
            throw;
        }

        if (old.IsLargeDocument) DeleteLargeDocumentOrThrow(old.LargeDocumentIndexPageId);
        return true;
    }

    private void MoveUpdatedDocumentToWritablePage(
        string col,
        CollectionState st,
        BsonValue id,
        BsonDocument updatedDocument,
        byte[] updatedBytes)
    {
        var requiredSize = DataPageAccess.GetEntrySize(updatedBytes.Length);

        while (true)
        {
            if (!st.Index.TryGet(id, out var oldLocation))
            {
                throw new InvalidOperationException($"Document '{id}' disappeared during update relocation.");
            }

            var (targetPage, _) = SelectWritableDataPage(st, requiredSize);
            try
            {
                using (st.EnterPageMutationLocks(new[] { oldLocation.PageId, targetPage.PageID }))
                {
                    if (!st.Index.TryGet(id, out var currentLocation) ||
                        currentLocation.PageId != oldLocation.PageId)
                    {
                        continue;
                    }

                    if (targetPage.Header.PageType != PageType.Data || targetPage.Header.FreeBytes < requiredSize)
                    {
                        MarkWritablePageUnavailable(st, targetPage.PageID);
                        continue;
                    }

                    var oldPage = _pageManager.GetPagePinned(currentLocation.PageId);
                    try
                    {
                        var oldEntries = _dataPageAccess.ReadDocumentsFromPage(oldPage);
                        if (currentLocation.EntryIndex >= oldEntries.Count)
                        {
                            throw new InvalidOperationException($"Primary index for document '{id}' points outside page {oldPage.PageID}.");
                        }

                        var currentOldEntry = oldEntries[currentLocation.EntryIndex];
                        if (!BsonValuesEqual(currentOldEntry.Id, id))
                        {
                            throw new InvalidOperationException($"Primary index for document '{id}' points to a different document.");
                        }

                        oldEntries[currentLocation.EntryIndex] = new PageDocumentEntry(updatedDocument, updatedBytes);
                        if (_dataPageAccess.CanFitInPage(oldPage, oldEntries))
                        {
                            _dataPageAccess.RewritePageWithDocuments(col, st, oldPage, oldEntries, (key, pId, idx) => st.Index.Set(key, new DocumentLocation(pId, idx)));
                            return;
                        }

                        oldEntries[currentLocation.EntryIndex] = currentOldEntry;
                        oldEntries.RemoveAt(currentLocation.EntryIndex);
                        st.Index.Remove(id);

                        if (oldEntries.Count == 0 && oldPage.PageID != targetPage.PageID)
                        {
                            FreeDataPage(st, oldPage);
                        }
                        else
                        {
                            _dataPageAccess.RewritePageWithDocuments(col, st, oldPage, oldEntries, (key, pId, idx) => st.Index.Set(key, new DocumentLocation(pId, idx)));
                        }

                        var entryIndex = targetPage.Header.ItemCount;
                        var beforeImage = _dataPageAccess.CaptureBeforeImageForWal(targetPage);
                        _dataPageAccess.AppendDocumentToPage(targetPage, updatedBytes);
                        st.Index.Set(id, new DocumentLocation(targetPage.PageID, entryIndex));
                        _dataPageAccess.PersistPageDeferred(targetPage, beforeImage);
                        return;
                    }
                    finally
                    {
                        oldPage.Unpin();
                    }
                }
            }
            finally
            {
                targetPage.Unpin();
            }
        }
    }

    private void FreeDataPage(CollectionState st, Page page)
    {
        st.OwnedPages.TryRemove(page.PageID, out _);
        Interlocked.CompareExchange(ref st.PageState.PageId, 0, page.PageID);

        _pageManager.FreePage(page.PageID);
        DecrementUsedPagesAndWriteHeader();
    }

    private void RestoreDocumentDataWithoutIndexUpdate(string col, CollectionState st, BsonValue id, PageDocumentEntry oldEntry)
    {
        if (st.Index.TryGet(id, out var currentLocation))
        {
            using (st.EnterPageMutationLock(currentLocation.PageId))
            {
                if (TryResolveDocumentLocation(col, st, id, out var currentPage, out var currentEntries, out var currentIndex) &&
                    currentIndex < currentEntries.Count)
                {
                    try
                    {
                        currentEntries.RemoveAt(currentIndex);
                        st.Index.Remove(id);

                        if (currentEntries.Count == 0)
                        {
                            FreeDataPage(st, currentPage);
                        }
                        else
                        {
                            _dataPageAccess.RewritePageWithDocuments(col, st, currentPage, currentEntries, (key, pId, idx) => st.Index.Set(key, new DocumentLocation(pId, idx)));
                        }
                    }
                    finally
                    {
                        currentPage.Unpin();
                    }
                }
            }
        }

        AppendDocumentBytesToWritableDataPage(st, id, oldEntry.RawMemory.Span);
    }

    private static BsonDocument PrepareDocumentForUpdate(string col, BsonDocument doc, out BsonValue id)
    {
        if (!doc.TryGetValue("_id", out var existingId) || existingId == null || existingId.IsNull)
        {
            id = BsonNull.Value;
            return doc;
        }

        id = existingId;

        // Ensure _collection field matches the target collection name.
        // This is critical for AOT-generated documents where _collection might differ from the runtime collection.
        if (!doc.TryGetValue("_collection", out var docCol) || docCol.ToString() != col)
        {
            doc = doc.Set("_collection", col);
        }

        return doc;
    }

    private int DeleteDocumentCore(string col, BsonValue id, CollectionState st, IndexManager idxMgr)
    {
        if (!st.Index.TryGet(id, out var loc)) return 0;

        using var durabilityScope = BeginImplicitWalTransaction();
        using (st.EnterPageMutationLock(loc.PageId))
        {
            if (!TryResolveDocumentLocation(col, st, id, out var p, out var e, out var i)) return 0;
            try
            {
                if (i >= e.Count) return 0;
                var entry = e[i];
                e.RemoveAt(i);
                st.Index.Remove(id);
                if (e.Count == 0)
                {
                    FreeDataPage(st, p);
                }
                else
                {
                    _dataPageAccess.RewritePageWithDocuments(col, st, p, e, (key, pId, idx) => st.Index.Set(key, new DocumentLocation(pId, idx)));
                }

                var indexDocument = entry.IsLargeDocument ? ResolveLargeDocument(entry.Document) : entry.Document;
                if (entry.IsLargeDocument) DeleteLargeDocumentOrThrow(entry.LargeDocumentIndexPageId);
                idxMgr.DeleteDocument(indexDocument, id);
                durabilityScope?.Commit();
                return 1;
            }
            finally
            {
                p.Unpin();
            }
        }
    }

    private void RollbackInsertedDocuments(
        string collectionName,
        IReadOnlyList<PreparedInsertPayload> insertedPayloads,
        CollectionState st,
        IndexManager idxMgr,
        List<Exception> exceptions)
    {
        HashSet<BsonValue>? rolledBackIds = null;

        for (var i = insertedPayloads.Count - 1; i >= 0; i--)
        {
            var id = insertedPayloads[i].Id;
            if (id == null || id.IsNull)
            {
                continue;
            }

            rolledBackIds ??= new HashSet<BsonValue>(BsonValueComparer.EqualityComparer);
            if (!rolledBackIds.Add(id))
            {
                continue;
            }

            try
            {
                DeleteDocumentCore(collectionName, id, st, idxMgr);
            }
            catch (Exception ex)
            {
                exceptions.Add(new InvalidOperationException($"Failed to rollback inserted document '{id}' in collection '{collectionName}'.", ex));
            }
        }
    }

    private BsonDocument PrepareDocumentForInsert(string col, BsonDocument doc, out BsonValue id)
    {
        bool hasId = doc.TryGetValue("_id", out var exId) && exId != null && !exId.IsNull;
        // 检查 _collection 是否已经存在且值正确（避免不必要的重建）
        bool hasCorrectCol = doc.TryGetValue("_collection", out var existingCol) && existingCol?.ToString() == col;
        bool hasIdFirst = doc.TryGetFirstKey(out var firstKey) &&
                          string.Equals(firstKey, "_id", StringComparison.Ordinal);

        // 快速路径：如果已有 _id 且 _collection 值正确，直接返回原文档
        if (hasId && hasCorrectCol && hasIdFirst)
        {
            id = exId!;
            return doc;
        }

        // 优化：直接创建新的 Builder，一次性添加所有字段，避免 ToBuilder() 的额外转换
        var builder = new BsonDocumentBuilder(doc.Count + 2);

        // 复制原文档的所有字段
        // 添加 _id（如果缺失）
        if (!hasId)
        {
            id = ObjectId.NewObjectId();
        }
        else
        {
            id = exId!;
        }

        // 强制设置 _collection 为实际使用的集合名称（覆盖 AOT 生成器设置的值）
        builder.Set("_id", id);
        foreach (var kvp in doc.Entries)
        {
            if (string.Equals(kvp.Key, "_id", StringComparison.Ordinal) ||
                string.Equals(kvp.Key, "_collection", StringComparison.Ordinal))
            {
                continue;
            }

            builder.Set(kvp.Key, kvp.Value);
        }

        builder.Set("_collection", col);

        return builder.Build();
    }

    private PreparedInsertPayload PrepareSerializedInsertPayload(string col, BsonDocument doc, out BsonValue id)
    {
        var prepared = PrepareDocumentForInsert(col, doc, out id);
        return PreparedInsertPayload.Create(prepared, id);
    }

    private List<PreparedInsertPayload> PrepareSerializedInsertPayloads(
        string col,
        IReadOnlyList<BsonDocument> docs,
        List<Exception> exceptions,
        CancellationToken cancellationToken = default)
    {
        var prepared = new List<PreparedInsertPayload>(docs.Count);
        for (int docIndex = 0; docIndex < docs.Count; docIndex++)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                exceptions.Add(new OperationCanceledException(cancellationToken));
                break;
            }

            var d = docs[docIndex];
            if (d == null) continue;

            PreparedInsertPayload? payload = null;
            try
            {
                payload = PrepareSerializedInsertPayload(col, d, out _);
                _metadataManager.ValidateDocumentForWrite(col, payload.Document, _options.SchemaValidationMode);
                prepared.Add(payload);
                payload = null;
            }
            catch (Exception ex)
            {
                exceptions.Add(ex);
            }
            finally
            {
                payload?.Dispose();
            }
        }

        return prepared;
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

    private static bool BsonValuesEqual(BsonValue? left, BsonValue? right) => BsonValueComparer.ValueEquals(left, right);

    private static bool IsCommittedDocumentMatch(string collectionName, BsonValue id, BsonDocument document)
    {
        return document.TryGetValue("_id", out var documentId) &&
               BsonValuesEqual(documentId, id) &&
               (!document.TryGetValue("_collection", out var collectionValue) ||
                collectionValue.ToString() == collectionName);
    }

    private void ReadCommittedPageLookups(
        string collectionName,
        Page page,
        List<(BsonValue Id, int Ordinal, DocumentLocation Location)> lookups,
        BsonDocument?[] results)
    {
        if (page.PageType != PageType.Data || page.Header.ItemCount == 0)
        {
            return;
        }

        if (ShouldReadCommittedLookupsSparse(lookups.Count, page.Header.ItemCount))
        {
            foreach (var lookup in lookups)
            {
                if (lookup.Location.EntryIndex >= page.Header.ItemCount)
                {
                    continue;
                }

                var entry = _dataPageAccess.ReadDocumentAt(page, lookup.Location.EntryIndex);
                if (entry == null)
                {
                    continue;
                }

                var document = entry.Value.Document;
                if (IsCommittedDocumentMatch(collectionName, lookup.Id, document))
                {
                    results[lookup.Ordinal] = document;
                }
            }

            return;
        }

        var entries = _dataPageAccess.ReadDocumentsFromPage(page);
        foreach (var lookup in lookups)
        {
            if (lookup.Location.EntryIndex >= entries.Count)
            {
                continue;
            }

            var document = entries[lookup.Location.EntryIndex].Document;
            if (IsCommittedDocumentMatch(collectionName, lookup.Id, document))
            {
                results[lookup.Ordinal] = document;
            }
        }
    }

    private static bool ShouldReadCommittedLookupsSparse(int lookupCount, int itemCount)
    {
        return lookupCount * 4 <= itemCount;
    }

    private BsonDocument? FindByIdFullScan(string col, BsonValue id, CollectionState st)
    {
        RecordFindByIdFullScan();

        var idPredicate = new[]
        {
            new ScanPredicate(
                Encoding.UTF8.GetBytes("_id"),
                Encoding.UTF8.GetBytes("id"),
                Encoding.UTF8.GetBytes("Id"),
                id,
                ExpressionType.Equal)
        };

        foreach (var result in ReadRawScanResultSnapshot(col, st, idPredicate))
        {
            var doc = DeserializeDocumentOrThrow(result.Slice);
            if (doc.TryGetValue("_collection", out var c) && c.ToString() != col)
            {
                continue;
            }

            doc = ResolveLargeDocument(doc);
            if (doc.TryGetValue("_id", out var documentId) && BsonValuesEqual(documentId, id))
            {
                RecordFindByIdFullScanHit(col, id);
                return doc;
            }
        }

        return null;
    }

    private void RecordFindByIdFullScan()
    {
        Interlocked.Increment(ref _findByIdFullScanCount);
    }

    private void RecordFindByIdFullScanHit(string col, BsonValue id)
    {
        var count = Interlocked.Increment(ref _findByIdFullScanHitCount);
        if (count == 1 || IsPowerOfTwo(count))
        {
            Log(
                TinyDbLogLevel.Warning,
                $"Primary key index miss in collection '{col}' for id '{id}'. Full-scan fallback found the document. HitCount={count}.");
        }
    }

    private void RecordFindByIdStaleIndexHit(string col, BsonValue id)
    {
        var count = Interlocked.Increment(ref _findByIdStaleIndexHitCount);
        if (count == 1 || IsPowerOfTwo(count))
        {
            Log(
                TinyDbLogLevel.Warning,
                $"Primary key index stale hit in collection '{col}' for id '{id}'. Falling back to full scan. StaleHitCount={count}.");
        }
    }

    private static bool IsPowerOfTwo(long value) => (value & (value - 1)) == 0;

    private bool TryResolveDocumentLocation(string col, CollectionState st, BsonValue id, out Page p, out List<PageDocumentEntry> e, out ushort i)
    {
        p = null!; e = null!; i = 0;
        if (!st.Index.TryGet(id, out var loc)) return false;
        p = _pageManager.GetPagePinned(loc.PageId);
        try
        {
            e = _dataPageAccess.ReadDocumentsFromPage(p);
            if (loc.EntryIndex < e.Count && IsCommittedDocumentMatch(col, id, e[loc.EntryIndex].Document))
            {
                i = loc.EntryIndex;
                return true;
            }
        }
        catch
        {
            p.Unpin();
            throw;
        }

        p.Unpin();
        p = null!;
        e = null!;
        return false;
    }

    private void BuildDocumentLocationCache(string col, CollectionState st)
    {
        st.Index.Clear();
        st.OwnedPages.Clear();
        uint total = _pageManager.TotalPages;
        for (uint pId = 1; pId <= total; pId++)
        {
            Page p;
            try
            {
                p = _pageManager.GetPage(pId);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(
                    $"Failed to rebuild collection '{col}' from page {pId}.", ex);
            }
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

    private void EnsureWriteDurability()
    {
        if (_writeAheadLog.IsInTransactionScope) return;
        _flushScheduler.EnsureDurability(_options.WriteConcern);
    }

    private Task EnsureWriteDurabilityAsync(CancellationToken cancellationToken = default) 
    { 
        if (_writeAheadLog.IsInTransactionScope) return Task.CompletedTask;
        return _flushScheduler.EnsureDurabilityAsync(_options.WriteConcern, cancellationToken); 
    }

    private IEnumerable<BsonDocument> MergeTransactionOperations(string col, IEnumerable<BsonDocument> ds, Transaction tx)
    {
        var dict = new Dictionary<BsonValue, BsonDocument?>(BsonValueComparer.EqualityComparer);

        foreach (var document in ds)
        {
            if (document.TryGetValue("_id", out var id) && id != null && !id.IsNull)
            {
                dict[id] = document;
            }
        }

        foreach (var op in tx.GetOperationsSnapshot().Where(o => o.CollectionName == col))
        {
            if (op.DocumentId == null || op.DocumentId.IsNull) continue;

            if (op.OperationType == TransactionOperationType.Delete)
            {
                dict[op.DocumentId] = null;
            }
            else if (op.NewDocument != null)
            {
                dict[op.DocumentId] = op.NewDocument;
            }
        }

        return dict.Values.Where(document => document != null).Select(document => document!);
    }

    private static bool TryGetTransactionDocument(Transaction tx, string collectionName, BsonValue id, out BsonDocument? document)
    {
        var operations = tx.GetOperationsSnapshot();
        return TryGetTransactionDocument(operations, collectionName, id, out document);
    }

    private static bool TryGetTransactionDocument(
        IReadOnlyList<TransactionOperation> operations,
        string collectionName,
        BsonValue id,
        out BsonDocument? document)
    {
        document = null;
        if (operations.Count == 0) return false;

        for (int i = operations.Count - 1; i >= 0; i--)
        {
            var op = operations[i];
            if (op.CollectionName != collectionName) continue;
            if (op.DocumentId == null || op.DocumentId.IsNull) continue;
            if (!BsonValuesEqual(op.DocumentId, id)) continue;

            if (op.OperationType == TransactionOperationType.Delete)
            {
                document = null;
                return true;
            }

            if (op.NewDocument != null)
            {
                document = op.NewDocument;
                return true;
            }

            document = null;
            return true;
        }

        return false;
    }

    internal void Log(TinyDbLogLevel level, string message, Exception? ex = null)
    {
        TinyDbLogging.SafeLog(_log, level, message, ex);
    }

    internal void MarkCorrupted(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);
        if (Interlocked.CompareExchange(ref _corruptionException, exception, null) == null)
        {
            _pageManager.MarkCorrupted(exception);
            _flushScheduler.MarkCorrupted(exception);
            Log(TinyDbLogLevel.Critical, "TinyDbEngine marked corrupted. Dispose and reopen the database to recover from WAL.", exception);
        }
    }

    private void DeleteLargeDocumentOrThrow(uint largeDocumentIndexPageId)
    {
        _largeDocumentStorage.DeleteLargeDocument(largeDocumentIndexPageId);
    }

    private void DisposeIndexManagers() { foreach (var m in _indexManagers.Values) m.Dispose(); _indexManagers.Clear(); }

    private static TimeSpan NormalizeInterval(TimeSpan i) => i == Timeout.InfiniteTimeSpan ? TimeSpan.Zero : i;

    private void EnsureInitialized() { if (!_isInitialized) throw new InvalidOperationException(); }

    private void ThrowIfDisposed()
    {
        if (Volatile.Read(ref _disposed) != 0) throw new ObjectDisposedException(nameof(TinyDbEngine));
        if (Volatile.Read(ref _corruptionException) is { } corruptionException)
        {
            throw new InvalidOperationException(
                "TinyDbEngine is corrupted after a failed compensation rollback. Dispose and reopen the database to recover from WAL.",
                corruptionException);
        }
    }

    private static string? GetCollectionNameFromEntityAttribute<T>() where T : class => typeof(T).GetCustomAttribute<EntityAttribute>()?.Name;

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 0)
        {
            Exception? flushException = null;
            Exception? metadataException = null;
            Exception? cleanupException = null;

            try
            {
                if (_isInitialized && Volatile.Read(ref _corruptionException) == null)
                {
                    _collectionMetaStore.SaveCollections(false);
                }
            }
            catch (Exception ex)
            {
                metadataException = new InvalidOperationException("Failed to save collection metadata during dispose.", ex);
            }

            try
            {
                if (Volatile.Read(ref _corruptionException) == null)
                {
                    FlushCore();
                }
            }
            catch (Exception ex)
            {
                flushException = ex;
            }

            try
            {
                foreach (var c in _collections.Values) c.Dispose();
                _collections.Clear();
                _collectionStates.Clear();
                _transactionManager.Dispose();
                DisposeIndexManagers();
            }
            catch (Exception ex)
            {
                cleanupException = new InvalidOperationException("Dispose cleanup failed.", ex);
            }
            finally
            {
                DisposeComponents();
                _isInitialized = false;
            }

            if (flushException != null)
            {
                var flushRelated = new List<Exception> { flushException };
                if (metadataException != null) flushRelated.Add(metadataException);
                if (cleanupException != null) flushRelated.Add(cleanupException);
                if (flushRelated.Count > 1)
                {
                    throw new AggregateException("One or more errors occurred during dispose.", flushRelated);
                }

                ExceptionDispatchInfo.Capture(flushException).Throw();
            }

            if (metadataException != null || cleanupException != null)
            {
                var disposeRelated = new List<Exception>();
                if (metadataException != null) disposeRelated.Add(metadataException);
                if (cleanupException != null) disposeRelated.Add(cleanupException);
                throw new AggregateException("One or more errors occurred during dispose.", disposeRelated);
            }
        }
    }
}


