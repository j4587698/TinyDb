using System;
using System.Buffers.Binary;
using System.Collections.Concurrent;
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

namespace TinyDb.Core;

/// <summary>
/// SimpleDb 数据库引擎
/// </summary>
public sealed class TinyDbEngine : IDisposable
{
    private readonly string _filePath;
    private readonly TinyDbOptions _options;
    private readonly DiskStream _diskStream;
    private readonly PageManager _pageManager;
    private readonly WriteAheadLog _writeAheadLog;
    private readonly FlushScheduler _flushScheduler;
    private readonly ConcurrentDictionary<string, IDocumentCollection> _collections;
    private readonly ConcurrentDictionary<string, IndexManager> _indexManagers;
    private readonly TransactionManager _transactionManager;
    private readonly ConcurrentDictionary<string, CollectionState> _collectionStates;
    private readonly LargeDocumentStorage _largeDocumentStorage;
    private readonly object _lock = new();
    private readonly AsyncLocal<Transaction?> _currentTransaction = new();
    private DatabaseHeader _header;
    private bool _disposed;
    private bool _isInitialized;

    private const int DocumentLengthPrefixSize = sizeof(int);
    private const int MinimumFreeSpaceThreshold = DocumentLengthPrefixSize + 64;

    private sealed class CollectionState
    {
        private int _cacheInitialized;

        public DataPageState PageState { get; } = new();
        public ConcurrentDictionary<string, DocumentLocation> DocumentLocations { get; } = new(StringComparer.Ordinal);
        public object CacheLock { get; } = new();

        public bool IsCacheInitialized => Volatile.Read(ref _cacheInitialized) == 1;

        public void MarkCacheInitialized() => Volatile.Write(ref _cacheInitialized, 1);
    }

    private sealed class DataPageState
    {
        public uint PageId;
        public readonly object SyncRoot = new();
    }

    private readonly struct DocumentLocation
    {
        public DocumentLocation(uint pageId, ushort entryIndex)
        {
            PageId = pageId;
            EntryIndex = entryIndex;
        }

        public uint PageId { get; }
        public ushort EntryIndex { get; }
    }

    private readonly struct PageDocumentEntry
    {
        public PageDocumentEntry(BsonDocument document, byte[] rawBytes, bool isLargeDocument = false, uint largeDocumentIndexPageId = 0, int largeDocumentSize = 0)
        {
            Document = document;
            RawBytes = rawBytes;
            IsLargeDocument = isLargeDocument;
            LargeDocumentIndexPageId = largeDocumentIndexPageId;
            LargeDocumentSize = largeDocumentSize;
        }

        public BsonDocument Document { get; }
        public byte[] RawBytes { get; }
        public bool IsLargeDocument { get; }
        public uint LargeDocumentIndexPageId { get; }
        public int LargeDocumentSize { get; }

        public BsonValue Id => Document.TryGetValue("_id", out var id) ? id : BsonNull.Value;
    }

    /// <summary>
    /// 文件路径
    /// </summary>
    public string FilePath => _filePath;

    /// <summary>
    /// 数据库选项
    /// </summary>
    public TinyDbOptions Options => _options;

    /// <summary>
    /// 数据库头部信息
    /// </summary>
    public DatabaseHeader Header => _header;

    /// <summary>
    /// 是否已初始化
    /// </summary>
    public bool IsInitialized => _isInitialized;

    /// <summary>
    /// 集合数量
    /// </summary>
    public int CollectionCount => _collections.Keys.Count(name => !name.StartsWith("__", StringComparison.Ordinal));

    /// <summary>
    /// 初始化 SimpleDb 数据库引擎
    /// </summary>
    /// <param name="filePath">数据库文件路径</param>
    /// <param name="options">数据库选项</param>
    public TinyDbEngine(string filePath, TinyDbOptions? options = null)
    {
        _filePath = Path.GetFullPath(filePath ?? throw new ArgumentNullException(nameof(filePath)));
        _options = options?.Clone() ?? new TinyDbOptions();
        _options.Validate();

        _diskStream = new DiskStream(_filePath);
        _pageManager = new PageManager(_diskStream, _options.PageSize, _options.CacheSize);
        if (!_options.EnableJournaling)
        {
            var walPath = Path.ChangeExtension(_filePath, ".wal");
            var walHasPendingEntries = false;
            try
            {
                var walInfo = new FileInfo(walPath);
                walHasPendingEntries = walInfo.Exists && walInfo.Length > 0;
            }
            catch
            {
                walHasPendingEntries = false;
            }

            if (walHasPendingEntries)
            {
                using var recoveryWal = new WriteAheadLog(_filePath, (int)_options.PageSize, enabled: true);
                try
                {
                    RecoverFromWriteAheadLog(recoveryWal);
                    recoveryWal.TruncateAsync().GetAwaiter().GetResult();
                }
                catch
                {
                    // Ignore recovery issues when attempting to drain legacy WAL
                }
            }
        }

        _writeAheadLog = new WriteAheadLog(_filePath, (int)_options.PageSize, _options.EnableJournaling);
        _flushScheduler = new FlushScheduler(
            _pageManager,
            _writeAheadLog,
            NormalizeInterval(_options.BackgroundFlushInterval),
            NormalizeInterval(_options.JournalFlushDelay));
        _collections = new ConcurrentDictionary<string, IDocumentCollection>();
        _indexManagers = new ConcurrentDictionary<string, IndexManager>();
        _collectionStates = new ConcurrentDictionary<string, CollectionState>();
        _transactionManager = new TransactionManager(this, _options.MaxTransactions, _options.TransactionTimeout);
        _largeDocumentStorage = new LargeDocumentStorage(_pageManager, (int)_options.PageSize);

        RecoverFromWriteAheadLog(_writeAheadLog);
        InitializeDatabase();
    }

    private static TimeSpan NormalizeInterval(TimeSpan interval)
    {
        if (interval == System.Threading.Timeout.InfiniteTimeSpan)
        {
            return TimeSpan.Zero;
        }

        return interval;
    }

    /// <summary>
    /// 验证并初始化数据库级安全配置
    /// </summary>
    private void EnsureDatabaseSecurity()
    {
        var password = _options.Password;
        var isSecure = DatabaseSecurity.IsDatabaseSecure(this);

        if (string.IsNullOrEmpty(password))
        {
            if (isSecure)
            {
                throw new UnauthorizedAccessException("数据库受密码保护，请提供正确密码");
            }

            return;
        }

        if (password.Length < 4)
        {
            throw new ArgumentException("密码长度至少4位", nameof(TinyDbOptions.Password));
        }

        if (isSecure)
        {
            if (!DatabaseSecurity.AuthenticateDatabase(this, password))
            {
                throw new UnauthorizedAccessException("数据库密码验证失败，无法访问数据库");
            }

            return;
        }

        DatabaseSecurity.CreateSecureDatabase(this, password);
    }

    /// <summary>
    /// 初始化数据库
    /// </summary>
    private void InitializeDatabase()
    {
        lock (_lock)
        {
            if (_isInitialized) return;

            try
            {
                // 读取或创建数据库头部
                if (_diskStream.Size == 0)
                {
                    // 新数据库，创建头部
                    _header = new DatabaseHeader();
                    _header.Initialize(_options.PageSize, _options.DatabaseName, _options.EnableJournaling);
                    if (_options.UserData?.Length > 0)
                    {
                        _header.UserData = _options.UserData;
                    }
                    // 注意：这里不立即写入头部，等待系统页面初始化完成后统一写入
                }
                else
                {
                    // 现有数据库，读取头部
                    ReadHeader();

                    // 验证头部
                    if (!_header.IsValid())
                    {
                        // 输出详细的头部信息用于调试
                        var calculatedChecksum = _header.CalculateChecksum();
                        throw new InvalidOperationException($"Invalid database header in file '{_filePath}'");
                    }

                    // 验证版本兼容性
                    if (_header.DatabaseVersion < DatabaseHeader.Version)
                    {
                        throw new NotSupportedException($"Database version {_header.DatabaseVersion:X8} is not supported");
                    }
                }

                // 同步日志模式设置到头部，用于后续写入
                _header.EnableJournaling = _options.EnableJournaling;

                // 初始化系统页面
                InitializeSystemPages();

                // 加载集合信息
                LoadCollections();

                _isInitialized = true;

                EnsureDatabaseSecurity();
            }
            catch (Exception)
            {
                _isInitialized = false;

                // 如果初始化失败，清理资源
                Dispose();
                throw;
            }
        }
    }

    private void RecoverFromWriteAheadLog(WriteAheadLog wal)
    {
        if (!wal.IsEnabled) return;

        try
        {
            wal.ReplayAsync((pageId, data) =>
            {
                _pageManager.RestorePage(pageId, data);
                return Task.CompletedTask;
            }).GetAwaiter().GetResult();
        }
        catch
        {
        }
    }

    /// <summary>
    /// 读取数据库头部
    /// </summary>
    private void ReadHeader()
    {
        var headerPage = _pageManager.GetPage(1); // 头部始终在页面1
        var headerData = headerPage.ReadData(0, DatabaseHeader.Size);
        _header = DatabaseHeader.FromByteArray(headerData);

        if (!_header.VerifyChecksum())
        {
            throw new InvalidOperationException("Database header checksum verification failed");
        }
    }

    /// <summary>
    /// 写入数据库头部
    /// </summary>
    private void WriteHeader()
    {
        // 同步PageManager的实际总页数到数据库头部
        var actualTotalPages = _pageManager.TotalPages;
        if (actualTotalPages != _header.TotalPages)
        {
            _header.TotalPages = actualTotalPages;
        }

        // 同步已用页数（基于PageManager的统计）
        var stats = _pageManager.GetStatistics();
        if (stats.UsedPages != _header.UsedPages)
        {
            _header.UsedPages = stats.UsedPages;
        }

        _header.UpdateModification();
        var headerData = _header.ToByteArray();

        var headerPage = _pageManager.GetPage(1);
        headerPage.UpdatePageType(PageType.Header);
        headerPage.WriteData(0, headerData);
        headerPage.UpdateStats((ushort)(headerPage.DataSize - headerData.Length), 1);

        _pageManager.SavePage(headerPage, forceFlush: _options.WriteConcern == WriteConcern.Synced);
    }

    /// <summary>
    /// 初始化系统页面
    /// </summary>
    private void InitializeSystemPages()
    {
        bool isNewDatabase = _header.CollectionInfoPage == 0 && _header.IndexInfoPage == 0;

        // 如果是新数据库，创建系统页面
        if (_header.CollectionInfoPage == 0)
        {
            _header.CollectionInfoPage = AllocateSystemPage(PageType.Collection, "CollectionInfo");
        }

        if (_header.IndexInfoPage == 0)
        {
            _header.IndexInfoPage = AllocateSystemPage(PageType.Index, "IndexInfo");
        }

        if (_header.EnableJournaling && _header.JournalInfoPage == 0)
        {
            _header.JournalInfoPage = AllocateSystemPage(PageType.Journal, "JournalInfo");
        }

        // 只在新数据库创建系统页面后写入头部，确保校验和正确
        if (isNewDatabase)
        {
            WriteHeader();
        }
    }

    /// <summary>
    /// 分配系统页面
    /// </summary>
    /// <param name="pageType">页面类型</param>
    /// <param name="name">页面名称</param>
    /// <returns>页面ID</returns>
    private uint AllocateSystemPage(PageType pageType, string name)
    {
        var page = _pageManager.NewPage(pageType);
        page.ClearData();

        // 写入系统页面标识
        var identifier = System.Text.Encoding.UTF8.GetBytes(name);
        var paddedIdentifier = new byte[32];
        Array.Copy(identifier, paddedIdentifier, Math.Min(identifier.Length, 32));

        page.WriteData(0, paddedIdentifier);
        page.UpdateStats((ushort)(page.DataSize - paddedIdentifier.Length), 1);

        _pageManager.SavePage(page, forceFlush: _options.WriteConcern == WriteConcern.Synced);

        return page.PageID;
    }

    /// <summary>
    /// 加载集合信息
    /// </summary>
    private void LoadCollections()
    {
        if (_header.CollectionInfoPage == 0) return;

        try
        {
            var collectionPage = _pageManager.GetPage(_header.CollectionInfoPage);
            var collectionData = collectionPage.ReadData(32, collectionPage.DataSize - 32); // 跳过标识符

            if (collectionData.Length >= 5)
            {
                var declaredLength = BitConverter.ToInt32(collectionData, 0);

                if (declaredLength > 0 &&
                    declaredLength <= collectionData.Length &&
                    collectionData[declaredLength - 1] == 0)
                {
                    var documentBytes = collectionData.AsSpan(0, declaredLength).ToArray();
                    var document = BsonSerializer.DeserializeDocument(documentBytes);

                    foreach (var kvp in document)
                    {
                        if (kvp.Value is BsonString collectionName)
                        {
                            // 创建集合实例但不立即加载
                            _collections.TryAdd(collectionName.Value, null!);
                        }
                    }
                }
            }
        }
        catch (Exception ex) when (ex is EndOfStreamException or FormatException or InvalidOperationException or OverflowException)
        {
            // AOT 模式下可能读取到未初始化的系统页面，忽略这些无效数据以保持向后兼容
        }
        catch
        {
            // 如果加载集合信息失败，记录警告但继续
        }
    }

    /// <summary>
    /// 保存集合信息
    /// </summary>
    private void SaveCollections()
    {
        if (_header.CollectionInfoPage == 0) return;

        var collectionInfo = new BsonDocument();
        foreach (var collectionName in _collections.Keys)
        {
            collectionInfo = collectionInfo.Set(collectionName, collectionName);
        }

        var collectionData = BsonSerializer.SerializeDocument(collectionInfo);

        var collectionPage = _pageManager.GetPage(_header.CollectionInfoPage);
        collectionPage.WriteData(32, collectionData);
        collectionPage.UpdateStats((ushort)(collectionPage.DataSize - 32 - collectionData.Length), 1);

        _pageManager.SavePage(collectionPage, forceFlush: _options.WriteConcern == WriteConcern.Synced);
    }

    /// <summary>
    /// 获取（或懒加载创建）文档集合
    /// </summary>
    /// <typeparam name="T">实体类型</typeparam>
    /// <returns>集合实例</returns>
    public ILiteCollection<T> GetCollection<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>() where T : class, new()
    {
        ThrowIfDisposed();
        EnsureInitialized();

        // 优先检查Entity特性，然后使用类型名称
        var collectionName = GetCollectionNameFromEntityAttribute<T>() ?? typeof(T).Name;

        return (ILiteCollection<T>)_collections.GetOrAdd(collectionName, _ =>
        {
            var collection = new DocumentCollection<T>(this, collectionName);
            RegisterCollection(collectionName);
            return collection;
        });
    }

    /// <summary>
    /// 获取指定名称的文档集合（仅用于特殊场景，如元数据管理）
    /// </summary>
    /// <typeparam name="T">实体类型</typeparam>
    /// <param name="collectionName">集合名称</param>
    /// <returns>集合实例</returns>
    public ILiteCollection<T> GetCollectionWithName<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(string collectionName) where T : class, new()
    {
        ThrowIfDisposed();
        EnsureInitialized();

        return (ILiteCollection<T>)_collections.GetOrAdd(collectionName, _ =>
        {
            var collection = new DocumentCollection<T>(this, collectionName);
            RegisterCollection(collectionName);
            return collection;
        });
    }

    
    /// <summary>
    /// 从Entity特性获取集合名称
    /// </summary>
    private static string? GetCollectionNameFromEntityAttribute<T>() where T : class
    {
        var entityAttribute = typeof(T).GetCustomAttribute<Attributes.EntityAttribute>();
        return entityAttribute?.CollectionName;
    }

    /// <summary>
    /// 开始事务
    /// </summary>
    /// <returns>事务实例</returns>
    public ITransaction BeginTransaction()
    {
        ThrowIfDisposed();
        EnsureInitialized();
        var transaction = _transactionManager.BeginTransaction();

        // 设置当前事务上下文
        if (transaction is Transaction concreteTransaction)
        {
            _currentTransaction.Value = concreteTransaction;
        }

        return transaction;
    }

    /// <summary>
    /// 获取当前活动事务
    /// </summary>
    /// <returns>当前事务，如果没有则返回null</returns>
    internal Transaction? GetCurrentTransaction()
    {
        return _currentTransaction.Value;
    }

    /// <summary>
    /// 清除当前事务上下文
    /// </summary>
    internal void ClearCurrentTransaction()
    {
        _currentTransaction.Value = null;
    }

    /// <summary>
    /// 获取事务管理器统计信息
    /// </summary>
    /// <returns>事务管理器统计信息</returns>
    public TransactionManagerStatistics GetTransactionStatistics()
    {
        ThrowIfDisposed();
        return _transactionManager.GetStatistics();
    }

    internal bool TryGetSecurityMetadata(out DatabaseSecurityMetadata metadata)
    {
        return _header.TryGetSecurityMetadata(out metadata);
    }

    internal void SetSecurityMetadata(DatabaseSecurityMetadata metadata)
    {
        lock (_lock)
        {
            _header.SetSecurityMetadata(metadata);
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

    
    /// <summary>
    /// 注册集合
    /// </summary>
    /// <param name="collectionName">集合名称</param>
    private void RegisterCollection(string collectionName)
    {
        SaveCollections();
    }

    /// <summary>
    /// 删除集合
    /// </summary>
    /// <param name="collectionName">集合名称</param>
    /// <returns>是否删除成功</returns>
    public bool DropCollection(string collectionName)
    {
        ThrowIfDisposed();
        EnsureInitialized();

        if (_collections.TryRemove(collectionName, out var collection))
        {
            if (collection is IDisposable disposable)
            {
                disposable.Dispose();
            }

            SaveCollections();
            _collectionStates.TryRemove(collectionName, out _);
            return true;
        }

        return false;
    }

    /// <summary>
    /// 获取所有集合名称
    /// </summary>
    /// <returns>集合名称列表</returns>
    public IEnumerable<string> GetCollectionNames()
    {
        ThrowIfDisposed();
        EnsureInitialized();

        return _collections.Keys.ToList();
    }

    /// <summary>
    /// 检查集合是否存在
    /// </summary>
    /// <param name="collectionName">集合名称</param>
    /// <returns>是否存在</returns>
    public bool CollectionExists(string collectionName)
    {
        ThrowIfDisposed();
        EnsureInitialized();

        return _collections.ContainsKey(collectionName);
    }

    private static int GetEntrySize(int documentLength) => DocumentLengthPrefixSize + documentLength;

    private int GetMaxDocumentSize()
    {
        var usable = (int)Math.Min(_pageManager.PageSize - PageHeader.Size - DocumentLengthPrefixSize, int.MaxValue);
        return Math.Max(usable, 0);
    }

    private void InitializeDataPage(Page page)
    {
        var freeBytes = (ushort)Math.Min(page.DataSize, ushort.MaxValue);
        page.UpdateStats(freeBytes, 0);
    }

    private (Page page, bool isNewPage) GetWritableDataPageLocked(string collectionName, DataPageState state, int requiredSize)
    {
        if (requiredSize > GetMaxDocumentSize())
        {
            throw new InvalidOperationException($"Document is too large: {requiredSize - DocumentLengthPrefixSize} bytes");
        }

        if (state.PageId != 0)
        {
            var existingPage = _pageManager.GetPage(state.PageId);
            if (existingPage.Header.FreeBytes >= requiredSize)
            {
                return (existingPage, false);
            }

            state.PageId = 0;
        }

        var page = _pageManager.NewPage(PageType.Data);
        InitializeDataPage(page);
        state.PageId = page.PageID;
        return (page, true);
    }

    private static int GetUsedBytes(Page page) => page.DataSize - page.Header.FreeBytes;

    private void PersistPage(Page page)
    {
        if (page == null) throw new ArgumentNullException(nameof(page));

        if (_writeAheadLog.IsEnabled)
        {
            _writeAheadLog.AppendPage(page);
        }
    }

    private void EnsureWriteDurability()
    {
        var concern = _options.WriteConcern;
        if (concern == WriteConcern.None)
        {
            return;
        }

        _flushScheduler.EnsureDurabilityAsync(concern).GetAwaiter().GetResult();
    }

    private BsonDocument PrepareDocumentForInsert(string collectionName, BsonDocument document, out BsonValue id)
    {
        if (!document.TryGetValue("_id", out var existingId) || existingId is null || existingId.IsNull)
        {
            id = ObjectId.NewObjectId();
            document = document.Set("_id", id);
        }
        else
        {
            id = existingId;
        }

        if (!DocumentBelongsToCollection(document, collectionName))
        {
            document = document.Set("_collection", collectionName);
        }

        return document;
    }

    private CollectionState GetCollectionState(string collectionName)
    {
        if (string.IsNullOrWhiteSpace(collectionName))
        {
            throw new ArgumentException("Collection name cannot be null or whitespace", nameof(collectionName));
        }

        var state = _collectionStates.GetOrAdd(collectionName, _ => new CollectionState());

        if (!state.IsCacheInitialized)
        {
            lock (state.CacheLock)
            {
                if (!state.IsCacheInitialized)
                {
                    BuildDocumentLocationCache(collectionName, state);
                    state.MarkCacheInitialized();
                }
            }
        }

        return state;
    }

    private void BuildDocumentLocationCache(string collectionName, CollectionState state)
    {
        state.DocumentLocations.Clear();
        uint candidatePageId = 0;

        var totalPages = _pageManager.TotalPages;
        for (uint pageId = 1; pageId <= totalPages; pageId++)
        {
            Page page;
            try
            {
                page = _pageManager.GetPage(pageId);
            }
            catch
            {
                continue;
            }

            if (page.PageType != PageType.Data || page.Header.ItemCount == 0)
            {
                continue;
            }

            var entries = ReadDocumentsFromPage(page);
            var hasCollectionDocument = false;

            for (ushort entryIndex = 0; entryIndex < entries.Count; entryIndex++)
            {
                var entry = entries[entryIndex];
                if (!DocumentBelongsToCollection(entry.Document, collectionName))
                {
                    continue;
                }

                if (!TryGetDocumentKey(entry.Id, out var key))
                {
                    continue;
                }

                state.DocumentLocations[key] = new DocumentLocation(page.PageID, entryIndex);
                hasCollectionDocument = true;
            }

            if (hasCollectionDocument && page.Header.FreeBytes >= MinimumFreeSpaceThreshold)
            {
                candidatePageId = page.PageID;
            }
        }

        state.PageState.PageId = candidatePageId;
    }

    private static bool TryGetDocumentKey(BsonValue id, out string key)
    {
        key = string.Empty;

        if (id == null || id.IsNull)
        {
            return false;
        }

        var value = id.ToString();
        if (string.IsNullOrEmpty(value))
        {
            return false;
        }

        key = value;
        return true;
    }

    private static void RemoveCachedDocumentsInPage(CollectionState state, uint pageId)
    {
        foreach (var kvp in state.DocumentLocations)
        {
            if (kvp.Value.PageId == pageId)
            {
                state.DocumentLocations.TryRemove(kvp.Key, out _);
            }
        }
    }

    private bool TryResolveDocumentLocation(string collectionName, CollectionState state, BsonValue id, string documentKey, out Page page, out List<PageDocumentEntry> entries, out ushort entryIndex)
    {
        page = default!;
        entries = default!;
        entryIndex = 0;

        if (!state.DocumentLocations.TryGetValue(documentKey, out var location))
        {
            return false;
        }

        var pageState = state.PageState;
        lock (pageState.SyncRoot)
        {
            try
            {
                page = _pageManager.GetPage(location.PageId);
            }
            catch
            {
                state.DocumentLocations.TryRemove(documentKey, out _);
                return false;
            }

            if (page.PageType != PageType.Data || page.Header.ItemCount == 0)
            {
                state.DocumentLocations.TryRemove(documentKey, out _);
                return false;
            }

            entries = ReadDocumentsFromPage(page);
            if (location.EntryIndex >= entries.Count)
            {
                state.DocumentLocations.TryRemove(documentKey, out _);
                return false;
            }

            var entry = entries[location.EntryIndex];
            if (!DocumentBelongsToCollection(entry.Document, collectionName) || !entry.Id.Equals(id))
            {
                state.DocumentLocations.TryRemove(documentKey, out _);
                return false;
            }

            entryIndex = location.EntryIndex;
            return true;
        }
    }

    private bool TryEnsureDocumentLocation(string collectionName, CollectionState state, BsonValue id, out Page page, out List<PageDocumentEntry> entries, out ushort entryIndex)
    {
        page = default!;
        entries = default!;
        entryIndex = 0;

        if (!TryGetDocumentKey(id, out var documentKey))
        {
            return false;
        }

        if (TryResolveDocumentLocation(collectionName, state, id, documentKey, out page, out entries, out entryIndex))
        {
            return true;
        }

        var document = FindByIdFullScan(collectionName, id, state);
        if (document == null)
        {
            return false;
        }

        return TryResolveDocumentLocation(collectionName, state, id, documentKey, out page, out entries, out entryIndex);
    }

    private bool TryReadDocumentFromLocation(string collectionName, CollectionState state, BsonValue id, out BsonDocument? document)
    {
        document = null;

        if (!TryGetDocumentKey(id, out var documentKey))
        {
            return false;
        }

        if (!TryResolveDocumentLocation(collectionName, state, id, documentKey, out var page, out var entries, out var entryIndex))
        {
            return false;
        }

        document = entries[entryIndex].Document;
        return true;
    }

    private BsonValue InsertPreparedDocument(string collectionName, BsonDocument document, BsonValue id, CollectionState collectionState, IndexManager? indexManager, bool updateIndexes)
    {
        var documentBytes = BsonSerializer.SerializeDocument(document);

        // 检查是否需要大文档存储
        if (LargeDocumentStorage.RequiresLargeDocumentStorage(documentBytes.Length, GetMaxDocumentSize()))
        {
            // 使用大文档存储
            return InsertLargeDocument(collectionName, document, documentBytes, id, collectionState, indexManager, updateIndexes);
        }
        else
        {
            // 使用常规单页面存储
            return InsertRegularDocument(collectionName, document, documentBytes, id, collectionState, indexManager, updateIndexes);
        }
    }

    private static BsonDocument CreateLargeDocumentIndexDocument(BsonValue id, string collectionName, uint indexPageId, int size)
    {
        var indexDocument = new BsonDocument();
        indexDocument = indexDocument.Set("_id", id);
        indexDocument = indexDocument.Set("_collection", BsonConversion.ToBsonValue(collectionName));
        indexDocument = indexDocument.Set("_largeDocumentIndex", BsonConversion.ToBsonValue((long)indexPageId));
        indexDocument = indexDocument.Set("_largeDocumentSize", BsonConversion.ToBsonValue(size));
        indexDocument = indexDocument.Set("_isLargeDocument", BsonConversion.ToBsonValue(true));
        return indexDocument;
    }

    private bool TryMaterializeLargeDocument(BsonDocument storedDocument, out BsonDocument materializedDocument, out uint indexPageId, out int largeDocumentSize)
    {
        materializedDocument = storedDocument;
        indexPageId = 0;
        largeDocumentSize = 0;

        if (!storedDocument.TryGetValue("_isLargeDocument", out var flagValue) ||
            flagValue is not BsonBoolean { Value: true })
        {
            return false;
        }

        if (!storedDocument.TryGetValue("_largeDocumentIndex", out var indexValue) || indexValue.IsNull)
        {
            throw new InvalidOperationException("Large document metadata is missing '_largeDocumentIndex'.");
        }

        indexPageId = indexValue switch
        {
            BsonInt32 int32 => unchecked((uint)int32.Value),
            BsonInt64 int64 => unchecked((uint)int64.Value),
            BsonDouble dbl => unchecked((uint)dbl.Value),
            _ => (uint)indexValue.ToUInt32(CultureInfo.InvariantCulture)
        };

        if (storedDocument.TryGetValue("_largeDocumentSize", out var sizeValue) && !sizeValue.IsNull)
        {
            largeDocumentSize = sizeValue switch
            {
                BsonInt32 int32 => int32.Value,
                BsonInt64 int64 => (int)int64.Value,
                BsonDouble dbl => System.Convert.ToInt32(dbl.Value, CultureInfo.InvariantCulture),
                _ => sizeValue.ToInt32(CultureInfo.InvariantCulture)
            };
        }

        var largeDocumentBytes = _largeDocumentStorage.ReadLargeDocument(indexPageId);
        materializedDocument = BsonSerializer.DeserializeDocument(largeDocumentBytes);
        return true;
    }

    /// <summary>
    /// 插入大文档（跨页面存储）
    /// </summary>
    private BsonValue InsertLargeDocument(string collectionName, BsonDocument document, byte[] documentBytes, BsonValue id, CollectionState collectionState, IndexManager? indexManager, bool updateIndexes)
    {
        return InsertLargeDocumentCore(collectionName, document, documentBytes, id, collectionState, indexManager, updateIndexes, persistPage: true, updateHeader: true, writeHeader: true);
    }

    private BsonValue InsertLargeDocumentCore(string collectionName, BsonDocument document, byte[] documentBytes, BsonValue id, CollectionState collectionState, IndexManager? indexManager, bool updateIndexes, bool persistPage, bool updateHeader, bool writeHeader)
    {
        try
        {
            var largeDocumentIndexPageId = _largeDocumentStorage.StoreLargeDocument(documentBytes, collectionName);
            var indexDocument = CreateLargeDocumentIndexDocument(id, collectionName, largeDocumentIndexPageId, documentBytes.Length);
            var indexDocumentBytes = BsonSerializer.SerializeDocument(indexDocument);
            var requiredSize = GetEntrySize(indexDocumentBytes.Length);

            var state = collectionState.PageState;
            bool isNewPage;
            uint pageId;
            ushort entryIndex;

            lock (state.SyncRoot)
            {
                Page page;
                (page, isNewPage) = GetWritableDataPageLocked(collectionName, state, requiredSize);
                entryIndex = page.Header.ItemCount;
                AppendDocumentToPage(page, indexDocumentBytes);
                pageId = page.PageID;

                if (page.Header.FreeBytes < MinimumFreeSpaceThreshold)
                {
                    state.PageId = 0;
                }
                else
                {
                    state.PageId = page.PageID;
                }

                if (persistPage)
                {
                    PersistPage(page);
                }
            }

            if (TryGetDocumentKey(id, out var documentKey))
            {
                collectionState.DocumentLocations[documentKey] = new DocumentLocation(pageId, entryIndex);
            }

            if (isNewPage && updateHeader)
            {
                lock (_lock)
                {
                    _header.UsedPages++;
                    if (writeHeader)
                    {
                        WriteHeader();
                    }
                }
            }

            if (updateIndexes && indexManager != null)
            {
                try
                {
                    indexManager.InsertDocument(document, id);
                }
                catch
                {
                }
            }

            return id;
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"Failed to insert large document {id}: {ex.Message}", ex);
        }
    }

    /// <summary>
    /// 插入常规文档（单页面存储）
    /// </summary>
    private BsonValue InsertRegularDocument(string collectionName, BsonDocument document, byte[] documentBytes, BsonValue id, CollectionState collectionState, IndexManager? indexManager, bool updateIndexes)
    {
        return InsertRegularDocumentCore(collectionName, document, documentBytes, id, collectionState, indexManager, updateIndexes, persistPage: true, updateHeader: true, writeHeader: true);
    }

    private BsonValue InsertRegularDocumentCore(string collectionName, BsonDocument document, byte[] documentBytes, BsonValue id, CollectionState collectionState, IndexManager? indexManager, bool updateIndexes, bool persistPage, bool updateHeader, bool writeHeader)
    {
        var requiredSize = GetEntrySize(documentBytes.Length);

        var state = collectionState.PageState;
        bool isNewPage;
        uint pageId;
        ushort entryIndex;

        lock (state.SyncRoot)
        {
            Page page;
            (page, isNewPage) = GetWritableDataPageLocked(collectionName, state, requiredSize);
            entryIndex = page.Header.ItemCount;
            AppendDocumentToPage(page, documentBytes);
            pageId = page.PageID;

            if (page.Header.FreeBytes < MinimumFreeSpaceThreshold)
            {
                state.PageId = 0;
            }
            else
            {
                state.PageId = page.PageID;
            }

            if (persistPage)
            {
                PersistPage(page);
            }
        }

        if (TryGetDocumentKey(id, out var documentKey))
        {
            collectionState.DocumentLocations[documentKey] = new DocumentLocation(pageId, entryIndex);
        }

        if (isNewPage && updateHeader)
        {
            lock (_lock)
            {
                _header.UsedPages++;
                if (writeHeader)
                {
                    WriteHeader();
                }
            }
        }

        if (updateIndexes && indexManager != null)
        {
            try
            {
                indexManager.InsertDocument(document, id);
            }
            catch
            {
            }
        }

        return id;
    }

    private void AppendDocumentToPage(Page page, ReadOnlySpan<byte> documentBytes)
    {
        var requiredSize = GetEntrySize(documentBytes.Length);
        if (page.Header.FreeBytes < requiredSize)
        {
            throw new InvalidOperationException("Insufficient space in page to append document.");
        }

        var usedBytes = GetUsedBytes(page);

        Span<byte> lengthBuffer = stackalloc byte[DocumentLengthPrefixSize];
        BinaryPrimitives.WriteInt32LittleEndian(lengthBuffer, documentBytes.Length);
        page.WriteData(usedBytes, lengthBuffer);
        page.WriteData(usedBytes + DocumentLengthPrefixSize, documentBytes);

        var newFreeBytes = (ushort)(page.Header.FreeBytes - requiredSize);
        var newItemCount = (ushort)Math.Min(page.Header.ItemCount + 1, ushort.MaxValue);
        page.UpdateStats(newFreeBytes, newItemCount);
    }

    private List<PageDocumentEntry> ReadDocumentsFromPage(Page page)
    {
        var documents = new List<PageDocumentEntry>(Math.Max(page.Header.ItemCount, (ushort)1));
        var usedBytes = GetUsedBytes(page);
        var offset = 0;

        for (int i = 0; i < page.Header.ItemCount && offset < usedBytes; i++)
        {
            var lengthBytes = page.ReadData(offset, DocumentLengthPrefixSize);
            if (lengthBytes.Length < DocumentLengthPrefixSize)
            {
                break;
            }

            var documentLength = BinaryPrimitives.ReadInt32LittleEndian(lengthBytes);
            offset += DocumentLengthPrefixSize;

            if (documentLength <= 0 || offset + documentLength > usedBytes + DocumentLengthPrefixSize)
            {
                break;
            }

            var documentBytes = page.ReadData(offset, documentLength);
            if (documentBytes.Length != documentLength)
            {
                break;
            }

            offset += documentLength;

            var document = BsonSerializer.DeserializeDocument(documentBytes);

            if (TryMaterializeLargeDocument(document, out var materializedDocument, out var indexPageId, out var largeDocumentSize))
            {
                documents.Add(new PageDocumentEntry(materializedDocument, documentBytes, true, indexPageId, largeDocumentSize));
            }
            else
            {
                documents.Add(new PageDocumentEntry(document, documentBytes));
            }
        }

        return documents;
    }

    private void RewritePageWithDocuments(string collectionName, CollectionState state, Page page, List<PageDocumentEntry> documents)
    {
        RemoveCachedDocumentsInPage(state, page.PageID);

        page.ClearData();
        page.UpdatePageType(PageType.Data);
        InitializeDataPage(page);

        ushort entryIndex = 0;
        foreach (var entry in documents)
        {
            AppendDocumentToPage(page, entry.RawBytes);

            if (DocumentBelongsToCollection(entry.Document, collectionName) && TryGetDocumentKey(entry.Id, out var key))
            {
                state.DocumentLocations[key] = new DocumentLocation(page.PageID, entryIndex);
            }

            entryIndex++;
        }

        if (page.Header.FreeBytes >= MinimumFreeSpaceThreshold)
        {
            state.PageState.PageId = page.PageID;
        }
        else if (state.PageState.PageId == page.PageID)
        {
            state.PageState.PageId = 0;
        }

        PersistPage(page);
    }

    private static bool DocumentBelongsToCollection(BsonDocument document, string collectionName)
    {
        if (document.TryGetValue("_collection", out var value) && value is BsonString collection)
        {
            return string.Equals(collection.Value, collectionName, StringComparison.Ordinal);
        }

        // 兼容旧数据：没有 _collection 字段时视为当前集合的文档
        return true;
    }

    /// <summary>
    /// 获取集合的文档缓存数量（基于 DocumentLocations）
    /// </summary>
    /// <param name="collectionName">集合名称</param>
    public int GetCachedDocumentCount(string collectionName)
    {
        var state = GetCollectionState(collectionName);
        return state.DocumentLocations.Count;
    }

    /// <summary>
    /// 插入文档到指定集合
    /// </summary>
    /// <param name="collectionName">集合名称</param>
    /// <param name="document">文档</param>
    /// <returns>文档ID</returns>
    internal BsonValue InsertDocument(string collectionName, BsonDocument document)
    {
        ThrowIfDisposed();
        EnsureInitialized();

        var preparedDocument = PrepareDocumentForInsert(collectionName, document, out var id);
        var indexManager = GetIndexManager(collectionName);
        var collectionState = GetCollectionState(collectionName);
        var result = InsertPreparedDocument(collectionName, preparedDocument, id, collectionState, indexManager, updateIndexes: true);
        EnsureWriteDurability();
        return result;
    }

    /// <summary>
    /// 根据ID查找文档
    /// </summary>
    /// <param name="collectionName">集合名称</param>
    /// <param name="id">文档ID</param>
    /// <returns>文档</returns>
    internal BsonDocument? FindById(string collectionName, BsonValue id)
    {
        ThrowIfDisposed();
        EnsureInitialized();

        if (id == null || id.IsNull)
        {
            return null;
        }

        var collectionState = GetCollectionState(collectionName);

        if (TryReadDocumentFromLocation(collectionName, collectionState, id, out var cachedDocument))
        {
            return cachedDocument;
        }

        return FindByIdFullScan(collectionName, id, collectionState);
    }

    private BsonDocument? FindByIdFullScan(string collectionName, BsonValue id, CollectionState state)
    {
        var totalPages = _pageManager.TotalPages;
        var candidatePageId = state.PageState.PageId;

        var pageState = state.PageState;
        lock (pageState.SyncRoot)
        {
            for (uint pageId = 1; pageId <= totalPages; pageId++)
            {
                Page page;
                try
                {
                    page = _pageManager.GetPage(pageId);
                }
                catch
                {
                    continue;
                }

                if (page.PageType != PageType.Data || page.Header.ItemCount == 0)
                {
                    continue;
                }

                var entries = ReadDocumentsFromPage(page);
                var hasCollectionDocument = false;
                for (ushort entryIndex = 0; entryIndex < entries.Count; entryIndex++)
                {
                    var entry = entries[entryIndex];
                    if (!DocumentBelongsToCollection(entry.Document, collectionName))
                    {
                        continue;
                    }

                    hasCollectionDocument = true;

                    if (TryGetDocumentKey(entry.Id, out var key))
                    {
                        state.DocumentLocations[key] = new DocumentLocation(page.PageID, entryIndex);
                    }

                    if (entry.Id.Equals(id))
                    {
                        if (page.Header.FreeBytes >= MinimumFreeSpaceThreshold)
                        {
                            candidatePageId = page.PageID;
                        }

                        state.PageState.PageId = candidatePageId;
                        return entry.Document;
                    }
                }

                if (hasCollectionDocument && page.Header.FreeBytes >= MinimumFreeSpaceThreshold)
                {
                    candidatePageId = page.PageID;
                }
            }
        }

        state.PageState.PageId = candidatePageId;
        return null;
    }

    /// <summary>
    /// 查找集合中的所有文档
    /// </summary>
    /// <param name="collectionName">集合名称</param>
    /// <returns>文档列表</returns>
    internal IEnumerable<BsonDocument> FindAll(string collectionName)
    {
        ThrowIfDisposed();
        EnsureInitialized();

        var collectionState = GetCollectionState(collectionName);
        var snapshot = collectionState.DocumentLocations.ToArray();

        // 获取基础文档（已提交的数据）
        IEnumerable<BsonDocument> baseDocuments;
        if (snapshot.Length == 0)
        {
            baseDocuments = FindAllByScanning(collectionName, collectionState);
        }
        else
        {
            var documents = FetchDocumentsByLocations(collectionName, collectionState, snapshot);
            if (documents.Count < snapshot.Length)
            {
                baseDocuments = FindAllByScanning(collectionName, collectionState);
            }
            else
            {
                baseDocuments = documents;
            }
        }

        // 检查是否有活动事务，如果有则合并事务中的临时操作
        var currentTransaction = GetCurrentTransaction();
        if (currentTransaction != null && currentTransaction.State == TransactionState.Active)
        {
            return MergeTransactionOperations(collectionName, baseDocuments, currentTransaction);
        }

        return baseDocuments;
    }

    /// <summary>
    /// 合并事务操作到基础文档集合中
    /// </summary>
    /// <param name="collectionName">集合名称</param>
    /// <param name="baseDocuments">基础文档（已提交的数据）</param>
    /// <param name="transaction">当前事务</param>
    /// <returns>合并后的文档列表</returns>
    private IEnumerable<BsonDocument> MergeTransactionOperations(string collectionName, IEnumerable<BsonDocument> baseDocuments, Transaction transaction)
    {
        // 创建结果集，从基础文档开始
        var results = new Dictionary<string, BsonDocument>();

        // 添加所有基础文档
        foreach (var doc in baseDocuments)
        {
            if (doc.TryGetValue("_id", out var id) && id != null && !id.IsNull)
            {
                results[id.ToString()] = doc;
            }
        }

        // 应用事务操作
        foreach (var operation in transaction.Operations)
        {
            if (operation.CollectionName != collectionName)
                continue;

            var docId = operation.DocumentId?.ToString();
            if (docId == null)
                continue;

            switch (operation.OperationType)
            {
                case TransactionOperationType.Insert:
                    // 添加新插入的文档
                    if (operation.NewDocument != null)
                    {
                        results[docId] = operation.NewDocument;
                    }
                    break;

                case TransactionOperationType.Update:
                    // 更新现有文档
                    if (operation.NewDocument != null)
                    {
                        results[docId] = operation.NewDocument;
                    }
                    break;

                case TransactionOperationType.Delete:
                    // 删除文档
                    results.Remove(docId);
                    break;
            }
        }

        return results.Values;
    }

    private IEnumerable<BsonDocument> FindAllByScanning(string collectionName, CollectionState state)
    {
        BuildDocumentLocationCache(collectionName, state);

        var snapshot = state.DocumentLocations.ToArray();
        if (snapshot.Length == 0)
        {
            return Array.Empty<BsonDocument>();
        }

        return FetchDocumentsByLocations(collectionName, state, snapshot);
    }

    private List<BsonDocument> FetchDocumentsByLocations(string collectionName, CollectionState state, KeyValuePair<string, DocumentLocation>[] snapshot)
    {
        var documents = new List<BsonDocument>(snapshot.Length);
        var pageGroups = new Dictionary<uint, List<KeyValuePair<string, DocumentLocation>>>();

        foreach (var kvp in snapshot)
        {
            if (!pageGroups.TryGetValue(kvp.Value.PageId, out var list))
            {
                list = new List<KeyValuePair<string, DocumentLocation>>();
                pageGroups[kvp.Value.PageId] = list;
            }
            list.Add(kvp);
        }

        var pageState = state.PageState;
        lock (pageState.SyncRoot)
        {
            foreach (var group in pageGroups)
            {
                var pageId = group.Key;
                var locations = group.Value;

                Page page;
                try
                {
                    page = _pageManager.GetPage(pageId);
                }
                catch
                {
                    foreach (var location in locations)
                    {
                        state.DocumentLocations.TryRemove(location.Key, out _);
                    }
                    continue;
                }

                if (page.PageType != PageType.Data || page.Header.ItemCount == 0)
                {
                    foreach (var location in locations)
                    {
                        state.DocumentLocations.TryRemove(location.Key, out _);
                    }
                    continue;
                }

                var entries = ReadDocumentsFromPage(page);
                foreach (var location in locations)
                {
                    if (location.Value.EntryIndex >= entries.Count)
                    {
                        state.DocumentLocations.TryRemove(location.Key, out _);
                        continue;
                    }

                    var entry = entries[location.Value.EntryIndex];
                    if (!DocumentBelongsToCollection(entry.Document, collectionName))
                    {
                        state.DocumentLocations.TryRemove(location.Key, out _);
                        continue;
                    }

                    documents.Add(entry.Document);
                }
            }
        }

        return documents;
    }

    /// <summary>
    /// 更新文档
    /// </summary>
    /// <param name="collectionName">集合名称</param>
    /// <param name="document">更新的文档</param>
    /// <returns>更新的文档数量</returns>
    internal int UpdateDocument(string collectionName, BsonDocument document)
    {
        ThrowIfDisposed();
        EnsureInitialized();

        if (!document.TryGetValue("_id", out var id))
        {
            throw new ArgumentException("Document must have _id field for update", nameof(document));
        }

        if (!TryGetDocumentKey(id, out var documentKey))
        {
            throw new ArgumentException("Document must have a valid _id value for update", nameof(document));
        }

        var collectionState = GetCollectionState(collectionName);
        var state = collectionState.PageState;
        var indexManager = GetIndexManager(collectionName);

        if (!TryEnsureDocumentLocation(collectionName, collectionState, id, out _, out _, out _))
        {
            return 0;
        }

        BsonDocument? oldDocumentSnapshot = null;
        BsonDocument? newDocumentSnapshot = null;
        var updated = false;

        PageDocumentEntry oldEntry;
        uint? newLargeDocumentIndexPageId = null;
        var requiresLargeStorage = false;
        var rewriteCompleted = false;

        lock (state.SyncRoot)
        {
            if (!TryResolveDocumentLocation(collectionName, collectionState, id, documentKey, out var page, out var entries, out var entryIndex))
            {
                return 0;
            }

            oldEntry = entries[entryIndex];
            var updatedDocument = document;

            if (!DocumentBelongsToCollection(updatedDocument, collectionName))
            {
                updatedDocument = updatedDocument.Set("_collection", collectionName);
            }

            if (!updatedDocument.TryGetValue("_id", out var newId) || !newId.Equals(id))
            {
                updatedDocument = updatedDocument.Set("_id", id);
            }

            var newDocumentBytes = BsonSerializer.SerializeDocument(updatedDocument);
            requiresLargeStorage = LargeDocumentStorage.RequiresLargeDocumentStorage(newDocumentBytes.Length, GetMaxDocumentSize());

            try
            {
                if (requiresLargeStorage)
                {
                    newLargeDocumentIndexPageId = _largeDocumentStorage.StoreLargeDocument(newDocumentBytes, collectionName);
                    var indexDocument = CreateLargeDocumentIndexDocument(id, collectionName, newLargeDocumentIndexPageId.Value, newDocumentBytes.Length);
                    var indexDocumentBytes = BsonSerializer.SerializeDocument(indexDocument);
                    entries[entryIndex] = new PageDocumentEntry(updatedDocument, indexDocumentBytes, true, newLargeDocumentIndexPageId.Value, newDocumentBytes.Length);
                }
                else
                {
                    if (GetEntrySize(newDocumentBytes.Length) > page.DataSize)
                    {
                        throw new InvalidOperationException($"Document is too large: {newDocumentBytes.Length} bytes, max {page.DataSize - DocumentLengthPrefixSize} bytes");
                    }

                    entries[entryIndex] = new PageDocumentEntry(updatedDocument, newDocumentBytes);
                }

                RewritePageWithDocuments(collectionName, collectionState, page, entries);

                oldDocumentSnapshot = oldEntry.Document;
                newDocumentSnapshot = updatedDocument;
                updated = true;
                rewriteCompleted = true;
            }
            finally
            {
                if (!rewriteCompleted && newLargeDocumentIndexPageId.HasValue)
                {
                    try
                    {
                        _largeDocumentStorage.DeleteLargeDocument(newLargeDocumentIndexPageId.Value);
                    }
                    catch
                    {
                    }
                }
            }
        }

        if (updated && oldEntry.IsLargeDocument)
        {
            var shouldDeleteOld = !requiresLargeStorage || (newLargeDocumentIndexPageId.HasValue && newLargeDocumentIndexPageId.Value != oldEntry.LargeDocumentIndexPageId);
            if (shouldDeleteOld)
            {
                try
                {
                    _largeDocumentStorage.DeleteLargeDocument(oldEntry.LargeDocumentIndexPageId);
                }
                catch
                {
                }
            }
        }

        if (!updated)
        {
            return 0;
        }

        try
        {
            indexManager?.UpdateDocument(oldDocumentSnapshot!, newDocumentSnapshot!, id);
        }
        catch
        {
        }

        EnsureWriteDurability();

        return 1;
    }

    /// <summary>
    /// 删除文档
    /// </summary>
    /// <param name="collectionName">集合名称</param>
    /// <param name="id">文档ID</param>
    /// <returns>删除的文档数量</returns>
    internal int DeleteDocument(string collectionName, BsonValue id)
    {
        ThrowIfDisposed();
        EnsureInitialized();

        if (id == null || id.IsNull)
        {
            return 0;
        }

        if (!TryGetDocumentKey(id, out var documentKey))
        {
            return 0;
        }

        var collectionState = GetCollectionState(collectionName);
        var state = collectionState.PageState;
        var indexManager = GetIndexManager(collectionName);

        if (!TryEnsureDocumentLocation(collectionName, collectionState, id, out _, out _, out _))
        {
            return 0;
        }

        var deletedEntry = default(PageDocumentEntry?);

        lock (state.SyncRoot)
        {
            if (!TryResolveDocumentLocation(collectionName, collectionState, id, documentKey, out var page, out var entries, out var entryIndex))
            {
                return 0;
            }

            var entry = entries[entryIndex];

            entries.RemoveAt(entryIndex);
            collectionState.DocumentLocations.TryRemove(documentKey, out _);

            if (entries.Count == 0)
            {
                RewritePageWithDocuments(collectionName, collectionState, page, entries);

                if (state.PageId == page.PageID)
                {
                    state.PageId = 0;
                }

                _pageManager.FreePage(page.PageID);

                lock (_lock)
                {
                    _header.UsedPages--;
                    WriteHeader();
                }
            }
            else
            {
                RewritePageWithDocuments(collectionName, collectionState, page, entries);
            }

            deletedEntry = entry;
        }

        if (deletedEntry == null)
        {
            return 0;
        }

        if (deletedEntry.Value.IsLargeDocument)
        {
            try
            {
                _largeDocumentStorage.DeleteLargeDocument(deletedEntry.Value.LargeDocumentIndexPageId);
            }
            catch
            {
            }
        }

        try
        {
            indexManager?.DeleteDocument(deletedEntry.Value.Document, id);
        }
        catch
        {
        }

        EnsureWriteDurability();

        return 1;
    }

    /// <summary>
    /// 批量插入文档 - 优化版本
    /// </summary>
    /// <param name="collectionName">集合名称</param>
    /// <param name="documents">文档列表</param>
    /// <returns>插入的文档数量</returns>
    internal int InsertDocuments(string collectionName, BsonDocument[] documents)
    {
        ThrowIfDisposed();
        EnsureInitialized();

        if (documents == null) throw new ArgumentNullException(nameof(documents));
        if (documents.Length == 0) return 0;

        var indexManager = GetIndexManager(collectionName);
        var collectionState = GetCollectionState(collectionName);

        var insertedCount = 0;
        for (int i = 0; i < documents.Length; i++)
        {
            var document = documents[i];
            if (document == null) continue;

            try
            {
                var preparedDocument = PrepareDocumentForInsert(collectionName, document, out var id);
                documents[i] = preparedDocument; // propagate metadata
                InsertPreparedDocument(collectionName, preparedDocument, id, collectionState, indexManager, updateIndexes: true);
                insertedCount++;
            }
            catch
            {
            }
        }

        if (insertedCount > 0)
        {
            EnsureWriteDurability();
        }

        return insertedCount;
    }

    /// <summary>
    /// 检查是否已初始化
    /// </summary>
    private void EnsureInitialized()
    {
        if (!_isInitialized)
        {
            throw new InvalidOperationException("Database engine is not initialized");
        }
    }

    /// <summary>
    /// 检查是否已释放
    /// </summary>
    private void ThrowIfDisposed()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(TinyDbEngine));
    }

    /// <summary>
    /// 获取WAL是否启用
    /// </summary>
    /// <returns>WAL是否启用</returns>
    public bool GetWalEnabled()
    {
        ThrowIfDisposed();
        return _writeAheadLog.IsEnabled;
    }

    /// <summary>
    /// 获取数据库统计信息
    /// </summary>
    /// <returns>统计信息</returns>
    public DatabaseStatistics GetStatistics()
    {
        ThrowIfDisposed();
        EnsureInitialized();

        var pageStats = _pageManager.GetStatistics();
        var fileStats = _diskStream.GetStatistics();

        return new DatabaseStatistics
        {
            FilePath = _filePath,
            DatabaseName = _header.DatabaseName,
            Version = _header.DatabaseVersion,
            CreatedAt = new DateTime(_header.CreatedAt, DateTimeKind.Utc),
            ModifiedAt = new DateTime(_header.ModifiedAt, DateTimeKind.Utc),
            PageSize = _header.PageSize,
            TotalPages = _header.TotalPages,
            UsedPages = _header.UsedPages,
            FreePages = _header.TotalPages - _header.UsedPages,
            CollectionCount = _collections.Count,
            FileSize = fileStats.Size,
            CachedPages = pageStats.CachedPages,
            CacheHitRatio = pageStats.CacheHitRatio,
            IsReadOnly = _options.ReadOnly,
            EnableJournaling = _options.EnableJournaling
        };
    }

    /// <summary>
    /// 刷新所有缓存到磁盘
    /// </summary>
    public void Flush()
    {
        ThrowIfDisposed();
        if (!_isInitialized)
        {
            // 如果未初始化，只刷新页面但不保存头部
            _flushScheduler.FlushAsync().GetAwaiter().GetResult();
            return;
        }

        try
        {
            // 先刷新所有页面
            _flushScheduler.FlushAsync().GetAwaiter().GetResult();

            // 强制同步磁盘流，确保所有页面数据已完全写入
            _diskStream.Flush();

            // 然后保存数据库头部（包含最新的校验和）
            WriteHeader();

            // 再次强制同步，确保头部也已写入
            _diskStream.Flush();
        }
        catch (Exception)
        {
            // 即使出现异常也要尝试保存头部
            try
            {
                WriteHeader();
                _diskStream.Flush();
            }
            catch
            {
                // 如果保存头部也失败，记录错误但不抛出异常
            }
            throw; // 重新抛出原始异常
        }
    }

    /// <summary>
    /// 异步刷新所有缓存到磁盘
    /// </summary>
    /// <param name="cancellationToken">取消令牌</param>
    public async Task FlushAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        EnsureInitialized();

        // 首先刷新所有页面
        await _flushScheduler.FlushAsync(cancellationToken).ConfigureAwait(false);

        // 然后保存数据库头部（包含最新的校验和）
        WriteHeader();
    }

    /// <summary>
    /// 删除文档（内部使用，不持久化，用于事务回滚）
    /// </summary>
    /// <param name="collectionName">集合名称</param>
    /// <param name="id">文档ID</param>
    /// <returns>删除的文档数量</returns>
    internal int DeleteDocumentInternal(string collectionName, BsonValue id)
    {
        ThrowIfDisposed();
        EnsureInitialized();

        if (id == null || id.IsNull)
        {
            return 0;
        }

        if (!TryGetDocumentKey(id, out var documentKey))
        {
            return 0;
        }

        var collectionState = GetCollectionState(collectionName);
        var state = collectionState.PageState;
        var indexManager = GetIndexManager(collectionName);

        if (!TryEnsureDocumentLocation(collectionName, collectionState, id, out _, out _, out _))
        {
            return 0;
        }

        var deletedEntry = default(PageDocumentEntry?);

        lock (state.SyncRoot)
        {
            if (!TryResolveDocumentLocation(collectionName, collectionState, id, documentKey, out var page, out var entries, out var entryIndex))
            {
                return 0;
            }

            var entry = entries[entryIndex];

            entries.RemoveAt(entryIndex);
            collectionState.DocumentLocations.TryRemove(documentKey, out _);

            if (entries.Count == 0)
            {
                RewritePageWithDocuments(collectionName, collectionState, page, entries);

                if (state.PageId == page.PageID)
                {
                    state.PageId = 0;
                }

                _pageManager.FreePage(page.PageID);

                lock (_lock)
                {
                    _header.UsedPages--;
                    // 注意：这里不调用WriteHeader()，因为这是事务回滚操作
                }
            }
            else
            {
                RewritePageWithDocuments(collectionName, collectionState, page, entries);
            }

            deletedEntry = entry;
        }

        if (deletedEntry == null)
        {
            return 0;
        }

        if (deletedEntry.Value.IsLargeDocument)
        {
            try
            {
                _largeDocumentStorage.DeleteLargeDocument(deletedEntry.Value.LargeDocumentIndexPageId);
            }
            catch
            {
            }
        }

        try
        {
            indexManager?.DeleteDocument(deletedEntry.Value.Document, id);
        }
        catch
        {
        }

        // 注意：这里不调用EnsureWriteDurability()，因为这是事务回滚操作

        return 1;
    }

    /// <summary>
    /// 更新文档（内部使用，不持久化，用于事务回滚）
    /// </summary>
    /// <param name="collectionName">集合名称</param>
    /// <param name="document">更新的文档</param>
    /// <returns>更新的文档数量</returns>
    internal int UpdateDocumentInternal(string collectionName, BsonDocument document)
    {
        ThrowIfDisposed();
        EnsureInitialized();

        if (!document.TryGetValue("_id", out var id))
        {
            throw new ArgumentException("Document must have _id field for update", nameof(document));
        }

        if (!TryGetDocumentKey(id, out var documentKey))
        {
            throw new ArgumentException("Document must have a valid _id value for update", nameof(document));
        }

        var collectionState = GetCollectionState(collectionName);
        var state = collectionState.PageState;
        var indexManager = GetIndexManager(collectionName);

        if (!TryEnsureDocumentLocation(collectionName, collectionState, id, out _, out _, out _))
        {
            return 0;
        }

        BsonDocument? oldDocumentSnapshot = null;
        BsonDocument? newDocumentSnapshot = null;
        var updated = false;

        PageDocumentEntry oldEntry;
        uint? newLargeDocumentIndexPageId = null;
        var requiresLargeStorage = false;
        var rewriteCompleted = false;

        lock (state.SyncRoot)
        {
            if (!TryResolveDocumentLocation(collectionName, collectionState, id, documentKey, out var page, out var entries, out var entryIndex))
            {
                return 0;
            }

            oldEntry = entries[entryIndex];
            var updatedDocument = document;

            if (!DocumentBelongsToCollection(updatedDocument, collectionName))
            {
                updatedDocument = updatedDocument.Set("_collection", collectionName);
            }

            if (!updatedDocument.TryGetValue("_id", out var newId) || !newId.Equals(id))
            {
                updatedDocument = updatedDocument.Set("_id", id);
            }

            var newDocumentBytes = BsonSerializer.SerializeDocument(updatedDocument);
            requiresLargeStorage = LargeDocumentStorage.RequiresLargeDocumentStorage(newDocumentBytes.Length, GetMaxDocumentSize());

            try
            {
                if (requiresLargeStorage)
                {
                    newLargeDocumentIndexPageId = _largeDocumentStorage.StoreLargeDocument(newDocumentBytes, collectionName);
                    var indexDocument = CreateLargeDocumentIndexDocument(id, collectionName, newLargeDocumentIndexPageId.Value, newDocumentBytes.Length);
                    var indexDocumentBytes = BsonSerializer.SerializeDocument(indexDocument);
                    entries[entryIndex] = new PageDocumentEntry(updatedDocument, indexDocumentBytes, true, newLargeDocumentIndexPageId.Value, newDocumentBytes.Length);
                }
                else
                {
                    if (GetEntrySize(newDocumentBytes.Length) > page.DataSize)
                    {
                        throw new InvalidOperationException($"Document is too large: {newDocumentBytes.Length} bytes, max {page.DataSize - DocumentLengthPrefixSize} bytes");
                    }

                    entries[entryIndex] = new PageDocumentEntry(updatedDocument, newDocumentBytes);
                }

                RewritePageWithDocuments(collectionName, collectionState, page, entries);

                oldDocumentSnapshot = oldEntry.Document;
                newDocumentSnapshot = updatedDocument;
                updated = true;
                rewriteCompleted = true;
            }
            finally
            {
                if (!rewriteCompleted && newLargeDocumentIndexPageId.HasValue)
                {
                    try
                    {
                        _largeDocumentStorage.DeleteLargeDocument(newLargeDocumentIndexPageId.Value);
                    }
                    catch
                    {
                    }
                }
            }
        }

        if (updated && oldEntry.IsLargeDocument)
        {
            var shouldDeleteOld = !requiresLargeStorage || (newLargeDocumentIndexPageId.HasValue && newLargeDocumentIndexPageId.Value != oldEntry.LargeDocumentIndexPageId);
            if (shouldDeleteOld)
            {
                try
                {
                    _largeDocumentStorage.DeleteLargeDocument(oldEntry.LargeDocumentIndexPageId);
                }
                catch
                {
                }
            }
        }

        if (!updated)
        {
            return 0;
        }

        try
        {
            indexManager?.UpdateDocument(oldDocumentSnapshot!, newDocumentSnapshot!, id);
        }
        catch
        {
        }

        // 注意：这里不调用EnsureWriteDurability()，因为这是事务回滚操作

        return 1;
    }

    /// <summary>
    /// 插入文档到指定集合（内部使用，不持久化，用于事务回滚）
    /// </summary>
    /// <param name="collectionName">集合名称</param>
    /// <param name="document">文档</param>
    /// <returns>文档ID</returns>
    internal BsonValue InsertDocumentInternal(string collectionName, BsonDocument document)
    {
        ThrowIfDisposed();
        EnsureInitialized();

        var preparedDocument = PrepareDocumentForInsert(collectionName, document, out var id);
        var indexManager = GetIndexManager(collectionName);
        var collectionState = GetCollectionState(collectionName);
        var result = InsertPreparedDocumentInternal(collectionName, preparedDocument, id, collectionState, indexManager, updateIndexes: true);
        // 注意：这里不调用EnsureWriteDurability()，因为这是事务回滚操作
        return result;
    }

    /// <summary>
    /// 插入准备好的文档（内部使用，不持久化）
    /// </summary>
    private BsonValue InsertPreparedDocumentInternal(string collectionName, BsonDocument document, BsonValue id, CollectionState collectionState, IndexManager? indexManager, bool updateIndexes)
    {
        var documentBytes = BsonSerializer.SerializeDocument(document);

        if (LargeDocumentStorage.RequiresLargeDocumentStorage(documentBytes.Length, GetMaxDocumentSize()))
        {
            return InsertLargeDocumentCore(collectionName, document, documentBytes, id, collectionState, indexManager, updateIndexes, persistPage: true, updateHeader: true, writeHeader: false);
        }

        return InsertRegularDocumentCore(collectionName, document, documentBytes, id, collectionState, indexManager, updateIndexes, persistPage: true, updateHeader: true, writeHeader: false);
    }

    /// <summary>
    /// 释放资源
    /// </summary>
    public void Dispose()
    {
        if (!_disposed)
        {
            try
            {
                // 保存集合信息
                if (_isInitialized)
                {
                    SaveCollections();
                }

                // 刷新所有缓存
                Flush();

                _flushScheduler?.Dispose();
                _writeAheadLog?.Dispose();

                // 释放所有集合
                foreach (var collection in _collections.Values)
                {
                    if (collection is IDisposable disposable)
                    {
                        disposable.Dispose();
                    }
                }
                _collections.Clear();
                _collectionStates.Clear();

                // 释放事务管理器
                _transactionManager?.Dispose();

                // 释放索引管理器
                DisposeIndexManagers();

                // 释放页面管理器
                _pageManager?.Dispose();

                // 释放磁盘流
                _diskStream?.Dispose();
            }
            catch
            {
            }
            finally
            {
                _disposed = true;
                _isInitialized = false;
            }
        }
    }

    /// <summary>
    /// 确保索引存在
    /// </summary>
    /// <param name="collectionName">集合名称</param>
    /// <param name="fieldName">字段名称</param>
    /// <param name="indexName">索引名称</param>
    /// <param name="unique">是否唯一索引</param>
    /// <returns>是否创建或验证成功</returns>
    public bool EnsureIndex(string collectionName, string fieldName, string indexName, bool unique = false)
    {
        ThrowIfDisposed();

        if (string.IsNullOrEmpty(collectionName))
            throw new ArgumentException("Collection name cannot be null or empty", nameof(collectionName));
        if (string.IsNullOrEmpty(fieldName))
            throw new ArgumentException("Field name cannot be null or empty", nameof(fieldName));
        if (string.IsNullOrEmpty(indexName))
            throw new ArgumentException("Index name cannot be null or empty", nameof(indexName));

        var indexManager = _indexManagers.GetOrAdd(collectionName, name => new IndexManager(name));
        return indexManager.CreateIndex(indexName, new[] { fieldName }, unique);
    }

    /// <summary>
    /// 获取指定集合的索引管理器
    /// </summary>
    /// <param name="collectionName">集合名称</param>
    /// <returns>索引管理器</returns>
    public IndexManager GetIndexManager(string collectionName)
    {
        ThrowIfDisposed();
        if (string.IsNullOrEmpty(collectionName)) throw new ArgumentException("Collection name cannot be null or empty", nameof(collectionName));

        return _indexManagers.GetOrAdd(collectionName, name => new IndexManager(name));
    }

    /// <summary>
    /// 释放索引管理器资源
    /// </summary>
    private void DisposeIndexManagers()
    {
        foreach (var indexManager in _indexManagers.Values)
        {
            indexManager.Dispose();
        }
        _indexManagers.Clear();
    }

    }

/// <summary>
/// 数据库统计信息
/// </summary>
public sealed class DatabaseStatistics
{
    public string FilePath { get; init; } = string.Empty;
    public string DatabaseName { get; init; } = string.Empty;
    public uint Version { get; init; }
    public DateTime CreatedAt { get; init; }
    public DateTime ModifiedAt { get; init; }
    public uint PageSize { get; init; }
    public uint TotalPages { get; init; }
    public uint UsedPages { get; init; }
    public uint FreePages { get; init; }
    public int CollectionCount { get; init; }
    public long FileSize { get; init; }
    public int CachedPages { get; init; }
    public double CacheHitRatio { get; init; }
    public bool IsReadOnly { get; init; }
    public bool EnableJournaling { get; init; }

    public override string ToString()
    {
        return $"Database[{DatabaseName}]: {UsedPages}/{TotalPages} pages, {CollectionCount} collections, " +
               $"{FileSize:N0} bytes, HitRatio={CacheHitRatio:P1}";
    }
}
