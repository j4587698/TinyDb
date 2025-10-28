using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using SimpleDb.Collections;
using SimpleDb.Serialization;
using SimpleDb.Storage;
using SimpleDb.Bson;
using SimpleDb.Index;
using System.Runtime.CompilerServices;

namespace SimpleDb.Core;

/// <summary>
/// SimpleDb 数据库引擎
/// </summary>
public sealed class SimpleDbEngine : IDisposable
{
    private readonly string _filePath;
    private readonly SimpleDbOptions _options;
    private readonly DiskStream _diskStream;
    private readonly PageManager _pageManager;
    private readonly ConcurrentDictionary<string, IDocumentCollection> _collections;
    private readonly ConcurrentDictionary<string, IndexManager> _indexManagers;
    private readonly TransactionManager _transactionManager;
    private readonly object _lock = new();
    private DatabaseHeader _header;
    private bool _disposed;
    private bool _isInitialized;

    /// <summary>
    /// 文件路径
    /// </summary>
    public string FilePath => _filePath;

    /// <summary>
    /// 数据库选项
    /// </summary>
    public SimpleDbOptions Options => _options;

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
    public int CollectionCount => _collections.Count;

    /// <summary>
    /// 初始化 SimpleDb 数据库引擎
    /// </summary>
    /// <param name="filePath">数据库文件路径</param>
    /// <param name="options">数据库选项</param>
    public SimpleDbEngine(string filePath, SimpleDbOptions? options = null)
    {
        _filePath = Path.GetFullPath(filePath ?? throw new ArgumentNullException(nameof(filePath)));
        _options = options?.Clone() ?? new SimpleDbOptions();
        _options.Validate();

        _diskStream = new DiskStream(_filePath);
        _pageManager = new PageManager(_diskStream, _options.PageSize, _options.CacheSize);
        _collections = new ConcurrentDictionary<string, IDocumentCollection>();
        _indexManagers = new ConcurrentDictionary<string, IndexManager>();
        _transactionManager = new TransactionManager(this, _options.MaxTransactions, _options.TransactionTimeout);

        InitializeDatabase();
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
                    _header.Initialize(_options.PageSize, _options.DatabaseName);
                    if (_options.UserData?.Length > 0)
                    {
                        _header.UserData = _options.UserData;
                    }
                    WriteHeader();
                }
                else
                {
                    // 现有数据库，读取头部
                    ReadHeader();

                    // 验证头部
                    if (!_header.IsValid())
                    {
                        throw new InvalidOperationException($"Invalid database header in file '{_filePath}'");
                    }

                    // 验证版本兼容性
                    if (_header.DatabaseVersion < DatabaseHeader.Version)
                    {
                        throw new NotSupportedException($"Database version {_header.DatabaseVersion:X8} is not supported");
                    }
                }

                // 初始化系统页面
                InitializeSystemPages();

                // 加载集合信息
                LoadCollections();

                _isInitialized = true;
            }
            catch (Exception)
            {
                // 如果初始化失败，清理资源
                Dispose();
                throw;
            }
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
        _header.UpdateModification();
        var headerData = _header.ToByteArray();

        var headerPage = _pageManager.GetPage(1);
        headerPage.UpdatePageType(PageType.Header);
        headerPage.WriteData(0, headerData);
        headerPage.UpdateStats((ushort)(headerPage.DataSize - headerData.Length), 1);

        _pageManager.SavePage(headerPage, _options.SynchronousWrites);
    }

    /// <summary>
    /// 初始化系统页面
    /// </summary>
    private void InitializeSystemPages()
    {
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

        WriteHeader();
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

        _pageManager.SavePage(page, _options.SynchronousWrites);

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

            if (collectionData.Length > 0)
            {
                var document = BsonSerializer.DeserializeDocument(collectionData);
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
        catch (Exception ex)
        {
            // 如果加载集合信息失败，记录警告但继续
            Console.WriteLine($"Warning: Failed to load collection info: {ex.Message}");
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

        _pageManager.SavePage(collectionPage, _options.SynchronousWrites);
    }

    /// <summary>
    /// 获取文档集合
    /// </summary>
    /// <typeparam name="T">文档类型</typeparam>
    /// <param name="name">集合名称</param>
    /// <returns>文档集合</returns>
    public ILiteCollection<T> GetCollection<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(string? name = null) where T : class, new()
    {
        ThrowIfDisposed();
        EnsureInitialized();

        var collectionName = name ?? typeof(T).Name;

        return (ILiteCollection<T>)_collections.GetOrAdd(collectionName, _ =>
        {
            var collection = new DocumentCollection<T>(this, collectionName);
            RegisterCollection(collectionName);
            return collection;
        });
    }

    /// <summary>
    /// 开始事务
    /// </summary>
    /// <returns>事务实例</returns>
    public ITransaction BeginTransaction()
    {
        ThrowIfDisposed();
        EnsureInitialized();
        return _transactionManager.BeginTransaction();
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

        // 生成或获取文档ID
        if (!document.TryGetValue("_id", out var id) || id.IsNull)
        {
            id = ObjectId.NewObjectId();
            document = document.Set("_id", id);
        }

        // 添加集合名称字段
        document = document.Set("_collection", collectionName);

        // 序列化文档
        var documentData = BsonSerializer.SerializeDocument(document);

        // 分配数据页面并写入
        var page = _pageManager.NewPage(PageType.Data);
        var offset = 0;

        // 如果文档太大，需要分页存储（简化实现，实际需要更复杂的逻辑）
        if (documentData.Length > page.DataSize)
        {
            throw new InvalidOperationException($"Document is too large: {documentData.Length} bytes, max {page.DataSize} bytes");
        }

        page.WriteData(offset, documentData);
        page.UpdateStats((ushort)(page.DataSize - documentData.Length), 1);

        _pageManager.SavePage(page, _options.SynchronousWrites);

        // 更新统计信息
        lock (_lock)
        {
            _header.UsedPages++;
            WriteHeader();
        }

        // 更新索引 - 在插入文档后更新所有相关索引
        try
        {
            var indexManager = GetIndexManager(collectionName);
            indexManager?.InsertDocument(document, id);
        }
        catch (Exception ex)
        {
            // 索引更新失败不应影响文档插入，但需要记录警告
            Console.WriteLine($"Warning: Failed to update indexes for document {id}: {ex.Message}");
        }

        return id;
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

        // 简化实现：线性搜索所有数据页面
        // 实际实现应该使用索引进行快速查找
        var totalPages = _pageManager.TotalPages;

        for (uint pageId = 1; pageId <= totalPages; pageId++)
        {
            try
            {
                var page = _pageManager.GetPage(pageId);
                // 跳过非数据页面和空页面
                if (page.PageType != PageType.Data) continue;
                if (page.Header.ItemCount == 0) continue; // 空页面没有有效数据

                var dataSize = page.DataSize - page.Header.FreeBytes;
                if (dataSize <= 0) continue; // 没有数据

                var documentData = page.ReadData(0, dataSize);
                var document = BsonSerializer.DeserializeDocument(documentData);

                // 按集合名称和ID过滤
                if (document.TryGetValue("_collection", out var collectionValue) &&
                    collectionValue is BsonString collectionStr &&
                    collectionStr.Value == collectionName &&
                    document.TryGetValue("_id", out var docId) && docId.Equals(id))
                {
                    return document;
                }
            }
            catch (Exception)
            {
                // 忽略损坏的页面，继续搜索
                continue;
            }
        }

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

        var documents = new List<BsonDocument>();
        var totalPages = _pageManager.TotalPages;

        for (uint pageId = 1; pageId <= totalPages; pageId++)
        {
            try
            {
                var page = _pageManager.GetPage(pageId);
                // 跳过非数据页面和空页面
                if (page.PageType != PageType.Data) continue;
                if (page.Header.ItemCount == 0) continue; // 空页面没有有效数据

                var dataSize = page.DataSize - page.Header.FreeBytes;
                if (dataSize <= 0) continue; // 没有数据

                var documentData = page.ReadData(0, dataSize);
                var document = BsonSerializer.DeserializeDocument(documentData);

                // 按集合名称过滤
                if (document.TryGetValue("_collection", out var collectionValue) &&
                    collectionValue is BsonString collectionStr &&
                    collectionStr.Value == collectionName)
                {
                    documents.Add(document);
                }
            }
            catch (Exception)
            {
                // 忽略损坏的页面，继续搜索
                continue;
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

        // 查找现有文档并获取其页面位置
        var totalPages = _pageManager.TotalPages;
        uint targetPageId = 0;
        Page? targetPage = null;
        BsonDocument? oldDocument = null;

        for (uint pageId = 1; pageId <= totalPages; pageId++)
        {
            try
            {
                var page = _pageManager.GetPage(pageId);
                // 跳过非数据页面和空页面
                if (page.PageType != PageType.Data) continue;
                if (page.Header.ItemCount == 0) continue;

                var dataSize = page.DataSize - page.Header.FreeBytes;
                if (dataSize <= 0) continue; // 没有数据

                var documentData = page.ReadData(0, dataSize);
                var existingDocument = BsonSerializer.DeserializeDocument(documentData);

                // 按集合名称和ID过滤
                if (existingDocument.TryGetValue("_collection", out var collectionValue) &&
                    collectionValue is BsonString collectionStr &&
                    collectionStr.Value == collectionName &&
                    existingDocument.TryGetValue("_id", out var docId) && docId.Equals(id))
                {
                    targetPageId = pageId;
                    targetPage = page;
                    oldDocument = existingDocument; // 保存旧文档用于索引更新
                    break;
                }
            }
            catch (Exception)
            {
                // 忽略损坏的页面，继续搜索
                continue;
            }
        }

        if (targetPage == null || oldDocument == null) return 0; // 文档不存在

        // 确保文档包含集合名称字段 - 这是关键修复！
        if (!document.TryGetValue("_collection", out _) ||
            !(document["_collection"] is BsonString existingCollectionStr) ||
            existingCollectionStr.Value != collectionName)
        {
            document = document.Set("_collection", collectionName);
        }

        // 序列化新文档
        var newDocumentData = BsonSerializer.SerializeDocument(document);

        // 如果新文档太大，需要分页存储（简化实现，实际需要更复杂的逻辑）
        if (newDocumentData.Length > targetPage.DataSize)
        {
            throw new InvalidOperationException($"Document is too large: {newDocumentData.Length} bytes, max {targetPage.DataSize} bytes");
        }

        // 直接在原页面覆盖更新
        targetPage.WriteData(0, newDocumentData);
        targetPage.UpdateStats((ushort)(targetPage.DataSize - newDocumentData.Length), 1);
        targetPage.UpdateChecksum();

        // 强制刷新到磁盘
        _pageManager.SavePage(targetPage, forceFlush: true);

        // 更新索引 - 在更新文档后更新所有相关索引
        try
        {
            var indexManager = GetIndexManager(collectionName);
            indexManager?.UpdateDocument(oldDocument, document, id);
        }
        catch (Exception ex)
        {
            // 索引更新失败不应影响文档更新，但需要记录警告
            Console.WriteLine($"Warning: Failed to update indexes for document {id}: {ex.Message}");
        }

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

        // 查找并删除文档
        var totalPages = _pageManager.TotalPages;
        BsonDocument? documentToDelete = null;

        for (uint pageId = 1; pageId <= totalPages; pageId++)
        {
            try
            {
                var page = _pageManager.GetPage(pageId);
                if (page.PageType != PageType.Data) continue;

                var dataSize = page.DataSize - page.Header.FreeBytes;
                if (dataSize <= 0) continue;

                var documentData = page.ReadData(0, dataSize);
                var document = BsonSerializer.DeserializeDocument(documentData);

                if (document.TryGetValue("_id", out var docId) && docId.Equals(id))
                {
                    documentToDelete = document; // 保存要删除的文档用于索引更新

                    // 释放页面
                    _pageManager.FreePage(pageId);

                    // 更新统计信息
                    lock (_lock)
                    {
                        _header.UsedPages--;
                        WriteHeader();
                    }

                    // 更新索引 - 在删除文档后更新所有相关索引
                    try
                    {
                        var indexManager = GetIndexManager(collectionName);
                        indexManager?.DeleteDocument(document, id);
                    }
                    catch (Exception ex)
                    {
                        // 索引更新失败不应影响文档删除，但需要记录警告
                        Console.WriteLine($"Warning: Failed to update indexes for deleted document {id}: {ex.Message}");
                    }

                    return 1;
                }
            }
            catch (Exception)
            {
                // 忽略损坏的页面，继续搜索
                continue;
            }
        }

        return 0;
    }

    /// <summary>
    /// 插入多个文档
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

        var count = 0;
        foreach (var document in documents)
        {
            try
            {
                InsertDocument(collectionName, document);
                count++;
            }
            catch (Exception)
            {
                // 如果某个文档插入失败，记录但继续处理其他文档
                continue;
            }
        }

        return count;
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
            throw new ObjectDisposedException(nameof(SimpleDbEngine));
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
        EnsureInitialized();

        _pageManager.FlushDirtyPages();
        _diskStream.Flush();
    }

    /// <summary>
    /// 异步刷新所有缓存到磁盘
    /// </summary>
    /// <param name="cancellationToken">取消令牌</param>
    public async Task FlushAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        EnsureInitialized();

        await _pageManager.FlushDirtyPagesAsync(cancellationToken);
        await _diskStream.FlushAsync(cancellationToken);
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

                // 释放所有集合
                foreach (var collection in _collections.Values)
                {
                    if (collection is IDisposable disposable)
                    {
                        disposable.Dispose();
                    }
                }
                _collections.Clear();

                // 释放事务管理器
                _transactionManager?.Dispose();

                // 释放索引管理器
                DisposeIndexManagers();

                // 释放页面管理器
                _pageManager?.Dispose();

                // 释放磁盘流
                _diskStream?.Dispose();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error during SimpleDbEngine disposal: {ex.Message}");
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

    /// <summary>
    /// AOT 友好的泛型方法
    /// </summary>
    /// <typeparam name="T">文档类型</typeparam>
    /// <returns>文档集合</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ILiteCollection<T> GetCollectionAOT<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>() where T : class, new()
    {
        return GetCollection<T>();
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
