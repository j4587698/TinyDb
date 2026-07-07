using TinyDb.Bson;
using TinyDb.Serialization;
using TinyDb.Storage;

namespace TinyDb.Index;

public sealed class IndexManager : IDisposable
{
    private readonly IndexCatalog _catalog = new();
    private readonly string _collectionName;
    private readonly PageManager _pm;
    private readonly Action<IReadOnlyList<PersistedIndexDefinition>, bool>? _persistDefinitions;
    private readonly SemaphoreSlim _mutationGate = new(1, 1);
    private readonly ReaderWriterLockSlim _rwLock = new();
    private readonly bool _ownsPageManager;
    private readonly string? _tempFilePath;
    private bool _disposed;

    /// <summary>
    /// 获取此管理器所属集合的名称。
    /// </summary>
    public string CollectionName => _collectionName;

    /// <summary>
    /// 获取管理的索引数量。
    /// </summary>
    public int IndexCount
    {
        get
        {
            _rwLock.EnterReadLock();
            try
            {
                return _catalog.Count;
            }
            finally
            {
                _rwLock.ExitReadLock();
            }
        }
    }

    /// <summary>
    /// 获取所有受管理索引的名称快照。
    /// </summary>
    public IEnumerable<string> IndexNames
    {
        get
        {
            _rwLock.EnterReadLock();
            try
            {
                return _catalog.Names;
            }
            finally
            {
                _rwLock.ExitReadLock();
            }
        }
    }

    /// <summary>
    /// 初始化 <see cref="IndexManager"/> 类的新实例。
    /// </summary>
    /// <param name="collectionName">集合名称。</param>
    /// <param name="pm">页面管理器。</param>
    public IndexManager(string collectionName, PageManager pm)
        : this(collectionName, pm, null, null)
    {
    }

    internal IndexManager(
        string collectionName,
        PageManager pm,
        IReadOnlyList<PersistedIndexDefinition>? persistedDefinitions,
        Action<IReadOnlyList<PersistedIndexDefinition>, bool>? persistDefinitions)
    {
        _collectionName = collectionName ?? throw new ArgumentNullException(nameof(collectionName));
        _pm = pm ?? throw new ArgumentNullException(nameof(pm));
        _persistDefinitions = persistDefinitions;
        _ownsPageManager = false;

        LoadPersistedIndexes(persistedDefinitions);
    }

    /// <summary>
    /// 使用临时存储初始化 <see cref="IndexManager"/> 类的新实例。
    /// </summary>
    /// <param name="collectionName">集合名称。</param>
    public IndexManager(string collectionName)
    {
        _collectionName = collectionName ?? throw new ArgumentNullException(nameof(collectionName));
        _tempFilePath = Path.Combine(Path.GetTempPath(), $"idx_mgr_{Guid.NewGuid():N}.db");
        var ds = new DiskStream(
            _tempFilePath,
            FileAccess.ReadWrite,
            FileShare.ReadWrite | FileShare.Delete,
            FileOptions.DeleteOnClose);

        _pm = new PageManager(ds);
        _persistDefinitions = null;
        _ownsPageManager = true;
    }

    /// <summary>
    /// 创建一个新索引。
    /// </summary>
    /// <param name="name">索引名称。</param>
    /// <param name="fields">要索引的字段。</param>
    /// <param name="unique">索引是否唯一。</param>
    /// <param name="sparse">索引是否跳过缺失字段的文档。</param>
    /// <returns>如果创建成功则为 true；如果已存在则为 false。</returns>
    public bool CreateIndex(string name, string[] fields, bool unique = false, bool sparse = false)
    {
        return CreateIndexCore(name, fields, unique, sparse, persistImmediately: true);
    }

    internal bool CreateIndexForBackfill(string name, string[] fields, bool unique = false, bool sparse = false)
    {
        return CreateIndexCore(name, fields, unique, sparse, persistImmediately: false);
    }

    internal void PersistCurrentDefinitions(bool forceFlush = false)
    {
        _rwLock.EnterReadLock();
        try
        {
            PersistDefinitions(forceFlush);
        }
        finally
        {
            _rwLock.ExitReadLock();
        }
    }

    /// <summary>
    /// 根据名称删除索引。
    /// </summary>
    /// <param name="name">要删除的索引名称。</param>
    /// <returns>如果删除成功则为 true；如果未找到则为 false。</returns>
    public bool DropIndex(string name)
    {
        if (string.IsNullOrEmpty(name)) throw new ArgumentException("Index name cannot be null or empty", nameof(name));

        _mutationGate.Wait();
        try
        {
            _rwLock.EnterWriteLock();
            try
            {
                if (!_catalog.Remove(name, out var index))
                {
                    return false;
                }

                index.DropStorage();
                index.Dispose();
                PersistDefinitions(forceFlush: false);
                return true;
            }
            finally
            {
                _rwLock.ExitWriteLock();
            }
        }
        finally
        {
            _mutationGate.Release();
        }
    }

    /// <summary>
    /// 根据名称检索索引。
    /// </summary>
    /// <param name="name">索引名称。</param>
    /// <returns>索引实例；如果未找到则为 null。</returns>
    public BTreeIndex? GetIndex(string name)
    {
        if (string.IsNullOrEmpty(name)) throw new ArgumentException("Index name cannot be null or empty", nameof(name));

        _rwLock.EnterReadLock();
        try
        {
            _catalog.TryGet(name, out var index);
            return index;
        }
        finally
        {
            _rwLock.ExitReadLock();
        }
    }

    /// <summary>
    /// 检查索引是否存在。
    /// </summary>
    /// <param name="name">索引名称。</param>
    /// <returns>如果存在则为 true。</returns>
    public bool IndexExists(string name)
    {
        if (string.IsNullOrEmpty(name)) throw new ArgumentException("Index name cannot be null or empty", nameof(name));

        _rwLock.EnterReadLock();
        try
        {
            return _catalog.Contains(name);
        }
        finally
        {
            _rwLock.ExitReadLock();
        }
    }

    /// <summary>
    /// 获取所有索引的统计信息。
    /// </summary>
    /// <returns>索引统计信息的集合。</returns>
    public IEnumerable<IndexStatistics> GetAllStatistics()
    {
        _rwLock.EnterReadLock();
        try
        {
            return _catalog.Indexes.Select(index => index.GetStatistics()).ToArray();
        }
        finally
        {
            _rwLock.ExitReadLock();
        }
    }

    internal IEnumerable<IndexStatistics> GetPlanningStatistics()
    {
        _rwLock.EnterReadLock();
        try
        {
            return _catalog.Indexes.Select(index => new IndexStatistics
            {
                Name = index.Name,
                Type = index.Type,
                Fields = index.Fields.ToArray(),
                IsUnique = index.IsUnique,
                IsSparse = index.IsSparse,
                NodeCount = 0,
                EntryCount = 0,
                AverageKeysPerNode = 0,
                TreeHeight = 0,
                MaxKeysPerNode = index.MaxKeys
            }).ToArray();
        }
        finally
        {
            _rwLock.ExitReadLock();
        }
    }

    /// <summary>
    /// 为一组查询字段找到最佳匹配索引。
    /// </summary>
    /// <param name="fields">查询字段。</param>
    /// <returns>最佳匹配索引，或 null。</returns>
    public BTreeIndex? GetBestIndex(string[] fields)
    {
        if (fields == null || fields.Length == 0) return null;

        _rwLock.EnterReadLock();
        try
        {
            return IndexSelectionPolicy.SelectBest(_catalog.Indexes, fields);
        }
        finally
        {
            _rwLock.ExitReadLock();
        }
    }

    /// <summary>
    /// 验证所有索引的完整性。
    /// </summary>
    public IndexValidationResult ValidateAllIndexes()
    {
        _rwLock.EnterReadLock();
        try
        {
            return _catalog.ValidateAll();
        }
        finally
        {
            _rwLock.ExitReadLock();
        }
    }

    /// <summary>
    /// 清空所有索引数据。
    /// </summary>
    public void ClearAllIndexes()
    {
        _mutationGate.Wait();
        try
        {
            _rwLock.EnterWriteLock();
            try
            {
                _catalog.ClearIndexes();
            }
            finally
            {
                _rwLock.ExitWriteLock();
            }
        }
        finally
        {
            _mutationGate.Release();
        }
    }

    /// <summary>
    /// 删除所有索引。
    /// </summary>
    public void DropAllIndexes()
    {
        IReadOnlyList<string> names;
        _rwLock.EnterReadLock();
        try
        {
            names = _catalog.Names;
        }
        finally
        {
            _rwLock.ExitReadLock();
        }

        foreach (var name in names)
        {
            DropIndex(name);
        }
    }

    /// <summary>
    /// 插入文档时更新所有索引。
    /// </summary>
    /// <param name="document">正在插入的文档。</param>
    /// <param name="documentId">文档 ID。</param>
    public void InsertDocument(BsonDocument document, BsonValue documentId)
    {
        _mutationGate.Wait();
        try
        {
            _rwLock.EnterReadLock();
            try
            {
                IndexMutationCoordinator.InsertDocument(_catalog.Indexes, document, documentId);
            }
            finally
            {
                _rwLock.ExitReadLock();
            }
        }
        finally
        {
            _mutationGate.Release();
        }
    }

    internal async Task InsertDocumentAsync(
        BsonDocument document,
        BsonValue documentId,
        CancellationToken cancellationToken = default)
    {
        BTreeIndex[] indexes;
        await _mutationGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            _rwLock.EnterReadLock();
            try
            {
                indexes = _catalog.Indexes.ToArray();
            }
            finally
            {
                _rwLock.ExitReadLock();
            }

            await IndexMutationCoordinator.InsertDocumentAsync(
                indexes,
                document,
                documentId,
                cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _mutationGate.Release();
        }
    }

    /// <summary>
    /// 使用现有文档重建指定索引。
    /// </summary>
    /// <param name="name">索引名称。</param>
    /// <param name="documents">用于回填的文档集合。</param>
    public void RebuildIndex(string name, IEnumerable<BsonDocument> documents)
    {
        if (string.IsNullOrEmpty(name)) throw new ArgumentException("Index name cannot be null or empty", nameof(name));
        if (documents == null) throw new ArgumentNullException(nameof(documents));

        _mutationGate.Wait();
        try
        {
            _rwLock.EnterWriteLock();
            try
            {
                if (!_catalog.TryGet(name, out var index))
                {
                    throw new InvalidOperationException($"Index '{name}' does not exist.");
                }

                index.Clear();
                IndexMutationCoordinator.RebuildIndex(index, documents);
            }
            finally
            {
                _rwLock.ExitWriteLock();
            }
        }
        finally
        {
            _mutationGate.Release();
        }
    }

    /// <summary>
    /// 更新文档时更新所有索引。
    /// </summary>
    /// <param name="oldDoc">原始文档。</param>
    /// <param name="newDoc">新文档。</param>
    /// <param name="id">文档 ID。</param>
    public void UpdateDocument(BsonDocument oldDoc, BsonDocument newDoc, BsonValue id)
    {
        _mutationGate.Wait();
        try
        {
            _rwLock.EnterReadLock();
            try
            {
                IndexMutationCoordinator.UpdateDocument(_catalog.Indexes, oldDoc, newDoc, id);
            }
            finally
            {
                _rwLock.ExitReadLock();
            }
        }
        finally
        {
            _mutationGate.Release();
        }
    }

    internal async Task UpdateDocumentAsync(
        BsonDocument oldDoc,
        BsonDocument newDoc,
        BsonValue id,
        CancellationToken cancellationToken = default)
    {
        BTreeIndex[] indexes;
        await _mutationGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            _rwLock.EnterReadLock();
            try
            {
                indexes = _catalog.Indexes.ToArray();
            }
            finally
            {
                _rwLock.ExitReadLock();
            }

            await IndexMutationCoordinator.UpdateDocumentAsync(
                indexes,
                oldDoc,
                newDoc,
                id,
                cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _mutationGate.Release();
        }
    }

    /// <summary>
    /// 删除文档时更新所有索引。
    /// </summary>
    /// <param name="doc">正在删除的文档。</param>
    /// <param name="id">文档 ID。</param>
    public void DeleteDocument(BsonDocument doc, BsonValue id)
    {
        _mutationGate.Wait();
        try
        {
            _rwLock.EnterReadLock();
            try
            {
                IndexMutationCoordinator.DeleteDocument(_catalog.Indexes, doc, id);
            }
            finally
            {
                _rwLock.ExitReadLock();
            }
        }
        finally
        {
            _mutationGate.Release();
        }
    }

    internal async Task DeleteDocumentAsync(
        BsonDocument doc,
        BsonValue id,
        CancellationToken cancellationToken = default)
    {
        BTreeIndex[] indexes;
        await _mutationGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            _rwLock.EnterReadLock();
            try
            {
                indexes = _catalog.Indexes.ToArray();
            }
            finally
            {
                _rwLock.ExitReadLock();
            }

            await IndexMutationCoordinator.DeleteDocumentAsync(
                indexes,
                doc,
                id,
                cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _mutationGate.Release();
        }
    }

    /// <summary>
    /// 释放索引管理器和所有索引。
    /// </summary>
    public void Dispose()
    {
        if (_disposed) return;

        _mutationGate.Wait();
        _disposed = true;
        try
        {
            _catalog.Dispose();

            if (_ownsPageManager)
            {
                _pm.Dispose();
                if (!string.IsNullOrEmpty(_tempFilePath) && File.Exists(_tempFilePath))
                {
                    File.Delete(_tempFilePath);
                }
            }
        }
        finally
        {
            _mutationGate.Release();
        }

        _rwLock.Dispose();
        _mutationGate.Dispose();
    }

    private bool CreateIndexCore(string name, string[] fields, bool unique, bool sparse, bool persistImmediately)
    {
        if (string.IsNullOrEmpty(name)) throw new ArgumentException("Index name cannot be null or empty", nameof(name));
        if (fields == null || fields.Length == 0) throw new ArgumentException("Fields cannot be null or empty", nameof(fields));

        _mutationGate.Wait();
        try
        {
            _rwLock.EnterWriteLock();
            try
            {
                var normalizedFields = fields.Select(BsonFieldName.ToCamelCase).ToArray();
                if (_catalog.TryGet(name, out var existing))
                {
                    EnsureExistingIndexIsCompatible(name, existing, normalizedFields, unique, sparse);
                    return false;
                }

                var index = new BTreeIndex(_pm, name, normalizedFields, unique, 200, sparse);
                AddNewIndex(index);

                if (persistImmediately)
                {
                    PersistDefinitions(forceFlush: false);
                }

                return true;
            }
            finally
            {
                _rwLock.ExitWriteLock();
            }
        }
        finally
        {
            _mutationGate.Release();
        }
    }

    private void AddNewIndex(BTreeIndex index)
    {
        try
        {
            _catalog.Add(index);
        }
        catch
        {
            try
            {
                index.DropStorage();
            }
            finally
            {
                index.Dispose();
            }

            throw;
        }
    }

    private void LoadPersistedIndexes(IReadOnlyList<PersistedIndexDefinition>? definitions)
    {
        if (definitions == null || definitions.Count == 0) return;

        foreach (var definition in definitions)
        {
            if (string.IsNullOrEmpty(definition.Name) ||
                definition.Fields == null ||
                definition.Fields.Length == 0 ||
                definition.RootPageId == 0)
            {
                continue;
            }

            var normalizedFields = definition.Fields.Select(BsonFieldName.ToCamelCase).ToArray();
            var index = new BTreeIndex(
                _pm,
                definition.Name,
                normalizedFields,
                definition.IsUnique,
                definition.RootPageId,
                definition.MaxKeys,
                definition.IsSparse);

            if (!_catalog.TryAddLoaded(index))
            {
                index.Dispose();
            }
        }
    }

    private void PersistDefinitions(bool forceFlush)
    {
        _persistDefinitions?.Invoke(_catalog.CreateDefinitionSnapshot(), forceFlush);
    }

    private static void EnsureExistingIndexIsCompatible(
        string name,
        BTreeIndex existing,
        string[] normalizedFields,
        bool unique,
        bool sparse)
    {
        if (existing.IsUnique != unique)
        {
            throw new InvalidOperationException($"Index '{name}' already exists with different uniqueness (existing: {existing.IsUnique}, new: {unique})");
        }

        if (existing.IsSparse != sparse)
        {
            throw new InvalidOperationException($"Index '{name}' already exists with different sparse setting (existing: {existing.IsSparse}, new: {sparse})");
        }

        if (!normalizedFields.SequenceEqual(existing.Fields))
        {
            throw new InvalidOperationException($"Index '{name}' already exists with different fields (existing: {string.Join(",", existing.Fields)}, new: {string.Join(",", normalizedFields)})");
        }
    }
}
