using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using TinyDb.Bson;
using TinyDb.Core;
using TinyDb.Serialization;
using TinyDb.Query;
using TinyDb.Index;
using TinyDb.Attributes;

namespace TinyDb.Collections;

/// <summary>
/// 文档集合实现
/// </summary>
/// <typeparam name="T">文档类型</typeparam>
public sealed class DocumentCollection<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T> : ITinyCollection<T>, IDocumentCollection where T : class, new()
{
    private readonly TinyDbEngine _engine;
    private readonly string _name;
    private readonly QueryExecutor _queryExecutor;
    private bool _disposed;

    /// <summary>
    /// 集合名称
    /// </summary>
    public string CollectionName => _name;

    /// <summary>
    /// 集合名称（IDocumentCollection 接口）
    /// </summary>
    string IDocumentCollection.Name => _name;

    /// <summary>
    /// 文档类型
    /// </summary>
    Type IDocumentCollection.DocumentType => typeof(T);

    /// <summary>
    /// 初始化文档集合
    /// </summary>
    /// <param name="engine">数据库引擎</param>
    /// <param name="name">集合名称</param>
    public DocumentCollection(TinyDbEngine engine, string name)
    {
        _engine = engine ?? throw new ArgumentNullException(nameof(engine));
        _name = name ?? throw new ArgumentNullException(nameof(name));
        _queryExecutor = new QueryExecutor(engine);

        // 自动扫描并创建基于属性的索引
        CreateAutoIndexes();
    }

    /// <summary>
    /// 创建自动索引
    /// </summary>
    private void CreateAutoIndexes()
    {
        IndexScanner.ScanAndCreateIndexes(_engine, typeof(T), _name);
    }

    /// <summary>
    /// 插入单个文档
    /// </summary>
    /// <param name="entity">要插入的实体</param>
    /// <returns>插入文档的ID</returns>
    public BsonValue Insert(T entity)
    {
        ThrowIfDisposed();
        if (entity == null) throw new ArgumentNullException(nameof(entity));

        // 确保实体有ID
        EnsureEntityHasId(entity);

        // 转换为BSON文档（AOT兼容）
        var document = AotBsonMapper.ToDocument(entity);

        // 如果文档没有ID（可能是因为它是不可变的BsonDocument），我们需要在这里生成一个新的文档带ID
        if (!document.ContainsKey("_id"))
        {
            var newId = ObjectId.NewObjectId();
            document = document.Set("_id", newId);
            // 尝试更新实体ID（如果是可变实体）
            UpdateEntityId(entity, newId);
        }

        // 检查是否在事务中
        var currentTransaction = _engine.GetCurrentTransaction();
        if (currentTransaction != null)
        {
            // 在事务中，记录操作而不是直接写入
            var documentId = ((Transaction)currentTransaction).RecordInsert(_name, document);

            // 更新实体的ID（如果需要）
            UpdateEntityId(entity, documentId);

            return documentId;
        }
        else
        {
            // 不在事务中，直接插入到数据库
            var id = _engine.InsertDocument(_name, document);

            // 更新实体的ID（如果需要）
            UpdateEntityId(entity, id);

            return id;
        }
    }

    /// <summary>
    /// 插入多个文档
    /// </summary>
    /// <param name="entities">要插入的实体集合</param>
    /// <returns>插入的文档数量</returns>
    public int Insert(IEnumerable<T> entities)
    {
        ThrowIfDisposed();
        if (entities == null) throw new ArgumentNullException(nameof(entities));

        int totalInserted = 0;
        const int BatchSize = 1000;
        var entityBatch = new List<T>(BatchSize);
        var docBatch = new List<BsonDocument>(BatchSize);

        foreach (var entity in entities)
        {
            if (entity == null) continue;

            // 确保实体有ID
            EnsureEntityHasId(entity);

            // 转换为BSON文档（AOT兼容）
            var document = AotBsonMapper.ToDocument(entity);
            
            // 如果文档没有ID（可能是因为它是不可变的BsonDocument），我们需要在这里生成一个新的文档带ID
            if (!document.ContainsKey("_id"))
            {
                var newId = ObjectId.NewObjectId();
                document = document.Set("_id", newId);
                // 尝试更新实体ID（如果是可变实体）
                UpdateEntityId(entity, newId);
            }
            
            entityBatch.Add(entity);
            docBatch.Add(document);

            if (entityBatch.Count >= BatchSize)
            {
                totalInserted += InsertBatch(entityBatch, docBatch);
                entityBatch.Clear();
                docBatch.Clear();
            }
        }

        if (entityBatch.Count > 0)
        {
            totalInserted += InsertBatch(entityBatch, docBatch);
        }

        return totalInserted;
    }

    private int InsertBatch(List<T> entities, List<BsonDocument> documents)
    {
        if (documents.Count == 0) return 0;

        // 批量插入到数据库
        var insertedCount = _engine.InsertDocuments(_name, documents.ToArray());

        // 更新实体的ID
        for (int i = 0; i < Math.Min(insertedCount, entities.Count); i++)
        {
            var entity = entities[i];
            if (entity != null && documents.Count > i)
            {
                var document = documents[i];
                if (document.TryGetValue("_id", out var id))
                {
                    UpdateEntityId(entity, id);
                }
            }
        }

        return insertedCount;
    }

    /// <summary>
    /// 更新文档
    /// </summary>
    /// <param name="entity">要更新的实体</param>
    /// <returns>更新的文档数量</returns>
    public int Update(T entity)
    {
        ThrowIfDisposed();
        if (entity == null) throw new ArgumentNullException(nameof(entity));

        // 确保实体有ID - 使用AOT兼容的访问器
        var hasValidId = AotIdAccessor<T>.HasValidId(entity);
        if (!hasValidId)
        {
                        throw new ArgumentException("Entity must have a valid ID for update", nameof(entity));
        }

        var id = AotIdAccessor<T>.GetId(entity);

        // 转换为BSON文档（AOT兼容）
        var document = AotBsonMapper.ToDocument(entity);

        // 检查是否在事务中
        var currentTransaction = _engine.GetCurrentTransaction();
        if (currentTransaction != null)
        {
            // 在事务中，需要先获取原始文档用于回滚
            var originalDocument = _engine.FindById(_name, id);
            if (originalDocument == null)
            {
                // 如果原始文档不存在，这是插入操作
                ((Transaction)currentTransaction).RecordInsert(_name, document);
                return 1;
            }
            else
            {
                // 记录更新操作
                ((Transaction)currentTransaction).RecordUpdate(_name, originalDocument, document);
                return 1;
            }
        }
        else
        {
            // 不在事务中，直接更新到数据库
            return _engine.UpdateDocument(_name, document);
        }
    }

    /// <summary>
    /// 更新多个文档
    /// </summary>
    /// <param name="entities">要更新的实体集合</param>
    /// <returns>更新的文档数量</returns>
    public int Update(IEnumerable<T> entities)
    {
        ThrowIfDisposed();
        if (entities == null) throw new ArgumentNullException(nameof(entities));

        var updatedCount = 0;
        foreach (var entity in entities)
        {
            if (entity != null)
            {
                updatedCount += Update(entity);
            }
        }

        return updatedCount;
    }

    /// <summary>
    /// 删除文档
    /// </summary>
    /// <param name="id">要删除的文档ID</param>
    /// <returns>删除的文档数量</returns>
    public int Delete(BsonValue id)
    {
        ThrowIfDisposed();
        if (id == null || id.IsNull) return 0;

        // 检查是否在事务中
        var currentTransaction = _engine.GetCurrentTransaction();
        if (currentTransaction != null)
        {
            // 在事务中，需要先获取要删除的文档用于回滚
            var documentToDelete = _engine.FindById(_name, id);
            if (documentToDelete == null)
            {
                return 0; // 文档不存在，无需删除
            }
            else
            {
                // 记录删除操作
                ((Transaction)currentTransaction).RecordDelete(_name, documentToDelete);
                return 1;
            }
        }
        else
        {
            // 不在事务中，直接删除
            return _engine.DeleteDocument(_name, id);
        }
    }

    /// <summary>
    /// 删除多个文档
    /// </summary>
    /// <param name="ids">要删除的文档ID集合</param>
    /// <returns>删除的文档数量</returns>
    public int Delete(IEnumerable<BsonValue> ids)
    {
        ThrowIfDisposed();
        if (ids == null) throw new ArgumentNullException(nameof(ids));

        var deletedCount = 0;
        foreach (var id in ids)
        {
            if (id != null && !id.IsNull)
            {
                deletedCount += Delete(id);
            }
        }

        return deletedCount;
    }

    /// <summary>
    /// 根据ID查找文档
    /// </summary>
    /// <param name="id">文档ID</param>
    /// <returns>找到的文档，如果不存在则返回null</returns>
    public T? FindById(BsonValue id)
    {
        ThrowIfDisposed();
        if (id == null || id.IsNull) return null;

        var document = _engine.FindById(_name, id);
        return document != null ? AotBsonMapper.FromDocument<T>(document) : default(T);
    }

    /// <summary>
    /// 查找所有文档
    /// </summary>
    /// <returns>所有文档的集合</returns>
    public IEnumerable<T> FindAll()
    {
        ThrowIfDisposed();

        var documents = _engine.FindAll(_name);
        foreach (var document in documents)
        {
            var entity = AotBsonMapper.FromDocument<T>(document);
            if (entity != null)
            {
                yield return entity;
            }
        }
    }

    /// <summary>
    /// 根据查询条件查找文档
    /// </summary>
    /// <param name="predicate">查询条件</param>
    /// <returns>匹配的文档集合</returns>
    public IEnumerable<T> Find(Expression<Func<T, bool>> predicate)
    {
        ThrowIfDisposed();
        if (predicate == null) throw new ArgumentNullException(nameof(predicate));

        return Query().Where(predicate);
    }

    /// <summary>
    /// 查找单个文档
    /// </summary>
    /// <param name="predicate">查询条件</param>
    /// <returns>找到的第一个文档，如果不存在则返回null</returns>
    public T? FindOne(Expression<Func<T, bool>> predicate)
    {
        ThrowIfDisposed();
        if (predicate == null) throw new ArgumentNullException(nameof(predicate));

        return Query().Where(predicate).FirstOrDefault();
    }

    /// <summary>
    /// 获取可查询对象
    /// </summary>
    /// <returns>可查询对象</returns>
    public IQueryable<T> Query()
    {
        ThrowIfDisposed();

        return new Queryable<T>(_queryExecutor, _name);
    }

    /// <summary>
    /// 创建支持 Include 的查询构建器
    /// </summary>
    /// <typeparam name="TProperty">属性类型</typeparam>
    /// <param name="expression">要包含的引用属性表达式</param>
    /// <returns>支持 Include 的查询构建器</returns>
    /// <example>
    /// <code>
    /// var orders = db.GetCollection&lt;Order&gt;("orders")
    ///     .Include(x => x.Customer)
    ///     .Include(x => x.Products)
    ///     .FindAll();
    /// </code>
    /// </example>
    public References.IncludeQueryBuilder<T> Include<TProperty>(Expression<Func<T, TProperty>> expression)
    {
        ThrowIfDisposed();
        return new References.IncludeQueryBuilder<T>(_engine, _name).Include(expression);
    }

    /// <summary>
    /// 创建支持 Include 的查询构建器（通过路径）
    /// </summary>
    /// <param name="path">要包含的引用属性路径</param>
    /// <returns>支持 Include 的查询构建器</returns>
    public References.IncludeQueryBuilder<T> Include(string path)
    {
        ThrowIfDisposed();
        return new References.IncludeQueryBuilder<T>(_engine, _name).Include(path);
    }

    /// <summary>
    /// 统计文档数量
    /// </summary>
    /// <returns>文档数量</returns>
    public long Count()
    {
        ThrowIfDisposed();

        // 关键修复：如果在事务中，必须使用 FindAll().Count() 以包含挂起的操作
        if (_engine.GetCurrentTransaction() != null)
        {
            return FindAll().LongCount();
        }

        return _engine.GetCachedDocumentCount(_name);
    }

    /// <summary>
    /// 根据条件统计文档数量
    /// </summary>
    /// <param name="predicate">查询条件</param>
    /// <returns>匹配的文档数量</returns>
    public long Count(Expression<Func<T, bool>> predicate)
    {
        ThrowIfDisposed();
        if (predicate == null) throw new ArgumentNullException(nameof(predicate));

        return Query().Where(predicate).LongCount();
    }

    /// <summary>
    /// 检查是否存在匹配的文档
    /// </summary>
    /// <param name="predicate">查询条件</param>
    /// <returns>是否存在匹配的文档</returns>
    public bool Exists(Expression<Func<T, bool>> predicate)
    {
        ThrowIfDisposed();
        if (predicate == null) throw new ArgumentNullException(nameof(predicate));

        return Query().Where(predicate).Any();
    }

    /// <summary>
    /// 删除所有文档
    /// </summary>
    /// <returns>删除的文档数量</returns>
    public int DeleteAll()
    {
        ThrowIfDisposed();

        var deletedCount = 0;
        var allDocuments = FindAll().ToList();

        foreach (var entity in allDocuments)
        {
            var id = GetEntityId(entity);
            if (id != null && !id.IsNull)
            {
                deletedCount += Delete(id);
            }
        }

        return deletedCount;
    }

    /// <summary>
    /// 根据条件删除文档
    /// </summary>
    /// <param name="predicate">查询条件</param>
    /// <returns>删除的文档数量</returns>
    public int DeleteMany(Expression<Func<T, bool>> predicate)
    {
        ThrowIfDisposed();
        if (predicate == null) throw new ArgumentNullException(nameof(predicate));

        var deletedCount = 0;
        var documentsToDelete = Find(predicate).ToList();

        foreach (var entity in documentsToDelete)
        {
            var id = GetEntityId(entity);
            if (id != null && !id.IsNull)
            {
                deletedCount += Delete(id);
            }
        }

        return deletedCount;
    }

    /// <summary>
    /// 插入或更新文档（如果存在ID则更新，否则插入）
    /// </summary>
    /// <param name="entity">要插入或更新的实体</param>
    /// <returns>操作类型和影响的文档数量</returns>
    public (UpdateType UpdateType, int Count) Upsert(T entity)
    {
        ThrowIfDisposed();
        if (entity == null) throw new ArgumentNullException(nameof(entity));

        var id = GetEntityId(entity);

        if (id == null || id.IsNull)
        {
            // 插入新文档
            Insert(entity);
            return (UpdateType.Insert, 1);
        }
        else
        {
            // 检查文档是否存在
            var existingDocument = FindById(id);
            if (existingDocument == null)
            {
                // 文档不存在，插入新文档
                Insert(entity);
                return (UpdateType.Insert, 1);
            }
            else
            {
                // 文档存在，更新
                var updateCount = Update(entity);
                return (UpdateType.Update, updateCount);
            }
        }
    }

    /// <summary>
    /// 确保实体有ID
    /// </summary>
    /// <param name="entity">实体</param>
    private void EnsureEntityHasId(T entity)
    {
        // 尝试使用新的ID生成系统
        if (AotIdAccessor<T>.GenerateIdIfNeeded(entity))
        {
            // ID生成成功，确保创建了索引
            EnsureIdIndex();
            return;
        }

        // 回退到原有的ObjectId生成逻辑
        var id = GetEntityId(entity);
        if (id == null || id.IsNull)
        {
            var newId = ObjectId.NewObjectId();
            UpdateEntityId(entity, newId);
            EnsureIdIndex();
        }
    }

    /// <summary>
    /// 确保ID索引存在
    /// </summary>
    private void EnsureIdIndex()
    {
        try
        {
            var idProperty = TinyDb.Serialization.EntityMetadata<T>.IdProperty;
            if (idProperty != null)
            {
                var indexName = $"idx_{_name}_id";
                _engine.EnsureIndex(_name, "_id", indexName, unique: true);
            }
        }
        catch
        {
            // 索引创建失败不应影响插入操作
        }
    }

    /// <summary>
    /// 获取实体的ID
    /// </summary>
    /// <param name="entity">实体</param>
    /// <returns>ID值</returns>
    private BsonValue GetEntityId(T entity)
    {
        try
        {
            return AotIdAccessor<T>.GetId(entity);
        }
        catch
        {
            return BsonNull.Value;
        }
    }

    /// <summary>
    /// 更新实体的ID
    /// </summary>
    /// <param name="entity">实体</param>
    /// <param name="id">新的ID值</param>
    private void UpdateEntityId(T entity, BsonValue id)
    {
        try
        {
            AotIdAccessor<T>.SetId(entity, id);
        }
        catch
        {
            // 如果实体没有ID属性，忽略更新
        }
    }

    /// <summary>
    /// 获取数据库引擎实例
    /// </summary>
    /// <returns>数据库引擎实例</returns>
    public TinyDbEngine Database => _engine;

    /// <summary>
    /// 获取索引管理器
    /// </summary>
    /// <returns>索引管理器实例</returns>
    public IndexManager GetIndexManager()
    {
        ThrowIfDisposed();
        return _engine.GetIndexManager(_name);
    }

    #region 异步方法实现

    /// <summary>
    /// 异步插入单个文档
    /// </summary>
    /// <param name="entity">要插入的实体</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>插入文档的ID</returns>
    public async Task<BsonValue> InsertAsync(T entity, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        if (entity == null) throw new ArgumentNullException(nameof(entity));

        // 确保实体有ID
        EnsureEntityHasId(entity);

        // 转换为BSON文档（AOT兼容）
        var document = AotBsonMapper.ToDocument(entity);

        // 如果文档没有ID（可能是因为它是不可变的BsonDocument），我们需要在这里生成一个新的文档带ID
        if (!document.ContainsKey("_id"))
        {
            var newId = ObjectId.NewObjectId();
            document = document.Set("_id", newId);
            // 尝试更新实体ID（如果是可变实体）
            UpdateEntityId(entity, newId);
        }

        // 检查是否在事务中
        var currentTransaction = _engine.GetCurrentTransaction();
        if (currentTransaction != null)
        {
            // 在事务中，记录操作而不是直接写入（事务不支持异步）
            var documentId = ((Transaction)currentTransaction).RecordInsert(_name, document);
            UpdateEntityId(entity, documentId);
            return documentId;
        }
        else
        {
            // 不在事务中，异步插入到数据库
            var id = await _engine.InsertDocumentAsync(_name, document, cancellationToken).ConfigureAwait(false);
            UpdateEntityId(entity, id);
            return id;
        }
    }

    /// <summary>
    /// 异步插入多个文档
    /// </summary>
    /// <param name="entities">要插入的实体集合</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>插入的文档数量</returns>
    public async Task<int> InsertAsync(IEnumerable<T> entities, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        if (entities == null) throw new ArgumentNullException(nameof(entities));

        int totalInserted = 0;
        const int BatchSize = 1000;
        var entityBatch = new List<T>(BatchSize);
        var docBatch = new List<BsonDocument>(BatchSize);

        foreach (var entity in entities)
        {
            if (entity == null) continue;
            cancellationToken.ThrowIfCancellationRequested();

            EnsureEntityHasId(entity);
            var document = AotBsonMapper.ToDocument(entity);
            
            // 如果文档没有ID（可能是因为它是不可变的BsonDocument），我们需要在这里生成一个新的文档带ID
            if (!document.ContainsKey("_id"))
            {
                var newId = ObjectId.NewObjectId();
                document = document.Set("_id", newId);
                // 尝试更新实体ID（如果是可变实体）
                UpdateEntityId(entity, newId);
            }
            
            entityBatch.Add(entity);
            docBatch.Add(document);

            if (entityBatch.Count >= BatchSize)
            {
                totalInserted += await InsertBatchAsync(entityBatch, docBatch, cancellationToken).ConfigureAwait(false);
                entityBatch.Clear();
                docBatch.Clear();
            }
        }

        if (entityBatch.Count > 0)
        {
            totalInserted += await InsertBatchAsync(entityBatch, docBatch, cancellationToken).ConfigureAwait(false);
        }

        return totalInserted;
    }

    private async Task<int> InsertBatchAsync(List<T> entities, List<BsonDocument> documents, CancellationToken cancellationToken)
    {
        if (documents.Count == 0) return 0;

        var insertedCount = await _engine.InsertDocumentsAsync(_name, documents.ToArray(), cancellationToken).ConfigureAwait(false);

        for (int i = 0; i < Math.Min(insertedCount, entities.Count); i++)
        {
            var entity = entities[i];
            if (entity != null && documents.Count > i)
            {
                var document = documents[i];
                if (document.TryGetValue("_id", out var id))
                {
                    UpdateEntityId(entity, id);
                }
            }
        }

        return insertedCount;
    }

    /// <summary>
    /// 异步更新文档
    /// </summary>
    /// <param name="entity">要更新的实体</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>更新的文档数量</returns>
    public async Task<int> UpdateAsync(T entity, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        if (entity == null) throw new ArgumentNullException(nameof(entity));

        var hasValidId = AotIdAccessor<T>.HasValidId(entity);
        if (!hasValidId)
        {
            throw new ArgumentException("Entity must have a valid ID for update", nameof(entity));
        }

        var id = AotIdAccessor<T>.GetId(entity);
        var document = AotBsonMapper.ToDocument(entity);

        var currentTransaction = _engine.GetCurrentTransaction();
        if (currentTransaction != null)
        {
            var originalDocument = _engine.FindById(_name, id);
            if (originalDocument == null)
            {
                ((Transaction)currentTransaction).RecordInsert(_name, document);
                return 1;
            }
            else
            {
                ((Transaction)currentTransaction).RecordUpdate(_name, originalDocument, document);
                return 1;
            }
        }
        else
        {
            return await _engine.UpdateDocumentAsync(_name, document, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// 异步更新多个文档
    /// </summary>
    /// <param name="entities">要更新的实体集合</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>更新的文档数量</returns>
    public async Task<int> UpdateAsync(IEnumerable<T> entities, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        if (entities == null) throw new ArgumentNullException(nameof(entities));

        var updatedCount = 0;
        foreach (var entity in entities)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (entity != null)
            {
                updatedCount += await UpdateAsync(entity, cancellationToken).ConfigureAwait(false);
            }
        }

        return updatedCount;
    }

    /// <summary>
    /// 异步删除文档
    /// </summary>
    /// <param name="id">要删除的文档ID</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>删除的文档数量</returns>
    public async Task<int> DeleteAsync(BsonValue id, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        if (id == null || id.IsNull) return 0;

        var currentTransaction = _engine.GetCurrentTransaction();
        if (currentTransaction != null)
        {
            var documentToDelete = _engine.FindById(_name, id);
            if (documentToDelete == null)
            {
                return 0;
            }
            else
            {
                ((Transaction)currentTransaction).RecordDelete(_name, documentToDelete);
                return 1;
            }
        }
        else
        {
            return await _engine.DeleteDocumentAsync(_name, id, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// 异步删除多个文档
    /// </summary>
    /// <param name="ids">要删除的文档ID集合</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>删除的文档数量</returns>
    public async Task<int> DeleteAsync(IEnumerable<BsonValue> ids, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        if (ids == null) throw new ArgumentNullException(nameof(ids));

        var deletedCount = 0;
        foreach (var id in ids)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (id != null && !id.IsNull)
            {
                deletedCount += await DeleteAsync(id, cancellationToken).ConfigureAwait(false);
            }
        }

        return deletedCount;
    }

    /// <summary>
    /// 异步删除所有文档
    /// </summary>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>删除的文档数量</returns>
    public async Task<int> DeleteAllAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();

        var deletedCount = 0;
        var allDocuments = FindAll().ToList();

        foreach (var entity in allDocuments)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var id = GetEntityId(entity);
            if (id != null && !id.IsNull)
            {
                deletedCount += await DeleteAsync(id, cancellationToken).ConfigureAwait(false);
            }
        }

        return deletedCount;
    }

    /// <summary>
    /// 异步根据条件删除文档
    /// </summary>
    /// <param name="predicate">查询条件</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>删除的文档数量</returns>
    public async Task<int> DeleteManyAsync(Expression<Func<T, bool>> predicate, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        if (predicate == null) throw new ArgumentNullException(nameof(predicate));

        var deletedCount = 0;
        var documentsToDelete = Find(predicate).ToList();

        foreach (var entity in documentsToDelete)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var id = GetEntityId(entity);
            if (id != null && !id.IsNull)
            {
                deletedCount += await DeleteAsync(id, cancellationToken).ConfigureAwait(false);
            }
        }

        return deletedCount;
    }

    /// <summary>
    /// 异步插入或更新文档（如果存在ID则更新，否则插入）
    /// </summary>
    /// <param name="entity">要插入或更新的实体</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>操作类型和影响的文档数量</returns>
    public async Task<(UpdateType UpdateType, int Count)> UpsertAsync(T entity, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        if (entity == null) throw new ArgumentNullException(nameof(entity));

        var id = GetEntityId(entity);

        if (id == null || id.IsNull)
        {
            await InsertAsync(entity, cancellationToken).ConfigureAwait(false);
            return (UpdateType.Insert, 1);
        }
        else
        {
            var existingDocument = FindById(id);
            if (existingDocument == null)
            {
                await InsertAsync(entity, cancellationToken).ConfigureAwait(false);
                return (UpdateType.Insert, 1);
            }
            else
            {
                var updateCount = await UpdateAsync(entity, cancellationToken).ConfigureAwait(false);
                return (UpdateType.Update, updateCount);
            }
        }
    }

    #endregion

    /// <summary>
    /// 检查是否已释放
    /// </summary>
    private void ThrowIfDisposed()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(DocumentCollection<T>));
    }

    /// <summary>
    /// 释放资源
    /// </summary>
    public void Dispose()
    {
        if (!_disposed)
        {
            _disposed = true;
        }
    }

    /// <summary>
    /// 重写 ToString 方法
    /// </summary>
    public override string ToString()
    {
        return $"DocumentCollection<{typeof(T).Name}>[{_name}]";
    }
}