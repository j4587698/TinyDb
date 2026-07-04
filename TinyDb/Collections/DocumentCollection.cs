using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using TinyDb.Bson;
using TinyDb.Core;
using TinyDb.Serialization;
using TinyDb.Query;
using TinyDb.Index;
using TinyDb.Attributes;
using TinyDb.IdGeneration;

namespace TinyDb.Collections;

/// <summary>
/// 文档集合实现
/// </summary>
/// <typeparam name="T">文档类型</typeparam>
public sealed class DocumentCollection<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T> : ITinyCollection<T>, IDocumentCollection where T : class, new()
{
    private readonly TinyDbEngine _engine;
    private readonly string _name;
    private readonly QueryExecutor _queryExecutor;
    private int _disposed;

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

        var document = PrepareDocumentForInsert(entity);

        // 检查是否在事务中
        var currentTransaction = _engine.GetCurrentTransaction();
        if (currentTransaction != null)
        {
            // 在事务中，记录操作而不是直接写入
            return ((Transaction)currentTransaction).RecordInsert(_name, document);
        }
        else
        {
            // 不在事务中，直接插入到数据库
            return _engine.InsertDocument(_name, document);
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

        var currentTransaction = _engine.GetCurrentTransaction();
        if (currentTransaction != null)
        {
            return InsertInTransaction(entities, (Transaction)currentTransaction);
        }

        int totalInserted = 0;
        const int BatchSize = 1000;
        var docBatch = new List<BsonDocument>(BatchSize);

        foreach (var entity in entities)
        {
            if (entity == null) continue;

            var document = PrepareDocumentForInsert(entity);
            
            docBatch.Add(document);

            if (docBatch.Count >= BatchSize)
            {
                totalInserted += InsertDocumentBatch(docBatch);
                docBatch.Clear();
            }
        }

        if (docBatch.Count > 0)
        {
            totalInserted += InsertDocumentBatch(docBatch);
        }

        return totalInserted;
    }

    private int InsertDocumentBatch(List<BsonDocument> documents)
    {
        if (documents.Count == 0) return 0;

        // 批量插入到数据库
        return _engine.InsertDocuments(_name, documents);
    }

    private int InsertInTransaction(IEnumerable<T> entities, Transaction transaction, CancellationToken cancellationToken = default)
    {
        var insertedCount = 0;
        foreach (var entity in entities)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (entity == null) continue;

            var document = PrepareDocumentForInsert(entity);
            var documentId = transaction.RecordInsert(_name, document);
            insertedCount++;
        }

        return insertedCount;
    }

    private BsonDocument PrepareDocumentForInsert(T entity)
    {
        EnsureEntityHasId(entity);

        var document = AotBsonMapper.ToDocument(entity);
        if (!document.ContainsKey("_id"))
        {
            var newId = ObjectId.NewObjectId();
            document = document.Set("_id", newId);
            UpdateEntityId(entity, newId);
        }

        return document;
    }

    private BsonDocument PrepareDocumentForUpdate(T entity, out BsonValue id)
    {
        if (!AotIdAccessor<T>.HasValidId(entity))
        {
            throw new ArgumentException("Entity must have a valid ID for update", nameof(entity));
        }

        id = AotIdAccessor<T>.GetId(entity);
        return AotBsonMapper.ToDocument(entity);
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

        var document = PrepareDocumentForUpdate(entity, out var id);

        // 检查是否在事务中
        var currentTransaction = _engine.GetCurrentTransaction();
        if (currentTransaction != null)
        {
            // 在事务中，需要先获取原始文档用于回滚
            var originalDocument = _engine.FindById(_name, id);
            if (originalDocument == null)
            {
                // 如果原始文档不存在，这是插入操作
                return 0;
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

        var currentTransaction = _engine.GetCurrentTransaction();
        if (currentTransaction != null)
        {
            return UpdateInTransaction(entities, (Transaction)currentTransaction);
        }

        int updatedCount = 0;
        const int BatchSize = 1000;
        var docBatch = new List<BsonDocument>(BatchSize);

        foreach (var entity in entities)
        {
            if (entity != null)
            {
                var document = PrepareDocumentForUpdate(entity, out _);
                docBatch.Add(document);

                if (docBatch.Count >= BatchSize)
                {
                    updatedCount += _engine.UpdateDocuments(_name, docBatch);
                    docBatch.Clear();
                }
            }
        }

        if (docBatch.Count > 0)
        {
            updatedCount += _engine.UpdateDocuments(_name, docBatch);
        }

        return updatedCount;
    }

    private int UpdateInTransaction(IEnumerable<T> entities, Transaction transaction)
    {
        var prepared = new List<(BsonDocument Document, BsonValue Id)>();
        var ids = new List<BsonValue>();

        foreach (var entity in entities)
        {
            if (entity == null) continue;

            var document = PrepareDocumentForUpdate(entity, out var id);
            prepared.Add((document, id));
            ids.Add(id);
        }

        if (prepared.Count == 0)
        {
            return 0;
        }

        var originalDocuments = _engine.FindByIds(_name, ids);
        return RecordPreparedUpdatesInTransaction(prepared, originalDocuments, transaction);
    }

    private int RecordPreparedUpdatesInTransaction(
        IReadOnlyList<(BsonDocument Document, BsonValue Id)> prepared,
        IReadOnlyList<BsonDocument?> originalDocuments,
        Transaction transaction)
    {
        var currentDocuments = new Dictionary<BsonValue, BsonDocument?>(BsonValueComparer.EqualityComparer);
        for (int i = 0; i < prepared.Count; i++)
        {
            currentDocuments.TryAdd(prepared[i].Id, originalDocuments[i]);
        }

        var updatedCount = 0;
        foreach (var (document, id) in prepared)
        {
            currentDocuments.TryGetValue(id, out var originalDocument);
            if (originalDocument == null)
            {
                continue;
            }
            else
            {
                transaction.RecordUpdate(_name, originalDocument, document);
            }

            currentDocuments[id] = document;
            updatedCount++;
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
        if (document == null) return default;

        if (typeof(T) == typeof(BsonDocument))
        {
            var patched = _engine.MetadataManager.ApplySchemaDefaults(_name, document);
            return (T)(object)patched;
        }

        return AotBsonMapper.FromDocument<T>(document);
    }

    /// <summary>
    /// 查找所有文档
    /// </summary>
    /// <returns>所有文档的集合</returns>
    public IEnumerable<T> FindAll()
    {
        ThrowIfDisposed();

        var documents = _engine.FindAll(_name);
        if (typeof(T) == typeof(BsonDocument))
        {
            foreach (var document in documents)
            {
                var patched = _engine.MetadataManager.ApplySchemaDefaults(_name, document);
                yield return (T)(object)patched;
            }

            yield break;
        }

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
        return Find(predicate, 0, int.MaxValue);
    }

    public IEnumerable<T> Find(string predicate, IReadOnlyDictionary<string, object?>? parameters = null)
    {
        return Find(predicate, parameters, 0, int.MaxValue);
    }

    public IEnumerable<T> FindSql(string sql, IReadOnlyDictionary<string, object?>? parameters = null)
    {
        return Execute<T>(sql, parameters).Rows;
    }

    public IEnumerable<BsonDocument> FindSqlDocuments(string sql, IReadOnlyDictionary<string, object?>? parameters = null)
    {
        return Execute(sql, parameters).Documents;
    }

    public IEnumerable<TProjection> FindSql<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] TProjection>(
        string sql,
        IReadOnlyDictionary<string, object?>? parameters = null)
        where TProjection : class, new()
    {
        return Execute<TProjection>(sql, parameters).Rows;
    }

    public SqlExecutionResult Execute(string sql, IReadOnlyDictionary<string, object?>? parameters = null)
    {
        ThrowIfDisposed();
        if (sql == null) throw new ArgumentNullException(nameof(sql));

        var statement = SqlQueryParser.Parse(sql, parameters);
        return Execute(statement);
    }

    internal SqlExecutionResult Execute(SqlStatement statement)
    {
        ThrowIfDisposed();
        if (statement == null) throw new ArgumentNullException(nameof(statement));

        ValidateSqlCollection(statement);

        return statement switch
        {
            SqlQuerySpec query => new SqlExecutionResult(
                SqlStatementKind.Select,
                0,
                FindDocuments(query, preserveProjectionNames: true).ToList()),
            SqlInsertStatement insert => new SqlExecutionResult(SqlStatementKind.Insert, ExecuteInsert(insert), Array.Empty<BsonDocument>()),
            SqlUpdateStatement update => new SqlExecutionResult(SqlStatementKind.Update, ExecuteUpdate(update), Array.Empty<BsonDocument>()),
            SqlDeleteStatement delete => new SqlExecutionResult(SqlStatementKind.Delete, ExecuteDelete(delete), Array.Empty<BsonDocument>()),
            _ => throw new NotSupportedException($"Unsupported SQL statement '{statement.Kind}'.")
        };
    }

    public SqlExecutionResult<TProjection> Execute<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] TProjection>(
        string sql,
        IReadOnlyDictionary<string, object?>? parameters = null)
        where TProjection : class, new()
    {
        ThrowIfDisposed();
        if (sql == null) throw new ArgumentNullException(nameof(sql));

        var statement = SqlQueryParser.Parse(sql, parameters);
        return Execute<TProjection>(statement);
    }

    internal SqlExecutionResult<TProjection> Execute<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] TProjection>(
        SqlStatement statement)
        where TProjection : class, new()
    {
        ThrowIfDisposed();
        if (statement == null) throw new ArgumentNullException(nameof(statement));

        ValidateSqlCollection(statement);

        return statement switch
        {
            SqlQuerySpec query => new SqlExecutionResult<TProjection>(
                SqlStatementKind.Select,
                0,
                FindDocuments(query, preserveProjectionNames: false)
                    .Select(AotBsonMapper.FromDocument<TProjection>)
                    .ToList()),
            SqlInsertStatement insert => new SqlExecutionResult<TProjection>(SqlStatementKind.Insert, ExecuteInsert(insert), Array.Empty<TProjection>()),
            SqlUpdateStatement update => new SqlExecutionResult<TProjection>(SqlStatementKind.Update, ExecuteUpdate(update), Array.Empty<TProjection>()),
            SqlDeleteStatement delete => new SqlExecutionResult<TProjection>(SqlStatementKind.Delete, ExecuteDelete(delete), Array.Empty<TProjection>()),
            _ => throw new NotSupportedException($"Unsupported SQL statement '{statement.Kind}'.")
        };
    }

    public IEnumerable<T> Find(Expression<Func<T, bool>> predicate, int skip, int limit)
    {
        ThrowIfDisposed();
        if (predicate == null) throw new ArgumentNullException(nameof(predicate));

        ValidatePaginationArguments(skip, limit);
        if (limit == 0) return Enumerable.Empty<T>();

        var query = Query().Where(predicate);
        if (skip > 0)
        {
            query = query.Skip(skip);
        }

        if (limit < int.MaxValue)
        {
            query = query.Take(limit);
        }

        return query;
    }

    public IEnumerable<T> Find(string predicate, IReadOnlyDictionary<string, object?>? parameters, int skip, int limit)
    {
        ThrowIfDisposed();
        if (predicate == null) throw new ArgumentNullException(nameof(predicate));

        var queryExpression = StringQueryParser.Parse(predicate, parameters);
        return Find(queryExpression, skip, limit);
    }

    private IEnumerable<T> Find(SqlQuerySpec query)
    {
        ValidateSqlCollection(query);

        ValidatePaginationArguments(query.Skip, query.Limit);
        if (query.Limit == 0) return Enumerable.Empty<T>();

        var result = _queryExecutor.Execute<T>(_name, query.Predicate);
        var orderBy = CreateStableSqlOrderBy(query);
        if (orderBy.Count > 0)
        {
            result = ApplySqlOrdering(result, orderBy);
        }

        if (query.Skip > 0)
        {
            result = result.Skip(query.Skip);
        }

        if (query.Limit < int.MaxValue)
        {
            result = result.Take(query.Limit);
        }

        return result;
    }

    private IEnumerable<BsonDocument> FindDocuments(SqlQuerySpec query, bool preserveProjectionNames)
    {
        var source = Find(query);
        if (!query.SelectAll)
        {
            return source.Select(item => ProjectSqlDocument(item, query.Projection, preserveProjectionNames));
        }

        if (typeof(T) == typeof(BsonDocument))
        {
            return source.Select(ToSqlDocument);
        }

        var projection = CreateSelectAllProjection();
        return source.Select(item => ProjectSqlDocument(item, projection, preserveProjectionNames));
    }

    private int ExecuteInsert(SqlInsertStatement statement)
    {
        ValidateNoDuplicateSqlStorageFields(statement.Assignments);

        var document = new BsonDocument();
        foreach (var assignment in statement.Assignments)
        {
            document = SetSqlDocumentField(document, assignment);
        }

        var entity = ToEntity(document);
        Insert(entity);
        return 1;
    }

    private int ExecuteUpdate(SqlUpdateStatement statement)
    {
        if (statement.Assignments.Any(static assignment => IsSqlIdFieldPath(assignment.FieldPath)))
        {
            throw new NotSupportedException("SQL UPDATE cannot modify primary key fields.");
        }

        ValidateNoDuplicateSqlStorageFields(statement.Assignments);

        var items = _queryExecutor.Execute<T>(_name, statement.Predicate).ToList();
        var updatedItems = new List<T>(items.Count);
        foreach (var item in items)
        {
            var document = ToSqlDocument(item);
            foreach (var assignment in statement.Assignments)
            {
                document = SetSqlDocumentField(document, assignment);
            }

            updatedItems.Add(ToEntity(document));
        }

        return Update(updatedItems);
    }

    private int ExecuteDelete(SqlDeleteStatement statement)
    {
        var ids = _queryExecutor.Execute<T>(_name, statement.Predicate)
            .Select(static item => AotIdAccessor<T>.GetId(item))
            .Where(static id => id != null && !id.IsNull)
            .ToList();

        return Delete(ids);
    }

    private static BsonDocument SetSqlDocumentField(BsonDocument document, SqlAssignment assignment)
    {
        var target = ResolveSqlStorageField(assignment.FieldPath);
        var value = NormalizeSqlAssignmentValue(assignment.Value, target.PropertyType, assignment.FieldPath);
        return document.Set(target.FieldName, BsonConversion.ToBsonValue(value!));
    }

    private static T ToEntity(BsonDocument document)
    {
        return typeof(T) == typeof(BsonDocument)
            ? (T)(object)document
            : AotBsonMapper.FromDocument<T>(document);
    }

    private void ValidateSqlCollection(SqlStatement statement)
    {
        if (!string.Equals(statement.CollectionName, _name, StringComparison.OrdinalIgnoreCase))
        {
            throw new ArgumentException(
                $"SQL collection '{statement.CollectionName}' does not match collection '{_name}'.",
                nameof(statement));
        }
    }

    private IEnumerable<T> Find(QueryExpression queryExpression, int skip, int limit)
    {
        ValidatePaginationArguments(skip, limit);
        if (limit == 0) return Enumerable.Empty<T>();

        var result = _queryExecutor.Execute<T>(_name, queryExpression);

        if (skip > 0)
        {
            result = result.Skip(skip);
        }

        if (limit < int.MaxValue)
        {
            result = result.Take(limit);
        }

        return result;
    }

    public IEnumerable<T> Find(Expression<Func<T, bool>> predicate, int skip, int limit, out long totalCount)
    {
        ThrowIfDisposed();
        if (predicate == null) throw new ArgumentNullException(nameof(predicate));

        ValidatePaginationArguments(skip, limit);

        var results = limit == int.MaxValue
            ? new List<T>()
            : new List<T>(Math.Max(0, limit));

        long matchedCount = 0;
        foreach (var item in Query().Where(predicate))
        {
            if (matchedCount >= skip && (limit == int.MaxValue || results.Count < limit))
            {
                results.Add(item);
            }

            matchedCount++;
        }

        totalCount = matchedCount;
        return results;
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

        // 事务内使用增量计数，避免为了包含挂起操作而物化整个集合。
        if (_engine.GetCurrentTransaction() is Transaction transaction)
        {
            return _engine.GetTransactionalDocumentCount(_name, transaction);
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

        var document = PrepareDocumentForInsert(entity);
        var currentTransaction = _engine.GetCurrentTransaction();
        if (currentTransaction != null)
        {
            var id = document.TryGetValue("_id", out var documentId) ? documentId : BsonNull.Value;
            var existingDocument = id.IsNull ? null : _engine.FindById(_name, id);
            if (existingDocument == null)
            {
                ((Transaction)currentTransaction).RecordInsert(_name, document);
                return (UpdateType.Insert, 1);
            }

            ((Transaction)currentTransaction).RecordUpdate(_name, existingDocument, document);
            return (UpdateType.Update, 1);
        }

        return _engine.UpsertDocument(_name, document);
    }

    /// <summary>
    /// 插入或更新多个文档
    /// </summary>
    /// <param name="entities">要插入或更新的实体集合</param>
    /// <returns>插入和更新的文档数量</returns>
    public (int InsertedCount, int UpdatedCount) Upsert(IEnumerable<T> entities)
    {
        ThrowIfDisposed();
        if (entities == null) throw new ArgumentNullException(nameof(entities));

        var currentTransaction = _engine.GetCurrentTransaction();
        if (currentTransaction != null)
        {
            var transactionInsertedCount = 0;
            var transactionUpdatedCount = 0;
            foreach (var entity in entities)
            {
                if (entity == null) continue;

                var (updateType, count) = Upsert(entity);
                if (updateType == UpdateType.Insert)
                {
                    transactionInsertedCount += count;
                }
                else
                {
                    transactionUpdatedCount += count;
                }
            }

            return (transactionInsertedCount, transactionUpdatedCount);
        }

        const int BatchSize = 1000;
        var insertedCount = 0;
        var updatedCount = 0;
        var docBatch = new List<BsonDocument>(BatchSize);

        foreach (var entity in entities)
        {
            if (entity == null) continue;

            var document = PrepareDocumentForInsert(entity);
            docBatch.Add(document);

            if (docBatch.Count >= BatchSize)
            {
                var result = _engine.UpsertDocuments(_name, docBatch);
                insertedCount += result.InsertedCount;
                updatedCount += result.UpdatedCount;
                docBatch.Clear();
            }
        }

        if (docBatch.Count > 0)
        {
            var result = _engine.UpsertDocuments(_name, docBatch);
            insertedCount += result.InsertedCount;
            updatedCount += result.UpdatedCount;
        }

        return (insertedCount, updatedCount);
    }

    /// <summary>
    /// 确保实体有ID
    /// </summary>
    /// <param name="entity">实体</param>
    private void EnsureEntityHasId(T entity)
    {
        if (AotIdAccessor<T>.TryGetIdInfo(out var idPropertyName, out var idPropertyType) &&
            (idPropertyType == typeof(int) || idPropertyType == typeof(long)) &&
            !AotIdAccessor<T>.HasValidId(entity))
        {
            AotIdAccessor<T>.TryGetIdGenerationInfo(out _, out var configuredSequenceName);
            var sequenceName = string.IsNullOrWhiteSpace(configuredSequenceName)
                ? idPropertyName
                : configuredSequenceName;
            var identityId = AutoIdGenerator.CreateIdentityValue(_engine, _name, sequenceName, idPropertyType);
            AotIdAccessor<T>.SetId(entity, identityId);
            EnsureIdIndex();
            return;
        }

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
            if (AotIdAccessor<T>.TryGetIdInfo(out _, out _))
            {
                var indexName = $"idx_{_name}_id";
                _engine.EnsureIndex(_name, "_id", indexName, unique: true);
            }
        }
        catch (Exception ex)
        {
            if (ex is ObjectDisposedException) throw;

            throw new InvalidOperationException(
                $"Failed to ensure unique _id index for collection '{_name}'.", ex);
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
        catch (InvalidOperationException)
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
        catch (InvalidOperationException)
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

    private static BsonDocument ToSqlDocument(T item)
    {
        return item is BsonDocument document
            ? document
            : AotBsonMapper.ToDocument(item);
    }

    private static BsonDocument ProjectSqlDocument(T item, IReadOnlyList<SqlProjectionField> projection, bool preserveProjectionNames)
    {
        var document = new BsonDocument();
        foreach (var field in projection)
        {
            var fieldExpression = CreateSqlFieldExpression(field.FieldPath);
            var value = ExpressionEvaluator.EvaluateValue<T>(fieldExpression, item);
            document = document.Set(
                NormalizeSqlProjectionFieldName(field.OutputName, preserveProjectionNames),
                BsonConversion.ToBsonValue(value!));
        }

        return document;
    }

    private static IReadOnlyList<SqlProjectionField> CreateSelectAllProjection()
    {
        var projection = new List<SqlProjectionField>(EntityMetadata<T>.Properties.Count);
        foreach (var property in EntityMetadata<T>.Properties)
        {
            projection.Add(new SqlProjectionField(property.Name, property.Name));
        }

        return projection;
    }

    private static IEnumerable<T> ApplySqlOrdering(IEnumerable<T> source, IReadOnlyList<SqlOrderBy> orderBy)
    {
        IOrderedEnumerable<T>? ordered = null;
        foreach (var sort in orderBy)
        {
            var sortExpression = CreateSqlFieldExpression(sort.FieldPath);
            Func<T, object?> keySelector = item => ExpressionEvaluator.EvaluateValue<T>(sortExpression, item);

            if (ordered == null)
            {
                ordered = sort.Descending
                    ? source.OrderByDescending(keySelector, SqlSortComparer.Instance)
                    : source.OrderBy(keySelector, SqlSortComparer.Instance);
                continue;
            }

            ordered = sort.Descending
                ? ordered.ThenByDescending(keySelector, SqlSortComparer.Instance)
                : ordered.ThenBy(keySelector, SqlSortComparer.Instance);
        }

        return ordered ?? source;
    }

    private static IReadOnlyList<SqlOrderBy> CreateStableSqlOrderBy(SqlQuerySpec query)
    {
        if ((query.Skip <= 0 && query.Limit >= int.MaxValue) || query.OrderBy.Count == 0)
        {
            return query.OrderBy;
        }

        if (query.OrderBy.Any(static sort => IsSqlIdFieldPath(sort.FieldPath)))
        {
            return query.OrderBy;
        }

        var result = new List<SqlOrderBy>(query.OrderBy.Count + 1);
        result.AddRange(query.OrderBy);
        result.Add(new SqlOrderBy(GetSqlIdFieldName(), false));
        return result;
    }

    private static QueryExpression CreateSqlFieldExpression(string fieldPath)
    {
        var segments = fieldPath.Split('.');
        QueryExpression expression = new TinyDb.Query.MemberExpression(
            NormalizeSqlFieldSegment(segments[0]),
            new TinyDb.Query.ParameterExpression("$"));
        for (var i = 1; i < segments.Length; i++)
        {
            expression = new TinyDb.Query.MemberExpression(NormalizeSqlFieldSegment(segments[i]), expression);
        }

        return expression;
    }

    private static string NormalizeSqlFieldSegment(string segment)
    {
        if (string.Equals(segment, "_id", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(segment, "id", StringComparison.OrdinalIgnoreCase))
        {
            return GetSqlIdFieldName();
        }

        if (typeof(T) == typeof(BsonDocument))
        {
            return segment;
        }

        foreach (var property in EntityMetadata<T>.Properties)
        {
            if (string.Equals(property.Name, segment, StringComparison.OrdinalIgnoreCase) ||
                string.Equals(ToCamelCase(property.Name), segment, StringComparison.OrdinalIgnoreCase))
            {
                return property.Name;
            }
        }

        return segment;
    }

    private static string NormalizeSqlProjectionFieldName(string name, bool preserveProjectionNames)
    {
        var outputName = name;
        var lastDot = outputName.LastIndexOf('.');
        if (lastDot >= 0)
        {
            outputName = outputName.Substring(lastDot + 1);
        }

        if (preserveProjectionNames)
        {
            return outputName;
        }

        if (string.Equals(outputName, "_id", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(outputName, "id", StringComparison.OrdinalIgnoreCase))
        {
            return "_id";
        }

        if (typeof(T) == typeof(BsonDocument))
        {
            return outputName;
        }

        foreach (var property in EntityMetadata<T>.Properties)
        {
            if (string.Equals(property.Name, outputName, StringComparison.OrdinalIgnoreCase) ||
                string.Equals(ToCamelCase(property.Name), outputName, StringComparison.OrdinalIgnoreCase))
            {
                return property == EntityMetadata<T>.IdProperty
                    ? "_id"
                    : ToCamelCase(property.Name);
            }
        }

        return ToCamelCase(outputName);
    }

    private static string NormalizeSqlStorageFieldName(string fieldPath)
        => ResolveSqlStorageField(fieldPath).FieldName;

    private static (string FieldName, Type? PropertyType) ResolveSqlStorageField(string fieldPath)
    {
        if (fieldPath.IndexOf('.') >= 0)
        {
            throw new NotSupportedException("SQL INSERT/UPDATE currently supports top-level fields only.");
        }

        if (typeof(T) == typeof(BsonDocument))
        {
            return (IsIdFieldPath(fieldPath) ? "_id" : fieldPath, null);
        }

        if (IsIdFieldPath(fieldPath))
        {
            return ("_id", EntityMetadata<T>.IdProperty?.PropertyType);
        }

        foreach (var property in EntityMetadata<T>.Properties)
        {
            if (string.Equals(property.Name, fieldPath, StringComparison.OrdinalIgnoreCase) ||
                string.Equals(ToCamelCase(property.Name), fieldPath, StringComparison.OrdinalIgnoreCase))
            {
                var fieldName = property == EntityMetadata<T>.IdProperty
                    ? "_id"
                    : ToCamelCase(property.Name);
                return (fieldName, property.PropertyType);
            }
        }

        return (ToCamelCase(fieldPath), null);
    }

    private static void ValidateNoDuplicateSqlStorageFields(IReadOnlyList<SqlAssignment> assignments)
    {
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var assignment in assignments)
        {
            var fieldName = ResolveSqlStorageField(assignment.FieldPath).FieldName;
            if (!seen.Add(fieldName))
            {
                throw new FormatException($"SQL DML contains duplicate field '{assignment.FieldPath}'.");
            }
        }
    }

    private static object? NormalizeSqlAssignmentValue(object? value, Type? targetType, string fieldPath)
    {
        if (targetType == null)
        {
            return value;
        }

        var nullableType = Nullable.GetUnderlyingType(targetType);
        var effectiveType = nullableType ?? targetType;
        if (value == null)
        {
            if (nullableType == null && targetType.IsValueType)
            {
                throw new InvalidOperationException(
                    $"SQL assignment for '{fieldPath}' cannot set null to non-nullable type '{targetType.Name}'.");
            }

            return null;
        }

        if (effectiveType.IsInstanceOfType(value))
        {
            return value;
        }

        try
        {
            if (value is BsonValue bsonValue && !typeof(BsonValue).IsAssignableFrom(effectiveType))
            {
                value = GetSqlAssignmentRawValue(bsonValue);
                if (value == null)
                {
                    if (nullableType == null && targetType.IsValueType)
                    {
                        throw new InvalidOperationException(
                            $"SQL assignment for '{fieldPath}' cannot set null to non-nullable type '{targetType.Name}'.");
                    }

                    return null;
                }

                if (effectiveType.IsInstanceOfType(value))
                {
                    return value;
                }
            }

            if (effectiveType.IsEnum)
            {
                return value is string enumText
                    ? Enum.Parse(effectiveType, enumText, ignoreCase: true)
                    : Enum.ToObject(effectiveType, Convert.ChangeType(value, Enum.GetUnderlyingType(effectiveType), CultureInfo.InvariantCulture)!);
            }

            if (effectiveType == typeof(Guid))
            {
                if (value is string guidText)
                {
                    return Guid.Parse(guidText);
                }

                throw new InvalidCastException($"Cannot convert '{value.GetType().Name}' to Guid.");
            }

            if (effectiveType == typeof(DateTime) && value is string dateText)
            {
                return DateTime.Parse(dateText, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
            }

            if (IsNumericType(effectiveType) && IsNumericType(value.GetType()))
            {
                return ConvertSqlNumericValue(value, effectiveType, fieldPath);
            }

            if (effectiveType == typeof(string))
            {
                return Convert.ToString(value, CultureInfo.InvariantCulture);
            }

            return Convert.ChangeType(value, effectiveType, CultureInfo.InvariantCulture);
        }
        catch (Exception ex) when (ex is FormatException or InvalidCastException or OverflowException or ArgumentException)
        {
            throw new InvalidOperationException(
                $"SQL assignment for '{fieldPath}' cannot convert value '{value}' to '{effectiveType.Name}'.",
                ex);
        }
    }

    private static object ConvertSqlNumericValue(object value, Type targetType, string fieldPath)
    {
        if (IsIntegralType(targetType))
        {
            var decimalValue = Convert.ToDecimal(value, CultureInfo.InvariantCulture);
            if (decimal.Truncate(decimalValue) != decimalValue)
            {
                throw new InvalidOperationException(
                    $"SQL assignment for '{fieldPath}' cannot convert fractional value '{value}' to '{targetType.Name}'.");
            }
        }

        return Convert.ChangeType(value, targetType, CultureInfo.InvariantCulture)!;
    }

    private static bool IsNumericType(Type type)
    {
        type = Nullable.GetUnderlyingType(type) ?? type;
        return type == typeof(byte) ||
               type == typeof(sbyte) ||
               type == typeof(short) ||
               type == typeof(ushort) ||
               type == typeof(int) ||
               type == typeof(uint) ||
               type == typeof(long) ||
               type == typeof(ulong) ||
               type == typeof(float) ||
               type == typeof(double) ||
               type == typeof(decimal);
    }

    private static bool IsIntegralType(Type type)
    {
        type = Nullable.GetUnderlyingType(type) ?? type;
        return type == typeof(byte) ||
               type == typeof(sbyte) ||
               type == typeof(short) ||
               type == typeof(ushort) ||
               type == typeof(int) ||
               type == typeof(uint) ||
               type == typeof(long) ||
               type == typeof(ulong);
    }

    private static object? GetSqlAssignmentRawValue(BsonValue bsonValue)
    {
        if (bsonValue.IsNull)
        {
            return null;
        }

        return bsonValue is BsonDecimal128 decimalValue
            ? decimalValue.ToDecimal(CultureInfo.InvariantCulture)
            : bsonValue.RawValue;
    }

    private static bool IsIdFieldPath(string fieldPath)
    {
        return string.Equals(fieldPath, "Id", StringComparison.OrdinalIgnoreCase) ||
               string.Equals(fieldPath, "_id", StringComparison.OrdinalIgnoreCase);
    }

    private static bool IsSqlIdFieldPath(string fieldPath)
    {
        if (fieldPath.IndexOf('.') >= 0)
        {
            return false;
        }

        if (IsIdFieldPath(fieldPath))
        {
            return true;
        }

        if (typeof(T) == typeof(BsonDocument))
        {
            return false;
        }

        var idProperty = EntityMetadata<T>.IdProperty;
        return idProperty != null &&
               (string.Equals(idProperty.Name, fieldPath, StringComparison.OrdinalIgnoreCase) ||
                string.Equals(ToCamelCase(idProperty.Name), fieldPath, StringComparison.OrdinalIgnoreCase));
    }

    private static string GetSqlIdFieldName()
    {
        if (typeof(T) == typeof(BsonDocument))
        {
            return "Id";
        }

        return EntityMetadata<T>.IdProperty?.Name ?? "Id";
    }

    private static string ToCamelCase(string name)
    {
        if (string.IsNullOrEmpty(name)) return name;
        if (name.Length == 1) return name.ToLowerInvariant();

        return char.ToLowerInvariant(name[0]) + name.Substring(1);
    }

    private sealed class SqlSortComparer : IComparer<object?>
    {
        public static readonly SqlSortComparer Instance = new();

        public int Compare(object? x, object? y)
        {
            return QueryValueComparer.Compare(x, y);
        }
    }

    private static void ValidatePaginationArguments(int skip, int limit)
    {
        if (skip < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(skip), "skip must be greater than or equal to 0.");
        }

        if (limit < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(limit), "limit must be greater than or equal to 0.");
        }
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

        var document = PrepareDocumentForInsert(entity);

        // 检查是否在事务中
        var currentTransaction = _engine.GetCurrentTransaction();
        if (currentTransaction != null)
        {
            // 在事务中，记录操作而不是直接写入（事务不支持异步）
            return ((Transaction)currentTransaction).RecordInsert(_name, document);
        }
        else
        {
            // 不在事务中，异步插入到数据库
            return await _engine.InsertDocumentAsync(_name, document, cancellationToken).ConfigureAwait(false);
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

        var currentTransaction = _engine.GetCurrentTransaction();
        if (currentTransaction != null)
        {
            return InsertInTransaction(entities, (Transaction)currentTransaction, cancellationToken);
        }

        int totalInserted = 0;
        const int BatchSize = 1000;
        var docBatch = new List<BsonDocument>(BatchSize);

        foreach (var entity in entities)
        {
            if (entity == null) continue;
            cancellationToken.ThrowIfCancellationRequested();

            var document = PrepareDocumentForInsert(entity);
            
            docBatch.Add(document);

            if (docBatch.Count >= BatchSize)
            {
                totalInserted += await InsertDocumentBatchAsync(docBatch, cancellationToken).ConfigureAwait(false);
                docBatch.Clear();
            }
        }

        if (docBatch.Count > 0)
        {
            totalInserted += await InsertDocumentBatchAsync(docBatch, cancellationToken).ConfigureAwait(false);
        }

        return totalInserted;
    }

    private async Task<int> InsertDocumentBatchAsync(List<BsonDocument> documents, CancellationToken cancellationToken)
    {
        if (documents.Count == 0) return 0;

        return await _engine.InsertDocumentsAsync(_name, documents, cancellationToken).ConfigureAwait(false);
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

        var document = PrepareDocumentForUpdate(entity, out var id);

        var currentTransaction = _engine.GetCurrentTransaction();
        if (currentTransaction != null)
        {
            var originalDocument = await _engine.FindByIdAsync(_name, id, cancellationToken).ConfigureAwait(false);
            if (originalDocument == null)
            {
                return 0;
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

        var currentTransaction = _engine.GetCurrentTransaction();
        if (currentTransaction != null)
        {
            return await UpdateInTransactionAsync(entities, (Transaction)currentTransaction, cancellationToken).ConfigureAwait(false);
        }

        int updatedCount = 0;
        const int BatchSize = 1000;
        var docBatch = new List<BsonDocument>(BatchSize);

        foreach (var entity in entities)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (entity != null)
            {
                var document = PrepareDocumentForUpdate(entity, out _);
                docBatch.Add(document);

                if (docBatch.Count >= BatchSize)
                {
                    updatedCount += await _engine.UpdateDocumentsAsync(_name, docBatch, cancellationToken).ConfigureAwait(false);
                    docBatch.Clear();
                }
            }
        }

        if (docBatch.Count > 0)
        {
            updatedCount += await _engine.UpdateDocumentsAsync(_name, docBatch, cancellationToken).ConfigureAwait(false);
        }

        return updatedCount;
    }

    private async Task<int> UpdateInTransactionAsync(
        IEnumerable<T> entities,
        Transaction transaction,
        CancellationToken cancellationToken)
    {
        var prepared = new List<(BsonDocument Document, BsonValue Id)>();
        var ids = new List<BsonValue>();

        foreach (var entity in entities)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (entity == null) continue;

            var document = PrepareDocumentForUpdate(entity, out var id);
            prepared.Add((document, id));
            ids.Add(id);
        }

        if (prepared.Count == 0)
        {
            return 0;
        }

        var originalDocuments = await _engine.FindByIdsAsync(_name, ids, cancellationToken).ConfigureAwait(false);
        return RecordPreparedUpdatesInTransaction(prepared, originalDocuments, transaction);
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
            var documentToDelete = await _engine.FindByIdAsync(_name, id, cancellationToken).ConfigureAwait(false);
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
    public async Task<T?> FindByIdAsync(BsonValue id, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        if (id == null || id.IsNull) return null;

        var document = await _engine.FindByIdAsync(_name, id, cancellationToken).ConfigureAwait(false);
        if (document == null) return default;

        if (typeof(T) == typeof(BsonDocument))
        {
            var patched = _engine.MetadataManager.ApplySchemaDefaults(_name, document);
            return (T)(object)patched;
        }

        return AotBsonMapper.FromDocument<T>(document);
    }

    public async Task<List<T>> FindAllAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();

        var documents = await _engine.FindAllAsync(_name, cancellationToken).ConfigureAwait(false);
        var results = new List<T>(documents.Count);

        if (typeof(T) == typeof(BsonDocument))
        {
            foreach (var document in documents)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var patched = _engine.MetadataManager.ApplySchemaDefaults(_name, document);
                results.Add((T)(object)patched);
            }

            return results;
        }

        foreach (var document in documents)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var entity = AotBsonMapper.FromDocument<T>(document);
            if (entity != null)
            {
                results.Add(entity);
            }
        }

        return results;
    }

    public async Task<List<T>> FindAsync(Expression<Func<T, bool>> predicate, CancellationToken cancellationToken = default)
    {
        return await FindAsync(predicate, 0, int.MaxValue, cancellationToken).ConfigureAwait(false);
    }

    public async Task<List<T>> FindAsync(Expression<Func<T, bool>> predicate, int skip, int limit, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        if (predicate == null) throw new ArgumentNullException(nameof(predicate));
        ValidatePaginationArguments(skip, limit);
        cancellationToken.ThrowIfCancellationRequested();
        if (limit == 0) return new List<T>();

        var shape = new QueryShape<T>
        {
            Predicate = predicate,
            PushedWhereCount = 1,
            Skip = skip > 0 ? skip : null,
            Take = limit < int.MaxValue ? limit : null
        };

        var results = limit == int.MaxValue
            ? new List<T>()
            : new List<T>(Math.Max(0, limit));

        await foreach (var item in _queryExecutor.ExecuteShapedAsync(_name, shape, out _, cancellationToken).ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();
            results.Add(item);
        }

        return results;
    }

    public async Task<T?> FindOneAsync(Expression<Func<T, bool>> predicate, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        if (predicate == null) throw new ArgumentNullException(nameof(predicate));
        cancellationToken.ThrowIfCancellationRequested();
        await foreach (var item in _queryExecutor.ExecuteAsync(_name, predicate, cancellationToken).ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();
            return item;
        }

        return default;
    }

    public Task<long> CountAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();

        // 关键修复：如果在事务中，必须使用 FindAllAsync().Count 以包含挂起的操作
        if (_engine.GetCurrentTransaction() is Transaction transaction)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.FromResult(_engine.GetTransactionalDocumentCount(_name, transaction));
        }

        return Task.FromResult((long)_engine.GetCachedDocumentCount(_name));
    }

    public async Task<long> CountAsync(Expression<Func<T, bool>> predicate, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        if (predicate == null) throw new ArgumentNullException(nameof(predicate));
        cancellationToken.ThrowIfCancellationRequested();
        long count = 0;
        await foreach (var _ in _queryExecutor.ExecuteAsync(_name, predicate, cancellationToken).ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();
            count++;
        }

        return count;
    }

    public async Task<bool> ExistsAsync(Expression<Func<T, bool>> predicate, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        if (predicate == null) throw new ArgumentNullException(nameof(predicate));
        cancellationToken.ThrowIfCancellationRequested();
        await foreach (var _ in _queryExecutor.ExecuteAsync(_name, predicate, cancellationToken).ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();
            return true;
        }

        return false;
    }

    public async Task<int> DeleteAllAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();

        var deletedCount = 0;
        var allDocuments = await FindAllAsync(cancellationToken).ConfigureAwait(false);

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
        var documentsToDelete = await FindAsync(predicate, cancellationToken).ConfigureAwait(false);

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

        cancellationToken.ThrowIfCancellationRequested();
        var document = PrepareDocumentForInsert(entity);
        var currentTransaction = _engine.GetCurrentTransaction();
        if (currentTransaction != null)
        {
            var id = document.TryGetValue("_id", out var documentId) ? documentId : BsonNull.Value;
            var existingDocument = id.IsNull
                ? null
                : await _engine.FindByIdAsync(_name, id, cancellationToken).ConfigureAwait(false);
            if (existingDocument == null)
            {
                ((Transaction)currentTransaction).RecordInsert(_name, document);
                return (UpdateType.Insert, 1);
            }

            ((Transaction)currentTransaction).RecordUpdate(_name, existingDocument, document);
            return (UpdateType.Update, 1);
        }

        return await _engine.UpsertDocumentAsync(_name, document, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// 异步插入或更新多个文档
    /// </summary>
    /// <param name="entities">要插入或更新的实体集合</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>插入和更新的文档数量</returns>
    public async Task<(int InsertedCount, int UpdatedCount)> UpsertAsync(IEnumerable<T> entities, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        if (entities == null) throw new ArgumentNullException(nameof(entities));

        var currentTransaction = _engine.GetCurrentTransaction();
        if (currentTransaction != null)
        {
            var transactionInsertedCount = 0;
            var transactionUpdatedCount = 0;
            foreach (var entity in entities)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (entity == null) continue;

                var (updateType, count) = await UpsertAsync(entity, cancellationToken).ConfigureAwait(false);
                if (updateType == UpdateType.Insert)
                {
                    transactionInsertedCount += count;
                }
                else
                {
                    transactionUpdatedCount += count;
                }
            }

            return (transactionInsertedCount, transactionUpdatedCount);
        }

        const int BatchSize = 1000;
        var insertedCount = 0;
        var updatedCount = 0;
        var docBatch = new List<BsonDocument>(BatchSize);

        foreach (var entity in entities)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (entity == null) continue;

            var document = PrepareDocumentForInsert(entity);
            docBatch.Add(document);

            if (docBatch.Count >= BatchSize)
            {
                var result = await _engine.UpsertDocumentsAsync(_name, docBatch, cancellationToken).ConfigureAwait(false);
                insertedCount += result.InsertedCount;
                updatedCount += result.UpdatedCount;
                docBatch.Clear();
            }
        }

        if (docBatch.Count > 0)
        {
            var result = await _engine.UpsertDocumentsAsync(_name, docBatch, cancellationToken).ConfigureAwait(false);
            insertedCount += result.InsertedCount;
            updatedCount += result.UpdatedCount;
        }

        return (insertedCount, updatedCount);
    }

    #endregion

    /// <summary>
    /// 检查是否已释放
    /// </summary>
    private void ThrowIfDisposed()
    {
        if (Volatile.Read(ref _disposed) != 0)
            throw new ObjectDisposedException(nameof(DocumentCollection<T>));
    }

    /// <summary>
    /// 释放资源
    /// </summary>
    public void Dispose()
    {
        Interlocked.Exchange(ref _disposed, 1);
    }

    /// <summary>
    /// 重写 ToString 方法
    /// </summary>
    public override string ToString()
    {
        return $"DocumentCollection<{typeof(T).Name}>[{_name}]";
    }
}
