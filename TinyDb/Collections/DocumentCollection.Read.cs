using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using TinyDb.Bson;
using TinyDb.Core;
using TinyDb.Query;
using TinyDb.Serialization;

namespace TinyDb.Collections;

public sealed partial class DocumentCollection<T> where T : class
{
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
        return FindAllIterator();
    }

    private IEnumerable<T> FindAllIterator()
    {
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

        totalCount = _queryExecutor.Count(_name, predicate);
        if (limit == 0)
        {
            return Enumerable.Empty<T>();
        }

        var result = _queryExecutor.Execute<T>(_name, predicate);
        if (skip > 0)
        {
            result = result.Skip(skip);
        }

        if (limit < int.MaxValue)
        {
            result = result.Take(limit);
        }

        return result.ToList();
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

}
