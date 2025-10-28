using SimpleDb.Bson;
using SimpleDb.Core;
using SimpleDb.Index;

namespace SimpleDb.Collections;

/// <summary>
/// LiteDB 风格的集合接口
/// </summary>
/// <typeparam name="T">文档类型</typeparam>
public interface ILiteCollection<T> where T : class
{
    /// <summary>
    /// 集合名称
    /// </summary>
    string CollectionName { get; }

    /// <summary>
    /// 插入单个文档
    /// </summary>
    /// <param name="entity">要插入的实体</param>
    /// <returns>插入文档的ID</returns>
    BsonValue Insert(T entity);

    /// <summary>
    /// 插入多个文档
    /// </summary>
    /// <param name="entities">要插入的实体集合</param>
    /// <returns>插入的文档数量</returns>
    int Insert(IEnumerable<T> entities);

    /// <summary>
    /// 更新文档
    /// </summary>
    /// <param name="entity">要更新的实体</param>
    /// <returns>更新的文档数量</returns>
    int Update(T entity);

    /// <summary>
    /// 更新多个文档
    /// </summary>
    /// <param name="entities">要更新的实体集合</param>
    /// <returns>更新的文档数量</returns>
    int Update(IEnumerable<T> entities);

    /// <summary>
    /// 删除文档
    /// </summary>
    /// <param name="id">要删除的文档ID</param>
    /// <returns>删除的文档数量</returns>
    int Delete(BsonValue id);

    /// <summary>
    /// 删除多个文档
    /// </summary>
    /// <param name="ids">要删除的文档ID集合</param>
    /// <returns>删除的文档数量</returns>
    int Delete(IEnumerable<BsonValue> ids);

    /// <summary>
    /// 根据ID查找文档
    /// </summary>
    /// <param name="id">文档ID</param>
    /// <returns>找到的文档，如果不存在则返回null</returns>
    T? FindById(BsonValue id);

    /// <summary>
    /// 查找所有文档
    /// </summary>
    /// <returns>所有文档的集合</returns>
    IEnumerable<T> FindAll();

    /// <summary>
    /// 根据查询条件查找文档
    /// </summary>
    /// <param name="predicate">查询条件</param>
    /// <returns>匹配的文档集合</returns>
    IEnumerable<T> Find(System.Linq.Expressions.Expression<Func<T, bool>> predicate);

    /// <summary>
    /// 查找单个文档
    /// </summary>
    /// <param name="predicate">查询条件</param>
    /// <returns>找到的第一个文档，如果不存在则返回null</returns>
    T? FindOne(System.Linq.Expressions.Expression<Func<T, bool>> predicate);

    /// <summary>
    /// 获取可查询对象
    /// </summary>
    /// <returns>可查询对象</returns>
    IQueryable<T> Query();

    /// <summary>
    /// 统计文档数量
    /// </summary>
    /// <returns>文档数量</returns>
    long Count();

    /// <summary>
    /// 根据条件统计文档数量
    /// </summary>
    /// <param name="predicate">查询条件</param>
    /// <returns>匹配的文档数量</returns>
    long Count(System.Linq.Expressions.Expression<Func<T, bool>> predicate);

    /// <summary>
    /// 检查是否存在匹配的文档
    /// </summary>
    /// <param name="predicate">查询条件</param>
    /// <returns>是否存在匹配的文档</returns>
    bool Exists(System.Linq.Expressions.Expression<Func<T, bool>> predicate);

    /// <summary>
    /// 删除所有文档
    /// </summary>
    /// <returns>删除的文档数量</returns>
    int DeleteAll();

    /// <summary>
    /// 根据条件删除文档
    /// </summary>
    /// <param name="predicate">查询条件</param>
    /// <returns>删除的文档数量</returns>
    int DeleteMany(System.Linq.Expressions.Expression<Func<T, bool>> predicate);

    /// <summary>
    /// 插入或更新文档（如果存在ID则更新，否则插入）
    /// </summary>
    /// <param name="entity">要插入或更新的实体</param>
    /// <returns>操作类型和影响的文档数量</returns>
    (UpdateType UpdateType, int Count) Upsert(T entity);

    /// <summary>
    /// 获取数据库引擎实例
    /// </summary>
    /// <returns>数据库引擎实例</returns>
    SimpleDbEngine Database { get; }

    /// <summary>
    /// 获取索引管理器
    /// </summary>
    /// <returns>索引管理器实例</returns>
    IndexManager GetIndexManager();
}

/// <summary>
/// 更新类型枚举
/// </summary>
public enum UpdateType
{
    /// <summary>
    /// 插入了新文档
    /// </summary>
    Insert,

    /// <summary>
    /// 更新了现有文档
    /// </summary>
    Update
}

/// <summary>
/// 文档集合的内部接口
/// </summary>
internal interface IDocumentCollection : IDisposable
{
    /// <summary>
    /// 集合名称
    /// </summary>
    string Name { get; }

    /// <summary>
    /// 文档类型
    /// </summary>
    Type DocumentType { get; }
}