using System;
using System.Linq;
using System.Linq.Expressions;
using TinyDb.Bson;

namespace TinyDb.Collections;

public sealed partial class DocumentCollection<T> where T : class
{
    /// <summary>
    /// 删除所有文档
    /// </summary>
    /// <returns>删除的文档数量</returns>
    public int DeleteAll()
    {
        ThrowIfDisposed();

        var deletedCount = 0;
        foreach (var id in _engine.FindAllIds(_name))
        {
            deletedCount += Delete(id);
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
        foreach (var entity in _queryExecutor.ExecuteFullTableScan<T>(_name, predicate))
        {
            var id = GetEntityId(entity);
            if (id != null && !id.IsNull)
            {
                deletedCount += Delete(id);
            }
        }

        return deletedCount;
    }

}
