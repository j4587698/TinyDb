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
        var ids = _engine.FindAllIds(_name).ToList();

        foreach (var id in ids)
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

}
