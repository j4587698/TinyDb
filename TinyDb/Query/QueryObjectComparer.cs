using System.Collections.Generic;

namespace TinyDb.Query;

internal sealed class QueryObjectComparer : IComparer<object>
{
    public static QueryObjectComparer Instance { get; } = new();

    private QueryObjectComparer()
    {
    }

    public int Compare(object? x, object? y)
    {
        return QueryValueComparer.Compare(x, y);
    }
}
