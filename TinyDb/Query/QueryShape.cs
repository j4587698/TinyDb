using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;

namespace TinyDb.Query;

internal sealed class QueryShape<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>
    where T : class, new()
{
    public Expression<Func<T, bool>>? Predicate { get; init; }
    public int PushedWhereCount { get; init; }
    public IReadOnlyList<QuerySortField> Sort { get; init; } = Array.Empty<QuerySortField>();
    public int? Skip { get; init; }
    public int? Take { get; init; }

    public bool HasTypeShapingOperator { get; init; }
}

internal readonly struct QuerySortField
{
    public string FieldName { get; }
    public Type MemberType { get; }
    public bool Descending { get; }

    public QuerySortField(string fieldName, Type memberType, bool descending)
    {
        FieldName = fieldName ?? throw new ArgumentNullException(nameof(fieldName));
        MemberType = memberType ?? throw new ArgumentNullException(nameof(memberType));
        Descending = descending;
    }
}

internal readonly struct QueryPushdownInfo
{
    public int WherePushedCount { get; init; }
    public int OrderPushedCount { get; init; }
    public int SkipPushedCount { get; init; }
    public int TakePushedCount { get; init; }

    public bool WherePushed => WherePushedCount > 0;
    public bool OrderPushed => OrderPushedCount > 0;
    public bool SkipPushed => SkipPushedCount > 0;
    public bool TakePushed => TakePushedCount > 0;
}
