using System.Globalization;

namespace TinyDb.Query;


internal abstract class SqlStatement
{
    protected SqlStatement(SqlStatementKind kind, string collectionName)
    {
        Kind = kind;
        CollectionName = collectionName ?? throw new ArgumentNullException(nameof(collectionName));
    }

    public SqlStatementKind Kind { get; }
    public string CollectionName { get; }
}

internal sealed class SqlQuerySpec : SqlStatement
{
    public SqlQuerySpec(
        string collectionName,
        QueryExpression? predicate,
        IReadOnlyList<SqlProjectionField> projection,
        IReadOnlyList<SqlOrderBy> orderBy,
        int skip,
        int limit)
        : base(SqlStatementKind.Select, collectionName)
    {
        Predicate = predicate;
        Projection = projection ?? Array.Empty<SqlProjectionField>();
        OrderBy = orderBy ?? Array.Empty<SqlOrderBy>();
        Skip = skip;
        Limit = limit;
    }

    public QueryExpression? Predicate { get; }
    public IReadOnlyList<SqlProjectionField> Projection { get; }
    public IReadOnlyList<SqlOrderBy> OrderBy { get; }
    public int Skip { get; }
    public int Limit { get; }
    public bool SelectAll => Projection.Count == 0;
}

internal sealed class SqlInsertStatement : SqlStatement
{
    public SqlInsertStatement(string collectionName, IReadOnlyList<SqlAssignment> assignments)
        : base(SqlStatementKind.Insert, collectionName)
    {
        Assignments = assignments ?? throw new ArgumentNullException(nameof(assignments));
    }

    public IReadOnlyList<SqlAssignment> Assignments { get; }
}

internal sealed class SqlUpdateStatement : SqlStatement
{
    public SqlUpdateStatement(string collectionName, IReadOnlyList<SqlAssignment> assignments, QueryExpression? predicate)
        : base(SqlStatementKind.Update, collectionName)
    {
        Assignments = assignments ?? throw new ArgumentNullException(nameof(assignments));
        Predicate = predicate;
    }

    public IReadOnlyList<SqlAssignment> Assignments { get; }
    public QueryExpression? Predicate { get; }
}

internal sealed class SqlDeleteStatement : SqlStatement
{
    public SqlDeleteStatement(string collectionName, QueryExpression? predicate)
        : base(SqlStatementKind.Delete, collectionName)
    {
        Predicate = predicate;
    }

    public QueryExpression? Predicate { get; }
}

internal readonly struct SqlAssignment
{
    public SqlAssignment(string fieldPath, object? value)
    {
        FieldPath = fieldPath ?? throw new ArgumentNullException(nameof(fieldPath));
        Value = value;
    }

    public string FieldPath { get; }
    public object? Value { get; }
}

internal readonly struct SqlProjectionField
{
    public SqlProjectionField(string fieldPath, string outputName)
    {
        FieldPath = fieldPath ?? throw new ArgumentNullException(nameof(fieldPath));
        OutputName = outputName ?? throw new ArgumentNullException(nameof(outputName));
    }

    public string FieldPath { get; }
    public string OutputName { get; }
}

internal readonly struct SqlOrderBy
{
    public SqlOrderBy(string fieldPath, bool descending)
    {
        FieldPath = fieldPath ?? throw new ArgumentNullException(nameof(fieldPath));
        Descending = descending;
    }

    public string FieldPath { get; }
    public bool Descending { get; }
}
