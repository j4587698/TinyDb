using TinyDb.Bson;

namespace TinyDb.Query;

public enum SqlStatementKind
{
    Select,
    Insert,
    Update,
    Delete
}

public sealed class SqlExecutionResult
{
    internal SqlExecutionResult(SqlStatementKind statementKind, int affectedRows, IReadOnlyList<BsonDocument> documents)
    {
        StatementKind = statementKind;
        AffectedRows = affectedRows;
        Documents = documents ?? Array.Empty<BsonDocument>();
    }

    public SqlStatementKind StatementKind { get; }
    public int AffectedRows { get; }
    public IReadOnlyList<BsonDocument> Documents { get; }
}

public sealed class SqlExecutionResult<T> where T : class
{
    internal SqlExecutionResult(SqlStatementKind statementKind, int affectedRows, IReadOnlyList<T> rows)
    {
        StatementKind = statementKind;
        AffectedRows = affectedRows;
        Rows = rows ?? Array.Empty<T>();
    }

    public SqlStatementKind StatementKind { get; }
    public int AffectedRows { get; }
    public IReadOnlyList<T> Rows { get; }
}
