using System.Globalization;

namespace TinyDb.Query;
internal sealed partial class SqlQueryParser
{
    private readonly string _text;
    private readonly IReadOnlyDictionary<string, object?> _parameters;
    private int _position;

    private SqlQueryParser(string sql, IReadOnlyDictionary<string, object?>? parameters)
    {
        if (string.IsNullOrWhiteSpace(sql))
        {
            throw new ArgumentException("SQL cannot be null or empty.", nameof(sql));
        }

        _text = TrimTerminator(sql);
        _parameters = parameters ?? new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
    }

    public static SqlStatement Parse(string sql, IReadOnlyDictionary<string, object?>? parameters = null)
    {
        var parser = new SqlQueryParser(sql, parameters);
        return parser.ParseStatement();
    }

    public static SqlQuerySpec ParseSelect(string sql, IReadOnlyDictionary<string, object?>? parameters = null)
    {
        var statement = Parse(sql, parameters);
        return statement as SqlQuerySpec
            ?? throw new FormatException($"Expected SELECT statement but found {statement.Kind}.");
    }

    private SqlStatement ParseStatement()
    {
        SkipWhitespace();
        if (IsKeywordAt(_position, "select")) return ParseSelect();
        if (IsKeywordAt(_position, "insert")) return ParseInsert();
        if (IsKeywordAt(_position, "update")) return ParseUpdate();
        if (IsKeywordAt(_position, "delete")) return ParseDelete();

        throw Error("Expected SELECT, INSERT, UPDATE, or DELETE.");
    }

    private SqlQuerySpec ParseSelect()
    {
        ExpectKeyword("select");
        SkipWhitespace();

        var projectionStart = _position;
        var fromStart = FindFromClauseStart(projectionStart);
        if (fromStart < 0)
        {
            throw Error("Expected FROM.");
        }

        var projectionText = _text.Substring(projectionStart, fromStart - projectionStart).Trim();
        var projection = ParseProjection(projectionText);
        _position = fromStart;
        ExpectKeyword("from");
        var collectionName = ReadIdentifier();

        QueryExpression? predicate = null;
        List<SqlOrderBy>? orderBy = null;
        int skip = 0;
        int limit = int.MaxValue;

        while (true)
        {
            SkipWhitespace();
            if (IsEnd) break;

            if (MatchKeyword("where"))
            {
                if (predicate != null) throw Error("Duplicate WHERE clause.");

                var start = _position;
                var end = FindNextWhereClauseStart(start);
                var whereText = _text.Substring(start, end - start).Trim();
                if (whereText.Length == 0) throw Error("WHERE clause cannot be empty.");

                predicate = StringQueryParser.Parse(whereText, _parameters);
                _position = end;
                continue;
            }

            if (MatchKeyword("order"))
            {
                ExpectKeyword("by");
                if (orderBy != null) throw Error("Duplicate ORDER BY clause.");

                var start = _position;
                var end = FindNextOrderClauseStart(start);
                var orderText = _text.Substring(start, end - start).Trim();
                orderBy = ParseOrderBy(orderText);
                _position = end;
                continue;
            }

            if (MatchKeyword("limit"))
            {
                limit = ReadNonNegativeInt("LIMIT");
                continue;
            }

            if (MatchKeyword("offset"))
            {
                skip = ReadNonNegativeInt("OFFSET");
                continue;
            }

            throw Error($"Unexpected SQL token near '{ReadRemainingPreview()}'.");
        }

        return new SqlQuerySpec(collectionName, predicate, projection, orderBy ?? new List<SqlOrderBy>(), skip, limit);
    }

    private SqlInsertStatement ParseInsert()
    {
        ExpectKeyword("insert");
        ExpectKeyword("into");
        var collectionName = ReadIdentifier();

        ExpectChar('(');
        var fields = ReadFieldList("INSERT field list");
        ExpectKeyword("values");
        ExpectChar('(');
        var values = ReadValueList("VALUES list");

        SkipWhitespace();
        if (!IsEnd) throw Error($"Unexpected SQL token near '{ReadRemainingPreview()}'.");
        if (fields.Count != values.Count)
        {
            throw Error("INSERT field count must match VALUES count.");
        }

        ValidateNoDuplicateFields(fields, "INSERT field list");

        var assignments = new List<SqlAssignment>(fields.Count);
        for (var i = 0; i < fields.Count; i++)
        {
            assignments.Add(new SqlAssignment(fields[i], values[i]));
        }

        return new SqlInsertStatement(collectionName, assignments);
    }

    private SqlUpdateStatement ParseUpdate()
    {
        ExpectKeyword("update");
        var collectionName = ReadIdentifier();
        ExpectKeyword("set");

        var start = _position;
        var whereStart = FindWhereClauseStart(start);
        var assignmentsText = whereStart < 0
            ? _text.Substring(start).Trim()
            : _text.Substring(start, whereStart - start).Trim();
        if (assignmentsText.Length == 0) throw Error("SET clause cannot be empty.");

        var assignments = ParseAssignments(assignmentsText);
        QueryExpression? predicate = null;
        if (whereStart >= 0)
        {
            _position = whereStart;
            ExpectKeyword("where");
            var whereText = _text.Substring(_position).Trim();
            if (whereText.Length == 0) throw Error("WHERE clause cannot be empty.");
            predicate = StringQueryParser.Parse(whereText, _parameters);
            _position = _text.Length;
        }
        else
        {
            _position = _text.Length;
        }

        return new SqlUpdateStatement(collectionName, assignments, predicate);
    }

    private SqlDeleteStatement ParseDelete()
    {
        ExpectKeyword("delete");
        ExpectKeyword("from");
        var collectionName = ReadIdentifier();

        QueryExpression? predicate = null;
        SkipWhitespace();
        if (!IsEnd)
        {
            ExpectKeyword("where");
            var whereText = _text.Substring(_position).Trim();
            if (whereText.Length == 0) throw Error("WHERE clause cannot be empty.");
            predicate = StringQueryParser.Parse(whereText, _parameters);
            _position = _text.Length;
        }

        return new SqlDeleteStatement(collectionName, predicate);
    }

    private static string TrimTerminator(string sql)
    {
        var text = sql.Trim();
        return text.EndsWith(";", StringComparison.Ordinal)
            ? text.Substring(0, text.Length - 1).TrimEnd()
            : text;
    }

    private static string NormalizeIdSegment(string segment)
    {
        return string.Equals(segment, "_id", StringComparison.OrdinalIgnoreCase) ||
               string.Equals(segment, "id", StringComparison.OrdinalIgnoreCase)
            ? "Id"
            : segment;
    }

}
