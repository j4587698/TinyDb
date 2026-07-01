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

internal sealed class SqlQueryParser
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

    private List<SqlOrderBy> ParseOrderBy(string orderText)
    {
        if (string.IsNullOrWhiteSpace(orderText))
        {
            throw Error("ORDER BY clause cannot be empty.");
        }

        var result = new List<SqlOrderBy>();
        foreach (var item in SplitTopLevelComma(orderText))
        {
            var itemParser = new OrderItemParser(item);
            result.Add(itemParser.Parse());
        }

        return result;
    }

    private List<SqlProjectionField> ParseProjection(string projectionText)
    {
        if (string.IsNullOrWhiteSpace(projectionText))
        {
            throw Error("SELECT list cannot be empty.");
        }

        if (projectionText == "*")
        {
            return new List<SqlProjectionField>();
        }

        var result = new List<SqlProjectionField>();
        foreach (var item in SplitTopLevelComma(projectionText))
        {
            if (string.IsNullOrWhiteSpace(item))
            {
                throw Error("SELECT field cannot be empty.");
            }

            var itemParser = new ProjectionItemParser(item);
            result.Add(itemParser.Parse());
        }

        return result;
    }

    private List<SqlAssignment> ParseAssignments(string assignmentsText)
    {
        var result = new List<SqlAssignment>();
        foreach (var item in SplitTopLevelComma(assignmentsText))
        {
            if (string.IsNullOrWhiteSpace(item))
            {
                throw Error("SET assignment cannot be empty.");
            }

            var itemParser = new AssignmentParser(item, _parameters);
            result.Add(itemParser.Parse());
        }

        ValidateNoDuplicateFields(result.Select(static assignment => assignment.FieldPath), "SET clause");
        return result;
    }

    private List<string> ReadFieldList(string clauseName)
    {
        var fields = new List<string>();
        while (true)
        {
            var field = ReadFieldPath();
            ValidateTopLevelFieldPath(field, clauseName);
            fields.Add(field);
            SkipWhitespace();

            if (MatchChar(','))
            {
                continue;
            }

            ExpectChar(')');
            break;
        }

        return fields;
    }

    private List<object?> ReadValueList(string clauseName)
    {
        var values = new List<object?>();
        while (true)
        {
            values.Add(ReadSqlValue());
            SkipWhitespace();

            if (MatchChar(','))
            {
                continue;
            }

            ExpectChar(')');
            break;
        }

        return values;
    }

    private void ValidateTopLevelFieldPath(string fieldPath, string clauseName)
    {
        if (fieldPath.IndexOf('.') >= 0)
        {
            throw Error($"{clauseName} supports top-level fields only.");
        }
    }

    private void ValidateNoDuplicateFields(IEnumerable<string> fields, string clauseName)
    {
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var field in fields)
        {
            if (!seen.Add(field))
            {
                throw Error($"{clauseName} contains duplicate field '{field}'.");
            }
        }
    }

    private string ReadFieldPath()
    {
        var segments = new List<string> { NormalizeIdSegment(ReadIdentifier()) };
        while (MatchChar('.'))
        {
            segments.Add(NormalizeIdSegment(ReadIdentifier()));
        }

        return string.Join(".", segments);
    }

    private object? ReadSqlValue()
    {
        SkipWhitespace();
        if (IsEnd) throw Error("Expected SQL value.");

        if (_text[_position] == '@')
        {
            _position++;
            var parameterName = ReadIdentifier();
            return ResolveParameter(parameterName);
        }

        if (_text[_position] is '\'' or '"')
        {
            return ReadStringLiteral(_text[_position]);
        }

        if (MatchKeyword("null")) return null;
        if (MatchKeyword("true")) return true;
        if (MatchKeyword("false")) return false;

        if (_text[_position] == '-' || char.IsDigit(_text[_position]))
        {
            return ReadNumericLiteral();
        }

        throw Error("Expected SQL literal or parameter.");
    }

    private object? ResolveParameter(string parameterName)
    {
        var normalized = QueryParams.NormalizeName(parameterName);
        if (_parameters.TryGetValue(normalized, out var value) ||
            _parameters.TryGetValue("@" + normalized, out value))
        {
            return value;
        }

        throw new KeyNotFoundException($"Query parameter '@{parameterName}' was not provided.");
    }

    private string ReadStringLiteral(char quote)
    {
        _position++;
        var builder = new System.Text.StringBuilder();
        while (_position < _text.Length)
        {
            var ch = _text[_position++];
            if (ch == quote)
            {
                if (_position < _text.Length && _text[_position] == quote)
                {
                    builder.Append(quote);
                    _position++;
                    continue;
                }

                return builder.ToString();
            }

            if (ch == '\\' && _position < _text.Length)
            {
                builder.Append(DecodeEscapedCharacter(_text[_position++]));
                continue;
            }

            builder.Append(ch);
        }

        throw Error("Unterminated string literal.");
    }

    private object ReadNumericLiteral()
    {
        var start = _position;
        if (_text[_position] == '-') _position++;

        while (_position < _text.Length && char.IsDigit(_text[_position])) _position++;

        var hasFraction = false;
        if (_position < _text.Length && _text[_position] == '.')
        {
            hasFraction = true;
            _position++;
            while (_position < _text.Length && char.IsDigit(_text[_position])) _position++;
        }

        if (_position < _text.Length && (_text[_position] == 'e' || _text[_position] == 'E'))
        {
            hasFraction = true;
            _position++;
            if (_position < _text.Length && (_text[_position] == '+' || _text[_position] == '-')) _position++;
            while (_position < _text.Length && char.IsDigit(_text[_position])) _position++;
        }

        var literal = _text.Substring(start, _position - start);
        return ParseNumericLiteral(literal, hasFraction);
    }

    private static IEnumerable<string> SplitTopLevelComma(string text)
    {
        var start = 0;
        var depth = 0;
        for (var i = 0; i < text.Length; i++)
        {
            var ch = text[i];
            if (ch is '\'' or '"' or '`')
            {
                i = SkipQuoted(text, i, ch);
                continue;
            }

            if (ch == '[')
            {
                i = SkipBracketIdentifier(text, i);
                continue;
            }

            if (ch == '(') depth++;
            else if (ch == ')' && depth > 0) depth--;
            else if (ch == ',' && depth == 0)
            {
                yield return text.Substring(start, i - start).Trim();
                start = i + 1;
            }
        }

        yield return text.Substring(start).Trim();
    }

    private int FindNextWhereClauseStart(int start)
        => FindNextClauseStart(start, includeOrderBy: true, CanParseWhereClausePrefix);

    private int FindNextOrderClauseStart(int start)
        => FindNextClauseStart(start, includeOrderBy: false, CanParseOrderByPrefix);

    private int FindNextClauseStart(int start, bool includeOrderBy, Func<string, bool> canParsePrefix)
    {
        var depth = 0;
        for (var i = start; i < _text.Length; i++)
        {
            var ch = _text[i];
            if (ch is '\'' or '"' or '`')
            {
                i = SkipQuoted(_text, i, ch);
                continue;
            }

            if (ch == '[')
            {
                i = SkipBracketIdentifier(_text, i);
                continue;
            }

            if (ch == '(')
            {
                depth++;
                continue;
            }

            if (ch == ')')
            {
                if (depth > 0) depth--;
                continue;
            }

            if (depth != 0) continue;

            if (includeOrderBy && IsOrderByAt(i) && IsValidClauseBoundary(start, i, canParsePrefix)) return i;
            if (IsKeywordAt(i, "limit") && IsValidClauseBoundary(start, i, canParsePrefix)) return i;
            if (IsKeywordAt(i, "offset") && IsValidClauseBoundary(start, i, canParsePrefix)) return i;
        }

        return _text.Length;
    }

    private bool IsValidClauseBoundary(int start, int position, Func<string, bool> canParsePrefix)
    {
        var prefix = _text.Substring(start, position - start).Trim();
        return prefix.Length > 0 && canParsePrefix(prefix);
    }

    private bool CanParseWhereClausePrefix(string text)
    {
        try
        {
            StringQueryParser.Parse(text, _parameters);
            return true;
        }
        catch (KeyNotFoundException)
        {
            return true;
        }
        catch (FormatException)
        {
            return false;
        }
    }

    private bool CanParseOrderByPrefix(string text)
    {
        try
        {
            ParseOrderBy(text);
            return true;
        }
        catch (FormatException)
        {
            return false;
        }
    }

    private int FindFromClauseStart(int start)
    {
        var depth = 0;
        for (var i = start; i < _text.Length; i++)
        {
            var ch = _text[i];
            if (ch is '\'' or '"' or '`')
            {
                i = SkipQuoted(_text, i, ch);
                continue;
            }

            if (ch == '[')
            {
                i = SkipBracketIdentifier(_text, i);
                continue;
            }

            if (ch == '(')
            {
                depth++;
                continue;
            }

            if (ch == ')')
            {
                if (depth > 0) depth--;
                continue;
            }

            if (depth == 0 && IsKeywordAt(i, "from"))
            {
                return i;
            }
        }

        return -1;
    }

    private int FindWhereClauseStart(int start)
    {
        var depth = 0;
        for (var i = start; i < _text.Length; i++)
        {
            var ch = _text[i];
            if (ch is '\'' or '"' or '`')
            {
                i = SkipQuoted(_text, i, ch);
                continue;
            }

            if (ch == '[')
            {
                i = SkipBracketIdentifier(_text, i);
                continue;
            }

            if (ch == '(')
            {
                depth++;
                continue;
            }

            if (ch == ')')
            {
                if (depth > 0) depth--;
                continue;
            }

            if (depth == 0 && IsKeywordAt(i, "where"))
            {
                return i;
            }
        }

        return -1;
    }

    private bool IsOrderByAt(int position)
    {
        if (!IsKeywordAt(position, "order")) return false;
        var next = position + "order".Length;
        while (next < _text.Length && char.IsWhiteSpace(_text[next])) next++;
        return IsKeywordAt(next, "by");
    }

    private int ReadNonNegativeInt(string clauseName)
    {
        SkipWhitespace();
        if (IsEnd) throw Error($"{clauseName} requires a value.");

        object? value;
        if (_text[_position] == '@')
        {
            _position++;
            var parameterName = ReadIdentifier();
            value = ResolveParameter(parameterName);
        }
        else
        {
            var start = _position;
            while (_position < _text.Length && char.IsDigit(_text[_position])) _position++;
            if (start == _position) throw Error($"{clauseName} requires a non-negative integer.");
            value = int.Parse(_text.Substring(start, _position - start), CultureInfo.InvariantCulture);
        }

        var result = Convert.ToInt32(value, CultureInfo.InvariantCulture);
        if (result < 0) throw Error($"{clauseName} cannot be negative.");
        return result;
    }

    private string ReadIdentifier()
    {
        SkipWhitespace();
        if (IsEnd) throw Error("Expected identifier.");

        var ch = _text[_position];
        if (ch == '[') return ReadBracketIdentifier();
        if (ch == '`') return ReadQuotedIdentifier('`');

        if (!IsIdentifierStart(ch)) throw Error("Expected identifier.");
        var start = _position++;
        while (_position < _text.Length && IsIdentifierPart(_text[_position])) _position++;
        return _text.Substring(start, _position - start);
    }

    private string ReadBracketIdentifier()
    {
        _position++;
        var start = _position;
        while (_position < _text.Length && _text[_position] != ']') _position++;
        if (_position >= _text.Length) throw Error("Unterminated bracket identifier.");
        var value = _text.Substring(start, _position - start);
        _position++;
        return value;
    }

    private string ReadQuotedIdentifier(char quote)
    {
        _position++;
        var start = _position;
        while (_position < _text.Length && _text[_position] != quote) _position++;
        if (_position >= _text.Length) throw Error("Unterminated quoted identifier.");
        var value = _text.Substring(start, _position - start);
        _position++;
        return value;
    }

    private bool MatchChar(char ch)
    {
        SkipWhitespace();
        if (IsEnd || _text[_position] != ch) return false;
        _position++;
        return true;
    }

    private void ExpectChar(char ch)
    {
        if (!MatchChar(ch))
        {
            throw Error($"Expected '{ch}'.");
        }
    }

    private bool MatchKeyword(string keyword)
    {
        SkipWhitespace();
        if (!IsKeywordAt(_position, keyword)) return false;
        _position += keyword.Length;
        return true;
    }

    private void ExpectKeyword(string keyword)
    {
        if (!MatchKeyword(keyword))
        {
            throw Error($"Expected {keyword.ToUpperInvariant()}.");
        }
    }

    private bool IsKeywordAt(int position, string keyword)
    {
        if (position < 0 || position + keyword.Length > _text.Length) return false;
        if (!_text.AsSpan(position, keyword.Length).Equals(keyword.AsSpan(), StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        var before = position == 0 ? '\0' : _text[position - 1];
        var afterPosition = position + keyword.Length;
        var after = afterPosition >= _text.Length ? '\0' : _text[afterPosition];
        return !IsIdentifierPart(before) && !IsIdentifierPart(after);
    }

    private static int SkipQuoted(string text, int start, char quote)
    {
        for (var i = start + 1; i < text.Length; i++)
        {
            if (text[i] == '\\')
            {
                i++;
                continue;
            }

            if (text[i] == quote)
            {
                if (i + 1 < text.Length && text[i + 1] == quote)
                {
                    i++;
                    continue;
                }

                return i;
            }
        }

        return text.Length - 1;
    }

    private static int SkipBracketIdentifier(string text, int start)
    {
        for (var i = start + 1; i < text.Length; i++)
        {
            if (text[i] == ']') return i;
        }

        return text.Length - 1;
    }

    private void SkipWhitespace()
    {
        while (_position < _text.Length && char.IsWhiteSpace(_text[_position])) _position++;
    }

    private bool IsEnd => _position >= _text.Length;

    private string ReadRemainingPreview()
    {
        var remaining = _text.Substring(_position).TrimStart();
        return remaining.Length <= 24 ? remaining : remaining.Substring(0, 24);
    }

    private static bool IsIdentifierStart(char ch)
        => char.IsLetter(ch) || ch == '_' || ch == '$';

    private static bool IsIdentifierPart(char ch)
        => char.IsLetterOrDigit(ch) || ch == '_' || ch == '$';

    private FormatException Error(string message)
        => new($"Invalid SQL at position {_position}: {message}");

    private static object ParseNumericLiteral(string literal, bool hasFraction)
    {
        if (hasFraction)
        {
            if (decimal.TryParse(literal, NumberStyles.Number, CultureInfo.InvariantCulture, out var decimalValue))
            {
                return decimalValue;
            }

            return double.Parse(literal, NumberStyles.Float, CultureInfo.InvariantCulture);
        }

        if (int.TryParse(literal, NumberStyles.Integer, CultureInfo.InvariantCulture, out var intValue))
        {
            return intValue;
        }

        return long.Parse(literal, NumberStyles.Integer, CultureInfo.InvariantCulture);
    }

    private static char DecodeEscapedCharacter(char escaped)
        => escaped switch
        {
            'n' => '\n',
            'r' => '\r',
            't' => '\t',
            '\\' => '\\',
            '\'' => '\'',
            '"' => '"',
            _ => escaped
        };

    private sealed class OrderItemParser
    {
        private readonly string _text;
        private int _position;

        public OrderItemParser(string text)
        {
            _text = text ?? throw new ArgumentNullException(nameof(text));
        }

        public SqlOrderBy Parse()
        {
            SkipWhitespace();
            var field = ReadFieldPath();
            SkipWhitespace();

            var descending = false;
            if (MatchKeyword("asc"))
            {
                descending = false;
            }
            else if (MatchKeyword("desc"))
            {
                descending = true;
            }

            SkipWhitespace();
            if (_position != _text.Length)
            {
                throw new FormatException($"Invalid ORDER BY item '{_text}'.");
            }

            return new SqlOrderBy(field, descending);
        }

        private string ReadFieldPath()
        {
            var segments = new List<string> { ReadIdentifier() };
            while (MatchChar('.'))
            {
                segments.Add(ReadIdentifier());
            }

            return string.Join(".", segments.Select(NormalizeIdSegment));
        }

        private string ReadIdentifier()
        {
            SkipWhitespace();
            if (_position >= _text.Length) throw new FormatException("Expected ORDER BY field.");

            var ch = _text[_position];
            if (ch == '[') return ReadBracketIdentifier();
            if (ch == '`') return ReadQuotedIdentifier('`');

            if (!IsIdentifierStart(ch)) throw new FormatException("Expected ORDER BY field.");
            var start = _position++;
            while (_position < _text.Length && IsIdentifierPart(_text[_position])) _position++;
            return _text.Substring(start, _position - start);
        }

        private string ReadBracketIdentifier()
        {
            _position++;
            var start = _position;
            while (_position < _text.Length && _text[_position] != ']') _position++;
            if (_position >= _text.Length) throw new FormatException("Unterminated bracket identifier.");
            var value = _text.Substring(start, _position - start);
            _position++;
            return value;
        }

        private string ReadQuotedIdentifier(char quote)
        {
            _position++;
            var start = _position;
            while (_position < _text.Length && _text[_position] != quote) _position++;
            if (_position >= _text.Length) throw new FormatException("Unterminated quoted identifier.");
            var value = _text.Substring(start, _position - start);
            _position++;
            return value;
        }

        private bool MatchChar(char ch)
        {
            SkipWhitespace();
            if (_position >= _text.Length || _text[_position] != ch) return false;
            _position++;
            return true;
        }

        private bool MatchKeyword(string keyword)
        {
            SkipWhitespace();
            if (_position + keyword.Length > _text.Length) return false;
            if (!_text.AsSpan(_position, keyword.Length).Equals(keyword.AsSpan(), StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }

            var before = _position == 0 ? '\0' : _text[_position - 1];
            var afterPosition = _position + keyword.Length;
            var after = afterPosition >= _text.Length ? '\0' : _text[afterPosition];
            if (IsIdentifierPart(before) || IsIdentifierPart(after)) return false;

            _position += keyword.Length;
            return true;
        }

        private void SkipWhitespace()
        {
            while (_position < _text.Length && char.IsWhiteSpace(_text[_position])) _position++;
        }
    }

    private sealed class ProjectionItemParser
    {
        private readonly string _text;
        private int _position;

        public ProjectionItemParser(string text)
        {
            _text = text ?? throw new ArgumentNullException(nameof(text));
        }

        public SqlProjectionField Parse()
        {
            SkipWhitespace();
            var field = ReadFieldPath(out var defaultOutputName);
            var outputName = defaultOutputName;
            SkipWhitespace();

            if (MatchKeyword("as"))
            {
                outputName = ReadIdentifier();
                SkipWhitespace();
            }

            if (_position != _text.Length)
            {
                throw new FormatException($"Invalid SELECT field '{_text}'. Use AS for aliases.");
            }

            return new SqlProjectionField(field, outputName);
        }

        private string ReadFieldPath(out string defaultOutputName)
        {
            var segment = ReadIdentifier();
            var defaultOutputSegment = segment;
            var segments = new List<string> { NormalizeIdSegment(segment) };
            while (MatchChar('.'))
            {
                segment = ReadIdentifier();
                defaultOutputSegment = segment;
                segments.Add(NormalizeIdSegment(segment));
            }

            defaultOutputName = defaultOutputSegment;
            return string.Join(".", segments);
        }

        private string ReadIdentifier()
        {
            SkipWhitespace();
            if (_position >= _text.Length) throw new FormatException("Expected SELECT field.");

            var ch = _text[_position];
            if (ch == '[') return ReadBracketIdentifier();
            if (ch == '`') return ReadQuotedIdentifier('`');

            if (!IsIdentifierStart(ch)) throw new FormatException("Expected SELECT field.");
            var start = _position++;
            while (_position < _text.Length && IsIdentifierPart(_text[_position])) _position++;
            return _text.Substring(start, _position - start);
        }

        private string ReadBracketIdentifier()
        {
            _position++;
            var start = _position;
            while (_position < _text.Length && _text[_position] != ']') _position++;
            if (_position >= _text.Length) throw new FormatException("Unterminated bracket identifier.");
            var value = _text.Substring(start, _position - start);
            _position++;
            return value;
        }

        private string ReadQuotedIdentifier(char quote)
        {
            _position++;
            var start = _position;
            while (_position < _text.Length && _text[_position] != quote) _position++;
            if (_position >= _text.Length) throw new FormatException("Unterminated quoted identifier.");
            var value = _text.Substring(start, _position - start);
            _position++;
            return value;
        }

        private bool MatchChar(char ch)
        {
            SkipWhitespace();
            if (_position >= _text.Length || _text[_position] != ch) return false;
            _position++;
            return true;
        }

        private bool MatchKeyword(string keyword)
        {
            SkipWhitespace();
            if (_position + keyword.Length > _text.Length) return false;
            if (!_text.AsSpan(_position, keyword.Length).Equals(keyword.AsSpan(), StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }

            var before = _position == 0 ? '\0' : _text[_position - 1];
            var afterPosition = _position + keyword.Length;
            var after = afterPosition >= _text.Length ? '\0' : _text[afterPosition];
            if (IsIdentifierPart(before) || IsIdentifierPart(after)) return false;

            _position += keyword.Length;
            return true;
        }

        private void SkipWhitespace()
        {
            while (_position < _text.Length && char.IsWhiteSpace(_text[_position])) _position++;
        }
    }

    private sealed class AssignmentParser
    {
        private readonly string _text;
        private readonly IReadOnlyDictionary<string, object?> _parameters;
        private int _position;

        public AssignmentParser(string text, IReadOnlyDictionary<string, object?> parameters)
        {
            _text = text ?? throw new ArgumentNullException(nameof(text));
            _parameters = parameters ?? throw new ArgumentNullException(nameof(parameters));
        }

        public SqlAssignment Parse()
        {
            SkipWhitespace();
            var field = ReadFieldPath();
            if (field.IndexOf('.') >= 0)
            {
                throw new FormatException("SQL UPDATE supports top-level fields only.");
            }

            SkipWhitespace();
            if (!MatchChar('='))
            {
                throw new FormatException($"Invalid SET assignment '{_text}'. Expected '='.");
            }

            var value = ReadSqlValue();
            SkipWhitespace();
            if (_position != _text.Length)
            {
                throw new FormatException($"Invalid SET assignment '{_text}'.");
            }

            return new SqlAssignment(field, value);
        }

        private string ReadFieldPath()
        {
            var segments = new List<string> { NormalizeIdSegment(ReadIdentifier()) };
            while (MatchChar('.'))
            {
                segments.Add(NormalizeIdSegment(ReadIdentifier()));
            }

            return string.Join(".", segments);
        }

        private object? ReadSqlValue()
        {
            SkipWhitespace();
            if (_position >= _text.Length) throw new FormatException("Expected SQL value.");

            if (_text[_position] == '@')
            {
                _position++;
                var parameterName = ReadIdentifier();
                return ResolveParameter(parameterName);
            }

            if (_text[_position] is '\'' or '"')
            {
                return ReadStringLiteral(_text[_position]);
            }

            if (MatchKeyword("null")) return null;
            if (MatchKeyword("true")) return true;
            if (MatchKeyword("false")) return false;

            if (_text[_position] == '-' || char.IsDigit(_text[_position]))
            {
                return ReadNumericLiteral();
            }

            throw new FormatException("Expected SQL literal or parameter.");
        }

        private object? ResolveParameter(string parameterName)
        {
            var normalized = QueryParams.NormalizeName(parameterName);
            if (_parameters.TryGetValue(normalized, out var value) ||
                _parameters.TryGetValue("@" + normalized, out value))
            {
                return value;
            }

            throw new KeyNotFoundException($"Query parameter '@{parameterName}' was not provided.");
        }

        private string ReadStringLiteral(char quote)
        {
            _position++;
            var builder = new System.Text.StringBuilder();
            while (_position < _text.Length)
            {
                var ch = _text[_position++];
                if (ch == quote)
                {
                    if (_position < _text.Length && _text[_position] == quote)
                    {
                        builder.Append(quote);
                        _position++;
                        continue;
                    }

                    return builder.ToString();
                }

                if (ch == '\\' && _position < _text.Length)
                {
                    builder.Append(DecodeEscapedCharacter(_text[_position++]));
                    continue;
                }

                builder.Append(ch);
            }

            throw new FormatException("Unterminated string literal.");
        }

        private object ReadNumericLiteral()
        {
            var start = _position;
            if (_text[_position] == '-') _position++;

            while (_position < _text.Length && char.IsDigit(_text[_position])) _position++;

            var hasFraction = false;
            if (_position < _text.Length && _text[_position] == '.')
            {
                hasFraction = true;
                _position++;
                while (_position < _text.Length && char.IsDigit(_text[_position])) _position++;
            }

            if (_position < _text.Length && (_text[_position] == 'e' || _text[_position] == 'E'))
            {
                hasFraction = true;
                _position++;
                if (_position < _text.Length && (_text[_position] == '+' || _text[_position] == '-')) _position++;
                while (_position < _text.Length && char.IsDigit(_text[_position])) _position++;
            }

            var literal = _text.Substring(start, _position - start);
            return ParseNumericLiteral(literal, hasFraction);
        }

        private string ReadIdentifier()
        {
            SkipWhitespace();
            if (_position >= _text.Length) throw new FormatException("Expected identifier.");

            var ch = _text[_position];
            if (ch == '[') return ReadBracketIdentifier();
            if (ch == '`') return ReadQuotedIdentifier('`');

            if (!IsIdentifierStart(ch)) throw new FormatException("Expected identifier.");
            var start = _position++;
            while (_position < _text.Length && IsIdentifierPart(_text[_position])) _position++;
            return _text.Substring(start, _position - start);
        }

        private string ReadBracketIdentifier()
        {
            _position++;
            var start = _position;
            while (_position < _text.Length && _text[_position] != ']') _position++;
            if (_position >= _text.Length) throw new FormatException("Unterminated bracket identifier.");
            var value = _text.Substring(start, _position - start);
            _position++;
            return value;
        }

        private string ReadQuotedIdentifier(char quote)
        {
            _position++;
            var start = _position;
            while (_position < _text.Length && _text[_position] != quote) _position++;
            if (_position >= _text.Length) throw new FormatException("Unterminated quoted identifier.");
            var value = _text.Substring(start, _position - start);
            _position++;
            return value;
        }

        private bool MatchChar(char ch)
        {
            SkipWhitespace();
            if (_position >= _text.Length || _text[_position] != ch) return false;
            _position++;
            return true;
        }

        private bool MatchKeyword(string keyword)
        {
            SkipWhitespace();
            if (_position + keyword.Length > _text.Length) return false;
            if (!_text.AsSpan(_position, keyword.Length).Equals(keyword.AsSpan(), StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }

            var before = _position == 0 ? '\0' : _text[_position - 1];
            var afterPosition = _position + keyword.Length;
            var after = afterPosition >= _text.Length ? '\0' : _text[afterPosition];
            if (IsIdentifierPart(before) || IsIdentifierPart(after)) return false;

            _position += keyword.Length;
            return true;
        }

        private void SkipWhitespace()
        {
            while (_position < _text.Length && char.IsWhiteSpace(_text[_position])) _position++;
        }
    }
}
