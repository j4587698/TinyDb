using System.Globalization;

namespace TinyDb.Query;

public static class QueryParams
{
    public static IReadOnlyDictionary<string, object?> Create(params (string Name, object? Value)[] values)
    {
        if (values == null) throw new ArgumentNullException(nameof(values));

        var result = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        foreach (var (name, value) in values)
        {
            if (string.IsNullOrWhiteSpace(name))
            {
                throw new ArgumentException("Parameter name cannot be null or empty.", nameof(values));
            }

            result[NormalizeName(name)] = value;
        }

        return result;
    }

    internal static string NormalizeName(string name)
        => name.Length > 0 && name[0] == '@' ? name.Substring(1) : name;
}

internal sealed class StringQueryParser
{
    private readonly string _text;
    private readonly IReadOnlyDictionary<string, object?> _parameters;
    private int _position;
    private Token _current;

    private StringQueryParser(string text, IReadOnlyDictionary<string, object?>? parameters)
    {
        _text = text ?? throw new ArgumentNullException(nameof(text));
        _parameters = parameters ?? new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        _current = NextToken();
    }

    public static QueryExpression Parse(string predicate, IReadOnlyDictionary<string, object?>? parameters = null)
    {
        if (string.IsNullOrWhiteSpace(predicate))
        {
            throw new ArgumentException("Predicate cannot be null or empty.", nameof(predicate));
        }

        var parser = new StringQueryParser(predicate, parameters);
        var expression = parser.ParseOr();
        parser.Expect(TokenKind.End);
        return expression;
    }

    private QueryExpression ParseOr()
    {
        var left = ParseAnd();
        while (MatchKeyword("or"))
        {
            var right = ParseAnd();
            left = new BinaryExpression(System.Linq.Expressions.ExpressionType.OrElse, left, right);
        }

        return left;
    }

    private QueryExpression ParseAnd()
    {
        var left = ParseNot();
        while (MatchKeyword("and"))
        {
            var right = ParseNot();
            left = new BinaryExpression(System.Linq.Expressions.ExpressionType.AndAlso, left, right);
        }

        return left;
    }

    private QueryExpression ParseNot()
    {
        if (MatchKeyword("not"))
        {
            return new UnaryExpression(System.Linq.Expressions.ExpressionType.Not, ParseNot(), typeof(bool));
        }

        return ParsePredicate();
    }

    private QueryExpression ParsePredicate()
    {
        var left = ParseValue();

        if (MatchKeyword("is"))
        {
            var isNot = MatchKeyword("not");
            if (!MatchKeyword("null"))
            {
                throw Error("Expected NULL after IS.");
            }

            return new BinaryExpression(
                isNot ? System.Linq.Expressions.ExpressionType.NotEqual : System.Linq.Expressions.ExpressionType.Equal,
                left,
                new ConstantExpression(null));
        }

        if (MatchKeyword("like"))
        {
            return BuildLikeExpression(left, ParseValue());
        }

        if (TryMatchStringFunctionOperator(out var functionName))
        {
            return new FunctionExpression(functionName, left, new[] { ParseValue() });
        }

        if (TryMatchComparisonOperator(out var op))
        {
            return new BinaryExpression(op, left, ParseValue());
        }

        return left;
    }

    private QueryExpression ParseValue()
    {
        if (Match(TokenKind.OpenParen))
        {
            var inner = ParseOr();
            Expect(TokenKind.CloseParen);
            return inner;
        }

        if (Match(TokenKind.Minus))
        {
            var value = ParseValue();
            if (value is ConstantExpression constant)
            {
                return new ConstantExpression(NegateConstant(constant.Value));
            }

            return new BinaryExpression(
                System.Linq.Expressions.ExpressionType.Subtract,
                new ConstantExpression(0),
                value);
        }

        if (_current.Kind == TokenKind.String)
        {
            var value = _current.Text;
            Advance();
            return new ConstantExpression(value);
        }

        if (_current.Kind == TokenKind.Number)
        {
            var value = ParseNumber(_current.Text);
            Advance();
            return new ConstantExpression(value);
        }

        if (_current.Kind == TokenKind.Parameter)
        {
            var parameterName = _current.Text;
            Advance();
            return new ConstantExpression(GetParameterValue(parameterName));
        }

        if (_current.Kind == TokenKind.Identifier)
        {
            var identifier = _current.Text;
            Advance();

            if (IsKeyword(identifier, "true")) return new ConstantExpression(true);
            if (IsKeyword(identifier, "false")) return new ConstantExpression(false);
            if (IsKeyword(identifier, "null")) return new ConstantExpression(null);

            if (Match(TokenKind.OpenParen))
            {
                return ParseFunctionCall(identifier);
            }

            return ParseMemberPath(identifier);
        }

        throw Error($"Unexpected token '{_current.Text}'.");
    }

    private QueryExpression ParseFunctionCall(string functionName)
    {
        var args = new List<QueryExpression>();
        if (!Match(TokenKind.CloseParen))
        {
            do
            {
                args.Add(ParseOr());
            }
            while (Match(TokenKind.Comma));

            Expect(TokenKind.CloseParen);
        }

        if (IsKeyword(functionName, "contains") ||
            IsKeyword(functionName, "startswith") ||
            IsKeyword(functionName, "endswith"))
        {
            if (args.Count != 2)
            {
                throw Error($"{functionName} requires two arguments.");
            }

            return new FunctionExpression(ToFunctionName(functionName), args[0], new[] { args[1] });
        }

        if (IsKeyword(functionName, "lower") ||
            IsKeyword(functionName, "upper") ||
            IsKeyword(functionName, "trim"))
        {
            if (args.Count != 1)
            {
                throw Error($"{functionName} requires one argument.");
            }

            var mapped = IsKeyword(functionName, "lower")
                ? "ToLower"
                : IsKeyword(functionName, "upper")
                    ? "ToUpper"
                    : "Trim";
            return new FunctionExpression(mapped, args[0], Array.Empty<QueryExpression>());
        }

        throw Error($"Function '{functionName}' is not supported.");
    }

    private QueryExpression ParseMemberPath(string firstSegment)
    {
        QueryExpression expression = new MemberExpression(firstSegment, new ParameterExpression("$"));
        while (Match(TokenKind.Dot))
        {
            if (_current.Kind != TokenKind.Identifier)
            {
                throw Error("Expected member name after '.'.");
            }

            var memberName = _current.Text;
            Advance();
            expression = new MemberExpression(memberName, expression);
        }

        return expression;
    }

    private QueryExpression BuildLikeExpression(QueryExpression left, QueryExpression patternExpression)
    {
        if (patternExpression is not ConstantExpression { Value: string pattern })
        {
            throw Error("LIKE pattern must be a string literal or string parameter.");
        }

        if (pattern == "%")
        {
            return new ConstantExpression(true);
        }

        var startsWithWildcard = pattern.StartsWith("%", StringComparison.Ordinal);
        var endsWithWildcard = pattern.EndsWith("%", StringComparison.Ordinal);
        var inner = pattern.Trim('%');

        if (inner.Contains('%') || pattern.Contains('_'))
        {
            throw Error("LIKE supports only prefix, suffix, contains, or exact patterns.");
        }

        if (startsWithWildcard && endsWithWildcard)
        {
            return new FunctionExpression("Contains", left, new[] { new ConstantExpression(inner) });
        }

        if (startsWithWildcard)
        {
            return new FunctionExpression("EndsWith", left, new[] { new ConstantExpression(inner) });
        }

        if (endsWithWildcard)
        {
            return new FunctionExpression("StartsWith", left, new[] { new ConstantExpression(inner) });
        }

        return new BinaryExpression(
            System.Linq.Expressions.ExpressionType.Equal,
            left,
            new ConstantExpression(pattern));
    }

    private object? GetParameterValue(string name)
    {
        var normalized = QueryParams.NormalizeName(name);
        if (_parameters.TryGetValue(normalized, out var value))
        {
            return value;
        }

        if (_parameters.TryGetValue("@" + normalized, out value))
        {
            return value;
        }

        throw new KeyNotFoundException($"Query parameter '{name}' was not provided.");
    }

    private static object ParseNumber(string text)
    {
        if (text.Contains('.') || text.Contains('e', StringComparison.OrdinalIgnoreCase))
        {
            if (decimal.TryParse(text, NumberStyles.Number, CultureInfo.InvariantCulture, out var decimalValue))
            {
                return decimalValue;
            }

            return double.Parse(text, NumberStyles.Float, CultureInfo.InvariantCulture);
        }

        if (int.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var intValue))
        {
            return intValue;
        }

        return long.Parse(text, NumberStyles.Integer, CultureInfo.InvariantCulture);
    }

    private static object? NegateConstant(object? value)
    {
        return value switch
        {
            int i => -i,
            long l => -l,
            double d => -d,
            decimal m => -m,
            float f => -f,
            short s => -s,
            byte b => -b,
            _ => throw new InvalidOperationException("Unary '-' requires a numeric value.")
        };
    }

    private bool TryMatchComparisonOperator(out System.Linq.Expressions.ExpressionType op)
    {
        op = default;

        if (_current.Kind == TokenKind.Operator)
        {
            op = _current.Text switch
            {
                "=" or "==" => System.Linq.Expressions.ExpressionType.Equal,
                "!=" or "<>" => System.Linq.Expressions.ExpressionType.NotEqual,
                ">" => System.Linq.Expressions.ExpressionType.GreaterThan,
                ">=" => System.Linq.Expressions.ExpressionType.GreaterThanOrEqual,
                "<" => System.Linq.Expressions.ExpressionType.LessThan,
                "<=" => System.Linq.Expressions.ExpressionType.LessThanOrEqual,
                _ => default
            };

            if (op != default)
            {
                Advance();
                return true;
            }
        }

        if (MatchKeyword("eq")) { op = System.Linq.Expressions.ExpressionType.Equal; return true; }
        if (MatchKeyword("ne")) { op = System.Linq.Expressions.ExpressionType.NotEqual; return true; }
        if (MatchKeyword("gt")) { op = System.Linq.Expressions.ExpressionType.GreaterThan; return true; }
        if (MatchKeyword("gte")) { op = System.Linq.Expressions.ExpressionType.GreaterThanOrEqual; return true; }
        if (MatchKeyword("lt")) { op = System.Linq.Expressions.ExpressionType.LessThan; return true; }
        if (MatchKeyword("lte")) { op = System.Linq.Expressions.ExpressionType.LessThanOrEqual; return true; }

        return false;
    }

    private bool TryMatchStringFunctionOperator(out string functionName)
    {
        functionName = string.Empty;

        if (MatchKeyword("contains"))
        {
            functionName = "Contains";
            return true;
        }

        if (MatchKeyword("startswith") || MatchKeyword("starts_with"))
        {
            functionName = "StartsWith";
            return true;
        }

        if (MatchKeyword("endswith") || MatchKeyword("ends_with"))
        {
            functionName = "EndsWith";
            return true;
        }

        return false;
    }

    private static string ToFunctionName(string name)
    {
        if (IsKeyword(name, "contains")) return "Contains";
        if (IsKeyword(name, "startswith")) return "StartsWith";
        if (IsKeyword(name, "endswith")) return "EndsWith";
        return name;
    }

    private bool MatchKeyword(string keyword)
    {
        if (_current.Kind == TokenKind.Identifier && IsKeyword(_current.Text, keyword))
        {
            Advance();
            return true;
        }

        return false;
    }

    private static bool IsKeyword(string value, string keyword)
        => string.Equals(value, keyword, StringComparison.OrdinalIgnoreCase);

    private bool Match(TokenKind kind)
    {
        if (_current.Kind != kind) return false;
        Advance();
        return true;
    }

    private void Expect(TokenKind kind)
    {
        if (!Match(kind))
        {
            throw Error($"Expected {kind}, got '{_current.Text}'.");
        }
    }

    private void Advance()
    {
        _current = NextToken();
    }

    private Token NextToken()
    {
        while (_position < _text.Length && char.IsWhiteSpace(_text[_position])) _position++;
        if (_position >= _text.Length) return new Token(TokenKind.End, string.Empty);

        var ch = _text[_position];
        if (ch == '(') { _position++; return new Token(TokenKind.OpenParen, "("); }
        if (ch == ')') { _position++; return new Token(TokenKind.CloseParen, ")"); }
        if (ch == ',') { _position++; return new Token(TokenKind.Comma, ","); }
        if (ch == '.') { _position++; return new Token(TokenKind.Dot, "."); }
        if (ch == '-') { _position++; return new Token(TokenKind.Minus, "-"); }

        if (ch == '@') return ReadParameter();
        if (ch == '\'' || ch == '"') return ReadString(ch);
        if (char.IsDigit(ch)) return ReadNumber();
        if (IsIdentifierStart(ch)) return ReadIdentifier();
        if (ch == '[') return ReadBracketIdentifier();
        if (ch == '`') return ReadBacktickIdentifier();

        if (ch is '=' or '!' or '<' or '>')
        {
            return ReadOperator();
        }

        throw Error($"Unexpected character '{ch}'.");
    }

    private Token ReadParameter()
    {
        _position++;
        var start = _position;
        while (_position < _text.Length && IsIdentifierPart(_text[_position])) _position++;
        if (start == _position) throw Error("Expected parameter name after '@'.");
        return new Token(TokenKind.Parameter, _text.Substring(start, _position - start));
    }

    private Token ReadString(char quote)
    {
        _position++;
        var sb = new System.Text.StringBuilder();
        while (_position < _text.Length)
        {
            var ch = _text[_position++];
            if (ch == quote)
            {
                if (_position < _text.Length && _text[_position] == quote)
                {
                    sb.Append(quote);
                    _position++;
                    continue;
                }

                return new Token(TokenKind.String, sb.ToString());
            }

            if (ch == '\\' && _position < _text.Length)
            {
                var escaped = _text[_position++];
                sb.Append(escaped switch
                {
                    'n' => '\n',
                    'r' => '\r',
                    't' => '\t',
                    '\\' => '\\',
                    '\'' => '\'',
                    '"' => '"',
                    _ => escaped
                });
                continue;
            }

            sb.Append(ch);
        }

        throw Error("Unterminated string literal.");
    }

    private Token ReadNumber()
    {
        var start = _position;
        while (_position < _text.Length && char.IsDigit(_text[_position])) _position++;
        if (_position < _text.Length && _text[_position] == '.')
        {
            _position++;
            while (_position < _text.Length && char.IsDigit(_text[_position])) _position++;
        }

        if (_position < _text.Length && (_text[_position] == 'e' || _text[_position] == 'E'))
        {
            _position++;
            if (_position < _text.Length && (_text[_position] == '+' || _text[_position] == '-')) _position++;
            while (_position < _text.Length && char.IsDigit(_text[_position])) _position++;
        }

        return new Token(TokenKind.Number, _text.Substring(start, _position - start));
    }

    private Token ReadIdentifier()
    {
        var start = _position;
        _position++;
        while (_position < _text.Length && IsIdentifierPart(_text[_position])) _position++;
        return new Token(TokenKind.Identifier, _text.Substring(start, _position - start));
    }

    private Token ReadBracketIdentifier()
    {
        _position++;
        var start = _position;
        while (_position < _text.Length && _text[_position] != ']') _position++;
        if (_position >= _text.Length) throw Error("Unterminated bracket identifier.");
        var value = _text.Substring(start, _position - start);
        _position++;
        return new Token(TokenKind.Identifier, value);
    }

    private Token ReadBacktickIdentifier()
    {
        _position++;
        var start = _position;
        while (_position < _text.Length && _text[_position] != '`') _position++;
        if (_position >= _text.Length) throw Error("Unterminated backtick identifier.");
        var value = _text.Substring(start, _position - start);
        _position++;
        return new Token(TokenKind.Identifier, value);
    }

    private Token ReadOperator()
    {
        var start = _position;
        _position++;
        if (_position < _text.Length)
        {
            var two = _text.Substring(start, 2);
            if (two is "==" or "!=" or "<>" or ">=" or "<=")
            {
                _position++;
                return new Token(TokenKind.Operator, two);
            }
        }

        return new Token(TokenKind.Operator, _text.Substring(start, 1));
    }

    private static bool IsIdentifierStart(char ch)
        => char.IsLetter(ch) || ch == '_' || ch == '$';

    private static bool IsIdentifierPart(char ch)
        => char.IsLetterOrDigit(ch) || ch == '_' || ch == '$';

    private FormatException Error(string message)
        => new($"Invalid query predicate at position {_position}: {message}");

    private readonly struct Token
    {
        public Token(TokenKind kind, string text)
        {
            Kind = kind;
            Text = text;
        }

        public TokenKind Kind { get; }
        public string Text { get; }
    }

    private enum TokenKind
    {
        End,
        Identifier,
        Parameter,
        String,
        Number,
        Operator,
        OpenParen,
        CloseParen,
        Comma,
        Dot,
        Minus
    }
}
