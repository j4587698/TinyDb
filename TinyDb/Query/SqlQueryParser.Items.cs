using System.Globalization;

namespace TinyDb.Query;

internal sealed partial class SqlQueryParser
{

    private abstract class SqlItemParserBase
    {
        protected readonly string _text;
        private readonly string _expectedIdentifierMessage;
        protected int _position;

        protected SqlItemParserBase(string text, string expectedIdentifierMessage)
        {
            _text = text ?? throw new ArgumentNullException(nameof(text));
            _expectedIdentifierMessage = expectedIdentifierMessage;
        }

        protected string ReadFieldPath()
        {
            var segments = new List<string> { NormalizeIdSegment(ReadIdentifier()) };
            while (MatchChar('.'))
            {
                segments.Add(NormalizeIdSegment(ReadIdentifier()));
            }

            return string.Join(".", segments);
        }

        protected string ReadFieldPath(out string defaultOutputName)
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

        protected string ReadIdentifier()
        {
            SkipWhitespace();
            if (_position >= _text.Length) throw new FormatException(_expectedIdentifierMessage);

            var ch = _text[_position];
            if (ch == '[') return ReadBracketIdentifier();
            if (ch == '`') return ReadQuotedIdentifier('`');

            if (!IsIdentifierStart(ch)) throw new FormatException(_expectedIdentifierMessage);
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

        protected bool MatchChar(char ch)
        {
            SkipWhitespace();
            if (_position >= _text.Length || _text[_position] != ch) return false;
            _position++;
            return true;
        }

        protected bool MatchKeyword(string keyword)
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

        protected void SkipWhitespace()
        {
            while (_position < _text.Length && char.IsWhiteSpace(_text[_position])) _position++;
        }
    }

    private sealed class OrderItemParser : SqlItemParserBase
    {
        public OrderItemParser(string text)
            : base(text, "Expected ORDER BY field.")
        {
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
    }

    private sealed class ProjectionItemParser : SqlItemParserBase
    {
        public ProjectionItemParser(string text)
            : base(text, "Expected SELECT field.")
        {
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
    }

    private sealed class AssignmentParser : SqlItemParserBase
    {
        private readonly IReadOnlyDictionary<string, object?> _parameters;

        public AssignmentParser(string text, IReadOnlyDictionary<string, object?> parameters)
            : base(text, "Expected identifier.")
        {
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
    }
}
