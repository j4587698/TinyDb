using System.Globalization;

namespace TinyDb.Query;

internal sealed partial class SqlQueryParser
{

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

}
