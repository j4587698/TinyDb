using System.Globalization;

namespace TinyDb.Query;

internal sealed partial class SqlQueryParser
{

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

}
