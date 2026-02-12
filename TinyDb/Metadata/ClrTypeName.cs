using System.Text;

namespace TinyDb.Metadata;

internal static class ClrTypeName
{
    public static string GetStableName(Type type)
    {
        if (type == null) throw new ArgumentNullException(nameof(type));

        if (type.IsGenericParameter) return type.Name;

        if (type.IsArray)
        {
            var elementType = type.GetElementType();
            return elementType == null ? type.Name : $"{GetStableName(elementType)}[]";
        }

        if (!type.IsGenericType)
        {
            return type.FullName ?? type.Name;
        }

        var genericDefinition = type.GetGenericTypeDefinition();
        var definitionName = genericDefinition.FullName ?? genericDefinition.Name;
        var tickIndex = definitionName.IndexOf('`', StringComparison.Ordinal);
        if (tickIndex >= 0) definitionName = definitionName.Substring(0, tickIndex);

        var genericArguments = type.GetGenericArguments().Select(GetStableName);
        return $"{definitionName}<{string.Join(",", genericArguments)}>";
    }

    public static string Normalize(string? typeName)
    {
        if (string.IsNullOrWhiteSpace(typeName)) return string.Empty;

        var trimmed = typeName.Trim();
        trimmed = StripGlobalPrefix(trimmed);
        trimmed = StripTopLevelAssemblyQualification(trimmed);
        trimmed = trimmed.Replace('+', '.');

        if (LooksLikeReflectionGenericName(trimmed))
        {
            try
            {
                return SimplifyReflectionTypeName(trimmed);
            }
            catch
            {
                return trimmed;
            }
        }

        return trimmed;
    }

    public static string NormalizeForComparison(string? typeName)
    {
        var normalized = Normalize(typeName);

        if (normalized.EndsWith("?", StringComparison.Ordinal))
        {
            normalized = normalized.Substring(0, normalized.Length - 1).TrimEnd();
        }

        if (TryUnwrapNullable(normalized, out var inner))
        {
            return inner;
        }

        return normalized;
    }

    public static string ToCSharpTypeSyntax(string? typeName, bool useAliases, out bool isAlreadyNullable)
    {
        isAlreadyNullable = false;

        var normalized = Normalize(typeName);
        if (string.IsNullOrWhiteSpace(normalized)) return "object";

        if (normalized.EndsWith("?", StringComparison.Ordinal))
        {
            isAlreadyNullable = true;
            normalized = normalized.Substring(0, normalized.Length - 1).TrimEnd();
        }

        if (TryUnwrapNullable(normalized, out var inner))
        {
            isAlreadyNullable = true;
            normalized = inner;
        }

        return ApplyAliases(normalized, useAliases);
    }

    private static bool TryUnwrapNullable(string typeName, out string innerTypeName)
    {
        innerTypeName = string.Empty;

        const string prefix = "System.Nullable<";
        if (!typeName.StartsWith(prefix, StringComparison.Ordinal) || !typeName.EndsWith(">", StringComparison.Ordinal))
        {
            return false;
        }

        var inner = typeName.Substring(prefix.Length, typeName.Length - prefix.Length - 1).Trim();
        if (string.IsNullOrWhiteSpace(inner)) return false;

        innerTypeName = inner;
        return true;
    }

    private static bool LooksLikeReflectionGenericName(string typeName)
    {
        return typeName.IndexOf('`', StringComparison.Ordinal) >= 0 &&
               typeName.IndexOf("[[", StringComparison.Ordinal) >= 0;
    }

    private static string StripGlobalPrefix(string typeName)
    {
        const string prefix = "global::";
        return typeName.StartsWith(prefix, StringComparison.Ordinal) ? typeName.Substring(prefix.Length) : typeName;
    }

    private static string StripTopLevelAssemblyQualification(string typeName)
    {
        var bracketDepth = 0;
        var angleDepth = 0;

        for (var i = 0; i < typeName.Length; i++)
        {
            var c = typeName[i];
            switch (c)
            {
                case '[':
                    bracketDepth++;
                    break;
                case ']':
                    if (bracketDepth > 0) bracketDepth--;
                    break;
                case '<':
                    angleDepth++;
                    break;
                case '>':
                    if (angleDepth > 0) angleDepth--;
                    break;
                case ',':
                    if (bracketDepth == 0 && angleDepth == 0)
                    {
                        return typeName.Substring(0, i).TrimEnd();
                    }
                    break;
            }
        }

        return typeName;
    }

    private static string ApplyAliases(string normalizedTypeName, bool useAliases)
    {
        if (!useAliases) return normalizedTypeName;

        return normalizedTypeName switch
        {
            "System.String" => "string",
            "System.Boolean" => "bool",
            "System.Byte" => "byte",
            "System.SByte" => "sbyte",
            "System.Char" => "char",
            "System.Int16" => "short",
            "System.UInt16" => "ushort",
            "System.Int32" => "int",
            "System.UInt32" => "uint",
            "System.Int64" => "long",
            "System.UInt64" => "ulong",
            "System.Single" => "float",
            "System.Double" => "double",
            "System.Decimal" => "decimal",
            "System.Object" => "object",
            "System.Byte[]" => "byte[]",
            _ => normalizedTypeName
        };
    }

    private static string SimplifyReflectionTypeName(string reflectionTypeName)
    {
        var i = 0;
        var simplified = ParseReflectionTypeName(reflectionTypeName, ref i);
        return simplified;
    }

    private static string ParseReflectionTypeName(string input, ref int i)
    {
        var baseName = ReadBaseName(input, ref i);

        if (i < input.Length && input[i] == '`')
        {
            i++; // skip `
            while (i < input.Length && char.IsDigit(input[i])) i++;

            if (i + 1 < input.Length && input[i] == '[' && input[i + 1] == '[')
            {
                i += 2; // skip [[
                var args = new List<string>();

                while (i < input.Length)
                {
                    if (input[i] == '[') i++; // skip optional [
                    var arg = ParseAssemblyQualifiedArgument(input, ref i);
                    if (!string.IsNullOrWhiteSpace(arg)) args.Add(arg);

                    if (i < input.Length && input[i] == ']') i++; // skip ]

                    if (i < input.Length && input[i] == ',')
                    {
                        i++; // skip ,
                        continue;
                    }

                    if (i < input.Length && input[i] == ']')
                    {
                        i++; // skip ] (outer)
                        break;
                    }

                    break;
                }

                baseName = $"{baseName}<{string.Join(",", args)}>";
            }
        }

        baseName += ReadArraySuffix(input, ref i);
        return baseName;
    }

    private static string ParseAssemblyQualifiedArgument(string input, ref int i)
    {
        var typePart = new StringBuilder();
        var bracketDepth = 0;
        var angleDepth = 0;

        while (i < input.Length)
        {
            var c = input[i];

            if (c == '[') bracketDepth++;
            if (c == ']')
            {
                if (bracketDepth == 0) break;
                bracketDepth--;
            }

            if (c == '<') angleDepth++;
            if (c == '>' && angleDepth > 0) angleDepth--;

            if (c == ',' && bracketDepth == 0 && angleDepth == 0)
            {
                while (i < input.Length && input[i] != ']') i++;
                break;
            }

            typePart.Append(c);
            i++;
        }

        var rawTypePart = typePart.ToString().Trim();
        rawTypePart = StripTopLevelAssemblyQualification(rawTypePart);

        var nestedIndex = 0;
        var simplified = ParseReflectionTypeName(rawTypePart, ref nestedIndex);
        return simplified;
    }

    private static string ReadBaseName(string input, ref int i)
    {
        var start = i;
        while (i < input.Length)
        {
            var c = input[i];
            if (c is '`' or '[' or ']' or ',') break;
            i++;
        }

        var baseName = input.Substring(start, i - start).Trim();
        return baseName.Replace('+', '.');
    }

    private static string ReadArraySuffix(string input, ref int i)
    {
        var sb = new StringBuilder();

        while (i + 1 < input.Length && input[i] == '[' && input[i + 1] == ']')
        {
            sb.Append("[]");
            i += 2;
        }

        return sb.ToString();
    }
}
