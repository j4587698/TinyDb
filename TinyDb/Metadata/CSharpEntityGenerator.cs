using System.Text;
using TinyDb.Bson;

namespace TinyDb.Metadata;

public static class CSharpEntityGenerator
{
    public static string Generate(MetadataDocument document, CSharpEntityGenerationOptions? options = null)
    {
        if (document == null) throw new ArgumentNullException(nameof(document));
        options ??= new CSharpEntityGenerationOptions();

        var className = ToValidTypeName(options.ClassName ?? GetDefaultClassName(document));
        var ns = string.IsNullOrWhiteSpace(options.Namespace) ? null : options.Namespace!.Trim();

        var sb = new StringBuilder();

        sb.AppendLine("using TinyDb.Attributes;");
        if (options.EmitMetadataAttributes)
        {
            sb.AppendLine("using TinyDb.Metadata;");
        }

        sb.AppendLine();

        if (!string.IsNullOrWhiteSpace(ns))
        {
            if (options.FileScopedNamespace)
            {
                sb.AppendLine($"namespace {ns};");
                sb.AppendLine();
            }
            else
            {
                sb.AppendLine($"namespace {ns}");
                sb.AppendLine("{");
            }
        }

        if (options.EmitEntityAttribute)
        {
            sb.AppendLine($"[Entity({ToCSharpStringLiteral(document.TableName)})]");
        }

        if (options.EmitMetadataAttributes && ShouldEmitEntityMetadata(document))
        {
            sb.AppendLine(BuildEntityMetadataAttribute(document));
        }

        sb.AppendLine($"public partial class {className}");
        sb.AppendLine("{");

        var usedNames = new HashSet<string>(StringComparer.Ordinal);
        foreach (var col in GetOrderedColumns(document.Columns))
        {
            var propertyName = GetPropertyName(col);
            propertyName = ToValidPropertyName(propertyName);
            propertyName = EnsureUnique(propertyName, usedNames);

            var typeNameRaw = GetOptionalString(col, "t") ?? "System.Object";
            var typeSyntaxBase = ClrTypeName.ToCSharpTypeSyntax(typeNameRaw, options.UseCSharpAliases, out var nullableFromType);
            var required = GetBool(col, "r");
            var isNullable = options.EmitNullableAnnotations && (!required || nullableFromType);

            var typeSyntax = isNullable ? $"{typeSyntaxBase}?" : typeSyntaxBase;

            if (options.EmitMetadataAttributes || options.EmitForeignKeyAttributes || IsPrimaryKey(col))
            {
                EmitPropertyAttributes(sb, col, propertyName, options);
            }

            var initializer = GetInitializer(typeNameRaw, typeSyntaxBase, isNullable, required, options);
            sb.AppendLine($"    public {typeSyntax} {propertyName} {{ get; set; }}{initializer}");
            sb.AppendLine();
        }

        if (sb.Length >= Environment.NewLine.Length * 2)
        {
            TrimTrailingBlankLine(sb);
        }

        sb.AppendLine("}");

        if (!string.IsNullOrWhiteSpace(ns) && !options.FileScopedNamespace)
        {
            sb.AppendLine("}");
        }

        return sb.ToString();
    }

    private static bool ShouldEmitEntityMetadata(MetadataDocument document)
    {
        if (!string.IsNullOrWhiteSpace(document.DisplayName) && !string.Equals(document.DisplayName, document.TableName, StringComparison.Ordinal))
        {
            return true;
        }

        return !string.IsNullOrWhiteSpace(document.Description);
    }

    private static string BuildEntityMetadataAttribute(MetadataDocument document)
    {
        var namedArgs = new List<string>();

        if (!string.IsNullOrWhiteSpace(document.DisplayName) && !string.Equals(document.DisplayName, document.TableName, StringComparison.Ordinal))
        {
            namedArgs.Add($"DisplayName = {ToCSharpStringLiteral(document.DisplayName)}");
        }

        if (!string.IsNullOrWhiteSpace(document.Description))
        {
            namedArgs.Add($"Description = {ToCSharpStringLiteral(document.Description)}");
        }

        if (namedArgs.Count == 0) return "[EntityMetadata]";

        return $"[EntityMetadata({string.Join(", ", namedArgs)})]";
    }

    private static void EmitPropertyAttributes(StringBuilder sb, BsonDocument columnDoc, string propertyName, CSharpEntityGenerationOptions options)
    {
        var isPk = IsPrimaryKey(columnDoc);
        if (isPk)
        {
            sb.AppendLine("    [Id]");
        }

        if (options.EmitForeignKeyAttributes)
        {
            var foreignKey = GetOptionalString(columnDoc, "fk");
            if (!string.IsNullOrWhiteSpace(foreignKey))
            {
                sb.AppendLine($"    [ForeignKey({ToCSharpStringLiteral(foreignKey)})]");
            }
        }

        if (!options.EmitMetadataAttributes) return;

        var displayName = GetOptionalString(columnDoc, "dn") ?? propertyName;
        var description = GetOptionalString(columnDoc, "desc");
        var order = GetInt(columnDoc, "o");
        var required = GetBool(columnDoc, "r");

        var args = new List<string> { ToCSharpStringLiteral(displayName) };
        var namedArgs = new List<string>();

        if (!string.IsNullOrWhiteSpace(description))
        {
            namedArgs.Add($"Description = {ToCSharpStringLiteral(description)}");
        }

        if (order != 0)
        {
            namedArgs.Add($"Order = {order}");
        }

        if (required)
        {
            namedArgs.Add("Required = true");
        }

        if (namedArgs.Count > 0)
        {
            args.AddRange(namedArgs);
        }

        sb.AppendLine($"    [PropertyMetadata({string.Join(", ", args)})]");
    }

    private static string GetInitializer(string rawTypeName, string typeSyntaxBase, bool isNullable, bool required, CSharpEntityGenerationOptions options)
    {
        if (!options.EmitNullableAnnotations || isNullable || !required) return string.Empty;

        var normalized = ClrTypeName.NormalizeForComparison(rawTypeName);
        if (IsKnownValueType(normalized)) return string.Empty;

        if (string.Equals(typeSyntaxBase, "string", StringComparison.Ordinal))
        {
            return " = string.Empty;";
        }

        return " = default!;";
    }

    private static bool IsKnownValueType(string normalizedTypeName)
    {
        if (string.IsNullOrWhiteSpace(normalizedTypeName)) return false;

        if (normalizedTypeName.EndsWith("[]", StringComparison.Ordinal)) return false;

        return normalizedTypeName switch
        {
            "bool" or
            "byte" or
            "sbyte" or
            "char" or
            "short" or
            "ushort" or
            "int" or
            "uint" or
            "long" or
            "ulong" or
            "float" or
            "double" or
            "decimal" or
            "System.Boolean" or
            "System.Byte" or
            "System.SByte" or
            "System.Char" or
            "System.Int16" or
            "System.UInt16" or
            "System.Int32" or
            "System.UInt32" or
            "System.Int64" or
            "System.UInt64" or
            "System.Single" or
            "System.Double" or
            "System.Decimal" or
            "System.DateTime" or
            "System.DateTimeOffset" or
            "System.Guid" or
            "TinyDb.Bson.ObjectId" => true,
            _ => false
        };
    }

    private static IEnumerable<BsonDocument> GetOrderedColumns(BsonArray columns)
    {
        return columns
            .OfType<BsonDocument>()
            .Select(doc => (Doc: doc, Order: GetInt(doc, "o")))
            .OrderBy(x => x.Order)
            .ThenBy(x => GetOptionalString(x.Doc, "n") ?? string.Empty, StringComparer.Ordinal)
            .Select(x => x.Doc);
    }

    private static bool IsPrimaryKey(BsonDocument columnDoc)
    {
        if (columnDoc.TryGetValue("pk", out var pkValue) && pkValue != null && !pkValue.IsNull && pkValue.ToBoolean())
        {
            return true;
        }

        var fieldName = GetOptionalString(columnDoc, "n");
        return string.Equals(fieldName, "_id", StringComparison.Ordinal);
    }

    private static string GetDefaultClassName(MetadataDocument document)
    {
        if (!string.IsNullOrWhiteSpace(document.TypeName))
        {
            var normalized = ClrTypeName.Normalize(document.TypeName);
            var genericStart = normalized.IndexOf('<', StringComparison.Ordinal);
            var noGeneric = genericStart >= 0 ? normalized.Substring(0, genericStart) : normalized;
            var lastDot = noGeneric.LastIndexOf('.');
            return lastDot >= 0 ? noGeneric.Substring(lastDot + 1) : noGeneric;
        }

        return ToPascalCase(document.TableName);
    }

    private static string GetPropertyName(BsonDocument columnDoc)
    {
        var explicitPropertyName = GetOptionalString(columnDoc, "pn");
        if (!string.IsNullOrWhiteSpace(explicitPropertyName)) return explicitPropertyName!;

        var fieldName = GetOptionalString(columnDoc, "n") ?? string.Empty;
        if (string.Equals(fieldName, "_id", StringComparison.Ordinal)) return "Id";
        return UpperFirstLetter(fieldName);
    }

    private static string UpperFirstLetter(string name)
    {
        if (string.IsNullOrWhiteSpace(name)) return name;
        if (name.Length == 1) return name.ToUpperInvariant();
        return char.ToUpperInvariant(name[0]) + name.Substring(1);
    }

    private static string ToPascalCase(string name)
    {
        if (string.IsNullOrWhiteSpace(name)) return name;

        var sb = new StringBuilder(name.Length);
        var upperNext = true;

        foreach (var ch in name)
        {
            if (!char.IsLetterOrDigit(ch))
            {
                upperNext = true;
                continue;
            }

            sb.Append(upperNext ? char.ToUpperInvariant(ch) : ch);
            upperNext = false;
        }

        return sb.Length == 0 ? "_" : sb.ToString();
    }

    private static string ToValidTypeName(string name)
    {
        return ToValidIdentifier(ToPascalCase(name), isType: true);
    }

    private static string ToValidPropertyName(string name)
    {
        return ToValidIdentifier(name, isType: false);
    }

    private static string ToValidIdentifier(string name, bool isType)
    {
        if (string.IsNullOrWhiteSpace(name)) return "_";

        var sb = new StringBuilder(name.Length);
        foreach (var ch in name)
        {
            if (sb.Length == 0)
            {
                if (char.IsLetter(ch) || ch == '_')
                {
                    sb.Append(ch);
                    continue;
                }

                if (char.IsDigit(ch))
                {
                    sb.Append('_').Append(ch);
                    continue;
                }
            }

            if (char.IsLetterOrDigit(ch) || ch == '_')
            {
                sb.Append(ch);
                continue;
            }

            sb.Append('_');
        }

        if (sb.Length == 0) sb.Append('_');

        var result = sb.ToString();
        if (IsKeyword(result)) result = "@" + result;

        if (isType && result.StartsWith("@", StringComparison.Ordinal))
        {
            result = result.Substring(1);
        }

        return result;
    }

    private static bool IsKeyword(string identifier)
    {
        return identifier is
            "abstract" or "as" or "base" or "bool" or "break" or "byte" or "case" or "catch" or "char" or "checked" or
            "class" or "const" or "continue" or "decimal" or "default" or "delegate" or "do" or "double" or "else" or
            "enum" or "event" or "explicit" or "extern" or "false" or "finally" or "fixed" or "float" or "for" or
            "foreach" or "goto" or "if" or "implicit" or "in" or "int" or "interface" or "internal" or "is" or "lock" or
            "long" or "namespace" or "new" or "null" or "object" or "operator" or "out" or "override" or "params" or
            "private" or "protected" or "public" or "readonly" or "ref" or "return" or "sbyte" or "sealed" or "short" or
            "sizeof" or "stackalloc" or "static" or "string" or "struct" or "switch" or "this" or "throw" or "true" or
            "try" or "typeof" or "uint" or "ulong" or "unchecked" or "unsafe" or "ushort" or "using" or "virtual" or
            "void" or "volatile" or "while";
    }

    private static string EnsureUnique(string name, HashSet<string> used)
    {
        if (used.Add(name)) return name;

        var i = 2;
        while (true)
        {
            var candidate = $"{name}_{i}";
            if (used.Add(candidate)) return candidate;
            i++;
        }
    }

    private static void TrimTrailingBlankLine(StringBuilder sb)
    {
        var text = sb.ToString();
        var trimmed = text.TrimEnd();
        sb.Clear();
        sb.Append(trimmed);
        sb.AppendLine();
    }

    private static string ToCSharpStringLiteral(string? value)
    {
        if (value == null) return "\"\"";

        var sb = new StringBuilder(value.Length + 2);
        sb.Append('"');

        foreach (var ch in value)
        {
            switch (ch)
            {
                case '\\':
                    sb.Append("\\\\");
                    break;
                case '"':
                    sb.Append("\\\"");
                    break;
                case '\r':
                    sb.Append("\\r");
                    break;
                case '\n':
                    sb.Append("\\n");
                    break;
                case '\t':
                    sb.Append("\\t");
                    break;
                default:
                    sb.Append(ch);
                    break;
            }
        }

        sb.Append('"');
        return sb.ToString();
    }

    private static string GetString(BsonDocument doc, string key)
    {
        if (!doc.TryGetValue(key, out var value) || value == null || value.IsNull) return string.Empty;
        return value.ToString();
    }

    private static string? GetOptionalString(BsonDocument doc, string key)
    {
        var s = GetString(doc, key);
        return string.IsNullOrWhiteSpace(s) ? null : s;
    }

    private static bool GetBool(BsonDocument doc, string key)
    {
        return doc.TryGetValue(key, out var value) && value != null && !value.IsNull && value.ToBoolean();
    }

    private static int GetInt(BsonDocument doc, string key)
    {
        if (!doc.TryGetValue(key, out var value) || value == null || value.IsNull) return 0;
        return value.ToInt32();
    }
}
