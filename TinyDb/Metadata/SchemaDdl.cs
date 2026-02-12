using System.Text;
using TinyDb.Bson;

namespace TinyDb.Metadata;

public static class SchemaDdl
{
    public static string Export(MetadataDocument document)
    {
        if (document == null) throw new ArgumentNullException(nameof(document));

        var sb = new StringBuilder();
        sb.AppendLine("-- TinyDbDDL v1");

        sb.Append("create table ").Append(Quote(document.TableName));

        if (!string.IsNullOrWhiteSpace(document.TypeName))
        {
            sb.Append(" type ").Append(Quote(document.TypeName));
        }

        if (!string.IsNullOrWhiteSpace(document.DisplayName))
        {
            sb.Append(" display ").Append(Quote(document.DisplayName));
        }

        if (!string.IsNullOrWhiteSpace(document.Description))
        {
            sb.Append(" desc ").Append(Quote(document.Description));
        }

        sb.AppendLine();
        sb.AppendLine("(");

        foreach (var columnDoc in GetOrderedColumns(document.Columns))
        {
            sb.Append("  ");
            var fieldName = GetString(columnDoc, "n");
            sb.Append(Quote(fieldName));
            sb.Append(' ');
            sb.Append(Quote(ClrTypeName.Normalize(GetString(columnDoc, "t"))));

            if (IsPrimaryKey(columnDoc)) sb.Append(" pk");
            if (GetBool(columnDoc, "r")) sb.Append(" required");

            var propertyName = GetOptionalString(columnDoc, "pn");
            propertyName ??= DerivePropertyName(fieldName);
            sb.Append(" pn ").Append(Quote(propertyName));

            var order = GetInt(columnDoc, "o");
            if (order != 0)
            {
                sb.Append(" order ").Append(order);
            }

            var displayName = GetOptionalString(columnDoc, "dn");
            if (!string.IsNullOrWhiteSpace(displayName))
            {
                sb.Append(" dn ").Append(Quote(displayName));
            }

            var description = GetOptionalString(columnDoc, "desc");
            if (!string.IsNullOrWhiteSpace(description))
            {
                sb.Append(" desc ").Append(Quote(description));
            }

            var foreignKey = GetOptionalString(columnDoc, "fk");
            if (!string.IsNullOrWhiteSpace(foreignKey))
            {
                sb.Append(" fk ").Append(Quote(foreignKey));
            }

            if (columnDoc.TryGetValue("dv", out var defaultValue) && defaultValue != null && !defaultValue.IsNull)
            {
                sb.Append(" dv ").Append(FormatDefaultValue(defaultValue));
            }

            sb.AppendLine(";");
        }

        sb.AppendLine(");");
        return sb.ToString();
    }

    public static string Export(IEnumerable<MetadataDocument> documents)
    {
        if (documents == null) throw new ArgumentNullException(nameof(documents));

        var sb = new StringBuilder();
        var first = true;

        foreach (var doc in documents)
        {
            if (!first) sb.AppendLine();
            first = false;
            sb.Append(Export(doc));
        }

        return sb.ToString();
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

    private static string Quote(string? value)
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

    private static string DerivePropertyName(string fieldName)
    {
        if (string.Equals(fieldName, "_id", StringComparison.Ordinal)) return "Id";
        if (string.IsNullOrWhiteSpace(fieldName)) return string.Empty;
        if (fieldName.Length == 1) return fieldName.ToUpperInvariant();
        return char.ToUpperInvariant(fieldName[0]) + fieldName.Substring(1);
    }

    private static string FormatDefaultValue(BsonValue value)
    {
        return value switch
        {
            BsonNull => "null",
            BsonBoolean b => b.Value ? "true" : "false",
            BsonInt32 i => i.Value.ToString(System.Globalization.CultureInfo.InvariantCulture),
            BsonInt64 l => l.Value.ToString(System.Globalization.CultureInfo.InvariantCulture),
            BsonDouble d => d.Value.ToString(System.Globalization.CultureInfo.InvariantCulture),
            BsonDecimal128 dec => dec.Value.ToString(System.Globalization.CultureInfo.InvariantCulture),
            BsonString s => Quote(s.Value),
            BsonDateTime dt => $"datetime({Quote(dt.Value.ToString("O", System.Globalization.CultureInfo.InvariantCulture))})",
            _ => Quote(value.ToString())
        };
    }
}
