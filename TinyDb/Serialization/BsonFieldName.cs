using System.Reflection;
using System.Text;
using TinyDb.Attributes;

namespace TinyDb.Serialization;

internal static class BsonFieldName
{
    private static readonly byte[] IdBytes = Encoding.UTF8.GetBytes("_id");
    private static readonly byte[] CollectionBytes = Encoding.UTF8.GetBytes("_collection");
    private static readonly byte[] IsLargeDocumentBytes = Encoding.UTF8.GetBytes("_isLargeDocument");
    private static readonly byte[] LargeDocumentIndexBytes = Encoding.UTF8.GetBytes("_largeDocumentIndex");
    private static readonly byte[] LargeDocumentSizeBytes = Encoding.UTF8.GetBytes("_largeDocumentSize");

    public static string ForProperty(PropertyInfo property, Type? entityType = null)
    {
        if (property == null) throw new ArgumentNullException(nameof(property));
        return IsIdProperty(property, entityType) ? "_id" : ToCamelCase(property.Name);
    }

    public static string Decode(ReadOnlySpan<byte> utf8Name)
    {
        if (utf8Name.SequenceEqual(IdBytes)) return "_id";
        if (utf8Name.SequenceEqual(CollectionBytes)) return "_collection";
        if (utf8Name.SequenceEqual(IsLargeDocumentBytes)) return "_isLargeDocument";
        if (utf8Name.SequenceEqual(LargeDocumentIndexBytes)) return "_largeDocumentIndex";
        if (utf8Name.SequenceEqual(LargeDocumentSizeBytes)) return "_largeDocumentSize";
        return Encoding.UTF8.GetString(utf8Name);
    }

    public static string ToCamelCase(string name)
    {
        if (string.IsNullOrEmpty(name)) return name;

        var firstLetter = 0;
        while (firstLetter < name.Length && name[firstLetter] == '_')
        {
            firstLetter++;
        }

        if (firstLetter >= name.Length || char.IsLower(name[firstLetter]))
        {
            return name;
        }

        var chars = name.ToCharArray();
        for (var i = firstLetter; i < chars.Length && char.IsUpper(chars[i]); i++)
        {
            if (i > firstLetter && i + 1 < chars.Length && char.IsLower(chars[i + 1]))
            {
                break;
            }

            chars[i] = char.ToLowerInvariant(chars[i]);
        }

        return new string(chars);
    }

    private static bool IsIdProperty(PropertyInfo property, Type? entityType)
    {
        var attribute = entityType?.GetCustomAttribute<EntityAttribute>();
        if (!string.IsNullOrWhiteSpace(attribute?.IdProperty) &&
            string.Equals(attribute.IdProperty, property.Name, StringComparison.Ordinal))
        {
            return true;
        }

        if (property.GetCustomAttribute<IdAttribute>() != null)
        {
            return true;
        }

        return property.Name is "Id" or "_id" or "ID";
    }
}
