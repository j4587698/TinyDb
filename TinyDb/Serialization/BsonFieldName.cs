using System.Reflection;
using TinyDb.Attributes;

namespace TinyDb.Serialization;

internal static class BsonFieldName
{
    public static string ForProperty(PropertyInfo property, Type? entityType = null)
    {
        if (property == null) throw new ArgumentNullException(nameof(property));
        return IsIdProperty(property, entityType) ? "_id" : ToCamelCase(property.Name);
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
