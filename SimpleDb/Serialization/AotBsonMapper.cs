using System.Diagnostics.CodeAnalysis;
using System.Linq;
using SimpleDb.Bson;

namespace SimpleDb.Serialization;

/// <summary>
/// AOT友好的BSON映射器，根据源生成器提供的辅助适配器进行序列化与反序列化。
/// </summary>
public static class AotBsonMapper
{
    public static BsonDocument ToDocument<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(T entity)
        where T : class, new()
    {
        if (entity == null) throw new ArgumentNullException(nameof(entity));

        if (AotHelperRegistry.TryGetAdapter<T>(out var adapter))
        {
            return adapter.ToDocument(entity);
        }

        return FallbackToDocument(entity);
    }

    public static T FromDocument<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(BsonDocument document)
        where T : class, new()
    {
        if (document == null) throw new ArgumentNullException(nameof(document));

        if (AotHelperRegistry.TryGetAdapter<T>(out var adapter))
        {
            return adapter.FromDocument(document);
        }

        return FallbackFromDocument<T>(document);
    }

    public static BsonValue GetId<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(T entity)
        where T : class, new()
    {
        if (entity == null) throw new ArgumentNullException(nameof(entity));

        if (AotHelperRegistry.TryGetAdapter<T>(out var adapter))
        {
            return adapter.GetId(entity);
        }

        var idProperty = EntityMetadata<T>.IdProperty;
        if (idProperty == null)
        {
            return BsonNull.Value;
        }

        var value = idProperty.GetValue(entity);
        return value != null ? BsonConversion.ToBsonValue(value) : BsonNull.Value;
    }

    public static void SetId<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(T entity, BsonValue id)
        where T : class, new()
    {
        if (entity == null) throw new ArgumentNullException(nameof(entity));

        if (AotHelperRegistry.TryGetAdapter<T>(out var adapter))
        {
            adapter.SetId(entity, id);
            return;
        }

        var idProperty = EntityMetadata<T>.IdProperty;
        if (idProperty == null || id == null || id.IsNull)
        {
            return;
        }

        var converted = BsonConversion.FromBsonValue(id, idProperty.PropertyType);
        idProperty.SetValue(entity, converted);
    }

    public static object? GetPropertyValue<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(T entity, string propertyName)
        where T : class, new()
    {
        if (entity == null) throw new ArgumentNullException(nameof(entity));
        if (string.IsNullOrWhiteSpace(propertyName)) throw new ArgumentNullException(nameof(propertyName));

        if (AotHelperRegistry.TryGetAdapter<T>(out var adapter))
        {
            return adapter.GetPropertyValue(entity, propertyName);
        }

        return EntityMetadata<T>.TryGetProperty(propertyName, out var property)
            ? property.GetValue(entity)
            : null;
    }

    private static BsonDocument FallbackToDocument<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(T entity)
        where T : class, new()
    {
        var document = new BsonDocument();

        var idProperty = EntityMetadata<T>.IdProperty;
        if (idProperty != null)
        {
            var id = idProperty.GetValue(entity);
            if (id != null)
            {
                document = document.Set("_id", BsonConversion.ToBsonValue(id));
            }
        }

        foreach (var property in EntityMetadata<T>.Properties)
        {
            if (idProperty != null && property.Name == idProperty.Name)
            {
                continue;
            }

            var value = property.GetValue(entity);

            var key = ToCamelCase(property.Name);
            var bsonValue = value != null ? BsonConversion.ToBsonValue(value) : BsonNull.Value;
            document = document.Set(key, bsonValue);
        }

        return document;
    }

    private static T FallbackFromDocument<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(BsonDocument document)
        where T : class, new()
    {
        var entity = new T();

        var idProperty = EntityMetadata<T>.IdProperty;
        var propertyMap = EntityMetadata<T>.Properties.ToDictionary(prop => ToCamelCase(prop.Name));

        foreach (var (key, bsonValue) in document)
        {
            if (key == "_id" && idProperty != null)
            {
                AotIdAccessor<T>.SetId(entity, bsonValue);
                continue;
            }

            if (!propertyMap.TryGetValue(key, out var property) || !property.CanWrite)
            {
                continue;
            }

            if (bsonValue == null || bsonValue.IsNull)
            {
                property.SetValue(entity, null);
                continue;
            }

            var value = BsonConversion.FromBsonValue(bsonValue, property.PropertyType);
            property.SetValue(entity, value);
        }

        return entity;
    }

    private static string ToCamelCase(string name)
    {
        if (string.IsNullOrEmpty(name)) return name;
        if (name.Length == 1) return name.ToLowerInvariant();
        return char.ToLowerInvariant(name[0]) + name.Substring(1);
    }
}
