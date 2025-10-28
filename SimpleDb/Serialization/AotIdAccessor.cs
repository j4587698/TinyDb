using System.Diagnostics.CodeAnalysis;
using SimpleDb.Bson;

namespace SimpleDb.Serialization;

/// <summary>
/// 为实体类型提供AOT友好的ID访问操作。
/// </summary>
public static class AotIdAccessor<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>
    where T : class, new()
{
    public static BsonValue GetId(T entity)
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

    public static void SetId(T entity, BsonValue id)
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

    public static bool HasValidId(T entity)
    {
        if (entity == null) return false;

        if (AotHelperRegistry.TryGetAdapter<T>(out var adapter))
        {
            return adapter.HasValidId(entity);
        }

        var idProperty = EntityMetadata<T>.IdProperty;
        if (idProperty == null)
        {
            return false;
        }

        var value = idProperty.GetValue(entity);
        if (value == null)
        {
            return false;
        }

        return value switch
        {
            ObjectId objectId => objectId != ObjectId.Empty,
            string str => !string.IsNullOrWhiteSpace(str),
            Guid guid => guid != Guid.Empty,
            int i => i != 0,
            long l => l != 0,
            _ => true
        };
    }
}
