using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Threading;
using TinyDb.Bson;

namespace TinyDb.Serialization;

/// <summary>
/// AOT友好的BSON映射器，根据源生成器提供的辅助适配器进行序列化与反序列化。
/// </summary>
public static partial class AotBsonMapper
{
    // 简单的循环引用检测，使用ThreadLocal来跟踪当前正在序列化的对象
    private static readonly AsyncLocal<HashSet<object>?> SerializingObjects = new();

    internal static Type SerializationContextType => SerializingObjects.GetType();

    private const DynamicallyAccessedMemberTypes EntityMemberRequirements =
        DynamicallyAccessedMemberTypes.PublicConstructors |
        DynamicallyAccessedMemberTypes.PublicProperties |
        DynamicallyAccessedMemberTypes.PublicFields |
        DynamicallyAccessedMemberTypes.PublicMethods;

    private const DynamicallyAccessedMemberTypes TypeInspectionRequirements =
        EntityMemberRequirements | DynamicallyAccessedMemberTypes.Interfaces;

    private sealed class ReferenceEqualityComparer : IEqualityComparer<object>
    {
        public static readonly ReferenceEqualityComparer Instance = new();

        public new bool Equals(object? x, object? y) => ReferenceEquals(x, y);

        public int GetHashCode(object obj) => RuntimeHelpers.GetHashCode(obj);
    }

    /// <summary>
    /// 将实体转换为 BSON 文档。
    /// </summary>
    /// <typeparam name="T">实体类型。</typeparam>
    /// <param name="entity">实体对象。</param>
    /// <returns>BSON 文档。</returns>
    public static BsonDocument ToDocument<[DynamicallyAccessedMembers(EntityMemberRequirements)] T>(T entity)
    {
        if (entity == null) throw new ArgumentNullException(nameof(entity));

        if (entity is BsonDocument doc)
        {
            return doc;
        }

        // 循环引用检测
        var trackReference = !typeof(T).IsValueType;
        var serializingObjects = trackReference ? GetOrCreateSerializingObjects() : null;

        if (trackReference && serializingObjects!.Contains(entity))
        {
            // 检测到循环引用，返回空文档（或只包含ID的文档）
            if (AotHelperRegistry.TryGetAdapter<T>(out var circularAdapter))
            {
                var id = circularAdapter.GetId(entity);
                if (id != null && !id.IsNull)
                {
                    return new BsonDocument().Set("_id", id);
                }
            }
            return new BsonDocument();
        }

        if (trackReference)
        {
            serializingObjects!.Add(entity);
        }

        try
        {
            if (AotHelperRegistry.TryGetAdapter<T>(out var adapter))
            {
                return adapter.ToDocument(entity);
            }

            throw new InvalidOperationException(
                $"Type '{typeof(T).FullName}' must have [Entity] attribute for AOT serialization. " +
                $"Add [Entity] attribute to the type to enable source generator support.");
        }
        finally
        {
            if (trackReference && SerializingObjects.Value != null)
            {
                SerializingObjects.Value.Remove(entity);
                if (SerializingObjects.Value.Count == 0)
                {
                    SerializingObjects.Value = null;
                }
            }
        }
    }

    /// <summary>
    /// 将 BSON 文档转换为实体对象。
    /// </summary>
    /// <typeparam name="T">实体类型。</typeparam>
    /// <param name="document">BSON 文档。</param>
    /// <returns>实体对象。</returns>
    public static T FromDocument<[DynamicallyAccessedMembers(EntityMemberRequirements)] T>(BsonDocument document)
    {
        if (document == null) throw new ArgumentNullException(nameof(document));

        // Special case: if T is BsonDocument, return the document directly
        if (typeof(T) == typeof(BsonDocument))
        {
            return (T)(object)document;
        }

        if (AotHelperRegistry.TryGetAdapter<T>(out var adapter))
        {
            return adapter.FromDocument(document);
        }

        throw new InvalidOperationException(
            $"Type '{typeof(T).FullName}' must have [Entity] attribute for AOT serialization. " +
            $"Add [Entity] attribute to the type to enable source generator support.");
    }

    /// <summary>
    /// 获取实体的 ID 值。
    /// </summary>
    /// <typeparam name="T">实体类型。</typeparam>
    /// <param name="entity">实体对象。</param>
    /// <returns>BSON 格式的 ID。</returns>
    public static BsonValue GetId<[DynamicallyAccessedMembers(EntityMemberRequirements)] T>(T entity)
        where T : class
    {
        if (entity == null) throw new ArgumentNullException(nameof(entity));

        // Special case: if entity is BsonDocument, get _id directly
        if (entity is BsonDocument doc)
        {
            return doc.ContainsKey("_id") ? doc["_id"] : BsonNull.Value;
        }

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

    /// <summary>
    /// 设置实体的 ID 值。
    /// </summary>
    /// <typeparam name="T">实体类型。</typeparam>
    /// <param name="entity">实体对象。</param>
    /// <param name="id">新的 ID 值。</param>
    public static void SetId<[DynamicallyAccessedMembers(EntityMemberRequirements)] T>(T entity, BsonValue id)
        where T : class
    {
        if (entity == null) throw new ArgumentNullException(nameof(entity));

        // Special case: if entity is BsonDocument, set _id directly
        // Note: BsonDocument.Set returns a new instance, so this only works for mutable operations
        // For BsonDocument, the caller should use doc.Set("_id", id) instead
        if (entity is BsonDocument)
        {
            throw new NotSupportedException("BsonDocument is immutable. Use document.Set(\"_id\", id) to create a new document with the requested ID.");
        }

        AotIdAccessor<T>.SetId(entity, id);
    }

    /// <summary>
    /// 获取实体的属性值。
    /// </summary>
    /// <typeparam name="T">实体类型。</typeparam>
    /// <param name="entity">实体对象。</param>
    /// <param name="propertyName">属性名称。</param>
    /// <returns>属性值。</returns>
    public static object? GetPropertyValue<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(T entity, string propertyName)
        where T : class
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

    private static HashSet<object> GetOrCreateSerializingObjects()
    {
        return SerializingObjects.Value ??= new HashSet<object>(ReferenceEqualityComparer.Instance);
    }
}
