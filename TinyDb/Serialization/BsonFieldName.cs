using System.Collections.Concurrent;
using System.Reflection;
using System.Text;
using System.Diagnostics.CodeAnalysis;
using TinyDb.Attributes;

namespace TinyDb.Serialization;

internal static class BsonFieldName
{
    /// <summary>
    /// 主键的保留存储字段名。它是类型擦除的主键通道：
    /// 引擎在没有 CLR 实体类型上下文时（裸 BsonDocument、冷启动重建主键索引）只能依赖这个字面量。
    /// </summary>
    public const string Id = "_id";

    private static readonly byte[] IdBytes = Encoding.UTF8.GetBytes("_id");
    private static readonly byte[] CollectionBytes = Encoding.UTF8.GetBytes("_collection");
    private static readonly byte[] IsLargeDocumentBytes = Encoding.UTF8.GetBytes("_isLargeDocument");
    private static readonly byte[] LargeDocumentIndexBytes = Encoding.UTF8.GetBytes("_largeDocumentIndex");
    private static readonly byte[] LargeDocumentSizeBytes = Encoding.UTF8.GetBytes("_largeDocumentSize");

    private static readonly ConcurrentDictionary<Type, string?> IdPropertyNameCache = new();

    public static string ForProperty(
        PropertyInfo property,
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] Type? entityType = null)
    {
        if (property == null) throw new ArgumentNullException(nameof(property));
        return IsIdProperty(property, entityType) ? Id : ToCamelCase(property.Name);
    }

    /// <summary>
    /// 查询层唯一的 “CLR 成员名 -&gt; BSON 存储字段名” 解析入口。
    /// </summary>
    /// <param name="memberName">CLR 属性名，例如 <c>Uid</c>。</param>
    /// <param name="declaringType">声明该成员的根实体类型；为 null 时退化为命名约定。</param>
    /// <remarks>
    /// 仅适用于根文档成员。<c>_id</c> 改名只发生在集合根文档上：
    /// 源生成器为内嵌复杂对象生成的序列化函数一律使用 camelCase，
    /// 不会把内嵌对象的 <c>Id</c> 写成 <c>_id</c>。
    /// 内嵌成员请直接使用 <see cref="ToCamelCase"/>。
    /// </remarks>
    public static string ForMember(
        string memberName,
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] Type? declaringType)
    {
        if (string.IsNullOrEmpty(memberName)) return memberName;

        return IsIdMember(memberName, declaringType) ? Id : ToCamelCase(memberName);
    }

    /// <summary>
    /// 判断某个 CLR 成员是否为 <paramref name="declaringType"/> 的主键属性。
    /// </summary>
    public static bool IsIdMember(
        string memberName,
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] Type? declaringType)
    {
        if (string.IsNullOrEmpty(memberName)) return false;

        if (declaringType != null && TryGetIdPropertyName(declaringType, out var idPropertyName))
        {
            return string.Equals(idPropertyName, memberName, StringComparison.Ordinal);
        }

        if (declaringType != null)
        {
            // 有类型上下文但该类型没有主键属性：没有任何成员会映射到 _id。
            return false;
        }

        // 无类型上下文（SQL / 字符串查询 / 手工构造的表达式）：
        // 与 SqlQueryParser.NormalizeIdSegment 一致，id / _id 大小写不敏感地视为主键别名。
        return string.Equals(memberName, "id", StringComparison.OrdinalIgnoreCase) ||
               string.Equals(memberName, Id, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// 解析某个实体类型的主键属性 CLR 名称。源生成器注册的适配器优先，其次反射特性/命名约定。
    /// </summary>
    public static bool TryGetIdPropertyName(
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] Type declaringType,
        [NotNullWhen(true)] out string? idPropertyName)
    {
        idPropertyName = IdPropertyNameCache.GetOrAdd(declaringType, static type => ResolveIdPropertyName(type));
        return idPropertyName != null;
    }

    [UnconditionalSuppressMessage("TrimAnalysis", "IL2070",
        Justification = "Only reached for types without a source-generated adapter; expression-tree queries already require reflection.")]
    private static string? ResolveIdPropertyName(Type type)
    {
        // 1) 源生成器注册的适配器 —— 编译期确定，与序列化端完全一致。
        if (AotHelperRegistry.TryGetAdapter(type, out var adapter) &&
            !string.IsNullOrWhiteSpace(adapter.IdPropertyName))
        {
            return adapter.IdPropertyName;
        }

        // 2) [Entity(IdProperty = ...)]
        var entityAttribute = type.GetCustomAttribute<EntityAttribute>();
        if (!string.IsNullOrWhiteSpace(entityAttribute?.IdProperty))
        {
            return entityAttribute!.IdProperty;
        }

        // 3) [Id] 特性 / 命名约定
        string? conventional = null;
        foreach (var property in type.GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            if (property.GetCustomAttribute<IdAttribute>() != null)
            {
                return property.Name;
            }

            if (conventional == null && IsConventionalIdName(property.Name))
            {
                conventional = property.Name;
            }
        }

        return conventional;
    }

    private static bool IsConventionalIdName(string name)
    {
        return name is "Id" or "_id" or "ID";
    }

    internal static void ClearIdPropertyNameCache() => IdPropertyNameCache.Clear();

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

    private static bool IsIdProperty(
        PropertyInfo property,
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] Type? entityType)
    {
        var attribute = entityType?.GetCustomAttribute<EntityAttribute>();
        if (!string.IsNullOrWhiteSpace(attribute?.IdProperty))
        {
            if (string.Equals(attribute.IdProperty, property.Name, StringComparison.Ordinal))
            {
                return true;
            }

            if (entityType?.GetProperty(attribute.IdProperty!) == null)
            {
                throw new InvalidOperationException(
                    $"Entity type '{entityType.FullName}' specifies IdProperty '{attribute.IdProperty}', but no public mapped property with that name exists.");
            }

            return false;
        }

        if (property.GetCustomAttribute<IdAttribute>() != null)
        {
            return true;
        }

        return property.Name is "Id" or "_id" or "ID";
    }
}
