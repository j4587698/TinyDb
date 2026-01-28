using System.Diagnostics.CodeAnalysis;
using TinyDb.Attributes;
using TinyDb.Bson;

namespace TinyDb.Tests.TestEntities;

/// <summary>
/// 用于测试 BsonIgnore 属性的实体（支持 AOT 源生成器）
/// </summary>
[Entity("users_with_ignored")]
[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)]
public partial class UserWithIgnoredFields
{
    [Id]
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();
    
    public string Username { get; set; } = "";
    public string Email { get; set; } = "";
    public bool IsActive { get; set; }
    
    /// <summary>
    /// 密码 - 使用 BsonIgnore 忽略，不会被序列化
    /// </summary>
    [BsonIgnore]
    public string Password { get; set; } = "";
    
    /// <summary>
    /// 临时会话Token - 使用 BsonIgnore 忽略
    /// </summary>
    [BsonIgnore]
    public string TempToken { get; set; } = "";
}

/// <summary>
/// 用于测试忽略计算属性的实体
/// </summary>
[Entity("products_with_computed")]
[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)]
public partial class ProductWithIgnoredComputed
{
    [Id]
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();
    
    public string Name { get; set; } = "";
    public decimal Price { get; set; }
    public int Quantity { get; set; }
    
    /// <summary>
    /// 计算属性：总价值 - 使用 BsonIgnore 忽略
    /// </summary>
    [BsonIgnore]
    public decimal TotalValue => Price * Quantity;
    
    /// <summary>
    /// 计算属性：显示信息 - 使用 BsonIgnore 忽略
    /// </summary>
    [BsonIgnore]
    public string DisplayInfo => $"{Name} x {Quantity}";
}

/// <summary>
/// 用于测试忽略缓存属性的实体
/// </summary>
[Entity("articles_with_cache")]
[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)]
public partial class ArticleWithIgnoredCache
{
    [Id]
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();
    
    public string Title { get; set; } = "";
    public string Content { get; set; } = "";
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    
    /// <summary>
    /// 缓存：字数统计 - 使用 BsonIgnore 忽略
    /// </summary>
    [BsonIgnore]
    public int CachedWordCount { get; set; }
    
    /// <summary>
    /// 缓存：缓存时间戳 - 使用 BsonIgnore 忽略
    /// </summary>
    [BsonIgnore]
    public DateTime? CacheTimestamp { get; set; }
}

/// <summary>
/// 用于测试忽略复杂类型的实体
/// </summary>
[Entity("entities_with_complex_ignored")]
[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)]
public partial class EntityWithIgnoredComplex
{
    [Id]
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();
    
    public string Name { get; set; } = "";
    
    /// <summary>
    /// 忽略的列表
    /// </summary>
    [BsonIgnore]
    public List<string>? IgnoredList { get; set; }
    
    /// <summary>
    /// 忽略的字典
    /// </summary>
    [BsonIgnore]
    public Dictionary<string, int>? IgnoredDict { get; set; }
}
