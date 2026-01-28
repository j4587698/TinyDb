using TinyDb.Core;
using TinyDb.Attributes;
using TinyDb.Bson;
using TinyDb.Serialization;

namespace TinyDb.Demo.Demos;

/// <summary>
/// BsonIgnore 属性演示
/// 展示如何使用 [BsonIgnore] 特性来排除不需要序列化的字段
/// </summary>
public static class BsonIgnoreDemo
{
    public static Task RunAsync()
    {
        Console.WriteLine("=== BsonIgnore 属性演示 ===");
        Console.WriteLine("演示如何使用 [BsonIgnore] 来忽略不需要序列化的字段");
        Console.WriteLine();

        // 创建临时数据库
        const string dbPath = "bson_ignore_demo.db";
        if (File.Exists(dbPath)) File.Delete(dbPath);

        using var engine = new TinyDbEngine(dbPath);
        var users = engine.GetCollection<UserWithIgnoredFields>();

        // 1. 演示敏感数据忽略
        Console.WriteLine("1. 敏感数据忽略演示:");
        Console.WriteLine("   创建用户实体，包含密码和临时Token（这些不应被存储）");
        
        var user = new UserWithIgnoredFields
        {
            Username = "admin",
            Email = "admin@example.com",
            Password = "super_secret_password_123",
            TempSessionToken = "temp_token_abc123",
            IsActive = true
        };
        
        Console.WriteLine($"   原始数据:");
        Console.WriteLine($"     - Username: {user.Username}");
        Console.WriteLine($"     - Email: {user.Email}");
        Console.WriteLine($"     - Password: {user.Password}");
        Console.WriteLine($"     - TempSessionToken: {user.TempSessionToken}");
        Console.WriteLine($"     - IsActive: {user.IsActive}");
        Console.WriteLine();

        // 插入数据
        var insertedId = users.Insert(user);
        Console.WriteLine($"   已插入用户 (ID: {insertedId})");

        // 从数据库读取
        var loadedUser = users.FindById(insertedId);
        Console.WriteLine($"   从数据库读取后:");
        Console.WriteLine($"     - Username: {loadedUser?.Username}");
        Console.WriteLine($"     - Email: {loadedUser?.Email}");
        Console.WriteLine($"     - Password: \"{loadedUser?.Password}\" (默认值，因为被忽略)");
        Console.WriteLine($"     - TempSessionToken: \"{loadedUser?.TempSessionToken}\" (默认值，因为被忽略)");
        Console.WriteLine($"     - IsActive: {loadedUser?.IsActive}");
        Console.WriteLine();

        // 2. 演示计算属性忽略
        Console.WriteLine("2. 计算属性忽略演示:");
        var products = engine.GetCollection<ProductWithComputedFields>();
        
        var product = new ProductWithComputedFields
        {
            Name = "高级笔记本电脑",
            Price = 5999.00m,
            Quantity = 10,
            DiscountPercent = 15
        };
        
        Console.WriteLine($"   产品数据:");
        Console.WriteLine($"     - Name: {product.Name}");
        Console.WriteLine($"     - Price: {product.Price:C}");
        Console.WriteLine($"     - Quantity: {product.Quantity}");
        Console.WriteLine($"     - DiscountPercent: {product.DiscountPercent}%");
        Console.WriteLine($"     - TotalValue (计算属性): {product.TotalValue:C}");
        Console.WriteLine($"     - DiscountedPrice (计算属性): {product.DiscountedPrice:C}");
        Console.WriteLine($"     - DisplayInfo (计算属性): {product.DisplayInfo}");
        Console.WriteLine();

        products.Insert(product);
        var loadedProduct = products.FindAll().FirstOrDefault();
        Console.WriteLine($"   从数据库读取后（计算属性会被重新计算）:");
        Console.WriteLine($"     - TotalValue: {loadedProduct?.TotalValue:C}");
        Console.WriteLine($"     - DiscountedPrice: {loadedProduct?.DiscountedPrice:C}");
        Console.WriteLine();

        // 3. 演示缓存属性忽略
        Console.WriteLine("3. 缓存属性忽略演示:");
        var articles = engine.GetCollection<ArticleWithCache>();
        
        var article = new ArticleWithCache
        {
            Title = "TinyDb使用指南",
            Content = "这是一篇关于TinyDb使用的详细文章..."
        };
        
        // 模拟设置缓存
        article.CachedWordCount = 1000;
        article.CacheTimestamp = DateTime.Now;
        article.CachedHtml = "<html><body>Cached HTML content</body></html>";
        
        Console.WriteLine($"   文章数据:");
        Console.WriteLine($"     - Title: {article.Title}");
        Console.WriteLine($"     - Content: {article.Content.Substring(0, Math.Min(30, article.Content.Length))}...");
        Console.WriteLine($"     - CachedWordCount: {article.CachedWordCount}");
        Console.WriteLine($"     - CacheTimestamp: {article.CacheTimestamp}");
        Console.WriteLine($"     - CachedHtml 长度: {article.CachedHtml?.Length ?? 0}");
        Console.WriteLine();

        articles.Insert(article);
        var loadedArticle = articles.FindAll().FirstOrDefault();
        Console.WriteLine($"   从数据库读取后（缓存属性不会被存储）:");
        Console.WriteLine($"     - Title: {loadedArticle?.Title}");
        Console.WriteLine($"     - CachedWordCount: {loadedArticle?.CachedWordCount} (默认值)");
        Console.WriteLine($"     - CacheTimestamp: {loadedArticle?.CacheTimestamp?.ToString() ?? "null"} (默认值)");
        Console.WriteLine($"     - CachedHtml: {loadedArticle?.CachedHtml ?? "null"} (默认值)");
        Console.WriteLine();

        // 4. 直接序列化演示
        Console.WriteLine("4. 直接序列化查看:");
        var doc = AotBsonMapper.ToDocument(user);
        Console.WriteLine($"   序列化后的 BsonDocument 键:");
        foreach (var key in doc.Keys)
        {
            Console.WriteLine($"     - {key}: {doc[key]}");
        }
        Console.WriteLine($"   注意: password 和 tempSessionToken 不在文档中");
        Console.WriteLine();

        // 清理
        if (File.Exists(dbPath)) File.Delete(dbPath);

        Console.WriteLine("✅ BsonIgnore 演示完成！");
        Console.WriteLine("📝 使用场景总结:");
        Console.WriteLine("   - 敏感数据（密码、Token等）");
        Console.WriteLine("   - 计算属性（派生自其他属性的值）");
        Console.WriteLine("   - 缓存数据（临时数据，不需要持久化）");
        Console.WriteLine("   - 运行时状态（如连接状态、锁对象等）");
        
        return Task.CompletedTask;
    }
}

/// <summary>
/// 演示用实体：包含敏感数据和临时数据的用户
/// </summary>
[Entity("users_with_ignored")]
public class UserWithIgnoredFields
{
    [Id]
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();
    
    public string Username { get; set; } = string.Empty;
    public string Email { get; set; } = string.Empty;
    public bool IsActive { get; set; }
    
    /// <summary>
    /// 密码 - 不应该被存储到数据库中
    /// 实际应用中应该存储哈希值，这里仅作演示
    /// </summary>
    [BsonIgnore]
    public string Password { get; set; } = string.Empty;
    
    /// <summary>
    /// 临时会话Token - 不需要持久化
    /// </summary>
    [BsonIgnore]
    public string TempSessionToken { get; set; } = string.Empty;
}

/// <summary>
/// 演示用实体：包含计算属性的产品
/// </summary>
[Entity("products_with_computed")]
public class ProductWithComputedFields
{
    [Id]
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();
    
    public string Name { get; set; } = string.Empty;
    public decimal Price { get; set; }
    public int Quantity { get; set; }
    public decimal DiscountPercent { get; set; }
    
    /// <summary>
    /// 计算属性：总价值 = 单价 * 数量
    /// </summary>
    [BsonIgnore]
    public decimal TotalValue => Price * Quantity;
    
    /// <summary>
    /// 计算属性：折扣后价格
    /// </summary>
    [BsonIgnore]
    public decimal DiscountedPrice => Price * (1 - DiscountPercent / 100);
    
    /// <summary>
    /// 计算属性：显示信息
    /// </summary>
    [BsonIgnore]
    public string DisplayInfo => $"{Name} - {Price:C} x {Quantity}";
}

/// <summary>
/// 演示用实体：包含缓存属性的文章
/// </summary>
[Entity("articles_with_cache")]
public class ArticleWithCache
{
    [Id]
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();
    
    public string Title { get; set; } = string.Empty;
    public string Content { get; set; } = string.Empty;
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    
    /// <summary>
    /// 缓存：字数统计
    /// </summary>
    [BsonIgnore]
    public int CachedWordCount { get; set; }
    
    /// <summary>
    /// 缓存：缓存时间戳
    /// </summary>
    [BsonIgnore]
    public DateTime? CacheTimestamp { get; set; }
    
    /// <summary>
    /// 缓存：渲染后的HTML
    /// </summary>
    [BsonIgnore]
    public string? CachedHtml { get; set; }
}
