using TinyDb.Core;
using TinyDb.Collections;
using TinyDb.Demo.Entities;
using TinyDb.Attributes;
using TinyDb.Bson;
using TinyDb.IdGeneration;

namespace TinyDb.Demo.Demos;

/// <summary>
/// ID生成策略功能演示
/// </summary>
public static class IdGenerationDemo
{
    public static async Task RunAsync()
    {
        Console.WriteLine("=== ID生成策略功能演示 ===");
        Console.WriteLine("展示各种ID类型的生成和特性");
        Console.WriteLine();

        const string dbPath = "idgeneration_demo.db";
        if (File.Exists(dbPath)) File.Delete(dbPath);

        using var engine = new TinyDbEngine(dbPath);

        // ObjectId演示
        Console.WriteLine("1. ObjectId ID生成策略:");
        await DemonstrateObjectId(engine);

        Console.WriteLine("\n2. GUID ID生成策略:");
        await DemonstrateGuid(engine);

        Console.WriteLine("\n3. 自增整数ID生成策略:");
        await DemonstrateIdentity(engine);

        Console.WriteLine("\n4. 复合ID生成策略:");
        await DemonstrateCompositeId(engine);

        Console.WriteLine("\n5. ID性能对比:");
        await CompareIdPerformance(engine);

        Console.WriteLine("\n6. ID特性分析:");
        await AnalyzeIdCharacteristics();

        Console.WriteLine("\n✅ ID生成策略演示完成！");
        Console.WriteLine("🔧 TinyDb支持多种ID策略，满足不同业务场景需求");
    }

    private static async Task DemonstrateObjectId(TinyDbEngine engine)
    {
        var products = engine.GetCollection<ProductWithObjectId>("products_objectid");

        Console.WriteLine("   🆔 ObjectId特性演示:");

        // 创建多个产品
        for (int i = 1; i <= 5; i++)
        {
            var product = new ProductWithObjectId
            {
                Name = $"产品 {i}",
                Category = "电子产品",
                Price = 100 * i,
                CreatedAt = DateTime.Now
            };

            products.Insert(product);
            Console.WriteLine($"      📦 {product.Name}: {product.Id}");
        }

        // 演示ObjectId的排序特性
        var allProducts = products.FindAll().OrderBy(p => p.Id).ToList();
        Console.WriteLine("   📊 ObjectId按时间排序（内置时间戳）:");
        foreach (var product in allProducts)
        {
            var timestamp = product.Id.CreationTime;
            Console.WriteLine($"      {product.Id} → {product.Name} (创建时间: {timestamp:yyyy-MM-dd HH:mm:ss.fff})");
        }

        // 演示ObjectId的生成时间
        var now = DateTime.Now;
        var newObjectId = ObjectId.NewObjectId();
        var extractedTime = newObjectId.CreationTime;
        Console.WriteLine($"   ⏰ 新生成的ObjectId: {newObjectId}");
        Console.WriteLine($"   🕐 提取的时间: {extractedTime:yyyy-MM-dd HH:mm:ss.fff}");
        Console.WriteLine($"   ⏱️ 时间差: {(now - extractedTime).TotalMilliseconds:F0}ms");
    }

    private static async Task DemonstrateGuid(TinyDbEngine engine)
    {
        var users = engine.GetCollection<UserWithGuid>("users_guid");

        Console.WriteLine("   🆔 GUID特性演示:");

        // 演示不同版本的GUID
        var guidVersions = new[]
        {
            ("GUID v4", Guid.NewGuid()),
            ("GUID v7", GenerateGuidV7())
        };

        foreach (var (version, guid) in guidVersions)
        {
            Console.WriteLine($"   🎲 {version}: {guid} (版本: {GetGuidVersion(guid)})");
        }

        // 创建用户
        for (int i = 1; i <= 3; i++)
        {
            var user = new UserWithGuid
            {
                Username = $"user{i}",
                Email = $"user{i}@example.com",
                CreatedAt = DateTime.Now
            };

            users.Insert(user);
            Console.WriteLine($"      👤 {user.Username}: {user.Id}");
        }

        // GUID唯一性验证
        var allGuids = users.FindAll().Select(u => u.Id).ToList();
        var uniqueGuids = allGuids.Distinct().ToList();
        Console.WriteLine($"   🔍 GUID唯一性检查: {allGuids.Count} 个ID, {uniqueGuids.Count} 个唯一值");
    }

    private static async Task DemonstrateIdentity(TinyDbEngine engine)
    {
        var categories = engine.GetCollection<CategoryWithIdentity>("categories_identity");

        Console.WriteLine("   🆔 自增整数ID特性演示:");

        // 创建分类
        var categoryNames = new[] { "电子产品", "服装", "食品", "图书", "家居" };
        foreach (var name in categoryNames)
        {
            var category = new CategoryWithIdentity
            {
                Name = name,
                Description = $"{name}相关商品",
                CreatedAt = DateTime.Now
            };

            categories.Insert(category);
            Console.WriteLine($"      📂 {category.Name}: ID = {category.Id}");
        }

        // 演示ID的连续性
        var allCategories = categories.FindAll().OrderBy(c => c.Id).ToList();
        Console.WriteLine("   📊 ID连续性验证:");
        for (int i = 0; i < allCategories.Count; i++)
        {
            var category = allCategories[i];
            var expectedId = i + 1;
            var isSequential = category.Id == expectedId;
            Console.WriteLine($"      ID {category.Id}: {category.Name} {(isSequential ? '✅' : '❌')}");
        }
    }

    private static async Task DemonstrateCompositeId(TinyDbEngine engine)
    {
        var orders = engine.GetCollection<OrderWithCompositeId>("orders_composite");

        Console.WriteLine("   🆔 复合ID特性演示:");

        // 创建订单（使用业务ID）
        for (int i = 1; i <= 5; i++)
        {
            var order = new OrderWithCompositeId
            {
                OrderNumber = $"ORD-{DateTime.Now:yyyyMMdd}-{i:D4}",
                CustomerName = $"客户{i}",
                TotalAmount = 100 * i,
                OrderDate = DateTime.Now
            };

            orders.Insert(order);
            Console.WriteLine($"      🛒 {order.CustomerName}: {order.OrderNumber}");
        }

        // 演示业务ID的可读性
        var allOrders = orders.FindAll().ToList();
        Console.WriteLine("   📊 业务ID可读性:");
        foreach (var order in allOrders)
        {
            Console.WriteLine($"      {order.OrderNumber}: {order.CustomerName} - ¥{order.TotalAmount:N0}");
        }
    }

    private static async Task CompareIdPerformance(TinyDbEngine engine)
    {
        const int itemCount = 10000;

        Console.WriteLine($"   ⚡ ID生成性能测试 ({itemCount:N0} 个ID):");

        // ObjectId性能测试
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        var objectIds = new List<ObjectId>();
        for (int i = 0; i < itemCount; i++)
        {
            objectIds.Add(ObjectId.NewObjectId());
        }
        stopwatch.Stop();
        Console.WriteLine($"      ObjectId生成: {stopwatch.ElapsedMilliseconds}ms ({(double)stopwatch.ElapsedMilliseconds / itemCount * 1000000:F2} ns/ID)");

        // GUID性能测试
        stopwatch.Restart();
        var guids = new List<Guid>();
        for (int i = 0; i < itemCount; i++)
        {
            guids.Add(Guid.NewGuid());
        }
        stopwatch.Stop();
        Console.WriteLine($"      GUID生成: {stopwatch.ElapsedMilliseconds}ms ({(double)stopwatch.ElapsedMilliseconds / itemCount * 1000000:F2} ns/ID)");

        // 自增ID模拟测试
        stopwatch.Restart();
        var identityIds = new List<int>();
        for (int i = 1; i <= itemCount; i++)
        {
            identityIds.Add(i);
        }
        stopwatch.Stop();
        Console.WriteLine($"      自增ID生成: {stopwatch.ElapsedMilliseconds}ms ({(double)stopwatch.ElapsedMilliseconds / itemCount * 1000000:F2} ns/ID)");
    }

    private static async Task AnalyzeIdCharacteristics()
    {
        Console.WriteLine("   📊 ID类型特性对比:");

        var comparisonTable = new[]
        {
            ("ObjectId", "12字节", "时间戳+机器ID+进程ID+计数器", "分布式友好", "中等"),
            ("GUID v4", "16字节", "随机数", "全局唯一", "最大"),
            ("GUID v7", "16字节", "时间戳+随机数", "有序唯一", "大"),
            ("自增整数", "4/8字节", "序列号", "简单有序", "最小"),
            ("业务ID", "可变", "业务规则", "可读性强", "可变")
        };

        Console.WriteLine("      类型        | 大小   | 生成方式      | 特点       | 存储开销");
        Console.WriteLine("      ------------|--------|---------------|------------|----------");
        foreach (var (type, size, method, feature, overhead) in comparisonTable)
        {
            Console.WriteLine($"      {type,-12} | {size,-6} | {method,-13} | {feature,-10} | {overhead}");
        }

        Console.WriteLine("\n   🎯 使用建议:");
        Console.WriteLine("      📱 分布式系统: ObjectId 或 GUID v7");
        Console.WriteLine("      🏢 单体应用: 自增整数或业务ID");
        Console.WriteLine("      🔒 安全敏感: GUID v4 或 ObjectId");
        Console.WriteLine("      📊 高性能场景: 自增整数");
        Console.WriteLine("      👥 用户界面: 业务ID（可读性好）");
    }

    // 辅助方法
    private static Guid GenerateGuidV7()
    {
        // 简化的GUID v7生成（实际实现可能更复杂）
        var timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var bytes = new byte[16];
        Array.Copy(BitConverter.GetBytes(timestamp), 0, bytes, 0, 8);
        Random.Shared.NextBytes(bytes.AsSpan(8));
        bytes[7] = (byte)((bytes[7] & 0x0F) | 0x70); // 设置版本为7
        bytes[8] = (byte)((bytes[8] & 0x3F) | 0x80); // 设置变体
        return new Guid(bytes);
    }

    private static int GetGuidVersion(Guid guid)
    {
        var bytes = guid.ToByteArray();
        return (bytes[15] & 0xF0) >> 4;
    }
}

/// <summary>
/// 使用ObjectId的产品实体
/// </summary>
[Entity("products_objectid")]
public class ProductWithObjectId
{
    [Id]
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();

    public string Name { get; set; } = string.Empty;
    public string Category { get; set; } = string.Empty;
    public decimal Price { get; set; }
    public DateTime CreatedAt { get; set; }
}

/// <summary>
/// 使用GUID的用户实体
/// </summary>
[Entity("users_guid")]
public class UserWithGuid
{
    [Id]
    public Guid Id { get; set; } = Guid.NewGuid();

    public string Username { get; set; } = string.Empty;
    public string Email { get; set; } = string.Empty;
    public DateTime CreatedAt { get; set; }
}

/// <summary>
/// 使用自增ID的分类实体
/// </summary>
[Entity("categories_identity")]
public class CategoryWithIdentity
{
    [Id]
    public int Id { get; set; }

    public string Name { get; set; } = string.Empty;
    public string Description { get; set; } = string.Empty;
    public DateTime CreatedAt { get; set; }
}

/// <summary>
/// 使用复合ID的订单实体
/// </summary>
[Entity("orders_composite")]
public class OrderWithCompositeId
{
    [Id]
    public string OrderNumber { get; set; } = string.Empty;

    public string CustomerName { get; set; } = string.Empty;
    public decimal TotalAmount { get; set; }
    public DateTime OrderDate { get; set; }
}