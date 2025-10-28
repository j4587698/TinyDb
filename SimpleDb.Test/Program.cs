using System;
using System.Linq;
using SimpleDb.Core;
using SimpleDb.Collections;
using SimpleDb.Attributes;
using SimpleDb.Bson;
using SimpleDb.Index;

namespace SimpleDb.Test;

/// <summary>
/// 自动索引功能测试程序
/// </summary>
public class Program
{
    public static async Task Main(string[] args)
    {
        Console.WriteLine("=== SimpleDb 自动索引功能测试 ===");
        Console.WriteLine();

        // 创建临时数据库
        var testDbFile = "autoindex_test.db";
        if (System.IO.File.Exists(testDbFile))
        {
            System.IO.File.Delete(testDbFile);
        }

        var options = new SimpleDbOptions
        {
            DatabaseName = "AutoIndexTestDb",
            PageSize = 8192,
            CacheSize = 1000
        };

        using var engine = new SimpleDbEngine(testDbFile, options);
        Console.WriteLine("✅ 数据库引擎创建成功！");

        // 测试基本自动索引创建
        await TestBasicAutoIndex(engine);

        // 测试唯一索引
        await TestUniqueIndex(engine);

        // 测试复合索引
        await TestCompositeIndex(engine);

        // 测试索引优先级
        await TestIndexPriority(engine);

        Console.WriteLine("\n=== 自动索引功能测试完成！ ===");
        Console.WriteLine($"数据库统计: {engine.GetStatistics()}");

        // 清理
        if (System.IO.File.Exists(testDbFile))
        {
            System.IO.File.Delete(testDbFile);
        }
    }

    /// <summary>
    /// 测试基本自动索引创建
    /// </summary>
    private static async Task TestBasicAutoIndex(SimpleDbEngine engine)
    {
        Console.WriteLine("--- 测试基本自动索引创建 ---");

        var users = engine.GetCollection<TestUser>("test_users");

        // 插入一些数据，自动索引会在集合创建时自动生成
        var testUsers = new[]
        {
            new TestUser
            {
                Name = "张三",
                Email = "zhangsan@test.com",
                Age = 25,
                Department = "研发部"
            },
            new TestUser
            {
                Name = "李四",
                Email = "lisi@test.com",
                Age = 30,
                Department = "销售部"
            },
            new TestUser
            {
                Name = "王五",
                Email = "wangwu@test.com",
                Age = 28,
                Department = "研发部"
            }
        };

        foreach (var user in testUsers)
        {
            users.Insert(user);
        }

        Console.WriteLine($"✅ 插入了 {testUsers.Length} 个测试用户，自动索引已创建");

        // 显示所有自动创建的索引
        var indexManager = users.GetIndexManager();
        var statistics = indexManager.GetAllStatistics();

        Console.WriteLine("📊 自动创建的索引:");
        foreach (var stat in statistics)
        {
            Console.WriteLine($"   - {stat}");
        }

        // 测试索引查询
        Console.WriteLine("\n🔍 测试索引查询:");
        var devUsers = users.Find(u => u.Department == "研发部").ToList();
        Console.WriteLine($"   研发部用户: {devUsers.Count} 人");

        var youngUsers = users.Find(u => u.Age < 30).ToList();
        Console.WriteLine($"   年轻用户 (<30): {youngUsers.Count} 人");

        // 清理数据
        foreach (var user in testUsers)
        {
            users.Delete(user.Id);
        }

        Console.WriteLine();
    }

    /// <summary>
    /// 测试唯一索引
    /// </summary>
    private static async Task TestUniqueIndex(SimpleDbEngine engine)
    {
        Console.WriteLine("--- 测试唯一索引 ---");

        var users = engine.GetCollection<TestUserWithUniqueEmail>("unique_test_users");

        // 插入第一个用户
        var user1 = new TestUserWithUniqueEmail
        {
            Name = "用户1",
            Email = "unique@test.com",
            Age = 25
        };
        users.Insert(user1);
        Console.WriteLine($"✅ 插入用户: {user1.Name} ({user1.Email})");

        // 尝试插入重复邮箱的用户
        var user2 = new TestUserWithUniqueEmail
        {
            Name = "用户2",
            Email = "unique@test.com", // 重复邮箱
            Age = 30
        };

        try
        {
            users.Insert(user2);
            Console.WriteLine($"❌ 意外成功: 插入了重复邮箱的用户");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"✅ 正确阻止重复邮箱: {ex.Message}");
        }

        // 插入不同邮箱的用户
        user2.Email = "different@test.com";
        users.Insert(user2);
        Console.WriteLine($"✅ 插入用户: {user2.Name} ({user2.Email})");

        // 清理数据
        users.Delete(user1.Id);
        users.Delete(user2.Id);

        Console.WriteLine();
    }

    /// <summary>
    /// 测试复合索引
    /// </summary>
    private static async Task TestCompositeIndex(SimpleDbEngine engine)
    {
        Console.WriteLine("--- 测试复合索引 ---");

        var orders = engine.GetCollection<TestOrder>("test_orders");

        // 插入订单数据
        var orderData = new[]
        {
            new TestOrder
            {
                OrderNumber = "ORD-001",
                CustomerId = "CUST-001",
                Status = "pending",
                Amount = 1000.50m
            },
            new TestOrder
            {
                OrderNumber = "ORD-002",
                CustomerId = "CUST-001",
                Status = "completed",
                Amount = 2500.75m
            },
            new TestOrder
            {
                OrderNumber = "ORD-003",
                CustomerId = "CUST-002",
                Status = "pending",
                Amount = 1500.00m
            }
        };

        foreach (var order in orderData)
        {
            orders.Insert(order);
        }

        Console.WriteLine($"✅ 插入了 {orderData.Length} 个测试订单");

        // 显示复合索引信息
        var indexManager = orders.GetIndexManager();
        var compositeIndex = indexManager.GetIndex("idx_customer_status");
        if (compositeIndex != null)
        {
            Console.WriteLine($"📊 复合索引信息: {compositeIndex.GetStatistics()}");
        }

        // 测试复合索引查询
        Console.WriteLine("\n🔍 测试复合索引查询:");
        var customerOrders = orders.Find(o => o.CustomerId == "CUST-001").ToList();
        Console.WriteLine($"   客户 CUST-001 的订单: {customerOrders.Count} 个");

        var pendingOrders = orders.Find(o => o.Status == "pending").ToList();
        Console.WriteLine($"   待处理订单: {pendingOrders.Count} 个");

        // 清理数据
        foreach (var order in orderData)
        {
            orders.Delete(order.Id);
        }

        Console.WriteLine();
    }

    /// <summary>
    /// 测试索引优先级
    /// </summary>
    private static async Task TestIndexPriority(SimpleDbEngine engine)
    {
        Console.WriteLine("--- 测试索引优先级 ---");

        var products = engine.GetCollection<TestProduct>("test_products");

        // 显示所有索引和优先级
        var allIndexes = IndexScanner.GetEntityIndexes(typeof(TestProduct));

        Console.WriteLine("📊 产品实体的索引定义 (按优先级排序):");
        foreach (var index in allIndexes)
        {
            Console.WriteLine($"   {index}");
        }

        Console.WriteLine();
    }
}

/// <summary>
/// 测试用户实体 - 基本索引
/// </summary>
[Entity("test_users")]
public class TestUser
{
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();

    [Index]
    public string Name { get; set; } = "";

    [Index(Unique = true)]
    public string Email { get; set; } = "";

    [Index]
    public int Age { get; set; }

    [Index]
    public string Department { get; set; } = "";

    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
}

/// <summary>
/// 测试用户实体 - 唯一邮箱索引
/// </summary>
[Entity("unique_test_users")]
public class TestUserWithUniqueEmail
{
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();

    public string Name { get; set; } = "";

    [Index(Unique = true)]
    public string Email { get; set; } = "";

    public int Age { get; set; }

    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
}

/// <summary>
/// 测试订单实体 - 复合索引
/// </summary>
[Entity("test_orders")]
[CompositeIndex("idx_customer_status", "CustomerId", "Status")]
public class TestOrder
{
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();

    [Index(Unique = true)]
    public string OrderNumber { get; set; } = "";

    [Index]
    public string CustomerId { get; set; } = "";

    [Index]
    public string Status { get; set; } = "";

    public decimal Amount { get; set; }

    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
}

/// <summary>
/// 测试产品实体 - 多个不同优先级的索引
/// </summary>
[Entity("test_products")]
public class TestProduct
{
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();

    [Index(Priority = 1)]
    public string Name { get; set; } = "";

    [Index(Priority = 5)]
    public string Category { get; set; } = "";

    [Index(Priority = 10)]
    public decimal Price { get; set; }

    [Index(Priority = 15)]
    public int Stock { get; set; }

    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
}