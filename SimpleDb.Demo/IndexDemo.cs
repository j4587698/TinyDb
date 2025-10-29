using System;
using System.Linq;
using SimpleDb.Core;
using SimpleDb.Collections;
using SimpleDb.Bson;
using SimpleDb.Attributes;
using SimpleDb.Index;

namespace SimpleDb.Demo;

/// <summary>
/// 索引功能演示
/// </summary>
public static class IndexDemo
{
    /// <summary>
    /// 运行索引演示
    /// </summary>
    public static Task RunAsync()
    {
        Console.WriteLine("=== SimpleDb 索引功能演示 ===");
        Console.WriteLine();

        // 创建临时数据库
        var testDbFile = "index_demo.db";
        if (System.IO.File.Exists(testDbFile))
        {
            System.IO.File.Delete(testDbFile);
        }

        var options = new SimpleDbOptions
        {
            DatabaseName = "IndexDemoDb",
            PageSize = 8192,
            CacheSize = 1000
        };

        using var engine = new SimpleDbEngine(testDbFile, options);
        var users = engine.GetCollection<IndexUser>("users");

        Console.WriteLine("✅ 数据库引擎创建成功！");

        // 演示基本索引操作
        BasicIndexDemo(users);

        // 演示复合索引
        CompositeIndexDemo(users);

        // 演示唯一索引
        UniqueIndexDemo(users);

        // 演示索引性能
        IndexPerformanceDemo(users);

        // 演示索引管理
        IndexManagementDemo(engine);

        Console.WriteLine("\n=== 索引演示完成！ ===");
        Console.WriteLine($"数据库统计: {engine.GetStatistics()}");

        // 清理
        if (System.IO.File.Exists(testDbFile))
        {
            System.IO.File.Delete(testDbFile);
        }

        return Task.CompletedTask;
    }

    /// <summary>
    /// 基本索引操作演示
    /// </summary>
    private static void BasicIndexDemo(ILiteCollection<IndexUser> users)
    {
        Console.WriteLine("--- 基本索引操作演示 ---");

        // 插入测试数据
        var testUsers = GenerateTestUsers(1000);
        foreach (var user in testUsers)
        {
            users.Insert(user);
        }

        Console.WriteLine($"✅ 插入了 {testUsers.Count} 个测试用户");

        // 创建单字段索引
        var indexManager = users.GetIndexManager();
        var created = indexManager.CreateIndex("idx_age", new[] { "age" });
        Console.WriteLine($"✅ 创建年龄索引: {(created ? "成功" : "失败")}");

        // 测试索引查询
        Console.WriteLine("🔍 测试索引查询:");

        var startTime = DateTime.UtcNow;

        // 查询年龄为25的用户
        var age25Users = users.Find(u => u.Age == 25).ToList();
        Console.WriteLine($"   年龄25的用户: {age25Users.Count} 个");

        // 查询年龄在20-30之间的用户
        var ageRangeUsers = users.Find(u => u.Age >= 20 && u.Age <= 30).ToList();
        Console.WriteLine($"   年龄20-30的用户: {ageRangeUsers.Count} 个");

        var endTime = DateTime.UtcNow;
        Console.WriteLine($"   ⏱️  查询耗时: {(endTime - startTime).TotalMilliseconds:F2} ms");

        // 显示索引统计
        var index = indexManager.GetIndex("idx_age");
        if (index != null)
        {
            var stats = index.GetStatistics();
            Console.WriteLine($"📊 索引统计: {stats}");
        }

        // 清理数据
        foreach (var user in testUsers)
        {
            users.Delete(user.Id);
        }

        Console.WriteLine();
    }

    /// <summary>
    /// 复合索引演示
    /// </summary>
    private static void CompositeIndexDemo(ILiteCollection<IndexUser> users)
    {
        Console.WriteLine("--- 复合索引演示 ---");

        // 插入测试数据
        var testUsers = GenerateTestUsers(500);
        foreach (var user in testUsers)
        {
            users.Insert(user);
        }

        Console.WriteLine($"✅ 插入了 {testUsers.Count} 个测试用户");

        // 创建复合索引
        var indexManager = users.GetIndexManager();
        var created = indexManager.CreateIndex("idx_age_city", new[] { "age", "city" });
        Console.WriteLine($"✅ 创建年龄+城市复合索引: {(created ? "成功" : "失败")}");

        // 测试复合索引查询
        Console.WriteLine("🔍 测试复合索引查询:");

        var startTime = DateTime.UtcNow;

        // 查询北京年龄25的用户
        var beijingUsers = users.Find(u => u.City == "北京" && u.Age == 25).ToList();
        Console.WriteLine($"   北京25岁用户: {beijingUsers.Count} 个");

        // 查询上海年龄30的用户
        var shanghaiUsers = users.Find(u => u.City == "上海" && u.Age == 30).ToList();
        Console.WriteLine($"   上海30岁用户: {shanghaiUsers.Count} 个");

        var endTime = DateTime.UtcNow;
        Console.WriteLine($"   ⏱️  查询耗时: {(endTime - startTime).TotalMilliseconds:F2} ms");

        // 显示索引统计
        var index = indexManager.GetIndex("idx_age_city");
        if (index != null)
        {
            var stats = index.GetStatistics();
            Console.WriteLine($"📊 复合索引统计: {stats}");
        }

        // 清理数据
        foreach (var user in testUsers)
        {
            users.Delete(user.Id);
        }

        Console.WriteLine();
    }

    /// <summary>
    /// 唯一索引演示
    /// </summary>
    private static void UniqueIndexDemo(ILiteCollection<IndexUser> users)
    {
        Console.WriteLine("--- 唯一索引演示 ---");

        // 创建唯一索引
        var indexManager = users.GetIndexManager();
        var created = indexManager.CreateIndex("idx_email", new[] { "email" }, true);
        Console.WriteLine($"✅ 创建邮箱唯一索引: {(created ? "成功" : "失败")}");

        // 插入第一个用户
        var user1 = new IndexUser
        {
            Name = "张三",
            Email = "zhangsan@example.com",
            Age = 25,
            City = "北京"
        };
        users.Insert(user1);
        Console.WriteLine($"✅ 插入用户: {user1.Name} ({user1.Email})");

        // 尝试插入重复邮箱的用户
        var user2 = new IndexUser
        {
            Name = "李四",
            Email = "zhangsan@example.com", // 重复邮箱
            Age = 30,
            City = "上海"
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
        user2.Email = "lisi@example.com";
        users.Insert(user2);
        Console.WriteLine($"✅ 插入用户: {user2.Name} ({user2.Email})");

        // 测试唯一索引查询
        var foundUser = users.FindOne(u => u.Email == "zhangsan@example.com");
        Console.WriteLine($"🔍 查询结果: {foundUser?.Name} ({foundUser?.Email})");

        // 清理数据
        users.Delete(user1.Id);
        users.Delete(user2.Id);

        Console.WriteLine();
    }

    /// <summary>
    /// 索引性能演示
    /// </summary>
    private static void IndexPerformanceDemo(ILiteCollection<IndexUser> users)
    {
        Console.WriteLine("--- 索引性能演示 ---");

        // 插入大量测试数据
        var testUsers = GenerateTestUsers(10000);
        foreach (var user in testUsers)
        {
            users.Insert(user);
        }

        Console.WriteLine($"✅ 插入了 {testUsers.Count} 个测试用户");

        // 无索引查询性能
        Console.WriteLine("🔍 无索引查询性能测试:");
        var startTime = DateTime.UtcNow;
        var noIndexResults = users.Find(u => u.Age == 25).ToList();
        var noIndexTime = DateTime.UtcNow - startTime;
        Console.WriteLine($"   查询年龄25的用户: {noIndexResults.Count} 个");
        Console.WriteLine($"   ⏱️  无索引查询耗时: {noIndexTime.TotalMilliseconds:F2} ms");

        // 创建索引
        var indexManager = users.GetIndexManager();
        indexManager.CreateIndex("idx_age_perf", new[] { "age" });
        Console.WriteLine("✅ 创建年龄索引");

        // 有索引查询性能
        Console.WriteLine("🔍 有索引查询性能测试:");
        startTime = DateTime.UtcNow;
        var indexedResults = users.Find(u => u.Age == 25).ToList();
        var indexedTime = DateTime.UtcNow - startTime;
        Console.WriteLine($"   查询年龄25的用户: {indexedResults.Count} 个");
        Console.WriteLine($"   ⏱️  有索引查询耗时: {indexedTime.TotalMilliseconds:F2} ms");

        // 性能提升计算
        if (noIndexTime.TotalMilliseconds > 0)
        {
            var improvement = (noIndexTime.TotalMilliseconds - indexedTime.TotalMilliseconds) / noIndexTime.TotalMilliseconds * 100;
            Console.WriteLine($"📈 性能提升: {improvement:F1}%");
        }

        // 清理数据
        foreach (var user in testUsers)
        {
            users.Delete(user.Id);
        }

        Console.WriteLine();
    }

    /// <summary>
    /// 索引管理演示
    /// </summary>
    private static void IndexManagementDemo(SimpleDbEngine engine)
    {
        Console.WriteLine("--- 索引管理演示 ---");

        var users = engine.GetCollection<IndexUser>("users");
        var indexManager = users.GetIndexManager();

        // 创建多个索引
        Console.WriteLine("📝 创建多个索引:");
        indexManager.CreateIndex("idx_name", new[] { "name" });
        Console.WriteLine("✅ 创建姓名索引");
        indexManager.CreateIndex("idx_age", new[] { "age" });
        Console.WriteLine("✅ 创建年龄索引");
        indexManager.CreateIndex("idx_city", new[] { "city" });
        Console.WriteLine("✅ 创建城市索引");
        indexManager.CreateIndex("idx_name_age", new[] { "name", "age" });
        Console.WriteLine("✅ 创建姓名+年龄复合索引");

        // 显示所有索引
        Console.WriteLine($"\n📋 当前索引数量: {indexManager.IndexCount}");
        foreach (var indexName in indexManager.IndexNames)
        {
            Console.WriteLine($"   - {indexName}");
        }

        // 获取所有索引统计
        Console.WriteLine("\n📊 索引统计信息:");
        var allStats = indexManager.GetAllStatistics();
        foreach (var stat in allStats)
        {
            Console.WriteLine($"   {stat}");
        }

        // 验证所有索引
        Console.WriteLine("\n🔍 验证所有索引:");
        var validationResult = indexManager.ValidateAllIndexes();
        Console.WriteLine($"   验证结果: {validationResult}");
        if (!validationResult.IsValid)
        {
            foreach (var error in validationResult.Errors)
            {
                Console.WriteLine($"   ❌ {error}");
            }
        }

        // 查找最佳索引
        Console.WriteLine("\n🎯 查找最佳索引:");
        var queryFields = new[] { "name", "age" };
        var bestIndex = indexManager.GetBestIndex(queryFields);
        if (bestIndex != null)
        {
            Console.WriteLine($"   查询字段 [{string.Join(", ", queryFields)}] 的最佳索引: {bestIndex.Name}");
        }
        else
        {
            Console.WriteLine($"   查询字段 [{string.Join(", ", queryFields)}] 没有合适的索引");
        }

        // 删除部分索引
        Console.WriteLine("\n🗑️  删除索引:");
        var deleted = indexManager.DropIndex("idx_city");
        Console.WriteLine($"   删除城市索引: {(deleted ? "成功" : "失败")}");

        Console.WriteLine($"📋 删除后索引数量: {indexManager.IndexCount}");

        // 清空所有索引
        Console.WriteLine("\n🧹 清空所有索引数据:");
        indexManager.ClearAllIndexes();
        Console.WriteLine("✅ 所有索引数据已清空");

        // 删除所有索引
        Console.WriteLine("\n💥 删除所有索引:");
        indexManager.DropAllIndexes();
        Console.WriteLine("✅ 所有索引已删除");
        Console.WriteLine($"📋 最终索引数量: {indexManager.IndexCount}");

        Console.WriteLine();
    }

    /// <summary>
    /// 生成测试用户数据
    /// </summary>
    private static List<IndexUser> GenerateTestUsers(int count)
    {
        var cities = new[] { "北京", "上海", "广州", "深圳", "杭州", "成都", "武汉", "西安" };
        var names = new[] { "张三", "李四", "王五", "赵六", "钱七", "孙八", "周九", "吴十" };
        var random = new Random(42); // 固定种子确保可重复性
        var users = new List<IndexUser>();

        for (int i = 0; i < count; i++)
        {
            users.Add(new IndexUser
            {
                Name = $"{names[random.Next(names.Length)]}{i}",
                Email = $"user{i}@example.com",
                Age = random.Next(18, 65),
                City = cities[random.Next(cities.Length)],
                Salary = random.Next(5000, 50000),
                Department = $"部门{random.Next(1, 10)}"
            });
        }

        return users;
    }
}

/// <summary>
/// 索引演示用户实体
/// </summary>
[Entity("index_users")]
public class IndexUser
{
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();
    public string Name { get; set; } = "";
    public string Email { get; set; } = "";
    public int Age { get; set; }
    public string City { get; set; } = "";
    public decimal Salary { get; set; }
    public string Department { get; set; } = "";
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
}
