using System;
using System.Linq;
using System.Diagnostics;
using SimpleDb.Core;
using SimpleDb.Collections;
using SimpleDb.Attributes;
using SimpleDb.Bson;
using SimpleDb.Index;

namespace SimpleDb.Test;

/// <summary>
/// 索引使用情况测试
/// </summary>
public static class IndexUsageTest
{
    public static async Task RunAsync()
    {
        Console.WriteLine("=== SimpleDb 索引使用情况测试 ===");
        Console.WriteLine();

        // 创建临时数据库
        var testDbFile = "index_usage_test.db";
        if (System.IO.File.Exists(testDbFile))
        {
            System.IO.File.Delete(testDbFile);
        }

        var options = new SimpleDbOptions
        {
            DatabaseName = "IndexUsageTestDb",
            PageSize = 8192,
            CacheSize = 1000
        };

        using var engine = new SimpleDbEngine(testDbFile, options);
        Console.WriteLine("✅ 数据库引擎创建成功！");

        // 测试1: 检查主键是否默认有索引
        await TestPrimaryKeyIndex(engine);

        // 测试2: 检查查询是否使用索引
        await TestQueryIndexUsage(engine);

        // 测试3: 检查更新删除时是否处理索引
        await TestUpdateDeleteIndexSync(engine);

        // 测试4: 检查Take/Skip是否使用索引
        await TestTakeSkipIndexUsage(engine);

        Console.WriteLine("\n=== 索引使用情况测试完成！ ===");

        // 清理
        if (System.IO.File.Exists(testDbFile))
        {
            System.IO.File.Delete(testDbFile);
        }
    }

    /// <summary>
    /// 测试主键是否默认有索引
    /// </summary>
    private static async Task TestPrimaryKeyIndex(SimpleDbEngine engine)
    {
        Console.WriteLine("--- 测试1: 主键索引检查 ---");

        var users = engine.GetCollection<TestUserForIndexUsage>("pk_test_users");

        // 检查索引管理器中的索引
        var indexManager = users.GetIndexManager();
        var allIndexes = indexManager.GetAllStatistics().ToList();

        Console.WriteLine($"📊 集合中的索引数量: {allIndexes.Count}");
        foreach (var index in allIndexes)
        {
            Console.WriteLine($"   - {index}");
        }

        // 检查是否有主键索引
        var hasIdIndex = allIndexes.Any(idx => idx.Name.Contains("_id") || idx.Name.Contains("id"));
        Console.WriteLine($"✅ 主键索引存在: {hasIdIndex}");

        if (!hasIdIndex)
        {
            Console.WriteLine("⚠️  警告: 主键索引不存在，这会影响FindById性能！");
        }

        Console.WriteLine();
    }

    /// <summary>
    /// 测试查询是否使用索引
    /// </summary>
    private static async Task TestQueryIndexUsage(SimpleDbEngine engine)
    {
        Console.WriteLine("--- 测试2: 查询索引使用检查 ---");

        var users = engine.GetCollection<TestUserForIndexUsage>("query_test_users");

        // 插入测试数据
        var testUsers = Enumerable.Range(1, 1000)
            .Select(i => new TestUserForIndexUsage
            {
                Name = $"User{i}",
                Email = $"user{i}@test.com",
                Age = i % 100,
                Department = $"Department{i % 10}"
            })
            .ToList();

        var sw = Stopwatch.StartNew();
        foreach (var user in testUsers)
        {
            users.Insert(user);
        }
        sw.Stop();

        Console.WriteLine($"✅ 插入 {testUsers.Count} 个用户，耗时: {sw.ElapsedMilliseconds}ms");

        // 测试FindById性能（应该使用主键索引）
        var targetUser = testUsers[500];
        sw.Restart();
        var foundUser = users.FindById(targetUser.Id);
        sw.Stop();

        Console.WriteLine($"🔍 FindById 查询耗时: {sw.ElapsedMilliseconds}ms");
        Console.WriteLine($"   查询结果: {(foundUser?.Name == targetUser.Name ? "成功" : "失败")}");

        // 测试普通查询性能（应该使用属性索引）
        sw.Restart();
        var ageQuery = users.Query().Where(u => u.Age == 25).ToList();
        sw.Stop();

        Console.WriteLine($"🔍 Age=25 查询耗时: {sw.ElapsedMilliseconds}ms，结果数量: {ageQuery.Count}");

        sw.Restart();
        var deptQuery = users.Query().Where(u => u.Department == "Department5").ToList();
        sw.Stop();

        Console.WriteLine($"🔍 Department=Department5 查询耗时: {sw.ElapsedMilliseconds}ms，结果数量: {deptQuery.Count}");

        // 检查索引数据
        var indexManager = users.GetIndexManager();
        var ageIndex = indexManager.GetIndex("idx_age");
        var deptIndex = indexManager.GetIndex("idx_department");

        Console.WriteLine($"📊 Age索引条目数: {ageIndex?.EntryCount ?? 0}");
        Console.WriteLine($"📊 Department索引条目数: {deptIndex?.EntryCount ?? 0}");

        if ((ageIndex?.EntryCount ?? 0) == 0 || (deptIndex?.EntryCount ?? 0) == 0)
        {
            Console.WriteLine("⚠️  警告: 索引数据为空，说明插入时没有更新索引！");
        }

        Console.WriteLine();
    }

    /// <summary>
    /// 测试更新删除时是否处理索引
    /// </summary>
    private static async Task TestUpdateDeleteIndexSync(SimpleDbEngine engine)
    {
        Console.WriteLine("--- 测试3: 更新删除索引同步检查 ---");

        var users = engine.GetCollection<TestUserForIndexUsage>("update_test_users");

        // 插入测试数据
        var testUser = new TestUserForIndexUsage
        {
            Name = "TestUser",
            Email = "test@test.com",
            Age = 30,
            Department = "TestDept"
        };
        users.Insert(testUser);

        var indexManager = users.GetIndexManager();
        var ageIndex = indexManager.GetIndex("idx_age");

        Console.WriteLine($"📊 插入后Age索引条目数: {ageIndex?.EntryCount ?? 0}");

        // 更新数据
        testUser.Age = 35;
        users.Update(testUser);

        Console.WriteLine($"📊 更新后Age索引条目数: {ageIndex?.EntryCount ?? 0}");

        // 验证查询
        var oldAgeQuery = users.Query().Where(u => u.Age == 30).ToList();
        var newAgeQuery = users.Query().Where(u => u.Age == 35).ToList();

        Console.WriteLine($"🔍 Age=30 查询结果: {oldAgeQuery.Count} 个");
        Console.WriteLine($"🔍 Age=35 查询结果: {newAgeQuery.Count} 个");

        // 删除数据
        users.Delete(testUser.Id);

        Console.WriteLine($"📊 删除后Age索引条目数: {ageIndex?.EntryCount ?? 0}");

        var deletedQuery = users.Query().Where(u => u.Age == 35).ToList();
        Console.WriteLine($"🔍 删除后Age=35 查询结果: {deletedQuery.Count} 个");

        if (ageIndex?.EntryCount > 0)
        {
            Console.WriteLine("⚠️  警告: 删除后索引仍有数据，说明删除时没有正确更新索引！");
        }

        Console.WriteLine();
    }

    /// <summary>
    /// 测试Take/Skip是否使用索引
    /// </summary>
    private static async Task TestTakeSkipIndexUsage(SimpleDbEngine engine)
    {
        Console.WriteLine("--- 测试4: Take/Skip索引使用检查 ---");

        var users = engine.GetCollection<TestUserForIndexUsage>("takeskip_test_users");

        // 插入测试数据
        var testUsers = Enumerable.Range(1, 1000)
            .Select(i => new TestUserForIndexUsage
            {
                Name = $"User{i}",
                Email = $"user{i}@test.com",
                Age = 25 + (i % 50), // 25-74岁
                Department = $"Department{i % 5}"
            })
            .ToList();

        foreach (var user in testUsers)
        {
            users.Insert(user);
        }

        Console.WriteLine($"✅ 插入 {testUsers.Count} 个用户");

        // 测试Take操作
        var sw = Stopwatch.StartNew();
        var takenUsers = users.Query().Where(u => u.Age >= 50).Take(10).ToList();
        sw.Stop();

        Console.WriteLine($"🔍 Age>=50 Take(10) 查询耗时: {sw.ElapsedMilliseconds}ms，结果数量: {takenUsers.Count}");

        // 测试Skip操作
        sw.Restart();
        var skippedUsers = users.Query().Where(u => u.Department == "Department1").Skip(5).Take(10).ToList();
        sw.Stop();

        Console.WriteLine($"🔍 Department=Department1 Skip(5) Take(10) 查询耗时: {sw.ElapsedMilliseconds}ms，结果数量: {skippedUsers.Count}");

        // 测试分页操作
        sw.Restart();
        var pagedUsers = users.Query().Where(u => u.Age >= 40).Skip(100).Take(50).ToList();
        sw.Stop();

        Console.WriteLine($"🔍 Age>=40 Skip(100) Take(50) 分页查询耗时: {sw.ElapsedMilliseconds}ms，结果数量: {pagedUsers.Count}");

        Console.WriteLine();
    }
}

/// <summary>
/// 测试用户实体
/// </summary>
[Entity("index_test_users")]
public class TestUserForIndexUsage
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