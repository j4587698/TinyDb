using System;
using System.Linq;
using SimpleDb.Core;
using SimpleDb.Collections;
using SimpleDb.Attributes;
using SimpleDb.Bson;
using SimpleDb.Index;
using SimpleDb.Serialization;

namespace SimpleDb.Test;

/// <summary>
/// 索引调试测试
/// </summary>
public static class IndexDebugTest
{
    public static async Task RunAsync()
    {
        Console.WriteLine("=== SimpleDb 索引调试测试 ===");
        Console.WriteLine();

        // 创建临时数据库
        var testDbFile = "index_debug_test.db";
        if (System.IO.File.Exists(testDbFile))
        {
            System.IO.File.Delete(testDbFile);
        }

        var options = new SimpleDbOptions
        {
            DatabaseName = "IndexDebugTestDb",
            PageSize = 8192,
            CacheSize = 1000
        };

        using var engine = new SimpleDbEngine(testDbFile, options);
        Console.WriteLine("✅ 数据库引擎创建成功！");

        var users = engine.GetCollection<DebugUser>("debug_users");

        // 检查索引状态
        var indexManager = users.GetIndexManager();
        var allIndexes = indexManager.GetAllStatistics().ToList();

        Console.WriteLine($"📊 创建了 {allIndexes.Count} 个索引:");
        foreach (var index in allIndexes)
        {
            Console.WriteLine($"   - {index}");
        }

        // 插入一个用户
        var testUser = new DebugUser
        {
            Name = "DebugUser",
            Email = "debug@test.com",
            Age = 25,
            Department = "DebugDept"
        };

        Console.WriteLine($"\n🔍 准备插入用户: {testUser.Name}");

        // 转换为BsonDocument查看字段
        var bsonDoc = AotBsonMapper.ToDocument(testUser);
        Console.WriteLine("📄 BSON文档字段:");
        foreach (var kvp in bsonDoc)
        {
            Console.WriteLine($"   - {kvp.Key}: {kvp.Value}");
        }

        users.Insert(testUser);
        Console.WriteLine("✅ 用户插入完成");

        // 再次检查索引状态
        Console.WriteLine("\n📊 插入后索引状态:");
        foreach (var index in allIndexes)
        {
            var updatedStats = indexManager.GetIndex(index.Name)?.GetStatistics();
            Console.WriteLine($"   - {updatedStats}");
        }

        // 测试查询
        var foundUser = users.Query().Where(u => u.Age == 25).FirstOrDefault();
        Console.WriteLine($"\n🔍 查询结果: {foundUser?.Name}");

        // 清理
        if (System.IO.File.Exists(testDbFile))
        {
            System.IO.File.Delete(testDbFile);
        }

        Console.WriteLine("\n=== 索引调试测试完成！ ===");
    }
}

/// <summary>
/// 调试用户实体
/// </summary>
[Entity("debug_users")]
public class DebugUser
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