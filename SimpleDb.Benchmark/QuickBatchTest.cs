using System;
using System.Diagnostics;
using SimpleDb.Core;
using SimpleDb.Collections;
using SimpleDb.Attributes;
using SimpleDb.Bson;

namespace SimpleDb.Benchmark;

public class QuickBatchTest
{
    public static void RunTest()
    {
        Console.WriteLine("=== 快速批量插入测试 ===");
        Console.WriteLine();

        const string DatabaseFile = "quick_batch_test.db";

        // 清理旧数据库文件
        if (System.IO.File.Exists(DatabaseFile))
        {
            System.IO.File.Delete(DatabaseFile);
        }

        var options = new SimpleDbOptions
        {
            DatabaseName = "QuickBatchTestDb",
            PageSize = 16384,
            CacheSize = 1000,
            EnableJournaling = false
        };

        using var engine = new SimpleDbEngine(DatabaseFile, options);
        var collection = engine.GetCollection<TestUser>("test_users");

        Console.WriteLine("✅ 测试环境已设置");

        // 测试1：单独插入100条记录
        Console.WriteLine("\n📊 测试1: 单独插入100条记录");
        var sw1 = Stopwatch.StartNew();

        for (int i = 0; i < 100; i++)
        {
            var user = new TestUser
            {
                Name = $"User{i}",
                Email = $"user{i}@test.com",
                Age = 20 + (i % 50)
            };
            collection.Insert(user);
        }

        sw1.Stop();
        Console.WriteLine($"   单独插入耗时: {sw1.ElapsedMilliseconds} ms");
        Console.WriteLine($"   平均每条: {(double)sw1.ElapsedMilliseconds / 100:F2} ms");

        // 清空数据
        var allUsers = collection.FindAll().ToList();
        foreach (var user in allUsers)
        {
            collection.Delete(user.Id);
        }

        // 测试2：批量插入100条记录
        Console.WriteLine("\n📊 测试2: 批量插入100条记录");
        var sw2 = Stopwatch.StartNew();

        var users = new List<TestUser>();
        for (int i = 0; i < 100; i++)
        {
            users.Add(new TestUser
            {
                Name = $"User{i}",
                Email = $"user{i}@test.com",
                Age = 20 + (i % 50)
            });
        }
        collection.Insert(users);

        sw2.Stop();
        Console.WriteLine($"   批量插入耗时: {sw2.ElapsedMilliseconds} ms");
        Console.WriteLine($"   平均每条: {(double)sw2.ElapsedMilliseconds / 100:F2} ms");

        // 计算性能提升
        var improvement = (double)(sw1.ElapsedMilliseconds - sw2.ElapsedMilliseconds) / sw1.ElapsedMilliseconds * 100;
        Console.WriteLine($"\n🚀 性能提升: {improvement:F1}%");

        // 验证数据正确性
        var finalCount = collection.FindAll().Count();
        Console.WriteLine($"✅ 数据验证: 插入成功 {finalCount} 条记录");

        // 清理
        engine.Dispose();
        if (System.IO.File.Exists(DatabaseFile))
        {
            System.IO.File.Delete(DatabaseFile);
        }

        Console.WriteLine("\n=== 快速批量插入测试完成 ===");
    }
}

[Entity("test_users")]
public class TestUser
{
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();

    [Index(Priority = 1)]
    public string Name { get; set; } = "";

    [Index(Unique = true, Priority = 2)]
    public string Email { get; set; } = "";

    [Index(Priority = 3)]
    public int Age { get; set; }
}