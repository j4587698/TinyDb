using System.Diagnostics;
using SimpleDb.Attributes;
using SimpleDb.Core;
using SimpleDb.IdGeneration;
using System.Diagnostics.CodeAnalysis;

namespace SimpleDb.Demo;

/// <summary>
/// 演示用实体类
/// </summary>
[Entity("demo_users_int")]
[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)]
public partial class DemoUserWithIntId
{
    [IdGeneration(IdGenerationStrategy.IdentityInt)]
    public int Id { get; set; }
    public string Name { get; set; } = "";
    public int Age { get; set; }
    public string Email { get; set; } = "";
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
}

[Entity("demo_users_long")]
[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)]
public partial class DemoUserWithLongId
{
    [IdGeneration(IdGenerationStrategy.IdentityLong, "demo_users_long_seq")]
    public long Id { get; set; }
    public string Name { get; set; } = "";
    public int Age { get; set; }
    public string Email { get; set; } = "";
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
}

[Entity("demo_users_guidv7")]
[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)]
public partial class DemoUserWithGuidV7Id
{
    [IdGeneration(IdGenerationStrategy.GuidV7)]
    public string Id { get; set; } = "";
    public string Name { get; set; } = "";
    public int Age { get; set; }
    public string Email { get; set; } = "";
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
}


/// <summary>
/// 自定义序列实体（用于演示）
/// </summary>
[Entity("custom_sequence")]
[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)]
public partial class CustomSequenceEntity
{
    [IdGeneration(IdGenerationStrategy.IdentityLong, "my_custom_sequence")]
    public long Id { get; set; }
    public string Name { get; set; } = "";
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
}

/// <summary>
/// ID生成功能演示
/// </summary>
public static class IdGenerationDemo
{
    public static async Task RunAsync()
    {
        Console.WriteLine("=== SimpleDb ID生成功能演示 ===\n");

        var demoDbPath = "id_generation_demo.db";

        // 清理之前的演示数据库
        if (File.Exists(demoDbPath))
        {
            File.Delete(demoDbPath);
        }

        using var engine = new SimpleDbEngine(demoDbPath);

        // 1. 演示int自增ID
        await DemonstrateIntIdentityAsync(engine);

        // 2. 演示long自增ID
        await DemonstrateLongIdentityAsync(engine);

        // 3. 演示GUID v7 ID
        await DemonstrateGuidV7Async(engine);

        // 4. 演示更多GUID v7 ID
        await DemonstrateMoreGuidV7Async(engine);

        // 5. 演示ObjectId ID
        await DemonstrateObjectIdAsync(engine);

        // 6. 演示自定义序列
        await DemonstrateCustomSequenceAsync(engine);

        Console.WriteLine("\n=== 演示完成 ===");

        // 清理演示数据库
        engine.Dispose();
        if (File.Exists(demoDbPath))
        {
            File.Delete(demoDbPath);
        }
    }

    private static Task DemonstrateIntIdentityAsync(SimpleDbEngine engine)
    {
        Console.WriteLine("1. Int自增ID演示:");
        var stopwatch = Stopwatch.StartNew();

        var collection = engine.GetCollection<DemoUserWithIntId>();

        Console.WriteLine("插入用户记录...");
        for (int i = 1; i <= 5; i++)
        {
            var user = new DemoUserWithIntId
            {
                Name = $"用户{i}",
                Age = 20 + i,
                Email = $"user{i}@example.com"
            };

            collection.Insert(user);
            Console.WriteLine($"  插入: {user.Name}, 生成的ID: {user.Id}");
        }

        stopwatch.Stop();
        Console.WriteLine($"   耗时: {stopwatch.ElapsedMilliseconds}ms\n");
        return Task.CompletedTask;
    }

    private static Task DemonstrateLongIdentityAsync(SimpleDbEngine engine)
    {
        Console.WriteLine("2. Long自增ID演示:");
        var stopwatch = Stopwatch.StartNew();

        var collection = engine.GetCollection<DemoUserWithLongId>();

        Console.WriteLine("插入用户记录...");
        for (int i = 1; i <= 5; i++)
        {
            var user = new DemoUserWithLongId
            {
                Name = $"长期用户{i}",
                Age = 25 + i,
                Email = $"longuser{i}@example.com"
            };

            collection.Insert(user);
            Console.WriteLine($"  插入: {user.Name}, 生成的ID: {user.Id}");
        }

        stopwatch.Stop();
        Console.WriteLine($"   耗时: {stopwatch.ElapsedMilliseconds}ms\n");
        return Task.CompletedTask;
    }

    private static async Task DemonstrateGuidV7Async(SimpleDbEngine engine)
    {
        Console.WriteLine("3. GUID v7 (字符串类型)演示:");
        var stopwatch = Stopwatch.StartNew();

        var collection = engine.GetCollection<DemoUserWithGuidV7Id>();

        Console.WriteLine("插入用户记录...");
        for (int i = 1; i <= 3; i++)
        {
            var user = new DemoUserWithGuidV7Id
            {
                Name = $"GUID用户{i}",
                Age = 30 + i,
                Email = $"guiduser{i}@example.com"
            };

            collection.Insert(user);

            // 解析GUID以显示时间排序特性
            if (Guid.TryParse(user.Id, out var guid))
            {
                Console.WriteLine($"  插入: {user.Name}, GUID: {user.Id}");
                Console.WriteLine($"    GUID版本: {guid.Version}, 创建时间: {DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff}");
            }

            await Task.Delay(1); // 确保时间差
        }

        stopwatch.Stop();
        Console.WriteLine($"   耗时: {stopwatch.ElapsedMilliseconds}ms\n");
    }

    private static async Task DemonstrateMoreGuidV7Async(SimpleDbEngine engine)
    {
        Console.WriteLine("4. 更多GUID v7演示:");
        var stopwatch = Stopwatch.StartNew();

        var collection = engine.GetCollection<DemoUserWithGuidV7Id>();

        Console.WriteLine("插入更多用户记录...");
        for (int i = 4; i <= 6; i++)
        {
            var user = new DemoUserWithGuidV7Id
            {
                Name = $"GUID用户{i}",
                Age = 30 + i,
                Email = $"guiduser{i}@example.com"
            };

            collection.Insert(user);

            if (Guid.TryParse(user.Id, out var guid))
            {
                Console.WriteLine($"  插入: {user.Name}, GUID: {user.Id}");
                Console.WriteLine($"    GUID版本: {guid.Version}");
            }

            await Task.Delay(1);
        }

        stopwatch.Stop();
        Console.WriteLine($"   耗时: {stopwatch.ElapsedMilliseconds}ms\n");
    }

    private static Task DemonstrateObjectIdAsync(SimpleDbEngine engine)
    {
        Console.WriteLine("5. ObjectId演示:");
        var stopwatch = Stopwatch.StartNew();

        var collection = engine.GetCollection<User>();

        Console.WriteLine("插入用户记录...");
        for (int i = 1; i <= 3; i++)
        {
            var user = new User
            {
                Name = $"ObjectId用户{i}",
                Age = 35 + i,
                Email = $"objectiduser{i}@example.com"
            };

            collection.Insert(user);
            Console.WriteLine($"  插入: {user.Name}, ObjectId: {user.Id}");
            Console.WriteLine($"    时间戳: {user.Id.Timestamp:yyyy-MM-dd HH:mm:ss}");
        }

        stopwatch.Stop();
        Console.WriteLine($"   耗时: {stopwatch.ElapsedMilliseconds}ms\n");
        return Task.CompletedTask;
    }

    private static Task DemonstrateCustomSequenceAsync(SimpleDbEngine engine)
    {
        Console.WriteLine("6. 自定义序列演示:");
        var stopwatch = Stopwatch.StartNew();

        var collection = engine.GetCollection<CustomSequenceEntity>();

        Console.WriteLine("插入自定义序列记录...");
        for (int i = 1; i <= 5; i++)
        {
            var entity = new CustomSequenceEntity
            {
                Name = $"自定义实体{i}"
            };

            collection.Insert(entity);
            Console.WriteLine($"  插入: {entity.Name}, 序列ID: {entity.Id}");
        }

        stopwatch.Stop();
        Console.WriteLine($"   耗时: {stopwatch.ElapsedMilliseconds}ms\n");
        return Task.CompletedTask;
    }
}
