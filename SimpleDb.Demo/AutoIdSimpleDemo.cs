using System.Diagnostics.CodeAnalysis;
using SimpleDb.Attributes;
using SimpleDb.Core;

namespace SimpleDb.Demo;

/// <summary>
/// 演示用实体类
/// </summary>
[Entity("auto_int_users")]
[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)]
public partial class AutoIntUser
{
    public int Id { get; set; }
    public string Name { get; set; } = "";
    public int Age { get; set; }
}

[Entity("auto_long_products")]
[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)]
public partial class AutoLongProduct
{
    public long Id { get; set; }
    public string Name { get; set; } = "";
    public decimal Price { get; set; }
}

[Entity("auto_guid_orders")]
[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)]
public partial class AutoGuidOrder
{
    public Guid Id { get; set; }
    public string OrderNumber { get; set; } = "";
    public decimal Amount { get; set; }
}

[Entity("auto_string_logs")]
[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)]
public partial class AutoStringLog
{
    public string Id { get; set; } = "";
    public string Message { get; set; } = "";
    public string Level { get; set; } = "INFO";
}

/// <summary>
/// 自动ID生成演示
/// </summary>
public static class AutoIdSimpleDemo
{
    public static async Task RunAsync()
    {
        Console.WriteLine("=== SimpleDb 自动ID生成演示 ===\n");

        var demoDbPath = "auto_id_demo.db";

        // 清理之前的演示数据库
        if (File.Exists(demoDbPath))
        {
            File.Delete(demoDbPath);
        }

        using var engine = new SimpleDbEngine(demoDbPath);

        // 演示1: int类型自动生成
        DemonstrateAutoIntId(engine);

        // 演示2: long类型自动生成
        DemonstrateAutoLongId(engine);

        // 演示3: Guid类型自动生成
        DemonstrateAutoGuidId(engine);

        // 演示4: string类型自动生成GUID
        DemonstrateAutoStringId(engine);

        Console.WriteLine("\n=== 自动ID生成演示完成 ===");

        // 清理演示数据库
        engine.Dispose();
        if (File.Exists(demoDbPath))
        {
            File.Delete(demoDbPath);
        }
    }

    private static void DemonstrateAutoIntId(SimpleDbEngine engine)
    {
        Console.WriteLine("1. int类型自动ID生成演示:");

        var collection = engine.GetCollection<AutoIntUser>();

        Console.WriteLine("插入用户记录（ID会自动生成）...");
        for (int i = 1; i <= 3; i++)
        {
            var user = new AutoIntUser
            {
                Name = $"用户{i}",
                Age = 20 + i
            };

            collection.Insert(user);
            Console.WriteLine($"  插入: {user.Name}, 自动生成的ID: {user.Id}");
        }

        Console.WriteLine();
    }

    private static void DemonstrateAutoLongId(SimpleDbEngine engine)
    {
        Console.WriteLine("2. long类型自动ID生成演示:");

        var collection = engine.GetCollection<AutoLongProduct>();

        Console.WriteLine("插入产品记录（ID会自动生成）...");
        for (int i = 1; i <= 3; i++)
        {
            var product = new AutoLongProduct
            {
                Name = $"产品{i}",
                Price = 10.99m * i
            };

            collection.Insert(product);
            Console.WriteLine($"  插入: {product.Name}, 自动生成的ID: {product.Id}");
        }

        Console.WriteLine();
    }

    private static void DemonstrateAutoGuidId(SimpleDbEngine engine)
    {
        Console.WriteLine("3. Guid类型自动ID生成演示:");

        var collection = engine.GetCollection<AutoGuidOrder>();

        Console.WriteLine("插入订单记录（GUID会自动生成）...");
        for (int i = 1; i <= 2; i++)
        {
            var order = new AutoGuidOrder
            {
                OrderNumber = $"ORD-{i:D3}",
                Amount = 100.00m * i
            };

            collection.Insert(order);
            Console.WriteLine($"  插入: {order.OrderNumber}, 自动生成的GUID: {order.Id}");
        }

        Console.WriteLine();
    }

    private static void DemonstrateAutoStringId(SimpleDbEngine engine)
    {
        Console.WriteLine("4. string类型自动GUID生成演示:");

        var collection = engine.GetCollection<AutoStringLog>();

        Console.WriteLine("插入日志记录（字符串GUID会自动生成）...");
        for (int i = 1; i <= 2; i++)
        {
            var log = new AutoStringLog
            {
                Message = $"日志消息 {i}",
                Level = i % 2 == 0 ? "INFO" : "WARNING"
            };

            collection.Insert(log);
            Console.WriteLine($"  插入: {log.Message}, 自动生成的GUID: {log.Id}");
        }

        Console.WriteLine();
    }
}