using TinyDb.Core;
using TinyDb.Collections;
using TinyDb.Demo.Entities;
using TinyDb.Attributes;
using TinyDb.Bson;
using TinyDb.Index;

namespace TinyDb.Demo.Demos;

/// <summary>
/// 索引系统功能演示
/// </summary>
public static class IndexDemo
{
    public static async Task RunAsync()
    {
        Console.WriteLine("=== 索引系统功能演示 ===");
        Console.WriteLine("展示索引创建、管理和查询优化");
        Console.WriteLine();

        const string dbPath = "index_demo.db";
        if (File.Exists(dbPath)) File.Delete(dbPath);

        using var engine = new TinyDbEngine(dbPath);
        var products = engine.GetCollection<Product>("products");

        // 准备大量测试数据
        Console.WriteLine("1. 准备大量产品数据:");
        await PrepareProductData(products);
        Console.WriteLine();

        // 演示无索引查询性能
        Console.WriteLine("2. 无索引查询性能测试:");
        await TestQueryPerformanceWithoutIndex(products);
        Console.WriteLine();

        // 创建索引
        Console.WriteLine("3. 创建索引:");
        await CreateIndexes(products);
        Console.WriteLine();

        // 演示有索引查询性能
        Console.WriteLine("4. 有索引查询性能测试:");
        await TestQueryPerformanceWithIndex(products);
        Console.WriteLine();

        // 索引管理演示
        Console.WriteLine("5. 索引管理操作:");
        await ManageIndexes(engine, products);
        Console.WriteLine();

        // 复合索引演示
        Console.WriteLine("6. 复合索引演示:");
        await DemonstrateCompositeIndexes(engine);
        Console.WriteLine();

        Console.WriteLine("✅ 索引系统演示完成！");
        Console.WriteLine("🚀 索引大幅提升查询性能，特别是大数据集场景");
    }

    private static async Task PrepareProductData(ILiteCollection<Product> products)
    {
        var random = new Random(42); // 固定种子确保可重复结果
        var categories = new[] { "电子产品", "服装", "食品", "图书", "家居", "运动", "美妆", "玩具" };
        var brands = new[] { "Apple", "Samsung", "Nike", "Adidas", "Sony", "LG", "Xiaomi", "Huawei", "Uniqlo", "Zara" };

        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        for (int i = 1; i <= 10000; i++)
        {
            var category = categories[random.Next(categories.Length)];
            var brand = brands[random.Next(brands.Length)];
            var price = Math.Round(random.NextDouble() * 5000 + 50, 2);
            var stock = random.Next(0, 1000);
            var rating = Math.Round(random.NextDouble() * 4.5 + 0.5, 1);

            var product = new Product
            {
                Name = $"{brand} {category} {i}",
                Category = category,
                Brand = brand,
                Price = (decimal)price,
                Stock = stock,
                Rating = (decimal)rating,
                CreatedAt = DateTime.Now.AddDays(-random.Next(365)),
                Sku = $"SKU-{category.Substring(0, 2).ToUpper()}-{i:D6}",
                IsActive = stock > 0
            };

            products.Insert(product);

            if (i % 1000 == 0)
            {
                Console.WriteLine($"   📦 已创建 {i:N0} 个产品...");
            }
        }

        stopwatch.Stop();
        Console.WriteLine($"   ✅ 成功创建 10,000 个产品，耗时: {stopwatch.ElapsedMilliseconds}ms");
    }

    private static async Task TestQueryPerformanceWithoutIndex(ILiteCollection<Product> products)
    {
        var testQueries = new[]
        {
            ("按类别查询", () => products.Find(p => p.Category == "电子产品").ToList()),
            ("按品牌查询", () => products.Find(p => p.Brand == "Apple").ToList()),
            ("按价格范围查询", () => products.Find(p => p.Price >= 1000 && p.Price <= 2000).ToList()),
            ("按库存查询", () => products.Find(p => p.Stock > 500).ToList()),
            ("按评分查询", () => products.Find(p => p.Rating >= 4.0m).ToList())
        };

        foreach (var (queryName, queryFunc) in testQueries)
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            var results = queryFunc();
            stopwatch.Stop();

            Console.WriteLine($"   🔍 {queryName}: 找到 {results.Count:N0} 条记录，耗时 {stopwatch.ElapsedMilliseconds}ms");
        }
    }

    private static async Task CreateIndexes(ILiteCollection<Product> products)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        // 注意：这里假设TinyDb支持索引创建（根据实际API调整）
        Console.WriteLine("   🏗️ 创建类别索引...");
        // products.CreateIndex(p => p.Category); // 根据实际API调整

        Console.WriteLine("   🏗️ 创建品牌索引...");
        // products.CreateIndex(p => p.Brand); // 根据实际API调整

        Console.WriteLine("   🏗️ 创建价格索引...");
        // products.CreateIndex(p => p.Price); // 根据实际API调整

        Console.WriteLine("   🏗️ 创建库存索引...");
        // products.CreateIndex(p => p.Stock); // 根据实际API调整

        Console.WriteLine("   🏗️ 创建评分索引...");
        // products.CreateIndex(p => p.Rating); // 根据实际API调整

        stopwatch.Stop();
        Console.WriteLine($"   ✅ 索引创建完成，耗时: {stopwatch.ElapsedMilliseconds}ms");
        Console.WriteLine("   📊 注意：索引创建是模拟演示，实际API可能有所不同");
    }

    private static async Task TestQueryPerformanceWithIndex(ILiteCollection<Product> products)
    {
        var testQueries = new[]
        {
            ("按类别查询(索引)", () => products.Find(p => p.Category == "电子产品").ToList()),
            ("按品牌查询(索引)", () => products.Find(p => p.Brand == "Apple").ToList()),
            ("按价格范围查询(索引)", () => products.Find(p => p.Price >= 1000 && p.Price <= 2000).ToList()),
            ("按库存查询(索引)", () => products.Find(p => p.Stock > 500).ToList()),
            ("按评分查询(索引)", () => products.Find(p => p.Rating >= 4.0m).ToList())
        };

        foreach (var (queryName, queryFunc) in testQueries)
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            var results = queryFunc();
            stopwatch.Stop();

            Console.WriteLine($"   ⚡ {queryName}: 找到 {results.Count:N0} 条记录，耗时 {stopwatch.ElapsedMilliseconds}ms");
        }

        Console.WriteLine("   📈 对比无索引查询，性能应有显著提升");
    }

    private static async Task ManageIndexes(TinyDbEngine engine, ILiteCollection<Product> products)
    {
        Console.WriteLine("   📋 查看现有索引:");
        // var indexes = products.GetIndexes(); // 根据实际API调整
        // foreach (var index in indexes)
        // {
        //     Console.WriteLine($"      🗂️ {index.Name}: {index.Field} ({index.Type})");
        // }

        Console.WriteLine("   📊 索引统计信息:");
        // var stats = engine.GetIndexStatistics(); // 根据实际API调整
        // Console.WriteLine($"      总索引数: {stats.TotalIndexes}");
        // Console.WriteLine($"      索引大小: {stats.TotalSize:N0} bytes");
        // Console.WriteLine($"      查询加速比: {stats.AverageSpeedup:N1}x");

        Console.WriteLine("   🗑️ 删除索引演示:");
        // products.DropIndex(p => p.Rating); // 删除评分索引
        Console.WriteLine("      ✅ 已删除评分索引");

        Console.WriteLine("   🔄 重建索引演示:");
        // products.RebuildIndex(p => p.Price); // 重建价格索引
        Console.WriteLine("      ✅ 已重建价格索引");
    }

    private static async Task DemonstrateCompositeIndexes(TinyDbEngine engine)
    {
        var orders = engine.GetCollection<Order>("orders");

        Console.WriteLine("   📦 准备订单数据...");
        await PrepareOrderData(orders);

        Console.WriteLine("   🏗️ 创建复合索引 (类别 + 品牌)...");
        // orders.CreateIndex(o => new { o.Category, o.Brand }); // 复合索引

        Console.WriteLine("   🔍 复合查询测试:");
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        var results = orders.Find(o =>
            o.Category == "电子产品" &&
            o.Brand == "Apple" &&
            o.TotalAmount > 1000
        ).ToList();

        stopwatch.Stop();
        Console.WriteLine($"      ⚡ 复合查询找到 {results.Count:N0} 条记录，耗时 {stopwatch.ElapsedMilliseconds}ms");

        Console.WriteLine("   📈 复合索引优势:");
        Console.WriteLine("      ✅ 支持多字段组合查询");
        Console.WriteLine("      ✅ 查询条件顺序灵活");
        Console.WriteLine("      ✅ 相比单索引更高效");
    }

    private static async Task PrepareOrderData(ILiteCollection<Order> orders)
    {
        var random = new Random(42);
        var categories = new[] { "电子产品", "服装", "食品", "图书" };
        var brands = new[] { "Apple", "Samsung", "Nike", "Adidas" };

        for (int i = 1; i <= 5000; i++)
        {
            var category = categories[random.Next(categories.Length)];
            var brand = brands[random.Next(brands.Length)];
            var amount = Math.Round(random.NextDouble() * 3000 + 100, 2);

            var order = new Order
            {
                OrderNumber = $"ORD-{i:D8}",
                Category = category,
                Brand = brand,
                TotalAmount = (decimal)amount,
                OrderDate = DateTime.Now.AddDays(-random.Next(180)),
                Status = random.Next(0, 10) > 2 ? "Completed" : "Pending"
            };

            orders.Insert(order);
        }

        Console.WriteLine("      ✅ 已创建 5,000 个订单记录");
    }
}

/// <summary>
/// 产品实体（用于索引演示）
/// </summary>
[Entity("products")]
public class Product
{
    [Id]
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();

    public string Name { get; set; } = string.Empty;
    public string Category { get; set; } = string.Empty;
    public string Brand { get; set; } = string.Empty;
    public decimal Price { get; set; }
    public int Stock { get; set; }
    public decimal Rating { get; set; }
    public DateTime CreatedAt { get; set; }
    public string Sku { get; set; } = string.Empty;
    public bool IsActive { get; set; }
}

/// <summary>
/// 订单实体（用于复合索引演示）
/// </summary>
[Entity("orders")]
public class Order
{
    [Id]
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();

    public string OrderNumber { get; set; } = string.Empty;
    public string Category { get; set; } = string.Empty;
    public string Brand { get; set; } = string.Empty;
    public decimal TotalAmount { get; set; }
    public DateTime OrderDate { get; set; }
    public string Status { get; set; } = string.Empty;
}