using TinyDb.Core;
using TinyDb.Collections;
using TinyDb.Demo.Entities;
using TinyDb.Attributes;
using TinyDb.Bson;

namespace TinyDb.Demo.Demos;

/// <summary>
/// 基于实际API的简化CRUD演示
/// </summary>
public static class SimpleCrudDemo
{
    public static async Task RunAsync()
    {
        Console.WriteLine("=== 简化CRUD操作演示 ===");
        Console.WriteLine("基于SimpleDb实际API的真实演示");
        Console.WriteLine();

        // 创建临时数据库
        const string dbPath = "simple_crud_demo.db";
        if (File.Exists(dbPath)) File.Delete(dbPath);

        using var engine = new TinyDbEngine(dbPath);
        var products = engine.GetCollection<Product>("products");

        // 1. 创建 (Create)
        Console.WriteLine("1. 创建产品记录:");
        var laptop = new Product
        {
            Name = "超薄笔记本",
            Price = 6999.99m,
            Category = "电子产品",
            Stock = 50
        };

        var insertedId = products.Insert(laptop);
        Console.WriteLine($"   ✅ 插入产品: {laptop.Name} (ID: {insertedId})");

        var mouse = new Product
        {
            Name = "无线鼠标",
            Price = 99.99m,
            Category = "电子产品",
            Stock = 200
        };

        products.Insert(mouse);
        Console.WriteLine($"   ✅ 插入产品: {mouse.Name} (ID: {mouse.Id})");

        // 2. 读取 (Read)
        Console.WriteLine("\n2. 查询产品记录:");
        var allProducts = products.FindAll().ToList();
        Console.WriteLine($"   📊 总产品数: {allProducts.Count}");

        // 条件查询
        var electronics = products.Find(p => p.Category == "电子产品").ToList();
        Console.WriteLine($"   🔌 电子产品数: {electronics.Count}");

        var expensiveProducts = products.Find(p => p.Price > 1000).ToList();
        Console.WriteLine($"   💰 高价产品(>1000元): {expensiveProducts.Count}");

        // 3. 更新 (Update)
        Console.WriteLine("\n3. 更新产品记录:");
        var updateProduct = products.Find(p => p.Name == "超薄笔记本").FirstOrDefault();
        if (updateProduct != null)
        {
            Console.WriteLine($"   更新前: {updateProduct.Name} - 库存: {updateProduct.Stock}, 价格: {updateProduct.Price}");
            updateProduct.Stock = 45;
            updateProduct.Price = 6499.99m;
            products.Update(updateProduct);
            Console.WriteLine($"   更新后: {updateProduct.Name} - 库存: {updateProduct.Stock}, 价格: {updateProduct.Price}");
        }

        // 4. 删除 (Delete)
        Console.WriteLine("\n4. 删除产品记录:");
        var deleteProduct = products.Find(p => p.Name == "无线鼠标").FirstOrDefault();
        if (deleteProduct != null)
        {
            Console.WriteLine($"   🗑️ 删除产品: {deleteProduct.Name}");
            products.Delete(deleteProduct.Id);
            Console.WriteLine($"   ✅ 删除成功");
        }

        // 验证删除结果
        var remainingProducts = products.FindAll().ToList();
        Console.WriteLine($"   📊 剩余产品数: {remainingProducts.Count}");

        // 5. 批量操作
        Console.WriteLine("\n5. 批量操作:");
        var batchProducts = new[]
        {
            new Product { Name = "机械键盘", Price = 299.99m, Category = "电子产品", Stock = 100 },
            new Product { Name = "显示器", Price = 1299.99m, Category = "电子产品", Stock = 30 },
            new Product { Name = "USB集线器", Price = 49.99m, Category = "电子产品", Stock = 150 }
        };

        foreach (var product in batchProducts)
        {
            products.Insert(product);
        }
        Console.WriteLine($"   📦 批量插入 {batchProducts.Length} 个产品");

        var finalCount = products.FindAll().Count();
        Console.WriteLine($"   📊 最终产品总数: {finalCount}");

        // 显示数据库统计信息
        Console.WriteLine($"\n数据库统计: {engine.GetStatistics()}");

        // 清理
        engine.Dispose();
        if (File.Exists(dbPath)) File.Delete(dbPath);

        Console.WriteLine("✅ 简化CRUD演示完成！");
    }
}

[Entity("demo_products")]
public class SimpleProduct
{
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();
    public string Name { get; set; } = string.Empty;
    public decimal Price { get; set; }
    public string Category { get; set; } = string.Empty;
    public int Stock { get; set; }
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
}