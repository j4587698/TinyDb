using System;
using System.Linq;
using SimpleDb.Core;
using SimpleDb.Collections;
using SimpleDb.Query;

namespace DebugLinqWhere
{
    public class Program
    {
        public static void Main()
        {
            Console.WriteLine("=== 调试多个Where调用问题 ===");

            var testFile = Path.Combine(Path.GetTempPath(), "debug_linq.db");
            if (File.Exists(testFile)) File.Delete(testFile);

            try
            {
                using var engine = new SimpleDbEngine(testFile);
                var collection = engine.GetCollection<TestProduct>("Products");

                // 插入测试数据
                var products = new[]
                {
                    new TestProduct { Name = "Laptop", Price = 999.99m, Category = "Electronics", InStock = true },
                    new TestProduct { Name = "Mouse", Price = 29.99m, Category = "Electronics", InStock = true },
                    new TestProduct { Name = "Monitor", Price = 299.99m, Category = "Electronics", InStock = false },
                    new TestProduct { Name = "Book", Price = 19.99m, Category = "Books", InStock = true }
                };

                Console.WriteLine("1. 插入测试数据:");
                foreach (var product in products)
                {
                    collection.Insert(product);
                    Console.WriteLine($"   - {product.Name}: {product.Category}, {product.Price}, InStock={product.InStock}");
                }

                // 创建Queryable
                var executor = new QueryExecutor(engine);
                var queryable = new Queryable<TestProduct>(executor, "Products");

                Console.WriteLine("\n2. 测试单个Where条件:");

                // 测试第一个Where条件
                var electronics = queryable.Where(p => p.Category == "Electronics").ToList();
                Console.WriteLine($"   Category == 'Electronics': {electronics.Count} 个结果");
                foreach (var item in electronics)
                {
                    Console.WriteLine($"     - {item.Name}: {item.Category}, {item.Price}, InStock={item.InStock}");
                }

                // 测试第二个Where条件
                var inStock = queryable.Where(p => p.InStock).ToList();
                Console.WriteLine($"   InStock == true: {inStock.Count} 个结果");
                foreach (var item in inStock)
                {
                    Console.WriteLine($"     - {item.Name}: {item.Category}, {item.Price}, InStock={item.InStock}");
                }

                // 测试第三个Where条件
                var expensive = queryable.Where(p => p.Price > 50).ToList();
                Console.WriteLine($"   Price > 50: {expensive.Count} 个结果");
                foreach (var item in expensive)
                {
                    Console.WriteLine($"     - {item.Name}: {item.Category}, {item.Price}, InStock={item.InStock}");
                }

                Console.WriteLine("\n3. 测试链式Where调用:");
                // 这是测试期望的组合
                var chainedResults = queryable
                    .Where(p => p.Category == "Electronics")
                    .Where(p => p.InStock)
                    .Where(p => p.Price > 50)
                    .ToList();

                Console.WriteLine($"   链式Where结果: {chainedResults.Count} 个结果");
                foreach (var item in chainedResults)
                {
                    Console.WriteLine($"     - {item.Name}: {item.Category}, {item.Price}, InStock={item.InStock}");
                }

                Console.WriteLine("\n4. 预期分析:");
                Console.WriteLine("   Category == 'Electronics' → Laptop, Mouse, Monitor (3个)");
                Console.WriteLine("   InStock == true → Laptop, Mouse (2个)");
                Console.WriteLine("   Price > 50 → Laptop (1个)");
                Console.WriteLine("   最终应该只有1个结果: Laptop");

                if (chainedResults.Count == 1 && chainedResults[0].Name == "Laptop")
                {
                    Console.WriteLine("\n✅ 测试通过！");
                }
                else
                {
                    Console.WriteLine($"\n❌ 测试失败！期望1个结果但得到{chainedResults.Count}个");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"\n❌ 测试失败: {ex.GetType().Name}: {ex.Message}");
                Console.WriteLine($"堆栈跟踪: {ex.StackTrace}");
            }
            finally
            {
                if (File.Exists(testFile)) File.Delete(testFile);
            }
        }
    }

    public class TestProduct
    {
        public string Name { get; set; } = string.Empty;
        public decimal Price { get; set; }
        public string Category { get; set; } = string.Empty;
        public bool InStock { get; set; }
    }
}