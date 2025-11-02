using TinyDb.Core;
using TinyDb.Collections;
using TinyDb.Demo.Entities;
using TinyDb.Attributes;
using TinyDb.Bson;

namespace TinyDb.Demo.Demos;

/// <summary>
/// 性能测试功能演示
/// </summary>
public static class PerformanceDemo
{
    public static async Task RunAsync()
    {
        Console.WriteLine("=== 性能测试功能演示 ===");
        Console.WriteLine("展示TinyDb在不同场景下的性能表现");
        Console.WriteLine();

        const string dbPath = "performance_demo.db";
        if (File.Exists(dbPath)) File.Delete(dbPath);

        // 基础性能指标
        Console.WriteLine("1. 基础性能指标测试:");
        await TestBasicPerformance(dbPath);
        Console.WriteLine();

        // 大数据集性能
        Console.WriteLine("2. 大数据集性能测试:");
        await TestLargeDatasetPerformance(dbPath);
        Console.WriteLine();

        // 并发性能测试
        Console.WriteLine("3. 并发性能测试:");
        await TestConcurrentPerformance(dbPath);
        Console.WriteLine();

        // 内存使用测试
        Console.WriteLine("4. 内存使用分析:");
        await TestMemoryUsage(dbPath);
        Console.WriteLine();

        // 存储效率测试
        Console.WriteLine("5. 存储效率测试:");
        await TestStorageEfficiency(dbPath);
        Console.WriteLine();

        Console.WriteLine("✅ 性能测试演示完成！");
        Console.WriteLine("📊 TinyDb在各种场景下表现出色，适合轻量级应用需求");
    }

    private static async Task TestBasicPerformance(string dbPath)
    {
        using var engine = new TinyDbEngine(dbPath);
        var items = engine.GetCollection<PerformanceItem>("items");

        const int itemCount = 10000;

        // 插入性能测试
        Console.WriteLine($"   📝 插入性能测试 ({itemCount:N0} 条记录):");
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        for (int i = 0; i < itemCount; i++)
        {
            var item = new PerformanceItem
            {
                Name = $"Item {i}",
                Value = i,
                Category = $"Category {i % 10}",
                CreatedAt = DateTime.Now,
                Data = new string('x', 100) // 100字节的测试数据
            };

            items.Insert(item);
        }

        stopwatch.Stop();
        var insertRate = (double)itemCount / stopwatch.Elapsed.TotalSeconds;
        Console.WriteLine($"      ⏱️ 总耗时: {stopwatch.ElapsedMilliseconds}ms");
        Console.WriteLine($"      📈 插入速率: {insertRate:N0} 记录/秒");

        // 查询性能测试
        Console.WriteLine($"\n   🔍 查询性能测试:");
        var queries = new[]
        {
            ("按ID查询", () => items.FindOne(i => i.Id == ObjectId.NewObjectId())),
            ("按值查询", () => items.Find(i => i.Value == 5000).FirstOrDefault()),
            ("按类别查询", () => items.Find(i => i.Category == "Category 5").Take(100).ToList()),
            ("范围查询", () => items.Find(i => i.Value >= 4000 && i.Value <= 6000).ToList()),
            ("全表扫描", () => items.FindAll().ToList())
        };

        foreach (var (queryName, queryFunc) in queries)
        {
            stopwatch.Restart();
            var result = queryFunc();
            stopwatch.Stop();

            var count = result switch
            {
                null => 0,
                PerformanceItem item => 1,
                System.Collections.Generic.IEnumerable<PerformanceItem> list => list.Count(),
                _ => 0
            };

            Console.WriteLine($"      🔍 {queryName}: {count:N0} 条记录, {stopwatch.ElapsedMilliseconds}ms");
        }

        // 更新性能测试
        Console.WriteLine($"\n   ✏️ 更新性能测试 (1000 条记录):");
        var updateItems = items.Find(i => i.Value % 10 == 0).Take(1000).ToList();

        stopwatch.Restart();
        foreach (var item in updateItems)
        {
            item.UpdatedAt = DateTime.Now;
            item.Value += 1000;
            items.Update(item);
        }
        stopwatch.Stop();

        Console.WriteLine($"      ⏱️ 更新耗时: {stopwatch.ElapsedMilliseconds}ms");
        Console.WriteLine($"      📈 更新速率: {updateItems.Count / (stopwatch.Elapsed.TotalSeconds):N0} 记录/秒");

        // 删除性能测试
        Console.WriteLine($"\n   🗑️ 删除性能测试 (1000 条记录):");
        var deleteItems = items.Find(i => i.Value % 15 == 0).Take(1000).ToList();

        stopwatch.Restart();
        foreach (var item in deleteItems)
        {
            items.Delete(item.Id);
        }
        stopwatch.Stop();

        Console.WriteLine($"      ⏱️ 删除耗时: {stopwatch.ElapsedMilliseconds}ms");
        Console.WriteLine($"      📈 删除速率: {deleteItems.Count / (stopwatch.Elapsed.TotalSeconds):N0} 记录/秒");
    }

    private static async Task TestLargeDatasetPerformance(string dbPath)
    {
        const int largeItemCount = 100000;

        Console.WriteLine($"   📊 大数据集测试 ({largeItemCount:N0} 条记录):");

        using var engine = new TinyDbEngine(dbPath);
        var largeItems = engine.GetCollection<LargeItem>("large_items");

        // 批量插入大数据
        Console.WriteLine("   📝 批量插入大数据集...");
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        for (int i = 0; i < largeItemCount; i++)
        {
            var item = new LargeItem
            {
                Id = ObjectId.NewObjectId(),
                Name = $"Large Item {i}",
                Description = $"This is a large description for item {i} with lots of text to simulate real-world data",
                Tags = new[] { $"tag{i % 10}", $"category{i % 20}", $"type{i % 5}" },
                Metadata = new Dictionary<string, object>
                {
                    ["created"] = DateTime.Now.AddDays(-i % 365),
                    ["priority"] = i % 5,
                    ["status"] = i % 3 == 0 ? "active" : "inactive"
                },
                LargeText = new string('A', i % 1000), // 变长文本
                NumberValue = i * 1.234m
            };

            largeItems.Insert(item);

            if (i % 10000 == 0 && i > 0)
            {
                Console.WriteLine($"      📦 已插入 {i:N0} 条记录...");
            }
        }

        stopwatch.Stop();
        Console.WriteLine($"      ✅ 插入完成: {stopwatch.ElapsedMilliseconds}ms");
        Console.WriteLine($"      📈 平均速率: {largeItemCount / stopwatch.Elapsed.TotalSeconds:N0} 记录/秒");

        // 大数据查询性能
        Console.WriteLine("\n   🔍 大数据查询测试:");

        var queries = new[]
        {
            ("简单条件", () => largeItems.Find(i => i.NumberValue > 50000).Take(100).ToList()),
            ("复杂条件", () => largeItems.Find(i => i.Tags.Contains("tag5") && i.NumberValue > 25000).ToList()),
            ("文本搜索", () => largeItems.Find(i => i.Description.Contains("large")).Take(50).ToList()),
            ("元数据查询", () => largeItems.Find(i => i.Metadata.ContainsKey("priority")).Take(200).ToList())
        };

        foreach (var (queryName, queryFunc) in queries)
        {
            stopwatch.Restart();
            var result = queryFunc();
            stopwatch.Stop();

            Console.WriteLine($"      🔍 {queryName}: {result.Count:N0} 条记录, {stopwatch.ElapsedMilliseconds}ms");
        }

        // 内存使用情况
        var beforeGC = GC.GetTotalMemory(false);
        var allItems = largeItems.FindAll().Take(1000).ToList(); // 只取1000条避免内存过大
        var afterLoad = GC.GetTotalMemory(false);

        Console.WriteLine($"\n   💾 内存使用情况:");
        Console.WriteLine($"      📊 加载前: {beforeGC / 1024 / 1024:N1} MB");
        Console.WriteLine($"      📊 加载1000条后: {afterLoad / 1024 / 1024:N1} MB");
        Console.WriteLine($"      📊 平均每条: {(afterLoad - beforeLoad) / 1000:N0} bytes");
    }

    private static async Task TestConcurrentPerformance(string dbPath)
    {
        const int concurrentThreads = 10;
        const int operationsPerThread = 1000;

        Console.WriteLine($"   🔄 并发性能测试 ({concurrentThreads} 线程, 每线程 {operationsPerThread} 操作):");

        using var engine = new TinyDbEngine(dbPath);
        var concurrentItems = engine.GetCollection<ConcurrentItem>("concurrent_items");

        var tasks = new List<Task>();
        var successCount = 0;
        var errorCount = 0;
        var lockObject = new object();

        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        // 启动并发写入任务
        for (int threadId = 0; threadId < concurrentThreads; threadId++)
        {
            var currentThreadId = threadId;
            var task = Task.Run(() =>
            {
                try
                {
                    for (int i = 0; i < operationsPerThread; i++)
                    {
                        var item = new ConcurrentItem
                        {
                            ThreadId = currentThreadId,
                            Sequence = i,
                            Message = $"Thread {currentThreadId} - Operation {i}",
                            Timestamp = DateTime.Now
                        };

                        concurrentItems.Insert(item);

                        lock (lockObject)
                        {
                            successCount++;
                        }
                    }
                }
                catch (Exception ex)
                {
                    lock (lockObject)
                    {
                        errorCount++;
                    }
                    Console.WriteLine($"      ❌ 线程 {currentThreadId} 错误: {ex.Message}");
                }
            });

            tasks.Add(task);
        }

        // 等待所有任务完成
        await Task.WhenAll(tasks);
        stopwatch.Stop();

        Console.WriteLine($"      ⏱️ 总耗时: {stopwatch.ElapsedMilliseconds}ms");
        Console.WriteLine($"      ✅ 成功操作: {successCount:N0}");
        Console.WriteLine($"      ❌ 失败操作: {errorCount:N0}");
        Console.WriteLine($"      📈 并发速率: {successCount / stopwatch.Elapsed.TotalSeconds:N0} 操作/秒");

        // 验证数据一致性
        var totalItems = concurrentItems.Count();
        Console.WriteLine($"      📊 数据库记录数: {totalItems:N0}");
        Console.WriteLine($"      🔍 数据一致性: {(totalItems == successCount ? "✅ 一致" : "❌ 不一致")}");
    }

    private static async Task TestMemoryUsage(string dbPath)
    {
        Console.WriteLine("   💾 内存使用分析:");

        using var engine = new TinyDbEngine(dbPath);
        var memoryItems = engine.GetCollection<MemoryItem>("memory_items");

        // 测试不同数据大小的内存使用
        var dataSizes = new[] { 100, 500, 1000, 5000, 10000 };

        foreach (var size in dataSizes)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();

            var beforeMemory = GC.GetTotalMemory(false);

            // 创建指定大小的数据
            var item = new MemoryItem
            {
                Name = $"Memory Test {size}",
                Data = new string('A', size),
                CreatedAt = DateTime.Now
            };

            memoryItems.Insert(item);

            var afterInsert = GC.GetTotalMemory(false);
            var memoryUsage = afterInsert - beforeMemory;

            // 读取数据
            var loadedItem = memoryItems.FindOne(i => i.Id == item.Id);
            var afterLoad = GC.GetTotalMemory(false);

            Console.WriteLine($"      📊 {size,5} 字节: 插入 {memoryUsage:N0} bytes, 读取 {(afterLoad - afterInsert):N0} bytes");
        }

        // 测试内存泄漏
        Console.WriteLine("\n   🔍 内存泄漏检测:");
        var initialMemory = GC.GetTotalMemory(true);

        for (int i = 0; i < 1000; i++)
        {
            var item = new MemoryItem
            {
                Name = $"Leak Test {i}",
                Data = new string('B', 100),
                CreatedAt = DateTime.Now
            };

            memoryItems.Insert(item);

            if (i % 100 == 0)
            {
                GC.Collect();
                var currentMemory = GC.GetTotalMemory(false);
                var growth = currentMemory - initialMemory;
                Console.WriteLine($"      📈 插入 {i:N0} 条后内存增长: {growth / 1024 / 1024:N1} MB");
            }
        }
    }

    private static async Task TestStorageEfficiency(string dbPath)
    {
        Console.WriteLine("   💽 存储效率测试:");

        using var engine = new TinyDbEngine(dbPath);
        var storageItems = engine.GetCollection<StorageItem>("storage_items");

        // 测试不同数据类型的存储效率
        var testItems = new[]
        {
            new StorageItem { Name = "Small Text", Data = "Small", Type = "text" },
            new StorageItem { Name = "Medium Text", Data = new string('X', 1000), Type = "text" },
            new StorageItem { Name = "Large Text", Data = new string('Y', 10000), Type = "text" },
            new StorageItem { Name = "Numeric Data", Data = 12345.6789m, Type = "numeric" },
            new StorageItem { Name = "Date Data", Data = DateTime.Now, Type = "date" },
            new StorageItem { Name = "Binary Data", Data = new byte[5000], Type = "binary" }
        };

        var initialFileSize = new FileInfo(dbPath).Length;

        foreach (var item in testItems)
        {
            var beforeSize = new FileInfo(dbPath).Length;
            storageItems.Insert(item);
            var afterSize = new FileInfo(dbPath).Length;

            var storageOverhead = afterSize - beforeSize;
            var dataSize = item.Data switch
            {
                string s => Encoding.UTF8.GetByteCount(s),
                byte[] b => b.Length,
                decimal => 16,
                DateTime => 8,
                _ => 0
            };

            var efficiency = (double)dataSize / storageOverhead * 100;

            Console.WriteLine($"      📦 {item.Name}: 数据 {dataSize} bytes, 存储 {storageOverhead} bytes, 效率 {efficiency:F1}%");
        }

        var finalFileSize = new FileInfo(dbPath).Length;
        var totalGrowth = finalFileSize - initialFileSize;

        Console.WriteLine($"\n      📊 总文件增长: {totalGrowth:N0} bytes");
        Console.WriteLine($"      📊 平均每条记录: {totalGrowth / testItems.Length:N0} bytes");
    }
}

/// <summary>
/// 性能测试项目
/// </summary>
[Entity("items")]
public class PerformanceItem
{
    [Id]
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();

    public string Name { get; set; } = string.Empty;
    public int Value { get; set; }
    public string Category { get; set; } = string.Empty;
    public DateTime CreatedAt { get; set; }
    public DateTime? UpdatedAt { get; set; }
    public string Data { get; set; } = string.Empty;
}

/// <summary>
/// 大数据项目
/// </summary>
[Entity("large_items")]
public class LargeItem
{
    [Id]
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();

    public string Name { get; set; } = string.Empty;
    public string Description { get; set; } = string.Empty;
    public string[] Tags { get; set; } = Array.Empty<string>();
    public Dictionary<string, object> Metadata { get; set; } = new();
    public string LargeText { get; set; } = string.Empty;
    public decimal NumberValue { get; set; }
}

/// <summary>
/// 并发测试项目
/// </summary>
[Entity("concurrent_items")]
public class ConcurrentItem
{
    [Id]
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();

    public int ThreadId { get; set; }
    public int Sequence { get; set; }
    public string Message { get; set; } = string.Empty;
    public DateTime Timestamp { get; set; }
}

/// <summary>
/// 内存测试项目
/// </summary>
[Entity("memory_items")]
public class MemoryItem
{
    [Id]
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();

    public string Name { get; set; } = string.Empty;
    public string Data { get; set; } = string.Empty;
    public DateTime CreatedAt { get; set; }
}

/// <summary>
/// 存储效率测试项目
/// </summary>
[Entity("storage_items")]
public class StorageItem
{
    [Id]
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();

    public string Name { get; set; } = string.Empty;
    public object Data { get; set; } = null!;
    public string Type { get; set; } = string.Empty;
    public DateTime CreatedAt { get; set; } = DateTime.Now;
}