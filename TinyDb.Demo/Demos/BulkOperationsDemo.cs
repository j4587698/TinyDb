using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using TinyDb.Core;
using TinyDb.Collections;
using TinyDb.Bson;
using TinyDb.Attributes;

namespace TinyDb.Demo.Demos;

/// <summary>
/// 批量操作和性能演示
/// </summary>
public static class BulkOperationsDemo
{
    public static Task RunAsync()
    {
        Console.WriteLine("=== TinyDb 批量操作与性能演示 ===");
        Console.WriteLine();

        var dbPath = "bulk_demo.db";
        if (File.Exists(dbPath)) File.Delete(dbPath);

        using var engine = new TinyDbEngine(dbPath);
        var logs = engine.GetCollection<LogEntry>();

        // 批量插入演示
        Console.WriteLine("1. 批量插入演示");
        Console.WriteLine(new string('-', 50));
        BulkInsertDemo(logs);
        Console.WriteLine();

        // 单条插入 vs 批量插入性能对比
        Console.WriteLine("2. 单条插入 vs 批量插入性能对比");
        Console.WriteLine(new string('-', 50));
        PerformanceComparisonDemo(engine);
        Console.WriteLine();

        // 批量更新演示
        Console.WriteLine("3. 批量更新演示");
        Console.WriteLine(new string('-', 50));
        BulkUpdateDemo(logs);
        Console.WriteLine();

        // 批量删除演示
        Console.WriteLine("4. 批量删除演示");
        Console.WriteLine(new string('-', 50));
        BulkDeleteDemo(logs);
        Console.WriteLine();

        // 大数据量处理演示
        Console.WriteLine("5. 大数据量处理演示");
        Console.WriteLine(new string('-', 50));
        LargeDatasetDemo(engine);
        Console.WriteLine();

        // 分批处理演示
        Console.WriteLine("6. 分批处理演示");
        Console.WriteLine(new string('-', 50));
        BatchProcessingDemo(engine);
        Console.WriteLine();

        // 清理
        if (File.Exists(dbPath)) File.Delete(dbPath);

        Console.WriteLine("✅ 批量操作与性能演示完成！");
        return Task.CompletedTask;
    }

    /// <summary>
    /// 批量插入演示
    /// </summary>
    private static void BulkInsertDemo(ITinyCollection<LogEntry> logs)
    {
        // 生成测试数据
        var testData = GenerateLogEntries(1000);

        var sw = Stopwatch.StartNew();
        var insertedCount = logs.Insert(testData);
        sw.Stop();

        Console.WriteLine($"✅ 批量插入 {insertedCount} 条记录");
        Console.WriteLine($"   总耗时: {sw.ElapsedMilliseconds}ms");
        Console.WriteLine($"   平均每条: {(double)sw.ElapsedMilliseconds / insertedCount:F3}ms");
        Console.WriteLine($"   吞吐量: {insertedCount * 1000.0 / sw.ElapsedMilliseconds:F0} 条/秒");
    }

    /// <summary>
    /// 性能对比演示
    /// </summary>
    private static void PerformanceComparisonDemo(TinyDbEngine engine)
    {
        const int count = 500;

        // 单条插入测试
        var singleLogs = engine.GetCollection<LogEntry>();
        singleLogs.DeleteAll();

        var singleData = GenerateLogEntries(count);
        var sw = Stopwatch.StartNew();
        foreach (var log in singleData)
        {
            singleLogs.Insert(log);
        }
        sw.Stop();
        var singleTime = sw.ElapsedMilliseconds;

        Console.WriteLine($"📊 单条插入 {count} 条记录:");
        Console.WriteLine($"   耗时: {singleTime}ms");
        Console.WriteLine($"   吞吐量: {count * 1000.0 / singleTime:F0} 条/秒");

        // 批量插入测试
        var batchLogs = engine.GetCollection<LogEntry>();
        batchLogs.DeleteAll();

        var batchData = GenerateLogEntries(count);
        sw.Restart();
        batchLogs.Insert(batchData);
        sw.Stop();
        var batchTime = sw.ElapsedMilliseconds;

        Console.WriteLine($"📊 批量插入 {count} 条记录:");
        Console.WriteLine($"   耗时: {batchTime}ms");
        Console.WriteLine($"   吞吐量: {count * 1000.0 / (batchTime == 0 ? 1 : batchTime):F0} 条/秒");

        // 性能对比
        if (batchTime > 0)
        {
            var speedup = (double)singleTime / batchTime;
            Console.WriteLine($"🚀 批量插入比单条插入快 {speedup:F1} 倍");
        }
    }

    /// <summary>
    /// 批量更新演示
    /// </summary>
    private static void BulkUpdateDemo(ITinyCollection<LogEntry> logs)
    {
        // 准备数据
        logs.DeleteAll();
        var testData = GenerateLogEntries(500);
        logs.Insert(testData);

        // 批量更新
        var logsToUpdate = logs.Find(l => l.Level == "INFO").ToList();
        Console.WriteLine($"📝 找到 {logsToUpdate.Count} 条INFO级别日志，准备批量更新");

        foreach (var log in logsToUpdate)
        {
            log.Level = "DEBUG";
            log.Message = "[已归档] " + log.Message;
        }

        var sw = Stopwatch.StartNew();
        var updateCount = logs.Update(logsToUpdate);
        sw.Stop();

        Console.WriteLine($"✅ 批量更新 {updateCount} 条记录");
        Console.WriteLine($"   耗时: {sw.ElapsedMilliseconds}ms");

        // 验证更新
        var debugCount = logs.Count(l => l.Level == "DEBUG");
        Console.WriteLine($"📊 验证: DEBUG级别日志数量: {debugCount}");
    }

    /// <summary>
    /// 批量删除演示
    /// </summary>
    private static void BulkDeleteDemo(ITinyCollection<LogEntry> logs)
    {
        // 准备数据
        logs.DeleteAll();
        var testData = GenerateLogEntries(1000);
        logs.Insert(testData);

        var totalBefore = logs.Count();
        Console.WriteLine($"📊 删除前记录数: {totalBefore}");

        // 条件批量删除
        var sw = Stopwatch.StartNew();
        var deletedCount = logs.DeleteMany(l => l.Level == "ERROR" || l.Level == "WARNING");
        sw.Stop();

        Console.WriteLine($"✅ 批量删除 ERROR 和 WARNING 级别日志: {deletedCount} 条");
        Console.WriteLine($"   耗时: {sw.ElapsedMilliseconds}ms");

        var totalAfter = logs.Count();
        Console.WriteLine($"📊 删除后记录数: {totalAfter}");

        // 按ID批量删除
        var idsToDelete = logs.FindAll().Take(100).Select(l => (BsonValue)l.Id).ToList();
        sw.Restart();
        var deletedByIds = logs.Delete(idsToDelete);
        sw.Stop();

        Console.WriteLine($"✅ 按ID批量删除: {deletedByIds} 条");
        Console.WriteLine($"   耗时: {sw.ElapsedMilliseconds}ms");

        // 全部删除
        sw.Restart();
        var deletedAll = logs.DeleteAll();
        sw.Stop();

        Console.WriteLine($"✅ 删除全部记录: {deletedAll} 条");
        Console.WriteLine($"   耗时: {sw.ElapsedMilliseconds}ms");
    }

    /// <summary>
    /// 大数据量处理演示
    /// </summary>
    private static void LargeDatasetDemo(TinyDbEngine engine)
    {
        var logs = engine.GetCollection<LogEntry>();
        logs.DeleteAll();

        const int totalRecords = 5000;

        Console.WriteLine($"🔄 准备插入 {totalRecords:N0} 条记录...");

        var sw = Stopwatch.StartNew();
        var data = GenerateLogEntries(totalRecords);
        logs.Insert(data);
        sw.Stop();

        Console.WriteLine($"✅ 插入完成");
        Console.WriteLine($"   总耗时: {sw.ElapsedMilliseconds}ms");
        Console.WriteLine($"   吞吐量: {totalRecords * 1000.0 / sw.ElapsedMilliseconds:F0} 条/秒");

        // 查询性能测试
        Console.WriteLine("\n📊 查询性能测试:");

        // 全表扫描
        sw.Restart();
        var count = logs.Count();
        sw.Stop();
        Console.WriteLine($"   Count(): {count} 条, 耗时: {sw.ElapsedMilliseconds}ms");

        // 条件查询
        sw.Restart();
        var errorLogs = logs.Find(l => l.Level == "ERROR").ToList();
        sw.Stop();
        Console.WriteLine($"   Find(ERROR): {errorLogs.Count} 条, 耗时: {sw.ElapsedMilliseconds}ms");

        // 复杂条件查询
        sw.Restart();
        var complexQuery = logs.Find(l => 
            l.Level == "ERROR" && 
            l.Source.Contains("Service"))
            .ToList();
        sw.Stop();
        Console.WriteLine($"   复杂查询: {complexQuery.Count} 条, 耗时: {sw.ElapsedMilliseconds}ms");

        // 排序查询
        sw.Restart();
        var sortedLogs = logs.FindAll()
            .OrderByDescending(l => l.Timestamp)
            .Take(100)
            .ToList();
        sw.Stop();
        Console.WriteLine($"   排序取前100条: 耗时: {sw.ElapsedMilliseconds}ms");
    }

    /// <summary>
    /// 分批处理演示
    /// </summary>
    private static void BatchProcessingDemo(TinyDbEngine engine)
    {
        var logs = engine.GetCollection<LogEntry>();
        logs.DeleteAll();

        const int totalRecords = 3000;
        const int batchSize = 500;

        Console.WriteLine($"🔄 分批插入 {totalRecords:N0} 条记录 (每批 {batchSize} 条)");

        var sw = Stopwatch.StartNew();
        var batches = 0;
        var totalInserted = 0;

        for (int i = 0; i < totalRecords; i += batchSize)
        {
            var currentBatchSize = Math.Min(batchSize, totalRecords - i);
            var batchData = GenerateLogEntries(currentBatchSize, i);
            
            var inserted = logs.Insert(batchData);
            totalInserted += inserted;
            batches++;

            Console.WriteLine($"   批次 {batches}: 插入 {inserted} 条");
        }

        sw.Stop();

        Console.WriteLine($"✅ 分批插入完成");
        Console.WriteLine($"   总批次: {batches}");
        Console.WriteLine($"   总记录: {totalInserted}");
        Console.WriteLine($"   总耗时: {sw.ElapsedMilliseconds}ms");

        // 分批查询演示
        Console.WriteLine("\n📖 分批查询演示:");
        const int queryBatchSize = 500;
        var offset = 0;
        var batchNum = 0;

        sw.Restart();
        while (true)
        {
            var batch = logs.FindAll()
                .Skip(offset)
                .Take(queryBatchSize)
                .ToList();

            if (batch.Count == 0) break;

            batchNum++;
            offset += batch.Count;
        }
        sw.Stop();

        Console.WriteLine($"   总共 {batchNum} 批次, 读取 {offset} 条记录");
        Console.WriteLine($"   耗时: {sw.ElapsedMilliseconds}ms");

        // 分批删除演示
        Console.WriteLine("\n🗑️ 分批删除演示:");
        sw.Restart();
        var deletedTotal = 0;
        while (true)
        {
            var batch = logs.FindAll().Take(batchSize).ToList();
            if (batch.Count == 0) break;

            var ids = batch.Select(l => (BsonValue)l.Id);
            deletedTotal += logs.Delete(ids);
        }
        sw.Stop();

        Console.WriteLine($"   删除 {deletedTotal} 条记录");
        Console.WriteLine($"   耗时: {sw.ElapsedMilliseconds}ms");
    }

    /// <summary>
    /// 生成测试日志数据
    /// </summary>
    private static IEnumerable<LogEntry> GenerateLogEntries(int count, int startIndex = 0)
    {
        var levels = new[] { "DEBUG", "INFO", "WARNING", "ERROR" };
        var sources = new[] { "UserService", "OrderService", "PaymentService", "NotificationService", "AuthService" };
        var random = new Random(42 + startIndex);

        for (int i = 0; i < count; i++)
        {
            yield return new LogEntry
            {
                Level = levels[random.Next(levels.Length)],
                Message = $"日志消息 #{startIndex + i}: 这是一条测试日志，包含一些随机数据 {random.Next(10000)}",
                Source = sources[random.Next(sources.Length)],
                Timestamp = DateTime.Now.AddMinutes(-random.Next(10000)),
                AdditionalData = $"附加数据 {random.Next(1000)}"
            };
        }
    }
}

/// <summary>
/// 日志实体
/// </summary>
[Entity("log_entries")]
public class LogEntry
{
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();
    public string Level { get; set; } = "INFO";
    public string Message { get; set; } = string.Empty;
    public string Source { get; set; } = string.Empty;
    public DateTime Timestamp { get; set; } = DateTime.Now;
    public string AdditionalData { get; set; } = string.Empty;
}
