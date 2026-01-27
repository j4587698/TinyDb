using System;
using System.Collections.Generic;
using System.Diagnostics;
using TinyDb.Core;
using TinyDb.Collections;
using TinyDb.Attributes;
using TinyDb.Bson;
using System.IO;
using System.Threading.Tasks;

namespace TinyDb.Benchmark;

public class QuickBatchTest
{
    private const string SyncEnvVar = "SIMPLEDB_BENCH_SYNC_WRITES";
    private const string WriteConcernEnvVar = "SIMPLEDB_BENCH_WRITE_CONCERN";

    public static void RunTest()
    {
        Console.WriteLine("=== 快速批量插入测试 ===");
        Console.WriteLine();

        const string DatabaseFile = "quick_batch_test.db";
        const int SampleSize = 1000;

        // 清理旧数据库文件
        if (System.IO.File.Exists(DatabaseFile))
        {
            System.IO.File.Delete(DatabaseFile);
        }

        var options = new TinyDbOptions
        {
            DatabaseName = "QuickBatchTestDb",
            PageSize = 16384,
            CacheSize = 1000,
            EnableJournaling = false,
            WriteConcern = ResolveWriteConcern()
        };

        using var engine = new TinyDbEngine(DatabaseFile, options);
        var collection = engine.GetCollection<TestUser>();

        Console.WriteLine("✅ 测试环境已设置");
        Console.WriteLine($"⚙️ 写入关注级别: {options.WriteConcern}");

        // 测试1：单独插入100条记录
        Console.WriteLine($"\n📊 测试1: 单独插入{SampleSize}条记录");
        var sw1 = Stopwatch.StartNew();

        for (int i = 0; i < SampleSize; i++)
        {
            var user = new TestUser
            {
                Name = $"User{i}",
                Email = $"user{i}@test.com",
                Age = 20 + (i % 50),
                Salary = 30000 + (i % 100) * 100
            };
            collection.Insert(user);
        }

        sw1.Stop();
        Console.WriteLine($"   单独插入耗时: {sw1.ElapsedMilliseconds} ms");
        Console.WriteLine($"   平均每条: {(double)sw1.ElapsedMilliseconds / SampleSize:F2} ms");
        
        var count1 = collection.FindAll().Count();
        Console.WriteLine($"   📊 测试1后数据量: {count1} (期望: {SampleSize})");

        // 清空数据
        var allUsers = collection.FindAll().ToList();
        foreach (var user in allUsers)
        {
            collection.Delete(user.Id);
        }

        // 测试2：批量插入100条记录
        Console.WriteLine($"\n📊 测试2: 批量插入{SampleSize}条记录");
        var sw2 = Stopwatch.StartNew();

        var users = new List<TestUser>();
        for (int i = 0; i < SampleSize; i++)
        {
            users.Add(new TestUser
            {
                Name = $"User{i}",
                Email = $"user{i}@test.com",
                Age = 20 + (i % 50),
                Salary = 30000 + (i % 100) * 100
            });
        }
        var insertedCount = collection.Insert(users);

        sw2.Stop();
        Console.WriteLine($"   批量插入耗时: {sw2.ElapsedMilliseconds} ms");
        Console.WriteLine($"   平均每条: {(double)sw2.ElapsedMilliseconds / SampleSize:F2} ms");
        Console.WriteLine($"   插入返回值: {insertedCount} (期望: {SampleSize})");

        // 计算性能提升
        var improvement = (double)(sw1.ElapsedMilliseconds - sw2.ElapsedMilliseconds) / sw1.ElapsedMilliseconds * 100;
        Console.WriteLine($"\n🚀 性能提升: {improvement:F1}%");

        // 验证数据正确性
        var finalCount = collection.FindAll().Count();
        Console.WriteLine($"✅ 数据验证: 插入成功 {finalCount} 条记录");

        // 测试3: 无索引查询性能
        Console.WriteLine($"\n📊 测试3: 无索引查询 (Salary > 35000)");
        var sw3 = Stopwatch.StartNew();
        var queryCount = collection.Find(u => u.Salary > 35000).Count();
        sw3.Stop();
        Console.WriteLine($"   查询耗时: {sw3.ElapsedMilliseconds} ms");
        Console.WriteLine($"   匹配数量: {queryCount}");

        // 测试4: 高选择性无索引查询
        Console.WriteLine($"\n📊 测试4: 高选择性无索引查询 (Salary > 39800)");
        var sw4 = Stopwatch.StartNew();
        var queryCount2 = collection.Find(u => u.Salary > 39800).Count();
        sw4.Stop();
        Console.WriteLine($"   查询耗时: {sw4.ElapsedMilliseconds} ms");
        Console.WriteLine($"   匹配数量: {queryCount2}");

        engine.Dispose();
        if (System.IO.File.Exists(DatabaseFile))
        {
            System.IO.File.Delete(DatabaseFile);
        }

        RunParallelInsertTest(options);

        RunAsyncInsertTest(options);

        Console.WriteLine("\n=== 快速批量插入测试完成 ===");
    }

    private static void RunParallelInsertTest(TinyDbOptions baseOptions)
    {
        const string ParallelDatabaseFile = "quick_batch_parallel.db";
        var options = baseOptions.Clone();

        if (System.IO.File.Exists(ParallelDatabaseFile))
        {
            System.IO.File.Delete(ParallelDatabaseFile);
        }

        using var engine = new TinyDbEngine(ParallelDatabaseFile, options);

        var threadCount = GetThreadCount();
        var perThread = GetParallelBatchSize();
        var total = threadCount * perThread;

        Console.WriteLine("\n🔁 测试3: 多线程单条插入");
        Console.WriteLine($"   线程数: {threadCount}, 每线程 {perThread} 条, 总计 {total} 条");

        var sw = Stopwatch.StartNew();
        Parallel.For(0, threadCount, worker =>
        {
            var collection = engine.GetCollectionWithName<ParallelUser>(GetParallelCollectionName(worker));
            var start = worker * perThread;
            var random = new Random(unchecked(start * 486187739) ^ Environment.TickCount);

            for (int i = 0; i < perThread; i++)
            {
                var index = start + i;
                var user = new ParallelUser
                {
                    Name = $"ParallelUser{index}",
                    Email = $"parallel{index}@test.com",
                    Age = 20 + random.Next(0, 50)
                };
                collection.Insert(user);
            }
        });
        sw.Stop();

        engine.Flush();

        Console.WriteLine($"   多线程插入耗时: {sw.ElapsedMilliseconds} ms");
        Console.WriteLine($"   平均每条: {(double)sw.ElapsedMilliseconds / total:F2} ms");

        var totals = new List<string>(threadCount);
        var count = 0;
        for (int worker = 0; worker < threadCount; worker++)
        {
            var collectionName = GetParallelCollectionName(worker);
            var bucketCount = engine.GetCachedDocumentCount(collectionName);
            totals.Add($"{collectionName}={bucketCount}");
            count += bucketCount;
        }
        Console.WriteLine($"   集合分布: {string.Join(", ", totals)}");
        Console.WriteLine($"✅ 多线程数据验证: 插入成功 {count} 条记录");

        engine.Dispose();
        if (System.IO.File.Exists(ParallelDatabaseFile))
        {
            System.IO.File.Delete(ParallelDatabaseFile);
        }
    }

    private static bool GetSynchronousWritesSetting()
    {
        var value = Environment.GetEnvironmentVariable(SyncEnvVar);
        if (bool.TryParse(value, out var result))
        {
            return result;
        }

        return false;
    }

    private static WriteConcern ResolveWriteConcern()
    {
        var raw = Environment.GetEnvironmentVariable(WriteConcernEnvVar);
        if (!string.IsNullOrWhiteSpace(raw))
        {
            return raw.Trim().ToLowerInvariant() switch
            {
                "none" or "0" => WriteConcern.None,
                "synced" or "sync" or "true" or "1" => WriteConcern.Synced,
                "journal" or "journaled" => WriteConcern.Journaled,
                _ => WriteConcern.Journaled
            };
        }

        return GetSynchronousWritesSetting() ? WriteConcern.Synced : WriteConcern.Journaled;
    }

    private static int GetThreadCount()
    {
        var raw = Environment.GetEnvironmentVariable("SIMPLEDB_BENCH_THREADS");
        if (int.TryParse(raw, out var value) && value > 0)
        {
            return Math.Min(value, Environment.ProcessorCount * 4);
        }

        return Math.Clamp(Environment.ProcessorCount, 2, 8);
    }

    private static int GetParallelBatchSize()
    {
        var raw = Environment.GetEnvironmentVariable("SIMPLEDB_BENCH_PARALLEL_BATCH");
        if (int.TryParse(raw, out var value) && value > 0)
        {
            return value;
        }

        return 500;
    }

    private static string GetParallelCollectionName(int worker) => $"parallel_users_{worker}";

    private static void RunAsyncInsertTest(TinyDbOptions baseOptions)
    {
        const string AsyncDatabaseFile = "quick_batch_async.db";
        const int SampleSize = 1000;

        // 清理旧数据库文件
        if (System.IO.File.Exists(AsyncDatabaseFile))
        {
            System.IO.File.Delete(AsyncDatabaseFile);
        }

        var options = baseOptions.Clone();

        Console.WriteLine("\n=== 异步插入性能测试 ===\n");
        Console.WriteLine($"⚙️ 写入关注级别: {options.WriteConcern}");

        using var engine = new TinyDbEngine(AsyncDatabaseFile, options);
        var collection = engine.GetCollection<TestUser>();

        // 测试1：同步单条插入
        Console.WriteLine($"\n📊 测试1: 同步单条插入 {SampleSize} 条记录");
        var sw1 = Stopwatch.StartNew();

        for (int i = 0; i < SampleSize; i++)
        {
            var user = new TestUser
            {
                Name = $"SyncUser{i}",
                Email = $"sync{i}@test.com",
                Age = 20 + (i % 50),
                Salary = 30000 + (i % 100) * 100
            };
            collection.Insert(user);
        }

        sw1.Stop();
        Console.WriteLine($"   同步插入耗时: {sw1.ElapsedMilliseconds} ms");
        Console.WriteLine($"   平均每条: {(double)sw1.ElapsedMilliseconds / SampleSize:F2} ms");

        // 清空数据
        var allUsers = collection.FindAll().ToList();
        foreach (var user in allUsers)
        {
            collection.Delete(user.Id);
        }

        // 测试2：异步单条插入
        Console.WriteLine($"\n📊 测试2: 异步单条插入 {SampleSize} 条记录");
        var sw2 = Stopwatch.StartNew();

        var asyncTask = RunAsyncInserts(collection, SampleSize);
        asyncTask.GetAwaiter().GetResult();

        sw2.Stop();
        Console.WriteLine($"   异步插入耗时: {sw2.ElapsedMilliseconds} ms");
        Console.WriteLine($"   平均每条: {(double)sw2.ElapsedMilliseconds / SampleSize:F2} ms");

        // 计算性能差异
        var asyncImpact = ((double)sw2.ElapsedMilliseconds / sw1.ElapsedMilliseconds - 1.0) * 100;
        if (asyncImpact > 0)
            Console.WriteLine($"\n📉 异步开销: +{asyncImpact:F1}% (预期：async/await 有少量开销)");
        else
            Console.WriteLine($"\n📈 异步提升: {-asyncImpact:F1}%");

        // 清空数据
        allUsers = collection.FindAll().ToList();
        foreach (var user in allUsers)
        {
            collection.Delete(user.Id);
        }

        // 测试3：同步批量插入
        Console.WriteLine($"\n📊 测试3: 同步批量插入 {SampleSize} 条记录");
        var users = new List<TestUser>();
        for (int i = 0; i < SampleSize; i++)
        {
            users.Add(new TestUser
            {
                Name = $"BatchUser{i}",
                Email = $"batch{i}@test.com",
                Age = 20 + (i % 50),
                Salary = 30000 + (i % 100) * 100
            });
        }

        var sw3 = Stopwatch.StartNew();
        collection.Insert(users);
        sw3.Stop();
        Console.WriteLine($"   同步批量插入耗时: {sw3.ElapsedMilliseconds} ms");
        Console.WriteLine($"   平均每条: {(double)sw3.ElapsedMilliseconds / SampleSize:F2} ms");

        // 清空数据
        allUsers = collection.FindAll().ToList();
        foreach (var user in allUsers)
        {
            collection.Delete(user.Id);
        }

        // 测试4：异步批量插入
        Console.WriteLine($"\n📊 测试4: 异步批量插入 {SampleSize} 条记录");
        var users2 = new List<TestUser>();
        for (int i = 0; i < SampleSize; i++)
        {
            users2.Add(new TestUser
            {
                Name = $"AsyncBatchUser{i}",
                Email = $"asyncbatch{i}@test.com",
                Age = 20 + (i % 50),
                Salary = 30000 + (i % 100) * 100
            });
        }

        var sw4 = Stopwatch.StartNew();
        var asyncBatchTask = collection.InsertAsync(users2);
        asyncBatchTask.GetAwaiter().GetResult();
        sw4.Stop();
        Console.WriteLine($"   异步批量插入耗时: {sw4.ElapsedMilliseconds} ms");
        Console.WriteLine($"   平均每条: {(double)sw4.ElapsedMilliseconds / SampleSize:F2} ms");

        // 测试5: 并发异步插入
        Console.WriteLine($"\n📊 测试5: 并发异步插入 (10个并发任务)");
        // 清空数据
        allUsers = collection.FindAll().ToList();
        foreach (var user in allUsers)
        {
            collection.Delete(user.Id);
        }

        var sw5 = Stopwatch.StartNew();
        var concurrentTasks = new List<Task>();
        for (int t = 0; t < 10; t++)
        {
            var taskId = t;
            concurrentTasks.Add(Task.Run(async () =>
            {
                for (int i = 0; i < SampleSize / 10; i++)
                {
                    var user = new TestUser
                    {
                        Name = $"ConcurrentUser{taskId}_{i}",
                        Email = $"concurrent{taskId}_{i}@test.com",
                        Age = 20 + (i % 50),
                        Salary = 30000 + (i % 100) * 100
                    };
                    await collection.InsertAsync(user);
                }
            }));
        }
        Task.WhenAll(concurrentTasks).GetAwaiter().GetResult();
        sw5.Stop();
        Console.WriteLine($"   并发异步插入耗时: {sw5.ElapsedMilliseconds} ms");
        Console.WriteLine($"   平均每条: {(double)sw5.ElapsedMilliseconds / SampleSize:F2} ms");

        // 最终统计
        Console.WriteLine("\n📊 性能对比总结:");
        Console.WriteLine($"   单条同步: {sw1.ElapsedMilliseconds} ms ({(double)sw1.ElapsedMilliseconds / SampleSize:F2} ms/条)");
        Console.WriteLine($"   单条异步: {sw2.ElapsedMilliseconds} ms ({(double)sw2.ElapsedMilliseconds / SampleSize:F2} ms/条)");
        Console.WriteLine($"   批量同步: {sw3.ElapsedMilliseconds} ms ({(double)sw3.ElapsedMilliseconds / SampleSize:F2} ms/条)");
        Console.WriteLine($"   批量异步: {sw4.ElapsedMilliseconds} ms ({(double)sw4.ElapsedMilliseconds / SampleSize:F2} ms/条)");
        Console.WriteLine($"   并发异步: {sw5.ElapsedMilliseconds} ms ({(double)sw5.ElapsedMilliseconds / SampleSize:F2} ms/条)");

        engine.Dispose();
        if (System.IO.File.Exists(AsyncDatabaseFile))
        {
            System.IO.File.Delete(AsyncDatabaseFile);
        }

        Console.WriteLine("\n=== 异步插入性能测试完成 ===");
    }

    private static async Task RunAsyncInserts(ITinyCollection<TestUser> collection, int count)
    {
        for (int i = 0; i < count; i++)
        {
            var user = new TestUser
            {
                Name = $"AsyncUser{i}",
                Email = $"async{i}@test.com",
                Age = 20 + (i % 50),
                Salary = 30000 + (i % 100) * 100
            };
            await collection.InsertAsync(user);
        }
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

    public decimal Salary { get; set; }
}

[Entity("parallel_users")]
public class ParallelUser
{
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();

    public string Name { get; set; } = "";

    public string Email { get; set; } = "";

    public int Age { get; set; }
}
