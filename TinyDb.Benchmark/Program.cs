using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using BenchmarkDotNet.Jobs;
using TinyDb.Core;
using TinyDb.Collections;
using TinyDb.Attributes;
using TinyDb.Bson;
using System.IO;

namespace TinyDb.Benchmark;

public class Program
{
    public static void Main(string[] args)
    {
        Console.WriteLine("=== TinyDb 快速索引性能测试 ===");
        Console.WriteLine();

        // 运行 WriteConcern 对比测试
        ComparisonTest.Run();

        // 先运行快速批量测试
        QuickBatchTest.RunTest();

        var skipFullBenchmark = string.Equals(
            Environment.GetEnvironmentVariable("TINYDB_BENCH_SKIP_FULL"),
            "1",
            StringComparison.OrdinalIgnoreCase);

        if (skipFullBenchmark)
        {
            Console.WriteLine("\n⚠️ 已根据环境变量 TINYDB_BENCH_SKIP_FULL 跳过 BenchmarkDotNet 基准测试。");
            return;
        }

        Console.WriteLine("\n" + new string('=', 60));

        // 运行完整基准测试
        var summary = BenchmarkRunner.Run<QuickIndexBenchmark>();

        Console.WriteLine("\n=== 快速基准测试完成 ===");
        Console.WriteLine($"测试结果保存在: {summary.ResultsDirectoryPath}");
    }
}

/// <summary>
/// 快速索引性能基准测试
/// </summary>
[MemoryDiagnoser]
[SimpleJob(RuntimeMoniker.Net90, warmupCount: 1, iterationCount: 5, launchCount: 1)]
public class QuickIndexBenchmark
{
    private TinyDbEngine? _engine;
    private ITinyCollection<QuickUser>? _collection;
    private const string DatabaseFile = "quick_benchmark.db";
    private const int SeedCount = 1000;
    private ObjectId _firstSeededUserId;

    [Params(true, false)]
    public bool SynchronousWrites { get; set; }

    /// <summary>
    /// 快速测试用户实体
    /// </summary>
    [Entity("quick_users")]
    public class QuickUser
    {
        public ObjectId Id { get; set; } = ObjectId.NewObjectId();

        [Index(Priority = 1)]
        public string Name { get; set; } = "";

        [Index(Unique = true, Priority = 2)]
        public string Email { get; set; } = "";

        [Index(Priority = 3)]
        public int Age { get; set; }

        // 没有索引的字段
        public decimal Salary { get; set; }
    }

    [IterationSetup(Target = nameof(Insert1000_Individual))]
    public void IterationSetup_InsertIndividual() => SetupInsertIteration();

    [IterationCleanup(Target = nameof(Insert1000_Individual))]
    public void IterationCleanup_InsertIndividual() => CleanupIteration();

    [IterationSetup(Target = nameof(Insert1000_Batch))]
    public void IterationSetup_InsertBatch() => SetupInsertIteration();

    [IterationCleanup(Target = nameof(Insert1000_Batch))]
    public void IterationCleanup_InsertBatch() => CleanupIteration();

    [IterationSetup(Target = nameof(QueryWithoutIndex))]
    public void IterationSetup_QueryWithoutIndex() => SetupQueryIteration();

    [IterationCleanup(Target = nameof(QueryWithoutIndex))]
    public void IterationCleanup_QueryWithoutIndex() => CleanupIteration();

    [IterationSetup(Target = nameof(QueryWithIndex))]
    public void IterationSetup_QueryWithIndex() => SetupQueryIteration();

    [IterationCleanup(Target = nameof(QueryWithIndex))]
    public void IterationCleanup_QueryWithIndex() => CleanupIteration();

    [IterationSetup(Target = nameof(QueryWithUniqueIndex))]
    public void IterationSetup_QueryWithUniqueIndex() => SetupQueryIteration();

    [IterationCleanup(Target = nameof(QueryWithUniqueIndex))]
    public void IterationCleanup_QueryWithUniqueIndex() => CleanupIteration();

    [IterationSetup(Target = nameof(FindById))]
    public void IterationSetup_FindById() => SetupQueryIteration();

    [IterationCleanup(Target = nameof(FindById))]
    public void IterationCleanup_FindById() => CleanupIteration();

    /// <summary>
    /// 插入1000条记录 - 逐个插入版本
    /// </summary>
    [Benchmark]
    public void Insert1000_Individual()
    {
        // 逐个插入1000条测试数据
        for (int i = 0; i < 1000; i++)
        {
            var user = new QuickUser
            {
                Name = $"User{i}",
                Email = $"user{i}@quick.com",
                Age = 20 + (i % 50),
                Salary = 30000 + (i % 100) * 100
            };
            _collection!.Insert(user);
        }
    }

    /// <summary>
    /// 批量插入1000条记录 - 优化版本
    /// </summary>
    [Benchmark]
    public void Insert1000_Batch()
    {
        // 批量插入1000条测试数据
        var users = new List<QuickUser>();
        for (int i = 0; i < 1000; i++)
        {
            users.Add(new QuickUser
            {
                Name = $"User{i}",
                Email = $"user{i}@quick.com",
                Age = 20 + (i % 50),
                Salary = 30000 + (i % 100) * 100
            });
        }
        _collection!.Insert(users);
    }

    /// <summary>
    /// 无索引查询（Salary字段）
    /// </summary>
    [Benchmark]
    public void QueryWithoutIndex()
    {
        var results = _collection!.Find(u => u.Salary >= 30000 && u.Salary < 40000)
            .Take(100)
            .ToList();

        if (results.Count == 0)
        {
            throw new InvalidOperationException("Unexpected empty result");
        }
    }

    /// <summary>
    /// 单字段索引查询（Age字段）
    /// </summary>
    [Benchmark]
    public void QueryWithIndex()
    {
        var results = _collection!.Find(u => u.Age == 25).ToList();

        if (results.Count == 0)
        {
            throw new InvalidOperationException("Unexpected empty result");
        }
    }

    /// <summary>
    /// 唯一索引查询（Email字段）
    /// </summary>
    [Benchmark]
    public void QueryWithUniqueIndex()
    {
        var results = _collection!.Find(u => u.Email == "user25@quick.com").ToList();

        if (results.Count == 0)
        {
            throw new InvalidOperationException("Unexpected empty result");
        }
    }

    /// <summary>
    /// 主键查找
    /// </summary>
    [Benchmark]
    public void FindById()
    {
        var result = _collection!.Find(u => u.Id == _firstSeededUserId).FirstOrDefault();
        if (result == null)
        {
            throw new InvalidOperationException("Unexpected null result");
        }
    }

    private void CreateEngine()
    {
        DisposeEngine();

        var options = new TinyDbOptions
        {
            DatabaseName = "QuickBenchmarkDb",
            PageSize = 16384,
            CacheSize = 1000,
            EnableJournaling = false,
            SynchronousWrites = SynchronousWrites
        };

        _engine = new TinyDbEngine(DatabaseFile, options);
        _collection = _engine.GetCollection<QuickUser>();
    }

    private void SeedData(int count)
    {
        if (_collection == null) throw new InvalidOperationException("Collection not initialized");

        for (int i = 0; i < count; i++)
        {
            var user = new QuickUser
            {
                Name = $"User{i}",
                Email = $"user{i}@quick.com",
                Age = 20 + (i % 50),
                Salary = 30000 + (i % 100) * 100
            };
            _collection.Insert(user);
            if (i == 0)
            {
                _firstSeededUserId = user.Id;
            }
        }
    }

    private void SetupInsertIteration()
    {
        CreateEngine();
    }

    private void SetupQueryIteration()
    {
        CreateEngine();
        SeedData(SeedCount);
    }

    private void CleanupIteration()
    {
        DisposeEngine();
    }

    private void DisposeEngine()
    {
        _engine?.Dispose();
        _engine = null;
        _collection = null;

        if (File.Exists(DatabaseFile))
        {
            File.Delete(DatabaseFile);
        }
    }
}
