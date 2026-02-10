using System;
using System.Collections.Generic;
using System.Diagnostics;
using TinyDb.Core;
using TinyDb.Collections;
using TinyDb.Attributes;
using TinyDb.Bson;
using System.IO;

namespace TinyDb.Benchmark;

public class ComparisonTest
{
    public static void Run()
    {
        Console.WriteLine("\n" + new string('=', 50));
        Console.WriteLine("🚀 WriteConcern 性能对比测试");
        Console.WriteLine(new string('=', 50));

        RunTestForConcern(WriteConcern.None, "None (最高性能)");
        RunTestForConcern(WriteConcern.Journaled, "Journaled (标准持久化)");
        RunTestForConcern(WriteConcern.Synced, "Synced (最强持久化/最慢)");
        
        Console.WriteLine("\n" + new string('=', 50));
    }

    private static void RunTestForConcern(WriteConcern concern, string label)
    {
        const string DbFile = "comparison_test.db";
        const int Count = 1000;

        if (File.Exists(DbFile)) File.Delete(DbFile);

        var options = new TinyDbOptions
        {
            DatabaseName = "ComparisonDb",
            WriteConcern = concern,
            EnableJournaling = true // 保持开启 WAL
        };

        Console.WriteLine($"\n--- 模式: {label} ---");

        using (var engine = new TinyDbEngine(DbFile, options))
        {
            var col = engine.GetCollection<BenchUser>();

            // 1. 单条插入
            var sw = Stopwatch.StartNew();
            for (int i = 0; i < Count; i++)
            {
                col.Insert(new BenchUser { Name = "User" + i, Age = i % 100 });
            }
            sw.Stop();
            Console.WriteLine($"   单条插入 {Count} 条: {sw.ElapsedMilliseconds} ms ({(double)sw.ElapsedMilliseconds / Count:F2} ms/条)");

            // 2. 批量插入
            // 先清理
            engine.DropCollection("bench_users");
            var col2 = engine.GetCollection<BenchUser>();
            
            var list = new List<BenchUser>();
            for (int i = 0; i < Count; i++) list.Add(new BenchUser { Name = "Batch" + i, Age = i % 100 });

            sw.Restart();
            col2.Insert(list);
            sw.Stop();
            Console.WriteLine($"   批量插入 {Count} 条: {sw.ElapsedMilliseconds} ms ({(double)sw.ElapsedMilliseconds / Count:F2} ms/条)");
        }

        if (File.Exists(DbFile)) File.Delete(DbFile);
    }

    [Entity("bench_users")]
    public class BenchUser
    {
        public ObjectId Id { get; set; } = ObjectId.NewObjectId();
        public string Name { get; set; } = "";
        public int Age { get; set; }
    }
}