using System;
using System.Linq;
using System.Diagnostics.CodeAnalysis;
using TinyDb.Core;
using TinyDb.Collections;
using TinyDb.Bson;
using TinyDb.Attributes;
using TinyDb.Demo.Demos;
using TinyDb.Metadata;
using TinyDb.Security;

namespace TinyDb.Demo;

public class Program
{
    public static async Task Main(string[] args)
    {
        Console.WriteLine("=== TinyDb v0.1 演示程序 ===");
        Console.WriteLine("⚠️ 这是一个早期测试版本，不建议生产环境使用");
        Console.WriteLine("📝 如果要在生产环境使用，请进行充分的测试");
        Console.WriteLine();

        // 删除现有数据库文件
        CleanupDemoFiles();

        // 完整功能演示
        var demos = new[]
        {
            ("基础CRUD操作", "1", SimpleCrudDemo.RunAsync),
            ("元数据系统", "2", MetadataDemo.RunAsync),
            ("数据库安全系统", "3", SimpleSecurityDemo.RunAsync),
            ("事务处理功能", "4", TransactionDemo.RunAsync),
            ("LINQ查询功能", "5", LinqQueryDemo.RunAsync),
            ("索引系统", "6", IndexDemo.RunAsync),
            ("ID生成策略", "7", IdGenerationDemo.RunAsync),
            ("性能测试", "8", PerformanceDemo.RunAsync)
        };

        Console.WriteLine("🎯 可用演示列表:");
        foreach (var (name, number, _) in demos)
        {
            Console.WriteLine($"   {number}. {name}");
        }
        Console.WriteLine();
        Console.WriteLine("📝 请选择要运行的演示 (输入数字，用逗号分隔多个选择，或输入 'all' 运行全部):");
        var input = Console.ReadLine()?.Trim().ToLower() ?? "all";

        var selectedDemos = new List<(string name, string number, Func<Task> run)>();

        if (input == "all")
        {
            selectedDemos.AddRange(demos);
        }
        else
        {
            var numbers = input.Split(',', StringSplitOptions.RemoveEmptyEntries);
            foreach (var num in numbers)
            {
                if (int.TryParse(num.Trim(), out var selectedNumber))
                {
                    var demo = demos.FirstOrDefault(d => d.number == num.Trim());
                    if (demo.name != null)
                    {
                        selectedDemos.Add(demo);
                    }
                }
            }
        }

        if (selectedDemos.Count == 0)
        {
            Console.WriteLine("❌ 无效选择，运行默认演示...");
            selectedDemos.Add(demos[0]); // 默认运行第一个演示
        }

        Console.WriteLine($"\n🚀 开始运行 {selectedDemos.Count} 个演示...\n");

        var totalStopwatch = System.Diagnostics.Stopwatch.StartNew();

        foreach (var (name, number, runDemo) in selectedDemos)
        {
            try
            {
                Console.WriteLine(new string('=', 80));
                Console.WriteLine($"{number}. {name}演示");
                Console.WriteLine(new string('=', 80));

                var demoStopwatch = System.Diagnostics.Stopwatch.StartNew();
                await runDemo();
                demoStopwatch.Stop();

                Console.WriteLine($"\n⏱️ {name}演示完成，耗时: {demoStopwatch.ElapsedMilliseconds}ms");

                if (selectedDemos.IndexOf((name, number, runDemo)) < selectedDemos.Count - 1)
                {
                    Console.WriteLine("⏸️ 按任意键继续下一个演示...");
                    Console.ReadKey(true);
                    Console.WriteLine();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"\n❌ {name}演示失败: {ex.Message}");
                Console.WriteLine($"🔍 错误详情: {ex}");
            }
        }

        totalStopwatch.Stop();

        Console.WriteLine("\n" + new string('=', 80));
        Console.WriteLine("✅ 所有选定演示完成！");
        Console.WriteLine("📊 演示数据基于真实运行结果");
        Console.WriteLine("🔧 如需生产使用，请进行充分测试");
        Console.WriteLine("🔐 现在通过Option支持数据库级别的密码保护");
        Console.WriteLine($"⏱️ 总演示时间: {totalStopwatch.ElapsedMilliseconds}ms ({totalStopwatch.Elapsed.TotalSeconds:F1}秒)");
        Console.WriteLine("🎯 TinyDb功能特性: CRUD、事务、查询、索引、安全、元数据、性能优化");
        Console.WriteLine(new string('=', 80));
    }

    private static void CleanupDemoFiles()
    {
        var demoFiles = new[]
        {
            "demo.db", "simple_crud_demo.db", "linq_demo.db",
            "transaction_demo.db", "performance_demo.db", "metadata_demo.db",
            "secure_demo.db", "normal_demo.db", "index_demo.db",
            "idgeneration_demo.db", "option_secure_demo.db", "advanced_demo.db"
        };

        // 清理WAL文件
        var walFiles = demoFiles.Select(f => $"{f}.wal").ToArray();

        foreach (var file in demoFiles.Concat(walFiles))
        {
            if (System.IO.File.Exists(file))
            {
                try
                {
                    System.IO.File.Delete(file);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"⚠️ 删除文件失败 {file}: {ex.Message}");
                }
            }
        }
    }
}