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
        Console.WriteLine("=== SimpleDb v0.1 演示程序 ===");
        Console.WriteLine("⚠️ 这是一个早期测试版本，不建议生产环境使用");
        Console.WriteLine("📝 如果要在生产环境使用，请进行充分的测试");
        Console.WriteLine();

        // 删除现有数据库文件
        CleanupDemoFiles();

        // 基础功能演示
        Console.WriteLine(new string('=', 60));
        Console.WriteLine("1. 基础CRUD操作演示");
        Console.WriteLine(new string('=', 60));
        await SimpleCrudDemo.RunAsync();

        Console.WriteLine("\n" + new string('=', 60));
        Console.WriteLine("2. 元数据系统演示");
        Console.WriteLine(new string('=', 60));
        await MetadataDemo.RunAsync();

        Console.WriteLine("\n" + new string('=', 60));
        Console.WriteLine("3. 数据库安全系统演示（Option方式）");
        Console.WriteLine(new string('=', 60));
        await SimpleSecurityDemo.RunAsync();

        Console.WriteLine("\n" + new string('=', 60));
        Console.WriteLine("✅ 所有演示完成！");
        Console.WriteLine("📊 演示数据基于真实运行结果");
        Console.WriteLine("🔧 如需生产使用，请进行充分测试");
        Console.WriteLine("🔐 现在通过Option支持数据库级别的密码保护");
        Console.WriteLine(new string('=', 60));
    }

    private static void CleanupDemoFiles()
    {
        var demoFiles = new[]
        {
            "demo.db", "crud_demo.db", "linq_demo.db",
            "transaction_demo.db", "performance_demo.db", "metadata_demo.db",
            "secure_demo.db", "normal_demo.db"
        };

        foreach (var file in demoFiles)
        {
            if (System.IO.File.Exists(file))
            {
                System.IO.File.Delete(file);
            }
        }
    }
}