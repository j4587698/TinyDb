using System;
using System.Linq;
using System.Diagnostics.CodeAnalysis;
using SimpleDb.Core;
using SimpleDb.Collections;
using SimpleDb.Bson;
using SimpleDb.Attributes;

namespace SimpleDb.Demo;

public class Program
{
    public static async Task Main(string[] args)
    {
        Console.WriteLine("=== SimpleDb AOT Demo ===");

        // 删除现有数据库文件
        if (System.IO.File.Exists("demo.db"))
        {
            System.IO.File.Delete("demo.db");
        }

        Console.WriteLine("=== 简化版SimpleDb AOT Demo ===");
        Console.WriteLine("用户现在只需要添加一个[Entity]属性！");
        Console.WriteLine("ID属性会自动识别'Id'、'_id'等标准名称。");
        Console.WriteLine();

        // 创建数据库引擎
        var options = new SimpleDbOptions
        {
            DatabaseName = "AotDemoDb",
            PageSize = 8192,
            CacheSize = 100,
            EnableJournaling = true
        };

        using var engine = new SimpleDbEngine("demo.db", options);
        Console.WriteLine("✅ 数据库创建成功！");

        // 获取集合
        var users = engine.GetCollection<User>("users");
        Console.WriteLine("✅ 集合创建成功！");

        // 插入测试数据
        Console.WriteLine("\n--- 插入测试数据 ---");
        InsertTestData(users);

        // 查询数据
        Console.WriteLine("\n--- 查询数据 ---");
        QueryData(users);

        // 更新数据
        Console.WriteLine("\n--- 更新数据 ---");
        UpdateData(users);

        // 删除数据
        Console.WriteLine("\n--- 删除数据 ---");
        DeleteData(users);

        Console.WriteLine("\n=== AOT Demo 完成！ ===");
        Console.WriteLine($"数据库统计: {engine.GetStatistics()}");

        // 运行自动ID生成功能演示
        Console.WriteLine("\n" + new string('=', 50));
        await AutoIdSimpleDemo.RunAsync();
    }

    static void InsertTestData(ILiteCollection<User> users)
    {
        var testUsers = new[]
        {
            new User { Name = "张三", Age = 25, Email = "zhangsan@example.com" },
            new User { Name = "李四", Age = 30, Email = "lisi@example.com" },
            new User { Name = "王五", Age = 28, Email = "wangwu@example.com" },
            new User { Name = "赵六", Age = 35, Email = "zhaoliu@example.com" },
            new User { Name = "钱七", Age = 22, Email = "qianqi@example.com" }
        };

        foreach (var user in testUsers)
        {
            users.Insert(user);
            Console.WriteLine($"✅ 插入用户: {user.Name} (ID: {user.Id})");
        }
    }

    static void QueryData(ILiteCollection<User> users)
    {
        // 查询所有用户
        var allUsers = users.FindAll().ToList();
        Console.WriteLine($"总用户数: {allUsers.Count}");

        foreach (var user in allUsers.Take(3)) // 只显示前3个
        {
            Console.WriteLine($"- {user.Name}, {user.Age}岁, {user.Email}");
        }

        if (allUsers.Count > 3)
        {
            Console.WriteLine($"... 还有 {allUsers.Count - 3} 个用户");
        }

        // 条件查询
        var adults = users.Find(u => u.Age >= 25).ToList();
        Console.WriteLine($"25岁以上用户数: {adults.Count}");

        var youngUsers = users.Find(u => u.Age < 25).ToList();
        Console.WriteLine($"25岁以下用户数: {youngUsers.Count}");

        // 复杂查询 - 分步执行避免类型转换问题，使用简单的相等比较
        var zhangUsers = users.Find(u => u.Name == "张三").ToList();
        var complexQuery = zhangUsers.OrderBy(u => u.Age).Take(5).ToList();
        Console.WriteLine($"复杂查询结果: {complexQuery.Count} 个用户");
    }

    static void UpdateData(ILiteCollection<User> users)
    {
        // 查找并更新第一个用户
        var user = users.Find(u => u.Name == "张三").FirstOrDefault();
        if (user != null)
        {
            Console.WriteLine($"更新用户: {user.Name} 年龄 {user.Age} → 26");
            user.Age = 26;
            users.Update(user);
            Console.WriteLine("✅ 用户更新成功！");

            // 验证更新
            var updatedUser = users.Find(u => u.Name == "张三").FirstOrDefault();
            if (updatedUser != null)
            {
                Console.WriteLine($"验证更新: {updatedUser.Name}, {updatedUser.Age}岁");
            }
        }
    }

    static void DeleteData(ILiteCollection<User> users)
    {
        // 删除指定用户
        var user = users.Find(u => u.Name == "李四").FirstOrDefault();
        if (user != null)
        {
            Console.WriteLine($"删除用户: {user.Name}");
            users.Delete(user.Id);
            Console.WriteLine("✅ 用户删除成功！");

            // 验证删除
            var remainingUsers = users.FindAll().ToList();
            Console.WriteLine($"剩余用户数: {remainingUsers.Count}");

            foreach (var remainingUser in remainingUsers.Take(3))
            {
                Console.WriteLine($"- {remainingUser.Name}, {remainingUser.Age}岁");
            }
        }
    }
}

[Entity("users")]
public partial class User
{
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();

    public string Name { get; set; } = "";
    public int Age { get; set; }
    public string Email { get; set; } = "";
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
}