using TinyDb.Core;
using TinyDb.Collections;
using TinyDb.Attributes;
using TinyDb.Bson;

namespace TinyDb.Security;

/// <summary>
/// 简化的安全系统演示程序（使用Option方式）
/// </summary>
public static class SimpleSecurityDemo
{
    /// <summary>
    /// 运行安全系统演示
    /// </summary>
    public static Task RunAsync()
    {
        Console.WriteLine("=== TinyDb Option方式密码保护演示 ===");
        Console.WriteLine();

        var dbPath = "option_secure_demo.db";

        // 清理现有文件
        if (System.IO.File.Exists(dbPath))
        {
            System.IO.File.Delete(dbPath);
        }

        try
        {
            // 1. 使用Option创建受密码保护的数据库
            Console.WriteLine("1. 使用Option创建受密码保护的数据库");
            Console.WriteLine(new string('-', 50));

            var options = new TinyDbOptions
            {
                Password = "MySecurePassword123!",
                DatabaseName = "SecureOptionDB",
                CacheSize = 1000
            };

            using var engine = new TinyDbEngine(dbPath, options);
            Console.WriteLine($"✅ 成功创建受密码保护的数据库");
            Console.WriteLine($"🔑 密码: {options.Password}");
            Console.WriteLine($"📊 数据库名: {options.DatabaseName}");

            // 添加测试数据
            var users = engine.GetCollection<DemoUser>();
            var user = new DemoUser
            {
                Id = ObjectId.NewObjectId(),
                Name = "测试用户",
                Email = "test@example.com",
                CreatedAt = DateTime.Now
            };
            users.Insert(user);
            Console.WriteLine("📝 已添加测试用户");

            // 2. 验证密码保护
            Console.WriteLine("\n2. 验证密码保护功能");
            Console.WriteLine(new string('-', 50));

            // 正确密码
            try
            {
                var correctOptions = new TinyDbOptions { Password = "MySecurePassword123!" };
                using var correctEngine = new TinyDbEngine(dbPath, correctOptions);
                var userCount = correctEngine.GetCollection<DemoUser>().Count();
                Console.WriteLine($"✅ 正确密码访问成功 - 用户数: {userCount}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ 正确密码访问失败: {ex.Message}");
            }

            // 错误密码
            try
            {
                var wrongOptions = new TinyDbOptions { Password = "WrongPassword456!" };
                using var wrongEngine = new TinyDbEngine(dbPath, wrongOptions);
                Console.WriteLine("❌ 错误密码访问不应该成功");
            }
            catch (UnauthorizedAccessException)
            {
                Console.WriteLine("✅ 错误密码被正确拒绝");
            }

            // 未提供密码
            try
            {
                using var noPasswordEngine = new TinyDbEngine(dbPath);
                Console.WriteLine("❌ 未提供密码访问不应该成功");
            }
            catch (UnauthorizedAccessException)
            {
                Console.WriteLine("✅ 未提供密码被正确拒绝");
            }

            // 3. 高级Option配置演示
            Console.WriteLine("\n3. 高级Option配置演示");
            Console.WriteLine(new string('-', 50));

            var advancedPath = "advanced_demo.db";
            if (System.IO.File.Exists(advancedPath))
            {
                System.IO.File.Delete(advancedPath);
            }

            var advancedOptions = new TinyDbOptions
            {
                Password = "AdvancedPass123!",
                DatabaseName = "AdvancedDB",
                PageSize = 8192,
                CacheSize = 2000,
                EnableJournaling = true,
                Timeout = TimeSpan.FromMinutes(5)
            };

            using var advancedEngine = new TinyDbEngine(advancedPath, advancedOptions);
            Console.WriteLine($"✅ 高级配置数据库创建成功");
            Console.WriteLine($"   📊 页面大小: {advancedOptions.PageSize}");
            Console.WriteLine($"   💾 缓存大小: {advancedOptions.CacheSize}");
            Console.WriteLine($"   ⏱️ 超时时间: {advancedOptions.Timeout.TotalMinutes}分钟");

            Console.WriteLine("\n✅ Option方式密码保护演示完成！");
            Console.WriteLine("🎯 推荐使用Option方式，API更简洁统一");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"❌ 演示失败: {ex.Message}");
        }
        finally
        {
            // 清理文件
            var filesToClean = new[] { dbPath, "advanced_demo.db" };
            foreach (var file in filesToClean)
            {
                if (System.IO.File.Exists(file))
                {
                    try
                    {
                        System.IO.File.Delete(file);
                    }
                    catch
                    {
                        // 忽略删除错误
                    }
                }
            }
        }

        return Task.CompletedTask;
    }
}

/// <summary>
/// 演示用户实体
/// </summary>
[Entity("demo_user")]
public class DemoUser
{
    [Id]
    public ObjectId Id { get; set; }

    public string Name { get; set; } = string.Empty;

    public string Email { get; set; } = string.Empty;

    public DateTime CreatedAt { get; set; }
}
