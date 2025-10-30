using System;
using System.IO;
using System.Linq;
using TinyDb.Core;

namespace TestDurabilityConsole
{
    class UserWithIntId
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public int Age { get; set; }
    }

    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("=== 数据库持久化测试 ===");

            string testFile = Path.GetTempFileName();
            Console.WriteLine($"测试文件: {testFile}");

            try
            {
                // 第一阶段：创建数据库并插入数据
                Console.WriteLine("\n阶段1: 创建数据库并插入数据");
                TestDurability(testFile);

                // 第二阶段：重新打开数据库验证持久性
                Console.WriteLine("\n阶段2: 重新打开数据库验证持久性");
                VerifyDurability(testFile);

                Console.WriteLine("\n✅ 持久化测试成功完成！");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"\n❌ 测试失败: {ex.Message}");
                Console.WriteLine($"堆栈跟踪: {ex.StackTrace}");
            }
            finally
            {
                // 清理测试文件
                if (File.Exists(testFile))
                {
                    File.Delete(testFile);
                    Console.WriteLine($"\n🧹 已清理测试文件: {testFile}");
                }
            }
        }

        static void TestDurability(string testFile)
        {
            using var engine = new TinyDbEngine(testFile);
            var collection = engine.GetCollection<UserWithIntId>();

            Console.WriteLine($"数据库引擎WAL状态: {(engine.GetWalEnabled() ? "启用" : "禁用")}");

            // 检查数据库文件初始状态
            var initialFileInfo = new FileInfo(testFile);
            Console.WriteLine($"📄 初始数据库文件大小: {initialFileInfo.Length} 字节");

            // 在事务中插入数据
            using var transaction = engine.BeginTransaction();
            var user = new UserWithIntId { Name = "DurableUser", Age = 25 };
            var userId = collection.Insert(user);
            transaction.Commit();

            Console.WriteLine($"✅ 插入用户成功: ID={userId}, Name={user.Name}, Age={user.Age}");

            // 检查事务提交后文件状态
            var afterCommitFileInfo = new FileInfo(testFile);
            Console.WriteLine($"📄 事务提交后文件大小: {afterCommitFileInfo.Length} 字节");

            // 验证数据在当前引擎实例中存在
            var foundUser = collection.FindById(userId);
            if (foundUser != null)
            {
                Console.WriteLine($"✅ 在当前会话中验证用户存在: ID={foundUser.Id}, Name={foundUser.Name}, Age={foundUser.Age}");
            }
            else
            {
                throw new InvalidOperationException("在当前会话中找不到插入的用户！");
            }

            // 确保所有数据都刷新到磁盘
            engine.Flush();
            Console.WriteLine("✅ 数据已刷新到磁盘");

            // 检查Flush后文件状态
            var afterFlushFileInfo = new FileInfo(testFile);
            Console.WriteLine($"📄 Flush后文件大小: {afterFlushFileInfo.Length} 字节");
        }

        static void VerifyDurability(string testFile)
        {
            Console.WriteLine("🔄 尝试重新创建引擎实例...");

            // 重新创建引擎实例（模拟重启）
            using var newEngine = new TinyDbEngine(testFile);
            var newCollection = newEngine.GetCollection<UserWithIntId>();

            Console.WriteLine("✅ 重新打开数据库成功");
            Console.WriteLine($"新引擎WAL状态: {(newEngine.GetWalEnabled() ? "启用" : "禁用")}");

            // 列出所有用户
            var allUsers = newCollection.FindAll().ToList();
            Console.WriteLine($"📊 数据库中用户总数: {allUsers.Count}");

            if (allUsers.Count == 0)
            {
                throw new InvalidOperationException("数据库中没有找到任何用户数据！");
            }

            var persistedUser = allUsers[0];
            Console.WriteLine($"✅ 找到持久化用户: ID={persistedUser.Id}, Name={persistedUser.Name}, Age={persistedUser.Age}");

            // 验证数据完整性
            if (persistedUser.Name != "DurableUser" || persistedUser.Age != 25)
            {
                throw new InvalidOperationException($"持久化数据不完整！期望 Name='DurableUser', Age=25，实际 Name='{persistedUser.Name}', Age={persistedUser.Age}");
            }

            Console.WriteLine("✅ 数据持久性验证通过");
        }
    }
}