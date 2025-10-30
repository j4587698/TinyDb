using System;
using System.IO;
using TinyDb.Core;

namespace WalTestConsole
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
            Console.WriteLine("=== WAL写入测试 ===");

            string testFile = Path.GetTempFileName();
            Console.WriteLine($"测试文件: {testFile}");

            try
            {
                // 第一阶段：创建数据库并插入数据
                Console.WriteLine("\n阶段1: 创建数据库并插入数据");
                TestWalBehavior(testFile);

                // 第二阶段：检查WAL文件
                Console.WriteLine("\n阶段2: 检查WAL文件");
                CheckWalFile(testFile);

                // 第三阶段：重新打开数据库验证持久性
                Console.WriteLine("\n阶段3: 重新打开数据库验证持久性");
                VerifyDurability(testFile);

                Console.WriteLine("\n✅ WAL测试成功完成！");
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

                string walFile = Path.ChangeExtension(testFile, ".wal");
                if (File.Exists(walFile))
                {
                    File.Delete(walFile);
                    Console.WriteLine($"🧹 已清理WAL文件: {walFile}");
                }
            }
        }

        static void TestWalBehavior(string testFile)
        {
            using var engine = new TinyDbEngine(testFile);
            var collection = engine.GetCollection<UserWithIntId>();

            Console.WriteLine($"数据库引擎WAL状态: {(engine.GetWalEnabled() ? "启用" : "禁用")}");

            // 在事务中插入数据
            using var transaction = engine.BeginTransaction();
            var user = new UserWithIntId { Name = "WalUser", Age = 30 };
            var userId = collection.Insert(user);
            transaction.Commit();

            Console.WriteLine($"✅ 插入用户成功: ID={userId}, Name={user.Name}, Age={user.Age}");

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
        }

        static void CheckWalFile(string testFile)
        {
            string walFile = Path.ChangeExtension(testFile, ".wal");
            if (File.Exists(walFile))
            {
                var walInfo = new FileInfo(walFile);
                Console.WriteLine($"✅ WAL文件存在: {walFile}, 大小: {walInfo.Length} 字节");

                // 读取WAL文件的前几个字节来验证内容
                using var fs = new FileStream(walFile, FileMode.Open, FileAccess.Read);
                var buffer = new byte[Math.Min(100, walInfo.Length)];
                var bytesRead = fs.Read(buffer, 0, buffer.Length);
                Console.WriteLine($"✅ WAL文件前{bytesRead}字节: {BitConverter.ToString(buffer, 0, bytesRead)}");
            }
            else
            {
                Console.WriteLine("❌ WAL文件不存在");
            }
        }

        static void VerifyDurability(string testFile)
        {
            // 重新创建引擎实例（模拟重启）
            using var newEngine = new TinyDbEngine(testFile);
            var newCollection = newEngine.GetCollection<UserWithIntId>();

            Console.WriteLine("✅ 重新打开数据库成功");

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
            if (persistedUser.Name != "WalUser" || persistedUser.Age != 30)
            {
                throw new InvalidOperationException($"持久化数据不完整！期望 Name='WalUser', Age=30，实际 Name='{persistedUser.Name}', Age={persistedUser.Age}");
            }

            Console.WriteLine("✅ 数据持久性验证通过");
        }
    }
}