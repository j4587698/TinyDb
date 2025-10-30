using System;
using System.IO;
using TinyDb.Core;

namespace DebugDurabilityConsole
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
            Console.WriteLine("=== 调试数据库持久化测试 ===");

            string testFile = Path.GetTempFileName();
            Console.WriteLine($"测试文件: {testFile}");

            try
            {
                // 第一阶段：创建数据库并插入数据，详细检查每一步
                Console.WriteLine("\n阶段1: 创建数据库并插入数据");
                TestWithDetailedDebugging(testFile);

                // 第二阶段：检查数据库文件内容
                Console.WriteLine("\n阶段2: 检查数据库文件内容");
                InspectDatabaseFile(testFile);

                // 第三阶段：重新打开数据库验证持久性
                Console.WriteLine("\n阶段3: 重新打开数据库验证持久性");
                VerifyDurability(testFile);

                Console.WriteLine("\n✅ 调试测试成功完成！");
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

        static void TestWithDetailedDebugging(string testFile)
        {
            using var engine = new TinyDbEngine(testFile);
            var collection = engine.GetCollection<UserWithIntId>();

            Console.WriteLine($"数据库引擎WAL状态: {(engine.GetWalEnabled() ? "启用" : "禁用")}");

            // 检查数据库文件初始状态
            var initialFileInfo = new FileInfo(testFile);
            Console.WriteLine($"📄 初始数据库文件大小: {initialFileInfo.Length} 字节");

            // 在事务中插入数据
            using var transaction = engine.BeginTransaction();
            var user = new UserWithIntId { Name = "DebugUser", Age = 35 };
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

            // 手动触发Flush并检查文件状态
            Console.WriteLine("🔄 手动触发Flush...");
            engine.Flush();

            // 检查Flush后文件状态
            var afterFlushFileInfo = new FileInfo(testFile);
            Console.WriteLine($"📄 Flush后文件大小: {afterFlushFileInfo.Length} 字节");

            Console.WriteLine("✅ 数据已刷新到磁盘");
        }

        static void InspectDatabaseFile(string testFile)
        {
            var fileInfo = new FileInfo(testFile);
            Console.WriteLine($"📊 数据库文件检查:");
            Console.WriteLine($"  - 文件大小: {fileInfo.Length} 字节");
            Console.WriteLine($"  - 创建时间: {fileInfo.CreationTime}");
            Console.WriteLine($"  - 修改时间: {fileInfo.LastWriteTime}");

            if (fileInfo.Length >= 64) // 至少应该有数据库头部
            {
                // 读取前64字节作为头部信息
                using var fs = new FileStream(testFile, FileMode.Open, FileAccess.Read);
                var header = new byte[64];
                var bytesRead = fs.Read(header, 0, 64);
                Console.WriteLine($"📖 头部前{bytesRead}字节: {BitConverter.ToString(header, 0, bytesRead)}");

                // 尝试读取可能的magic number
                if (bytesRead >= 4)
                {
                    var magic = BitConverter.ToInt32(header, 0);
                    Console.WriteLine($"🔖 Magic Number: 0x{magic:X8}");
                }
            }
            else
            {
                Console.WriteLine("❌ 文件太小，可能头部未正确写入");
            }
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
            if (persistedUser.Name != "DebugUser" || persistedUser.Age != 35)
            {
                throw new InvalidOperationException($"持久化数据不完整！期望 Name='DebugUser', Age=35，实际 Name='{persistedUser.Name}', Age={persistedUser.Age}");
            }

            Console.WriteLine("✅ 数据持久性验证通过");
        }
    }
}