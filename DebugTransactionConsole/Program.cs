using System;
using System.IO;
using System.Threading.Tasks;
using System.Linq;
using TinyDb.Core;
using TinyDb.Collections;
using TinyDb.Attributes;
using TinyDb.IdGeneration;

namespace DebugTransactionConsole
{
    [Entity("users_int")]
    class UserWithIntId
    {
        [IdGeneration(IdGenerationStrategy.IdentityInt)]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public int Age { get; set; }
        public string Email { get; set; } = string.Empty;
        public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    }

    [Entity("users_long")]
    class UserWithLongId
    {
        [IdGeneration(IdGenerationStrategy.IdentityLong, "users_long_seq")]
        public long Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public int Age { get; set; }
        public string Email { get; set; } = string.Empty;
        public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    }

    class Program
    {
        static async Task Main(string[] args)
        {
            Console.WriteLine("=== 事务隔离性调试测试 ===");

            string testFile = Path.GetTempFileName();
            Console.WriteLine($"测试文件: {testFile}");

            try
            {
                using var engine = new TinyDbEngine(testFile);

                // 先检查是否有初始数据（模拟测试环境可能的数据污染）
                var collection1 = engine.GetCollection<UserWithIntId>();
                var collection2 = engine.GetCollection<UserWithLongId>();

                var initialUsers1 = collection1.FindAll().ToList();
                var initialUsers2 = collection2.FindAll().ToList();

                Console.WriteLine($"初始状态 - Collection1 用户数: {initialUsers1.Count}");
                Console.WriteLine($"初始状态 - Collection2 用户数: {initialUsers2.Count}");

                if (initialUsers1.Count > 0 || initialUsers2.Count > 0)
                {
                    Console.WriteLine("⚠️  警告：检测到初始数据污染！");
                    foreach (var user in initialUsers1)
                    {
                        Console.WriteLine($"  Collection1 初始用户: ID={user.Id}, Name={user.Name}, Age={user.Age}");
                    }
                    foreach (var user in initialUsers2)
                    {
                        Console.WriteLine($"  Collection2 初始用户: ID={user.Id}, Name={user.Name}, Age={user.Age}");
                    }
                }

                Console.WriteLine("\n阶段1: 并发插入测试");
                await TestConcurrentInsertion(collection1, collection2);

                Console.WriteLine("\n阶段2: 验证数据隔离");
                await VerifyDataIsolation(collection1, collection2);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"\n❌ 错误: {ex.Message}");
                Console.WriteLine($"堆栈跟踪:\n{ex.StackTrace}");
            }
            finally
            {
                if (File.Exists(testFile))
                {
                    File.Delete(testFile);
                    Console.WriteLine($"\n🧹 已清理测试文件: {testFile}");
                }
            }
        }

        static async Task TestConcurrentInsertion(ILiteCollection<UserWithIntId> collection1, ILiteCollection<UserWithLongId> collection2)
        {
            var task1 = Task.Run(() =>
            {
                try
                {
                    using var transaction = collection1.Database.BeginTransaction();
                    var user = new UserWithIntId { Name = "Task1User", Age = 25 };
                    var id = collection1.Insert(user);
                    transaction.Commit();
                    Console.WriteLine($"✅ Task1: 插入用户成功，ID={id}");
                    return id;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"❌ Task1 失败: {ex.Message}");
                    throw;
                }
            });

            var task2 = Task.Run(() =>
            {
                try
                {
                    using var transaction = collection2.Database.BeginTransaction();
                    var user = new UserWithLongId { Name = "Task2User", Age = 30 };
                    var id = collection2.Insert(user);
                    transaction.Commit();
                    Console.WriteLine($"✅ Task2: 插入用户成功，ID={id}");
                    return id;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"❌ Task2 失败: {ex.Message}");
                    throw;
                }
            });

            await Task.WhenAll(task1, task2);
        }

        static async Task VerifyDataIsolation(ILiteCollection<UserWithIntId> collection1, ILiteCollection<UserWithLongId> collection2)
        {
            var users1 = collection1.FindAll().ToList();
            var users2 = collection2.FindAll().ToList();

            Console.WriteLine($"📊 Collection1 (UserWithIntId) 用户数量: {users1.Count}");
            foreach (var user in users1)
            {
                Console.WriteLine($"  - ID={user.Id}, Name={user.Name}, Age={user.Age}");
            }

            Console.WriteLine($"📊 Collection2 (UserWithLongId) 用户数量: {users2.Count}");
            foreach (var user in users2)
            {
                Console.WriteLine($"  - ID={user.Id}, Name={user.Name}, Age={user.Age}");
            }

            // 验证隔离性
            if (users1.Count != 1)
            {
                throw new InvalidOperationException($"Collection1 应该有1个用户，但实际有 {users1.Count} 个");
            }

            if (users2.Count != 1)
            {
                throw new InvalidOperationException($"Collection2 应该有1个用户，但实际有 {users2.Count} 个");
            }

            if (users1[0].Name != "Task1User")
            {
                throw new InvalidOperationException($"Collection1 用户名称错误: 期望 'Task1User', 实际 '{users1[0].Name}'");
            }

            if (users2[0].Name != "Task2User")
            {
                throw new InvalidOperationException($"Collection2 用户名称错误: 期望 'Task2User', 实际 '{users2[0].Name}'");
            }

            Console.WriteLine("✅ 事务隔离性验证通过");
        }
    }
}