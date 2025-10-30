using System;
using System.IO;
using System.Linq;
using TinyDb.Core;
using TinyDb.Collections;
using TinyDb.Attributes;
using TinyDb.IdGeneration;

namespace DebugTransactionRollbackConsole
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

    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("=== 事务回滚调试测试 ===");

            string testFile = Path.GetTempFileName();
            Console.WriteLine($"测试文件: {testFile}");

            try
            {
                using var engine = new TinyDbEngine(testFile);
                var collection = engine.GetCollection<UserWithIntId>();

                // 检查初始状态
                var initialUsers = collection.FindAll().ToList();
                Console.WriteLine($"初始状态 - 用户数量: {initialUsers.Count}");

                Console.WriteLine("\n阶段1: 事务插入测试");
                Console.WriteLine("开始事务...");

                using var transaction = engine.BeginTransaction();
                Console.WriteLine($"事务状态: {transaction}");

                var users = new[]
                {
                    new UserWithIntId { Name = "User1", Age = 25 },
                    new UserWithIntId { Name = "User2", Age = 30 }
                };

                var insertedIds = new System.Collections.Generic.List<int>();
                foreach (var user in users)
                {
                    Console.WriteLine($"插入用户: {user.Name}");
                    var id = collection.Insert(user);
                    Console.WriteLine($"  -> 插入成功，ID={id}, user.Id={user.Id}");
                    insertedIds.Add(user.Id);
                }

                // 检查事务期间的数据
                var usersInTransaction = collection.FindAll().ToList();
                Console.WriteLine($"\n事务期间 - 用户数量: {usersInTransaction.Count}");
                foreach (var user in usersInTransaction)
                {
                    Console.WriteLine($"  - ID={user.Id}, Name={user.Name}, Age={user.Age}");
                }

                Console.WriteLine($"\n初始数量: {initialUsers.Count}, 事务期间数量: {usersInTransaction.Count}");
                Console.WriteLine($"事务期间数量是否大于初始数量: {usersInTransaction.Count > initialUsers.Count}");

                Console.WriteLine("\n阶段2: 回滚事务");
                transaction.Rollback();
                Console.WriteLine("事务已回滚");

                // 检查回滚后的数据
                var finalUsers = collection.FindAll().ToList();
                Console.WriteLine($"\n回滚后 - 用户数量: {finalUsers.Count}");
                foreach (var user in finalUsers)
                {
                    Console.WriteLine($"  - ID={user.Id}, Name={user.Name}, Age={user.Age}");
                }

                Console.WriteLine($"\n最终数量: {finalUsers.Count}, 初始数量: {initialUsers.Count}");
                Console.WriteLine($"回滚后数量是否等于初始数量: {finalUsers.Count == initialUsers.Count}");

                // 验证插入的数据是否都不存在
                Console.WriteLine("\n验证插入的数据是否已被删除:");
                foreach (var userId in insertedIds)
                {
                    var foundUser = collection.FindById(userId);
                    Console.WriteLine($"  查找ID={userId}: {(foundUser != null ? "找到" : "未找到")}");
                }

                if (usersInTransaction.Count > initialUsers.Count && finalUsers.Count == initialUsers.Count)
                {
                    Console.WriteLine("\n✅ 事务回滚机制正常工作！");
                }
                else
                {
                    Console.WriteLine("\n❌ 事务回滚机制存在问题！");
                }
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
    }
}