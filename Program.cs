using System;
using System.IO;
using System.Linq;
using TinyDb.Core;
using TinyDb.Collections;
using TinyDb.Attributes;
using TinyDb.IdGeneration;

namespace DebugTransactionTest
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
            Console.WriteLine("=== 事务原子性深度调试测试 ===");

            string testFile = Path.GetTempFileName();
            Console.WriteLine($"测试文件: {testFile}");

            try
            {
                using var engine = new TinyDbEngine(testFile);
                var collection = engine.GetCollection<UserWithIntId>();

                // 检查初始状态
                var initialUsers = collection.FindAll().ToList();
                Console.WriteLine($"初始状态 - 用户数量: {initialUsers.Count}");

                Console.WriteLine("\n=== 测试1: 模拟单元测试的精确流程 ===");
                TestAtomicityLikeUnitTest(collection);

                Console.WriteLine("\n=== 测试2: 检查事务状态和操作记录 ===");
                TestTransactionStateAndOperations(engine, collection);

                Console.WriteLine("\n=== 测试3: 验证回滚后的数据完整性 ===");
                TestDataIntegrityAfterRollback(collection);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"\n❌ 错误: {ex.Message}");
                Console.WriteLine($"\n堆栈跟踪:\n{ex.StackTrace}");
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

        static void TestAtomicityLikeUnitTest(ILiteCollection<UserWithIntId> collection)
        {
            Console.WriteLine("开始事务原子性测试（模拟单元测试）...");

            var initialCount = collection.FindAll().Count();
            Console.WriteLine($"事务前数量: {initialCount}");

            // Act - 在事务中执行操作但回滚
            using var transaction = collection.Database.BeginTransaction();

            var users = new[]
            {
                new UserWithIntId { Name = "User1", Age = 25 },
                new UserWithIntId { Name = "User2", Age = 30 }
            };

            var insertedIds = new System.Collections.Generic.List<int>();
            foreach (var user in users)
            {
                var id = collection.Insert(user);
                insertedIds.Add(user.Id);
                Console.WriteLine($"插入用户: {user.Name}, ID={id}, user.Id={user.Id}");
            }

            // 验证在事务中数据是可见的
            var countDuringTransaction = collection.FindAll().Count();
            Console.WriteLine($"事务期间数量: {countDuringTransaction}");
            Console.WriteLine($"事务期间数量 > 初始数量: {countDuringTransaction > initialCount}");

            // 回滚事务
            Console.WriteLine("开始回滚事务...");
            transaction.Rollback();
            Console.WriteLine("事务已回滚");

            // Assert - 验证回滚后所有操作都被撤销
            var finalCount = collection.FindAll().Count();
            Console.WriteLine($"回滚后数量: {finalCount}");
            Console.WriteLine($"期望数量: {initialCount}, 实际数量: {finalCount}");
            Console.WriteLine($"回滚成功: {finalCount == initialCount}");

            // 验证插入的数据都不存在
            Console.WriteLine("验证插入的数据是否已被删除:");
            foreach (var userId in insertedIds)
            {
                var foundUser = collection.FindById(userId);
                Console.WriteLine($"  查找ID={userId}: {(foundUser != null ? "找到" : "未找到")}");
            }

            if (finalCount == initialCount)
            {
                Console.WriteLine("✅ 事务原子性测试通过！");
            }
            else
            {
                Console.WriteLine("❌ 事务原子性测试失败！");
            }
        }

        static void TestTransactionStateAndOperations(TinyDbEngine engine, ILiteCollection<UserWithIntId> collection)
        {
            Console.WriteLine("检查事务状态和操作记录...");

            using var transaction = engine.BeginTransaction();
            Console.WriteLine($"事务状态: {transaction.State}");

            var user = new UserWithIntId { Name = "TestUser", Age = 99 };
            var id = collection.Insert(user);
            Console.WriteLine($"插入用户后事务状态: {transaction.State}");

            // 检查事务操作数量
            if (transaction is Transaction concreteTransaction)
            {
                Console.WriteLine($"事务操作数量: {concreteTransaction.Operations.Count}");
                foreach (var op in concreteTransaction.Operations)
                {
                    Console.WriteLine($"  操作: {op.OperationType}, 集合: {op.CollectionName}, ID: {op.DocumentId}");
                }
            }

            transaction.Rollback();
            Console.WriteLine($"回滚后事务状态: {transaction.State}");
        }

        static void TestDataIntegrityAfterRollback(ILiteCollection<UserWithIntId> collection)
        {
            Console.WriteLine("验证回滚后的数据完整性...");

            // 多次测试以确保一致性
            for (int testRound = 1; testRound <= 3; testRound++)
            {
                Console.WriteLine($"\n--- 测试轮次 {testRound} ---");

                var beforeCount = collection.FindAll().Count();

                using var transaction = collection.Database.BeginTransaction();
                var testUser = new UserWithIntId { Name = $"Round{testRound}User", Age = testRound * 10 };
                var testId = collection.Insert(testUser);

                var duringCount = collection.FindAll().Count();
                Console.WriteLine($"  事务前: {beforeCount}, 事务中: {duringCount}");

                transaction.Rollback();

                var afterCount = collection.FindAll().Count();
                Console.WriteLine($"  回滚后: {afterCount}");
                Console.WriteLine($"  一致性: {(afterCount == beforeCount ? "✅" : "❌")}");
            }
        }
    }
}