using System;
using System.IO;
using System.Linq;
using TinyDb.Core;
using TinyDb.Attributes;
using TinyDb.IdGeneration;

namespace DebugTransactionRollbackWithIntId
{
    [Entity("users_int")]
    public class UserWithIntId
    {
        [IdGeneration(IdGenerationStrategy.IdentityInt)]
        public int Id { get; set; }
        public string Name { get; set; } = "";
        public int Age { get; set; }
        public string Email { get; set; } = "";
        public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    }

    class Program
    {
        static void Main()
        {
            Console.WriteLine("调试带有Int ID的事务回滚机制...");

            try
            {
                // 测试1: 简单事务回滚 - 使用UserWithIntId
                Console.WriteLine("\n=== 测试1: 简单事务回滚 - UserWithIntId ===");
                TestSimpleRollbackWithIntId();

                // 测试2: 复杂事务回滚 - 使用UserWithIntId
                Console.WriteLine("\n=== 测试2: 复杂事务回滚 - UserWithIntId ===");
                TestComplexRollbackWithIntId();

                Console.WriteLine("\n✅ 所有Int ID事务调试测试完成！");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"\n❌ 发生异常: {ex.Message}");
                Console.WriteLine($"异常类型: {ex.GetType().Name}");
                Console.WriteLine($"堆栈跟踪:\n{ex.StackTrace}");
            }
        }

        static void TestSimpleRollbackWithIntId()
        {
            var testFile = Path.GetTempFileName();
            Console.WriteLine($"测试文件: {testFile}");

            try
            {
                using var engine = new TinyDbEngine(testFile);
                var collection = engine.GetCollection<UserWithIntId>();

                // 初始状态检查
                var initialCount = collection.FindAll().Count();
                Console.WriteLine($"初始记录数: {initialCount}");

                // 开始事务，插入数据然后回滚
                using var transaction = engine.BeginTransaction();

                var entity1 = new UserWithIntId { Name = "Test1", Age = 25 };
                var entity2 = new UserWithIntId { Name = "Test2", Age = 30 };

                var id1 = collection.Insert(entity1);
                var id2 = collection.Insert(entity2);

                Console.WriteLine($"事务内插入记录: ID1={id1}, ID2={id2}");
                Console.WriteLine($"实体ID: entity1.Id={entity1.Id}, entity2.Id={entity2.Id}");

                // 验证事务内可见性
                var inTransactionCount = collection.FindAll().Count();
                Console.WriteLine($"事务内记录数: {inTransactionCount}");

                // 回滚事务
                transaction.Rollback();
                Console.WriteLine("事务已回滚");

                // 验证回滚后状态
                var afterRollbackCount = collection.FindAll().Count();
                Console.WriteLine($"回滚后记录数: {afterRollbackCount}");

                // 验证数据确实被回滚了
                var found1 = collection.FindById(id1);
                var found2 = collection.FindById(id2);

                Console.WriteLine($"查找ID1: {(found1 != null ? $"找到 - {found1.Name}" : "未找到")}");
                Console.WriteLine($"查找ID2: {(found2 != null ? $"找到 - {found2.Name}" : "未找到")}");

                var success = initialCount == afterRollbackCount && found1 == null && found2 == null;
                Console.WriteLine($"Int ID简单回滚测试: {(success ? "✅ 通过" : "❌ 失败")}");
            }
            finally
            {
                if (File.Exists(testFile))
                {
                    File.Delete(testFile);
                }
            }
        }

        static void TestComplexRollbackWithIntId()
        {
            var testFile = Path.GetTempFileName();
            Console.WriteLine($"测试文件: {testFile}");

            try
            {
                using var engine = new TinyDbEngine(testFile);
                var collection = engine.GetCollection<UserWithIntId>();

                // 先插入一些基础数据
                var baseEntity = new UserWithIntId { Name = "Base", Age = 50 };
                var baseId = collection.Insert(baseEntity);
                Console.WriteLine($"基础数据: ID={baseId}, Name={baseEntity.Name}");

                // 开始复杂事务
                using var transaction = engine.BeginTransaction();

                // 更新基础数据
                var baseToUpdate = collection.FindById(baseId);
                if (baseToUpdate != null)
                {
                    baseToUpdate.Age = 999;
                    collection.Update(baseToUpdate);
                    Console.WriteLine($"更新基础数据: Age={baseToUpdate.Age}");
                }

                // 插入新数据
                var newEntity1 = new UserWithIntId { Name = "New1", Age = 1000 };
                var newEntity2 = new UserWithIntId { Name = "New2", Age = 2000 };
                var newId1 = collection.Insert(newEntity1);
                var newId2 = collection.Insert(newEntity2);

                Console.WriteLine($"插入新数据: ID1={newId1}, ID2={newId2}");

                // 验证事务内状态
                var inTransactionBase = collection.FindById(baseId);
                var inTransactionNew1 = collection.FindById(newId1);
                var inTransactionNew2 = collection.FindById(newId2);
                var inTransactionCount = collection.FindAll().Count();

                Console.WriteLine($"事务内状态:");
                Console.WriteLine($"  基础数据Age: {inTransactionBase?.Age}");
                Console.WriteLine($"  新数据1: {(inTransactionNew1 != null ? "存在" : "不存在")}");
                Console.WriteLine($"  新数据2: {(inTransactionNew2 != null ? "存在" : "不存在")}");
                Console.WriteLine($"  总记录数: {inTransactionCount}");

                // 回滚事务
                transaction.Rollback();
                Console.WriteLine("复杂事务已回滚");

                // 验证回滚后状态
                var afterRollbackBase = collection.FindById(baseId);
                var afterRollbackNew1 = collection.FindById(newId1);
                var afterRollbackNew2 = collection.FindById(newId2);
                var afterRollbackCount = collection.FindAll().Count();

                Console.WriteLine($"回滚后状态:");
                Console.WriteLine($"  基础数据Age: {afterRollbackBase?.Age}");
                Console.WriteLine($"  新数据1: {(afterRollbackNew1 != null ? "存在" : "不存在")}");
                Console.WriteLine($"  新数据2: {(afterRollbackNew2 != null ? "存在" : "不存在")}");
                Console.WriteLine($"  总记录数: {afterRollbackCount}");

                var success = afterRollbackBase?.Age == 50 &&
                             afterRollbackNew1 == null &&
                             afterRollbackNew2 == null &&
                             afterRollbackCount == 1;
                Console.WriteLine($"Int ID复杂回滚测试: {(success ? "✅ 通过" : "❌ 失败")}");
            }
            finally
            {
                if (File.Exists(testFile))
                {
                    File.Delete(testFile);
                }
            }
        }
    }
}