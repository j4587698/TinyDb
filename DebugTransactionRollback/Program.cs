using System;
using System.IO;
using System.Linq;
using TinyDb.Core;

namespace DebugTransactionRollback
{
    class Program
    {
        static void Main()
        {
            Console.WriteLine("调试事务回滚机制...");

            try
            {
                // 测试1: 简单事务回滚
                Console.WriteLine("\n=== 测试1: 简单事务回滚 ===");
                TestSimpleRollback();

                // 测试2: 复杂事务回滚
                Console.WriteLine("\n=== 测试2: 复杂事务回滚 ===");
                TestComplexRollback();

                // 测试3: 事务持久性验证
                Console.WriteLine("\n=== 测试3: 事务持久性验证 ===");
                TestTransactionDurability();

                Console.WriteLine("\n✅ 所有事务调试测试完成！");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"\n❌ 发生异常: {ex.Message}");
                Console.WriteLine($"异常类型: {ex.GetType().Name}");
                Console.WriteLine($"堆栈跟踪:\n{ex.StackTrace}");
            }
        }

        static void TestSimpleRollback()
        {
            var testFile = Path.GetTempFileName();
            Console.WriteLine($"测试文件: {testFile}");

            try
            {
                using var engine = new TinyDbEngine(testFile);
                var collection = engine.GetCollection<TestEntity>();

                // 初始状态检查
                var initialCount = collection.FindAll().Count();
                Console.WriteLine($"初始记录数: {initialCount}");

                // 开始事务，插入数据然后回滚
                using var transaction = engine.BeginTransaction();

                var entity1 = new TestEntity { Name = "Test1", Value = 100 };
                var entity2 = new TestEntity { Name = "Test2", Value = 200 };

                var id1 = collection.Insert(entity1);
                var id2 = collection.Insert(entity2);

                Console.WriteLine($"事务内插入记录: ID1={id1}, ID2={id2}");

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
                Console.WriteLine($"简单回滚测试: {(success ? "✅ 通过" : "❌ 失败")}");
            }
            finally
            {
                if (File.Exists(testFile))
                {
                    File.Delete(testFile);
                }
            }
        }

        static void TestComplexRollback()
        {
            var testFile = Path.GetTempFileName();
            Console.WriteLine($"测试文件: {testFile}");

            try
            {
                using var engine = new TinyDbEngine(testFile);
                var collection = engine.GetCollection<TestEntity>();

                // 先插入一些基础数据
                var baseEntity = new TestEntity { Name = "Base", Value = 50 };
                var baseId = collection.Insert(baseEntity);
                Console.WriteLine($"基础数据: ID={baseId}, Name={baseEntity.Name}");

                // 开始复杂事务
                using var transaction = engine.BeginTransaction();

                // 更新基础数据
                var baseToUpdate = collection.FindById(baseId);
                if (baseToUpdate != null)
                {
                    baseToUpdate.Value = 999;
                    collection.Update(baseToUpdate);
                    Console.WriteLine($"更新基础数据: Value={baseToUpdate.Value}");
                }

                // 插入新数据
                var newEntity1 = new TestEntity { Name = "New1", Value = 1000 };
                var newEntity2 = new TestEntity { Name = "New2", Value = 2000 };
                var newId1 = collection.Insert(newEntity1);
                var newId2 = collection.Insert(newEntity2);

                Console.WriteLine($"插入新数据: ID1={newId1}, ID2={newId2}");

                // 验证事务内状态
                var inTransactionBase = collection.FindById(baseId);
                var inTransactionNew1 = collection.FindById(newId1);
                var inTransactionNew2 = collection.FindById(newId2);
                var inTransactionCount = collection.FindAll().Count();

                Console.WriteLine($"事务内状态:");
                Console.WriteLine($"  基础数据Value: {inTransactionBase?.Value}");
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
                Console.WriteLine($"  基础数据Value: {afterRollbackBase?.Value}");
                Console.WriteLine($"  新数据1: {(afterRollbackNew1 != null ? "存在" : "不存在")}");
                Console.WriteLine($"  新数据2: {(afterRollbackNew2 != null ? "存在" : "不存在")}");
                Console.WriteLine($"  总记录数: {afterRollbackCount}");

                var success = afterRollbackBase?.Value == 50 &&
                             afterRollbackNew1 == null &&
                             afterRollbackNew2 == null &&
                             afterRollbackCount == 1;
                Console.WriteLine($"复杂回滚测试: {(success ? "✅ 通过" : "❌ 失败")}");
            }
            finally
            {
                if (File.Exists(testFile))
                {
                    File.Delete(testFile);
                }
            }
        }

        static void TestTransactionDurability()
        {
            var testFile = Path.GetTempFileName();
            Console.WriteLine($"测试文件: {testFile}");

            try
            {
                // 第一阶段：提交事务
                Console.WriteLine("第一阶段：提交事务");
                using (var engine = new TinyDbEngine(testFile))
                {
                    var collection = engine.GetCollection<TestEntity>();

                    using var transaction = engine.BeginTransaction();
                    var entity = new TestEntity { Name = "Durable", Value = 42 };
                    var entityId = collection.Insert(entity);
                    transaction.Commit();

                    Console.WriteLine($"提交的实体: ID={entityId}, Name={entity.Name}, Value={entity.Value}");
                }

                // 第二阶段：重新打开引擎验证数据持久性
                Console.WriteLine("第二阶段：验证数据持久性");
                using (var newEngine = new TinyDbEngine(testFile))
                {
                    var newCollection = newEngine.GetCollection<TestEntity>();
                    var allEntities = newCollection.FindAll().ToList();

                    Console.WriteLine($"持久化后的记录数: {allEntities.Count}");
                    if (allEntities.Count > 0)
                    {
                        var persistedEntity = allEntities[0];
                        Console.WriteLine($"持久化实体: ID={persistedEntity.Id}, Name={persistedEntity.Name}, Value={persistedEntity.Value}");
                    }

                    var success = allEntities.Count == 1 &&
                                 allEntities[0].Name == "Durable" &&
                                 allEntities[0].Value == 42;
                    Console.WriteLine($"持久性测试: {(success ? "✅ 通过" : "❌ 失败")}");
                }
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

    public class TestEntity
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public int Value { get; set; }
    }
}