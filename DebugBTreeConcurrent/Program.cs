using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using TinyDb.Bson;
using TinyDb.Index;

namespace DebugBTreeConcurrent
{
    class Program
    {
        private const int SmallMaxKeys = 4; // 使用小的键数量便于测试边界条件

        static async Task Main(string[] args)
        {
            Console.WriteLine("调试B+树并发操作...");

            try
            {
                // 测试1: 基本B+树操作
                Console.WriteLine("\n=== 测试1: 基本B+树操作 ===");
                await TestBasicBTreeOperations();

                // 测试2: B+树节点分裂
                Console.WriteLine("\n=== 测试2: B+树节点分裂 ===");
                await TestBTreeSplitting();

                // 测试3: B+树并发操作
                Console.WriteLine("\n=== 测试3: B+树并发操作 ===");
                await TestBTreeConcurrentOperations();

                Console.WriteLine("\n✅ 所有B+树调试测试完成！");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"\n❌ 发生异常: {ex.Message}");
                Console.WriteLine($"异常类型: {ex.GetType().Name}");
                Console.WriteLine($"堆栈跟踪:\n{ex.StackTrace}");
            }
        }

        static async Task TestBasicBTreeOperations()
        {
            using var index = new BTreeIndex("test", new[] { "testField" }, false, SmallMaxKeys);

            // 插入一些基本数据
            for (int i = 1; i <= 3; i++)
            {
                var key = new IndexKey(new BsonInt32(i));
                var docId = new BsonString($"doc_{i}");
                var inserted = index.Insert(key, docId);
                Console.WriteLine($"插入键 {i}: {(inserted ? "成功" : "失败")}");
            }

            // 验证索引状态
            Console.WriteLine($"节点数: {index.NodeCount}");
            Console.WriteLine($"条目数: {index.EntryCount}");
            Console.WriteLine($"索引验证: {(index.Validate() ? "通过" : "失败")}");

            // 测试查找
            for (int i = 1; i <= 3; i++)
            {
                var key = new IndexKey(new BsonInt32(i));
                var found = index.FindExact(key);
                Console.WriteLine($"查找键 {i}: {(found != null ? $"找到 {found}" : "未找到")}");
            }
        }

        static async Task TestBTreeSplitting()
        {
            using var index = new BTreeIndex("test", new[] { "testField" }, false, SmallMaxKeys);

            Console.WriteLine("插入数据直到节点分裂...");

            // 插入刚好达到最大键数量的数据
            for (int i = 1; i <= SmallMaxKeys; i++)
            {
                var key = new IndexKey(new BsonInt32(i));
                var docId = new BsonString($"doc_{i}");
                var inserted = index.Insert(key, docId);
                Console.WriteLine($"插入键 {i}: {(inserted ? "成功" : "失败")}");
            }

            Console.WriteLine($"插入{SmallMaxKeys}个键后 - 节点数: {index.NodeCount}, 验证: {(index.Validate() ? "通过" : "失败")}");

            // 插入第SmallMaxKeys + 1个键，应该触发分裂
            var extraKey = new IndexKey(new BsonInt32(SmallMaxKeys + 1));
            var extraDocId = new BsonString($"doc_{SmallMaxKeys + 1}");
            var splitInserted = index.Insert(extraKey, extraDocId);
            Console.WriteLine($"插入分裂键 {SmallMaxKeys + 1}: {(splitInserted ? "成功" : "失败")}");

            Console.WriteLine($"分裂后 - 节点数: {index.NodeCount}, 条目数: {index.EntryCount}, 验证: {(index.Validate() ? "通过" : "失败")}");

            // 验证所有键都能找到
            for (int i = 1; i <= SmallMaxKeys + 1; i++)
            {
                var key = new IndexKey(new BsonInt32(i));
                var found = index.FindExact(key);
                Console.WriteLine($"查找键 {i}: {(found != null ? $"找到 {found}" : "未找到")}");
            }
        }

        static async Task TestBTreeConcurrentOperations()
        {
            using var index = new BTreeIndex("test", new[] { "testField" }, false, SmallMaxKeys);
            var tasks = new List<Task>();
            var exceptions = new List<Exception>();
            const int operationsPerTask = 20;

            Console.WriteLine("启动并发操作...");

            // 启动4个并发任务
            for (int taskId = 0; taskId < 4; taskId++)
            {
                var currentTaskId = taskId;
                tasks.Add(Task.Run(() =>
                {
                    try
                    {
                        for (int j = 0; j < operationsPerTask; j++)
                        {
                            var key = new IndexKey(new BsonInt32(currentTaskId * 1000 + j));
                            var docId = new BsonString($"doc_{currentTaskId}_{j}");

                            // 插入操作
                            var inserted = index.Insert(key, docId);
                            if (!inserted)
                            {
                                Console.WriteLine($"任务 {currentTaskId}: 插入键 {currentTaskId * 1000 + j} 失败");
                            }

                            // 随机删除一些之前插入的数据
                            if (j > 5)
                            {
                                var deleteKey = new IndexKey(new BsonInt32(currentTaskId * 1000 + j - 5));
                                var deleted = index.Delete(deleteKey, new BsonString($"doc_{currentTaskId}_{j - 5}"));
                                // 删除失败是正常的，因为可能已经被其他操作删除
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        lock (exceptions)
                        {
                            exceptions.Add(ex);
                        }
                    }
                }));
            }

            // 等待所有任务完成
            await Task.WhenAll(tasks);

            Console.WriteLine($"并发操作完成 - 异常数: {exceptions.Count}");
            if (exceptions.Count > 0)
            {
                foreach (var ex in exceptions.Take(3)) // 只显示前3个异常
                {
                    Console.WriteLine($"异常: {ex.Message}");
                }
            }

            // 最终状态验证
            Console.WriteLine($"最终状态 - 节点数: {index.NodeCount}, 条目数: {index.EntryCount}");
            var isValid = index.Validate();
            Console.WriteLine($"索引验证: {(isValid ? "✅ 通过" : "❌ 失败")}");

            if (!isValid)
            {
                Console.WriteLine("⚠️ 索引验证失败，这解释了为什么单元测试失败");

                // 尝试获取更多验证信息
                try
                {
                    // 检查一些基本的查找操作
                    var testKey = new IndexKey(new BsonInt32(0));
                    var found = index.FindExact(testKey);
                    Console.WriteLine($"测试查找键 0: {(found != null ? $"找到 {found}" : "未找到")}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"测试查找时发生异常: {ex.Message}");
                }
            }
        }
    }
}