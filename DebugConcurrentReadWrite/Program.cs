using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using TinyDb.Core;
using TinyDb.Attributes;
using TinyDb.IdGeneration;

namespace DebugConcurrentReadWrite
{
    [Entity("users_int_debug")]
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
        static async Task Main(string[] args)
        {
            Console.WriteLine("调试并发读写数据一致性问题...");

            try
            {
                await DebugConcurrentReadWriteIssue();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"\n❌ 发生异常: {ex.Message}");
                Console.WriteLine($"\n异常类型: {ex.GetType().Name}");
                Console.WriteLine($"\n堆栈跟踪:\n{ex.StackTrace}");
            }
        }

        static async Task DebugConcurrentReadWriteIssue()
        {
            var testFile = Path.GetTempFileName();
            Console.WriteLine($"测试文件: {testFile}");

            try
            {
                using var engine = new TinyDbEngine(testFile);
                var collection = engine.GetCollection<UserWithIntId>();

                const int baseRecordCount = 1000;
                const int concurrentOperations = 100;

                Console.WriteLine($"\n=== 准备阶段：插入 {baseRecordCount} 条基础数据 ===");

                // 插入基础数据
                var baseUsers = Enumerable.Range(1, baseRecordCount)
                    .Select(i => new UserWithIntId
                    {
                        Name = $"BaseUser_{i}",
                        Age = 20 + (i % 50)
                    })
                    .ToArray();

                var insertStopwatch = System.Diagnostics.Stopwatch.StartNew();
                foreach (var user in baseUsers)
                {
                    collection.Insert(user);
                }
                insertStopwatch.Stop();

                Console.WriteLine($"基础数据插入完成: {baseRecordCount} 条记录，耗时 {insertStopwatch.Elapsed.TotalSeconds:F2}s");
                Console.WriteLine($"插入后验证: {collection.FindAll().Count()} 条记录");

                Console.WriteLine($"\n=== 并发操作阶段：{concurrentOperations} 个并发任务 ===");

                var exceptions = new List<Exception>();
                var results = new List<string>();
                var tasks = new List<Task>();
                var operationCounts = new Dictionary<string, int>
                {
                    ["Read"] = 0,
                    ["Write"] = 0,
                    ["Update"] = 0,
                    ["UpdateFailed"] = 0
                };

                var concurrentStopwatch = System.Diagnostics.Stopwatch.StartNew();

                // 并发读写操作
                for (int i = 0; i < concurrentOperations; i++)
                {
                    var taskId = i;
                    tasks.Add(Task.Run(() =>
                    {
                        try
                        {
                            // 读取操作
                            var readUsers = collection.FindAll().Take(10).ToList();
                            lock (results)
                            {
                                results.Add($"Task_{taskId}_Read_{readUsers.Count}");
                                operationCounts["Read"]++;
                            }

                            // 写入操作
                            var newUser = new UserWithIntId
                            {
                                Name = $"ConcurrentUser_{taskId}",
                                Age = 25 + (taskId % 30)
                            };
                            var insertedId = collection.Insert(newUser);
                            lock (results)
                            {
                                results.Add($"Task_{taskId}_Write_{insertedId}");
                                operationCounts["Write"]++;
                            }

                            // 更新操作
                            if (taskId < baseRecordCount)
                            {
                                var userToUpdate = collection.FindById(taskId + 1);
                                if (userToUpdate != null)
                                {
                                    var oldAge = userToUpdate.Age;
                                    userToUpdate.Age = userToUpdate.Age + 1;
                                    collection.Update(userToUpdate);
                                    lock (results)
                                    {
                                        results.Add($"Task_{taskId}_Update_{userToUpdate.Id}_{oldAge}_{userToUpdate.Age}");
                                        operationCounts["Update"]++;
                                    }
                                }
                                else
                                {
                                    lock (results)
                                    {
                                        results.Add($"Task_{taskId}_UpdateFailed_UserNotFound");
                                        operationCounts["UpdateFailed"]++;
                                    }
                                }
                            }
                            else
                            {
                                lock (results)
                                {
                                    results.Add($"Task_{taskId}_UpdateSkipped_TaskIdTooHigh");
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            lock (exceptions)
                            {
                                exceptions.Add(ex);
                                Console.WriteLine($"\n任务 {taskId} 异常: {ex.Message}");
                            }
                        }
                    }));
                }

                await Task.WhenAll(tasks);
                concurrentStopwatch.Stop();

                Console.WriteLine($"\n=== 并发操作结果分析 ===");
                Console.WriteLine($"并发操作耗时: {concurrentStopwatch.Elapsed.TotalSeconds:F2}s");
                Console.WriteLine($"异常数: {exceptions.Count}");

                if (exceptions.Count > 0)
                {
                    Console.WriteLine("\n异常详情:");
                    foreach (var ex in exceptions.Take(3))
                    {
                        Console.WriteLine($"  - {ex.GetType().Name}: {ex.Message}");
                    }
                }

                Console.WriteLine("\n操作统计:");
                foreach (var kvp in operationCounts)
                {
                    Console.WriteLine($"  {kvp.Key}: {kvp.Value}");
                }

                Console.WriteLine($"\n结果记录数: {results.Count}");
                Console.WriteLine($"期望结果数: {concurrentOperations * 3}");
                Console.WriteLine($"匹配状态: {(results.Count == concurrentOperations * 3 ? "✅ 匹配" : "❌ 不匹配")}");

                // 分析结果类型
                var readResults = results.Count(r => r.Contains("_Read_"));
                var writeResults = results.Count(r => r.Contains("_Write_"));
                var updateResults = results.Count(r => r.Contains("_Update_") && !r.Contains("UpdateFailed") && !r.Contains("UpdateSkipped"));
                var updateFailedResults = results.Count(r => r.Contains("UpdateFailed"));
                var updateSkippedResults = results.Count(r => r.Contains("UpdateSkipped"));

                Console.WriteLine($"\n结果类型分析:");
                Console.WriteLine($"  读取操作结果: {readResults}");
                Console.WriteLine($"  写入操作结果: {writeResults}");
                Console.WriteLine($"  更新操作成功: {updateResults}");
                Console.WriteLine($"  更新操作失败: {updateFailedResults}");
                Console.WriteLine($"  更新操作跳过: {updateSkippedResults}");

                // 验证数据一致性
                var finalUserCount = collection.FindAll().Count();
                var expectedFinalCount = baseRecordCount + concurrentOperations; // 基础数据 + 新写入

                Console.WriteLine($"\n=== 数据一致性验证 ===");
                Console.WriteLine($"最终记录数: {finalUserCount}");
                Console.WriteLine($"期望记录数: {expectedFinalCount}");
                Console.WriteLine($"数据一致性: {(finalUserCount == expectedFinalCount ? "✅ 一致" : "❌ 不一致")}");

                // 验证更新操作
                var updatedUser = collection.FindById(1);
                if (updatedUser != null)
                {
                    var expectedAge = 20 + 1; // 初始年龄20 + 至少一次更新
                    Console.WriteLine($"用户1年龄: {updatedUser.Age} (期望 > {expectedAge})");
                    Console.WriteLine($"更新验证: {(updatedUser.Age > expectedAge ? "✅ 已更新" : "❌ 未更新或更新不足")}");
                }

                // 检查更新操作的具体情况
                Console.WriteLine($"\n=== 更新操作详细分析 ===");
                for (int i = 0; i < Math.Min(10, concurrentOperations); i++)
                {
                    var user = collection.FindById(i + 1);
                    if (user != null)
                    {
                        var expectedBaseAge = 20 + ((i + 1) % 50);
                        Console.WriteLine($"用户 {i + 1}: 年龄 {user.Age} (基础: {expectedBaseAge}, 更新: {(user.Age > expectedBaseAge ? "是" : "否")})");
                    }
                }

                // 尝试找出问题根源
                if (results.Count < concurrentOperations * 3)
                {
                    var missing = (concurrentOperations * 3) - results.Count;
                    Console.WriteLine($"\n=== 问题分析 ===");
                    Console.WriteLine($"缺失 {missing} 个操作结果");

                    if (missing == concurrentOperations)
                    {
                        Console.WriteLine("🔍 分析: 可能所有更新操作都没有成功记录结果");
                        Console.WriteLine("   原因可能是:");
                        Console.WriteLine("   1. FindById 返回 null");
                        Console.WriteLine("   2. Update 操作抛出异常");
                        Console.WriteLine("   3. Update 操作没有真正执行");
                    }
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
}