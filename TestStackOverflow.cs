using System;
using System.Linq;
using SimpleDb.Core;
using SimpleDb.Collections;
using SimpleDb.Query;

namespace TestStackOverflow
{
    public class Program
    {
        public static void Main()
        {
            Console.WriteLine("=== 测试栈溢出修复 ===");

            var testFile = Path.Combine(Path.GetTempPath(), "test_stack.db");
            if (File.Exists(testFile)) File.Delete(testFile);

            try
            {
                using var engine = new SimpleDbEngine(testFile);
                var collection = engine.GetCollection<TestUser>("Users");

                // 插入测试数据
                var users = new[]
                {
                    new TestUser { Name = "Alice", Age = 25, Active = true },
                    new TestUser { Name = "Bob", Age = 30, Active = false },
                    new TestUser { Name = "Charlie", Age = 35, Active = true }
                };

                foreach (var user in users)
                {
                    collection.Insert(user);
                }

                // 创建Queryable
                var executor = new QueryExecutor(engine);
                var queryable = new Queryable<TestUser>(executor, "Users");

                Console.WriteLine("测试链式Where调用...");

                // 这应该会导致栈溢出
                var result = queryable
                    .Where(u => u.Active)
                    .Where(u => u.Age > 25)
                    .ToList();

                Console.WriteLine($"结果: {result.Count} 个用户");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"错误: {ex.GetType().Name}: {ex.Message}");
            }
            finally
            {
                if (File.Exists(testFile)) File.Delete(testFile);
            }
        }
    }

    public class TestUser
    {
        public string Name { get; set; } = string.Empty;
        public int Age { get; set; }
        public bool Active { get; set; }
    }
}