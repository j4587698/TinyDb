using System;
using System.IO;
using SimpleDb.Core;
using SimpleDb.Collections;
using SimpleDb.Bson;

namespace DebugUpdateIssue
{
    public class Program
    {
        public static void Main()
        {
            Console.WriteLine("=== 调试Update问题 ===");

            var testFile = Path.Combine(Path.GetTempPath(), "debug_update.db");
            if (File.Exists(testFile)) File.Delete(testFile);

            try
            {
                using var engine = new SimpleDbEngine(testFile);
                var collection = engine.GetCollection<DebugUser>("DebugUsers");

                // 1. 插入用户
                var user = new DebugUser
                {
                    Id = ObjectId.NewObjectId(),
                    Name = "Original Name",
                    Age = 25,
                    Email = "original@example.com"
                };

                Console.WriteLine($"1. 插入用户: Id={user.Id}, Name={user.Name}");
                var insertedId = collection.Insert(user);
                Console.WriteLine($"   插入成功，ID: {insertedId}");

                // 2. 立即查找插入的文档
                var foundBeforeUpdate = collection.FindById(insertedId);
                Console.WriteLine($"2. Update前查找: {(foundBeforeUpdate != null ? $"找到 - Name={foundBeforeUpdate.Name}" : "未找到")}");

                if (foundBeforeUpdate != null)
                {
                    // 4. 更新用户
                    foundBeforeUpdate.Name = "Updated Name";
                    foundBeforeUpdate.Age = 26;
                    Console.WriteLine($"\n4. 更新用户: Name={foundBeforeUpdate.Name}, Age={foundBeforeUpdate.Age}");

                    var updateCount = collection.Update(foundBeforeUpdate);
                    Console.WriteLine($"   更新操作返回: {updateCount}");

                    // 6. 再次查找用户
                    var foundAfterUpdate = collection.FindById(insertedId);
                    Console.WriteLine($"\n6. Update后查找: {(foundAfterUpdate != null ? $"找到 - Name={foundAfterUpdate.Name}" : "未找到")}");

                    if (foundAfterUpdate == null)
                    {
                        Console.WriteLine("\n❌ 问题确认：Update后无法找到文档！");
                        Console.WriteLine("可能的原因:");
                        Console.WriteLine("1. Update破坏了BSON格式");
                        Console.WriteLine("2. _collection字段丢失");
                        Console.WriteLine("3. 页面数据结构损坏");
                    }
                    else
                    {
                        Console.WriteLine("\n✅ Update成功，文档可以正常找到");
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"\n❌ 测试失败: {ex.GetType().Name}: {ex.Message}");
                Console.WriteLine($"堆栈跟踪: {ex.StackTrace}");
            }
            finally
            {
                if (File.Exists(testFile)) File.Delete(testFile);
            }
        }
    }

    public class DebugUser
    {
        public ObjectId Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public int Age { get; set; }
        public string Email { get; set; } = string.Empty;
    }
}
