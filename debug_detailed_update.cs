using System;
using System.IO;
using System.Linq;
using SimpleDb.Core;
using SimpleDb.Collections;
using SimpleDb.Bson;

public class DebugDetailedUpdate
{
    public static void Main()
    {
        var testFile = Path.Combine(Path.GetTempPath(), "debug_detailed_update.db");
        if (File.Exists(testFile)) File.Delete(testFile);

        try
        {
            Console.WriteLine("=== 详细调试Update功能 ===");

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

            // 2. 查看原始数据库状态
            Console.WriteLine("\n2. 数据库状态（插入后）:");
            var allDocsBefore = engine.FindAll("DebugUsers").ToList();
            Console.WriteLine($"   文档总数: {allDocsBefore.Count}");
            foreach (var doc in allDocsBefore)
            {
                Console.WriteLine($"   - ID: {doc["_id"]}, Name: {doc["Name"]}, Age: {doc["Age"]}, Collection: {doc["_collection"]}");
            }

            // 3. 查找插入的用户
            var foundUser = collection.FindById(insertedId);
            Console.WriteLine($"\n3. 查找结果: {(foundUser != null ? $"找到 - Name={foundUser.Name}, Age={foundUser.Age}" : "未找到")}");

            if (foundUser != null)
            {
                // 4. 更新用户
                foundUser.Name = "Updated Name";
                foundUser.Age = 26;
                Console.WriteLine($"\n4. 更新用户: Id={foundUser.Id}, Name={foundUser.Name}, Age={foundUser.Age}");

                var updateCount = collection.Update(foundUser);
                Console.WriteLine($"   更新操作返回: {updateCount}");

                // 5. 查看更新后数据库状态
                Console.WriteLine("\n5. 数据库状态（更新后）:");
                var allDocsAfter = engine.FindAll("DebugUsers").ToList();
                Console.WriteLine($"   文档总数: {allDocsAfter.Count}");
                foreach (var doc in allDocsAfter)
                {
                    Console.WriteLine($"   - ID: {doc["_id"]}, Name: {doc["Name"]}, Age: {doc["Age"]}, Collection: {doc["_collection"]}");
                }

                // 6. 再次查找用户
                var updatedUser = collection.FindById(insertedId);
                Console.WriteLine($"\n6. 最终查找结果: {(updatedUser != null ? $"找到 - Name={updatedUser.Name}, Age={updatedUser.Age}" : "未找到")}");

                if (updatedUser == null)
                {
                    Console.WriteLine("\n❌ 错误：更新后的用户无法找到！");
                    Console.WriteLine("   可能的原因：");
                    Console.WriteLine("   1. Update操作没有正确保存数据");
                    Console.WriteLine("   2. FindById方法有问题");
                    Console.WriteLine("   3. 页面缓存不一致");

                    // 尝试直接从引擎查找
                    Console.WriteLine("\n7. 尝试直接从引擎查找:");
                    var engineDoc = engine.FindById("DebugUsers", insertedId);
                    if (engineDoc != null)
                    {
                        Console.WriteLine($"   引擎找到: Name={engineDoc["Name"]}, Age={engineDoc["Age"]}");
                    }
                    else
                    {
                        Console.WriteLine("   引擎也未找到");
                    }
                }
                else
                {
                    Console.WriteLine("\n✅ 成功：用户更新并正确找到！");
                    Console.WriteLine($"   最终Name: {updatedUser.Name}");
                    Console.WriteLine($"   最终Age: {updatedUser.Age}");
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"❌ 测试失败: {ex.GetType().Name}: {ex.Message}");
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