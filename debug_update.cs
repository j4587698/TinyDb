using System;
using System.IO;
using SimpleDb.Core;
using SimpleDb.Collections;
using SimpleDb.Bson;

public class DebugUpdate
{
    public static void Main()
    {
        var testFile = Path.Combine(Path.GetTempPath(), "debug_update.db");
        if (File.Exists(testFile)) File.Delete(testFile);

        try
        {
            Console.WriteLine("Testing Update functionality...");

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

            Console.WriteLine($"1. Inserting user: Id={user.Id}, Name={user.Name}, Age={user.Age}");
            var insertedId = collection.Insert(user);
            Console.WriteLine($"   Inserted with ID: {insertedId}");

            // 2. 查找插入的用户
            var foundUser = collection.FindById(insertedId);
            Console.WriteLine($"2. Found user: {(foundUser != null ? $"Id={foundUser.Id}, Name={foundUser.Name}, Age={foundUser.Age}" : "null")}");

            if (foundUser != null)
            {
                // 3. 更新用户 (使用原始对象)
                foundUser.Name = "Updated Name";
                foundUser.Age = 26;
                Console.WriteLine($"3. Updating user: Id={foundUser.Id}, Name={foundUser.Name}, Age={foundUser.Age}");

                var updateCount = collection.Update(foundUser);
                Console.WriteLine($"   Update count: {updateCount}");

                // 4. 再次查找用户
                var updatedUser = collection.FindById(insertedId);
                Console.WriteLine($"4. Updated user: {(updatedUser != null ? $"Id={updatedUser.Id}, Name={updatedUser.Name}, Age={updatedUser.Age}" : "null")}");

                if (updatedUser == null)
                {
                    Console.WriteLine("ERROR: Updated user is null! Checking raw documents...");

                    // 检查原始文档
                    var allDocs = engine.FindAll("DebugUsers");
                    int docCount = 0;
                    foreach (var doc in allDocs)
                    {
                        docCount++;
                        Console.WriteLine($"   Raw doc {docCount}: {doc}");
                    }
                    Console.WriteLine($"   Total documents: {docCount}");
                }
                else
                {
                    Console.WriteLine("SUCCESS: User updated and found successfully!");
                    Console.WriteLine($"   Name: {updatedUser.Name}");
                    Console.WriteLine($"   Age: {updatedUser.Age}");
                    Console.WriteLine($"   Email: {updatedUser.Email}");
                }
            }
            else
            {
                Console.WriteLine("ERROR: Could not find inserted user!");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Test failed: {ex.GetType().Name}: {ex.Message}");
            Console.WriteLine($"Stack trace: {ex.StackTrace}");
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