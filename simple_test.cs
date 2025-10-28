using System;
using System.IO;
using SimpleDb.Core;
using SimpleDb.Collections;
using SimpleDb.Bson;

public class SimpleTest
{
    public static void Main()
    {
        var testFile = Path.Combine(Path.GetTempPath(), "simple_test.db");
        if (File.Exists(testFile)) File.Delete(testFile);

        try
        {
            Console.WriteLine("Starting simple test...");

            using var engine = new SimpleDbEngine(testFile);
            var collection = engine.GetCollection<SimpleUser>("Users");

            // 插入用户
            var user = new SimpleUser { Id = ObjectId.NewObjectId(), Name = "Original", Age = 25 };
            Console.WriteLine($"Inserting user: Id={user.Id}, Name={user.Name}, Age={user.Age}");
            var insertedId = collection.Insert(user);
            Console.WriteLine($"Inserted with ID: {insertedId}");

            // 查找插入的用户
            var foundUser = collection.FindById(insertedId);
            Console.WriteLine($"Found user: {(foundUser != null ? $"Id={foundUser.Id}, Name={foundUser.Name}, Age={foundUser.Age}" : "null")}");

            if (foundUser != null)
            {
                // 更新用户
                foundUser.Name = "Updated";
                foundUser.Age = 26;
                Console.WriteLine($"Updating user: Id={foundUser.Id}, Name={foundUser.Name}, Age={foundUser.Age}");
                var updateCount = collection.Update(foundUser);
                Console.WriteLine($"Update count: {updateCount}");

                // 再次查找用户
                var updatedUser = collection.FindById(insertedId);
                Console.WriteLine($"Updated user: {(updatedUser != null ? $"Id={updatedUser.Id}, Name={updatedUser.Name}, Age={updatedUser.Age}" : "null")}");

                if (updatedUser == null)
                {
                    Console.WriteLine("ERROR: Updated user is null!");
                }
                else
                {
                    Console.WriteLine("SUCCESS: User updated and found successfully!");
                }
            }
            else
            {
                Console.WriteLine("ERROR: Could not find inserted user!");
            }

            Console.WriteLine("Test completed.");
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

public class SimpleUser
{
    public ObjectId Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public int Age { get; set; }
}