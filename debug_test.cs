using System;
using System.IO;
using SimpleDb.Core;
using SimpleDb.Collections;
using SimpleDb.Bson;

namespace SimpleDb.Tests.Debug;

public class DebugCollectionTests
{
    public void TestUpdateIssue()
    {
        var testFile = Path.Combine(Path.GetTempPath(), "debug_test.db");
        if (File.Exists(testFile)) File.Delete(testFile);

        try
        {
            using var engine = new SimpleDbEngine(testFile);
            var collection = engine.GetCollection<DebugUser>("TestUsers");

            // 1. 插入文档
            var user = new DebugUser { Id = ObjectId.NewObjectId(), Name = "Original", Age = 25 };
            Console.WriteLine($"Inserting user: Id={user.Id}, Name={user.Name}, Age={user.Age}");
            var insertedId = collection.Insert(user);
            Console.WriteLine($"Inserted with ID: {insertedId}");

            // 2. 查找插入的文档
            var foundUser = collection.FindById(insertedId);
            Console.WriteLine($"Found user: Id={foundUser?.Id}, Name={foundUser?.Name}, Age={foundUser?.Age}");

            // 3. 更新文档
            user.Name = "Updated";
            user.Age = 26;
            Console.WriteLine($"Updating user: Id={user.Id}, Name={user.Name}, Age={user.Age}");
            var updateCount = collection.Update(user);
            Console.WriteLine($"Update count: {updateCount}");

            // 4. 再次查找文档
            var updatedUser = collection.FindById(insertedId);
            Console.WriteLine($"Updated user: Id={updatedUser?.Id}, Name={updatedUser?.Name}, Age={updatedUser?.Age}");

            // 5. 检查所有文档
            Console.WriteLine("All documents in collection:");
            foreach (var doc in collection.FindAll())
            {
                Console.WriteLine($"  - Id={doc.Id}, Name={doc.Name}, Age={doc.Age}");
            }
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
}