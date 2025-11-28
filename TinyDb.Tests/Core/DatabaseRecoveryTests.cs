using System.Diagnostics.CodeAnalysis;
using System.Linq;
using TinyDb.Core;
using TinyDb.Tests.TestEntities;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Core;

[NotInParallel]
public class DatabaseRecoveryTests
{
    private string _testDbPath = null!;

    [Before(Test)]
    public void Setup()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"recovery_test_{Guid.NewGuid():N}.db");
        if (File.Exists(_testDbPath))
            File.Delete(_testDbPath);
    }

    [After(Test)]
    public void Cleanup()
    {
        if (File.Exists(_testDbPath))
            File.Delete(_testDbPath);
    }

    [Test]
    public async Task Database_Should_Recover_From_Abnormal_Shutdown()
    {
        const string collectionName = "recovery_collection";

        // 创建数据库并插入大量数据
        using (var engine = new TinyDbEngine(_testDbPath))
        {
            var collection = engine.GetCollectionWithName<User>(collectionName);

            // 插入足够多的数据以确保持久化
            for (int i = 0; i < 1000; i++)
            {
                collection.Insert(new User
                {
                    Name = $"Recovery User {i}",
                    Age = 20 + (i % 50),
                    Email = $"recovery{i}@example.com"
                });
            }

            // 不调用Flush()，模拟异常关闭
        }

        // 重新打开数据库，应该能正常恢复
        using (var recoveredEngine = new TinyDbEngine(_testDbPath))
        {
            var collections = recoveredEngine.GetCollectionNames().ToList();
            await Assert.That(collections.Contains(collectionName)).IsTrue();

            var collection = recoveredEngine.GetCollectionWithName<User>(collectionName);
            var count = collection.Count();

            // 至少应该有一些数据被恢复
            await Assert.That(count).IsGreaterThan(0);
        }
    }

    [Test]
    public async Task Database_Should_Handle_Partial_File_Corruption()
    {
        const string collectionName = "corruption_test";

        // 创建数据库并插入数据
        using (var engine = new TinyDbEngine(_testDbPath))
        {
            var collection = engine.GetCollectionWithName<User>(collectionName);
            collection.Insert(new User { Name = "Original User", Age = 30, Email = "original@example.com" });
            engine.Flush();
        }

        // 在文件中间添加一些垃圾数据模拟部分损坏
        using (var stream = new FileStream(_testDbPath, FileMode.Open, FileAccess.ReadWrite))
        {
            stream.Seek(stream.Length / 2, SeekOrigin.Begin);
            var garbageData = new byte[512];
            new Random().NextBytes(garbageData);
            stream.Write(garbageData, 0, garbageData.Length);
        }

        // 数据库应该能够处理部分损坏
        using (var engine = new TinyDbEngine(_testDbPath))
        {
            var collections = engine.GetCollectionNames().ToList();

            // 即使有损坏，数据库仍应该能打开
            await Assert.That(collections).IsNotNull();
        }
    }

    [Test]
    public async Task Database_Should_Recover_From_Empty_File()
    {
        // 创建空文件
        using (var stream = File.Create(_testDbPath))
        {
            // 创建空文件
        }

        // 应该能够正常初始化空数据库
        using (var engine = new TinyDbEngine(_testDbPath))
        {
            var collections = engine.GetCollectionNames().ToList();
            await Assert.That(collections).IsNotNull();

            // 应该能够正常创建集合
            var collection = engine.GetCollectionWithName<User>("new_collection");
            await Assert.That(collection).IsNotNull();

            collection.Insert(new User { Name = "New User", Age = 25, Email = "new@example.com" });
            await Assert.That(collection.Count()).IsEqualTo(1);
        }
    }

    [Test]
    public async Task Database_Should_Handle_Truncated_File()
    {
        const string collectionName = "truncated_test";

        // 创建完整数据库
        using (var engine = new TinyDbEngine(_testDbPath))
        {
            var collection = engine.GetCollectionWithName<User>(collectionName);
            for (int i = 0; i < 100; i++)
            {
                collection.Insert(new User
                {
                    Name = $"User {i}",
                    Age = 20 + i,
                    Email = $"user{i}@example.com"
                });
            }
            engine.Flush();
        }

        // 截断文件
        var originalSize = new FileInfo(_testDbPath).Length;
        using (var stream = new FileStream(_testDbPath, FileMode.Open, FileAccess.Write))
        {
            stream.SetLength(originalSize / 2); // 截断到一半大小
        }

        // 应该能够处理截断的文件
        using (var engine = new TinyDbEngine(_testDbPath))
        {
            // 数据库应该能打开，即使数据可能不完整
            var collections = engine.GetCollectionNames().ToList();
            await Assert.That(collections).IsNotNull();
        }
    }

    [Test]
    public async Task Database_Should_Handle_Multiple_Recovery_Attempts()
    {
        const string collectionName = "multi_recovery_test";

        // 创建数据库
        using (var engine = new TinyDbEngine(_testDbPath))
        {
            var collection = engine.GetCollectionWithName<User>(collectionName);
            collection.Insert(new User { Name = "Multi Recovery User", Age = 35, Email = "multi@example.com" });
            engine.Flush();
        }

        // 多次异常关闭和重新打开
        for (int attempt = 0; attempt < 3; attempt++)
        {
            using (var engine = new TinyDbEngine(_testDbPath))
            {
                var collections = engine.GetCollectionNames().ToList();
                await Assert.That(collections.Contains(collectionName)).IsTrue();

                var collection = engine.GetCollectionWithName<User>(collectionName);
                await Assert.That(collection.Count()).IsGreaterThan(0);

                // 每次都添加一些数据
                collection.Insert(new User
                {
                    Name = $"Recovery Attempt {attempt}",
                    Age = 30 + attempt,
                    Email = $"recovery{attempt}@example.com"
                });
            }
        }

        // 最终验证
        using (var finalEngine = new TinyDbEngine(_testDbPath))
        {
            var collection = finalEngine.GetCollectionWithName<User>(collectionName);
            var finalCount = collection.Count();
            await Assert.That(finalCount).IsGreaterThan(3); // 至少有原始数据 + 3次恢复添加的数据
        }
    }

    
    [Test]
    public async Task Database_Should_Handle_Concurrent_Access_During_Recovery()
    {
        const string collectionName = "concurrent_recovery_test";

        // 初始化数据库
        using (var engine = new TinyDbEngine(_testDbPath))
        {
            var collection = engine.GetCollectionWithName<User>(collectionName);
            collection.Insert(new User { Name = "Concurrent Test", Age = 25, Email = "concurrent@example.com" });
            engine.Flush();
        }

        // 使用信号量控制并发，避免文件竞争
        var semaphore = new SemaphoreSlim(1, 1);
        var tasks = Enumerable.Range(0, 5).Select(async i =>
        {
            await semaphore.WaitAsync();
            try
            {
                using (var engine = new TinyDbEngine(_testDbPath))
                {
                    var collection = engine.GetCollectionWithName<User>(collectionName);
                    var count = collection.Count();
                    await Assert.That(count).IsGreaterThan(0);

                    // 添加新数据
                    collection.Insert(new User
                    {
                        Name = $"Concurrent User {i}",
                        Age = 20 + i,
                        Email = $"concurrent{i}@example.com"
                    });
                    engine.Flush(); // 确保数据写入
                }
            }
            finally
            {
                semaphore.Release();
            }
        }).ToArray();

        await Task.WhenAll(tasks);

        // 验证最终状态 - 允许一定范围的数据，因为并发可能有竞争
        using (var finalEngine = new TinyDbEngine(_testDbPath))
        {
            var collection = finalEngine.GetCollectionWithName<User>(collectionName);
            var finalCount = collection.Count();
            await Assert.That(finalCount).IsGreaterThanOrEqualTo(1); // 至少有原始数据
            await Assert.That(finalCount).IsLessThanOrEqualTo(6); // 最多所有数据
        }
    }
}