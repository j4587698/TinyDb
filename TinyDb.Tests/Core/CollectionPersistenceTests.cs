using System.Diagnostics.CodeAnalysis;
using System.Linq;
using TinyDb.Core;
using TinyDb.Collections;
using TinyDb.Tests.TestEntities;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Core;

[NotInParallel]
public class CollectionPersistenceTests
{
    private string _testDbPath = null!;

    [Before(Test)]
    public void Setup()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"collection_persistence_{Guid.NewGuid():N}.db");
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
    public async Task Collection_Persistence_Should_Work_Multiple_Reopen_Cycles()
    {
        const string collectionName = "persistent_test";

        // 创建集合并插入数据
        using (var engine = new TinyDbEngine(_testDbPath))
        {
            var collection = engine.GetCollectionWithName<User>(collectionName);
            collection.Insert(new User { Name = "Persistent User", Age = 30, Email = "persistent@example.com" });
            engine.Flush();
        }

        // 多次重新打开验证
        for (int cycle = 0; cycle < 3; cycle++)
        {
            using (var engine = new TinyDbEngine(_testDbPath))
            {
                var collections = engine.GetCollectionNames().ToList();
                await Assert.That(collections.Contains(collectionName)).IsTrue();

                var collection = engine.GetCollectionWithName<User>(collectionName);
                var count = collection.Count();
                await Assert.That(count).IsEqualTo(1);
            }
        }
    }

    [Test]
    public async Task DropCollection_Should_Persist_Across_Sessions()
    {
        const string collectionToDrop = "to_be_dropped";
        const string collectionToKeep = "to_be_kept";

        // 创建两个集合
        using (var engine = new TinyDbEngine(_testDbPath))
        {
            engine.GetCollectionWithName<User>(collectionToDrop);
            engine.GetCollectionWithName<Product>(collectionToKeep);

            // 插入数据
            engine.GetCollectionWithName<User>(collectionToDrop)
                  .Insert(new User { Name = "Will Be Dropped", Age = 25, Email = "drop@example.com" });

            engine.Flush();

            // 删除一个集合
            var dropResult = engine.DropCollection(collectionToDrop);
            await Assert.That(dropResult).IsTrue();

            engine.Flush();
        }

        // 重新打开验证删除操作持久化
        using (var reopenedEngine = new TinyDbEngine(_testDbPath))
        {
            var collections = reopenedEngine.GetCollectionNames().ToList();
            await Assert.That(collections.Contains(collectionToDrop)).IsFalse();
            await Assert.That(collections.Contains(collectionToKeep)).IsTrue();
            await Assert.That(collections).Count().IsEqualTo(1);
        }
    }

    [Test]
    public async Task Empty_Collections_Should_Persist()
    {
        const string emptyCollectionName = "empty_collection";

        // 创建空集合
        using (var engine = new TinyDbEngine(_testDbPath))
        {
            var emptyCollection = engine.GetCollectionWithName<User>(emptyCollectionName);
            await Assert.That(emptyCollection.Count()).IsEqualTo(0);
            engine.Flush();
        }

        // 重新打开验证空集合仍然存在
        using (var reopenedEngine = new TinyDbEngine(_testDbPath))
        {
            var collections = reopenedEngine.GetCollectionNames().ToList();
            await Assert.That(collections.Contains(emptyCollectionName)).IsTrue();

            var emptyCollection = reopenedEngine.GetCollectionWithName<User>(emptyCollectionName);
            await Assert.That(emptyCollection.Count()).IsEqualTo(0);
        }
    }
}