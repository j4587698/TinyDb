using System;
using System.Globalization;
using System.IO;
using System.Linq;
using TinyDb.Core;
using TinyDb.IdGeneration;
using TinyDb.Index;
using TinyDb.Tests.TestEntities;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Integration;

[NotInParallel]
public class EndToEndDatabaseTests
{
    private string _databasePath = null!;
    private TinyDbOptions _options = null!;

    [Before(Test)]
    public void Setup()
    {
        _databasePath = Path.Combine(Path.GetTempPath(), $"tinydb_e2e_{Guid.NewGuid():N}.db");
        if (File.Exists(_databasePath))
            File.Delete(_databasePath);

        _options = new TinyDbOptions
        {
            DatabaseName = "TinyDbEndToEnd",
            PageSize = 4096,
            CacheSize = 256,
            EnableJournaling = true
        };

        IdentitySequences.ResetAll();
    }

    [After(Test)]
    public void Cleanup()
    {
        if (File.Exists(_databasePath))
            File.Delete(_databasePath);
    }

    /// <summary>
    /// 端到端校验：事务插入 + 索引查询 + 重新打开数据库验证持久化
    /// </summary>
    [Test]
    public async Task OrderLifecycle_ShouldPersistAcrossSessions_WithIndexes()
    {
        using (var engine = new TinyDbEngine(_databasePath, _options))
        {
            var collection = engine.GetCollection<Order>("orders");

            var indexCreated = engine.EnsureIndex("orders", "OrderNumber", "order_number_idx", unique: true);
            await Assert.That(indexCreated).IsTrue();

            using (var transaction = engine.BeginTransaction())
            {
                var orders = new[]
                {
                    new Order { OrderNumber = "SO-1001", TotalAmount = 120.75m, IsCompleted = false, OrderDate = DateTime.Parse("2024-05-01", CultureInfo.InvariantCulture) },
                    new Order { OrderNumber = "SO-1002", TotalAmount = 89.99m, IsCompleted = true, OrderDate = DateTime.Parse("2024-05-03", CultureInfo.InvariantCulture) },
                    new Order { OrderNumber = "SO-1003", TotalAmount = 230.50m, IsCompleted = false, OrderDate = DateTime.Parse("2024-05-10", CultureInfo.InvariantCulture) }
                };

                foreach (var order in orders)
                {
                    collection.Insert(order);
                }

                transaction.Commit();
            }

            var indexManager = engine.GetIndexManager("orders");
            var index = indexManager.GetIndex("order_number_idx");
            await Assert.That(index).IsNotNull();

            var lookupKey = new IndexKey(new BsonValue[] { new BsonString("SO-1002") });
            var locatedDocumentId = index!.FindExact(lookupKey);
            await Assert.That(locatedDocumentId).IsNotNull();

            var resolvedId = ((BsonObjectId)locatedDocumentId!).Value;
            var persistedOrder = collection.FindById(resolvedId);
            await Assert.That(persistedOrder).IsNotNull();
            await Assert.That(persistedOrder!.TotalAmount).IsEqualTo(89.99m);
            await Assert.That(persistedOrder.IsCompleted).IsTrue();
        }

        using (var reopenedEngine = new TinyDbEngine(_databasePath, _options))
        {
            var collection = reopenedEngine.GetCollection<Order>("orders");
            var allOrders = collection.FindAll().ToList();

            await Assert.That(allOrders).HasCount(3);
            await Assert.That(allOrders.Any(o => o.OrderNumber == "SO-1003")).IsTrue();
        }
    }
}
