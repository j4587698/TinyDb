using System;
using System.Linq;
using TinyDb.Bson;
using TinyDb.Index;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Index;

public class IndexAdvancedTests
{
    [Test]
    public async Task IndexManager_Should_Handle_Lifecycle()
    {
        using var manager = new IndexManager("testCol");
        
        await Assert.That(manager.CreateIndex("idx1", new[] { "field1" })).IsTrue();
        await Assert.That(manager.IndexExists("idx1")).IsTrue();
        await Assert.That(manager.IndexCount).IsEqualTo(1);
        
        var index = manager.GetIndex("idx1");
        await Assert.That(index).IsNotNull();
        await Assert.That(index!.Name).IsEqualTo("idx1");
        
        await Assert.That(manager.DropIndex("idx1")).IsTrue();
        await Assert.That(manager.IndexExists("idx1")).IsFalse();
        await Assert.That(manager.IndexCount).IsEqualTo(0);
    }

    [Test]
    public async Task BTreeIndex_Clear_Should_Work()
    {
        using var index = new BTreeIndex("idx", new[] { "f" });
        index.Insert(new IndexKey(new BsonInt32(1)), new BsonInt32(100));
        
        await Assert.That(index.EntryCount).IsEqualTo(1);
        
        index.Clear();
        await Assert.That(index.EntryCount).IsEqualTo(0);
        await Assert.That(index.NodeCount).IsEqualTo(1);
    }

    [Test]
    public async Task IndexManager_GetBestIndex_ShouldWork()
    {
        using var manager = new IndexManager("testCol");
        manager.CreateIndex("idx_a", new[] { "a" });
        manager.CreateIndex("idx_ab", new[] { "a", "b" });
        
        var best = manager.GetBestIndex(new[] { "a", "b" });
        await Assert.That(best!.Name).IsEqualTo("idx_ab");
        
        var bestA = manager.GetBestIndex(new[] { "a" });
        // According to current scoring logic: score = (count - i) * 10
        // idx_a: (1 - 0) * 10 = 10
        // idx_ab: (2 - 0) * 10 = 20
        await Assert.That(bestA!.Name).IsEqualTo("idx_ab");
    }

    [Test]
    public async Task BTreeIndex_GetStatistics_ShouldWork()
    {
        using var index = new BTreeIndex("idx", new[] { "f" });
        index.Insert(new IndexKey(new BsonInt32(1)), new BsonInt32(100));
        
        var stats = index.GetStatistics();
        await Assert.That(stats.Name).IsEqualTo("idx");
        await Assert.That(stats.EntryCount).IsEqualTo(1);
        await Assert.That(stats.TreeHeight).IsGreaterThanOrEqualTo(1);
    }
}
