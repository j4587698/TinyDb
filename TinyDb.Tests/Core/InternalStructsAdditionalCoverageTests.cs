using System.Linq;
using TinyDb.Bson;
using TinyDb.Core;
using TinyDb.Index;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Core;

public class InternalStructsAdditionalCoverageTests
{
    [Test]
    public async Task CollectionState_CacheInitialization_ShouldToggle()
    {
        var state = new CollectionState();

        await Assert.That(state.IsCacheInitialized).IsFalse();

        state.MarkCacheInitialized();

        await Assert.That(state.IsCacheInitialized).IsTrue();
    }

    [Test]
    public async Task MemoryDocumentIndex_GetAll_ShouldExposeEntries()
    {
        IDocumentIndex index = new MemoryDocumentIndex();
        index.Set("a", new DocumentLocation(1, 2));

        var all = index.GetAll().ToList();

        await Assert.That(all.Any(kvp => kvp.Key == "a")).IsTrue();
    }

    [Test]
    public async Task DocumentLocation_IsEmpty_ShouldWork()
    {
        await Assert.That(DocumentLocation.Empty.IsEmpty).IsTrue();
        await Assert.That(new DocumentLocation(1, 1).IsEmpty).IsFalse();
    }

    [Test]
    public async Task PageDocumentEntry_LargeDocumentSize_ShouldBeExposed()
    {
        var doc = new BsonDocument().Set("_id", 1);
        var entry = new PageDocumentEntry(doc, new byte[] { 1 }, isLargeDocument: true, largeDocumentIndexPageId: 123, largeDocumentSize: 456);

        await Assert.That(entry.LargeDocumentSize).IsEqualTo(456);
    }

    [Test]
    public async Task IndexStatistics_RootIsLeaf_ShouldBeStored()
    {
        var stats = new IndexStatistics { Name = "idx", RootIsLeaf = true };

        await Assert.That(stats.RootIsLeaf).IsTrue();
    }
}

