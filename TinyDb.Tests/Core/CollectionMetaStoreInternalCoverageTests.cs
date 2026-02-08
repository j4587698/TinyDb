using System;
using System.IO;
using System.Linq;
using TinyDb.Bson;
using TinyDb.Core;
using TinyDb.Serialization;
using TinyDb.Storage;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Core;

public sealed class CollectionMetaStoreInternalCoverageTests
{
    [Test]
    public async Task LoadCollections_WhenPageIdIsZero_ShouldReturnWithoutError()
    {
        using var ctx = new MetaStoreTestContext();
        ctx.MetaStore.LoadCollections();

        await Assert.That(ctx.MetaStore.GetCollectionNames()).IsEmpty();
    }

    [Test]
    public async Task LoadCollections_WhenLegacyStringValuesPresent_ShouldCreateEmptyMetadata()
    {
        using var ctx = new MetaStoreTestContext();
        var page = ctx.PageManager.NewPage(PageType.Collection);
        ctx.CollectionPageId = page.PageID;

        var legacy = new BsonDocument().Set("users", new BsonString("users"));
        var bytes = BsonSerializer.SerializeDocument(legacy);

        page.WriteData(247, bytes);
        ctx.PageManager.SavePage(page, forceFlush: true);

        ctx.MetaStore.LoadCollections();

        await Assert.That(ctx.MetaStore.IsKnown("users")).IsTrue();
        await Assert.That(ctx.MetaStore.GetMetadata("users").Count()).IsEqualTo(0);
    }

    [Test]
    public async Task LoadCollections_WhenStoredBytesInvalid_ShouldSwallowDeserializeError()
    {
        using var ctx = new MetaStoreTestContext();
        var page = ctx.PageManager.NewPage(PageType.Collection);
        ctx.CollectionPageId = page.PageID;

        var invalid = new byte[] { 8, 0, 0, 0, 1, 2, 3, 4 };
        page.WriteData(247, invalid);
        ctx.PageManager.SavePage(page, forceFlush: true);

        ctx.MetaStore.LoadCollections();

        await Assert.That(ctx.MetaStore.GetCollectionNames()).IsEmpty();
    }

    [Test]
    public async Task LoadCollections_WhenPageMissing_ShouldSwallowPageReadError()
    {
        using var ctx = new MetaStoreTestContext();
        ctx.CollectionPageId = 123456;

        ctx.MetaStore.LoadCollections();

        await Assert.That(ctx.MetaStore.GetCollectionNames()).IsEmpty();
    }

    [Test]
    public async Task LoadCollections_WhenPageManagerIsNull_ShouldSwallowOuterErrors()
    {
        var metaStore = new CollectionMetaStore(null!, () => 1u, _ => { });

        metaStore.LoadCollections();

        await Assert.That(metaStore.GetCollectionNames()).IsEmpty();
    }

    [Test]
    public async Task SaveCollections_WhenNoPageAllocated_ShouldCreateCollectionPageId()
    {
        using var ctx = new MetaStoreTestContext();

        ctx.MetaStore.RegisterCollection("alpha", forceFlush: true);

        await Assert.That(ctx.CollectionPageId).IsNotEqualTo(0u);
        await Assert.That(ctx.MetaStore.IsKnown("alpha")).IsTrue();
    }

    [Test]
    public async Task SaveCollections_WhenNoMetadataAndNoPageId_ShouldReturnWithoutAllocating()
    {
        using var ctx = new MetaStoreTestContext();

        ctx.MetaStore.SaveCollections(forceFlush: true);

        await Assert.That(ctx.CollectionPageId).IsEqualTo(0u);
    }

    [Test]
    public async Task SaveCollections_WhenMetadataTooLarge_ShouldThrow()
    {
        using var ctx = new MetaStoreTestContext();

        var huge = new string('x', 9000);
        var metadata = new BsonDocument().Set("payload", new BsonString(huge));

        await Assert.That(() => ctx.MetaStore.UpdateMetadata("big", metadata, forceFlush: true))
            .ThrowsExactly<InvalidOperationException>();
    }

    private sealed class MetaStoreTestContext : IDisposable
    {
        private readonly string _dbFile;
        private readonly DiskStream _diskStream;

        public uint CollectionPageId;
        public PageManager PageManager { get; }
        public CollectionMetaStore MetaStore { get; }

        public MetaStoreTestContext()
        {
            _dbFile = Path.Combine(Path.GetTempPath(), $"meta_store_internal_{Guid.NewGuid():N}.db");
            _diskStream = new DiskStream(_dbFile);
            PageManager = new PageManager(_diskStream);
            MetaStore = new CollectionMetaStore(PageManager, () => CollectionPageId, v => CollectionPageId = v);
        }

        public void Dispose()
        {
            PageManager.Dispose();
            _diskStream.Dispose();
            try { if (File.Exists(_dbFile)) File.Delete(_dbFile); } catch { }
        }
    }
}
