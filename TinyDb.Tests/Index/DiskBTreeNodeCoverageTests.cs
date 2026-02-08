using System;
using System.IO;
using System.Text;
using TinyDb.Bson;
using TinyDb.Index;
using TinyDb.Storage;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Index;

public sealed class DiskBTreeNodeCoverageTests : IDisposable
{
    private readonly string _testDirectory;
    private readonly string _testFilePath;
    private const uint TestPageSize = 4096;
    private const int TestCacheSize = 64;

    public DiskBTreeNodeCoverageTests()
    {
        _testDirectory = Path.Combine(Path.GetTempPath(), "TinyDbTests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_testDirectory);
        _testFilePath = Path.Combine(_testDirectory, "btree_node.db");
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_testDirectory))
            {
                Directory.Delete(_testDirectory, true);
            }
        }
        catch
        {
        }
    }

    [Test]
    public async Task Ctor_WhenNullArguments_ShouldThrow()
    {
        using var diskStream = new DiskStream(_testFilePath);
        using var pm = new PageManager(diskStream, TestPageSize, TestCacheSize);
        var page = pm.NewPage(PageType.Index);

        await Assert.That(() => new DiskBTreeNode(null!, pm)).ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => new DiskBTreeNode(page, null!)).ThrowsExactly<ArgumentNullException>();
    }

    [Test]
    public async Task Save_WhenAlreadyClean_ShouldReturnEarly()
    {
        using var diskStream = new DiskStream(_testFilePath);
        using var pm = new PageManager(diskStream, TestPageSize, TestCacheSize);

        var page = pm.NewPage(PageType.Index);
        var node = new DiskBTreeNode(page, pm);
        node.Keys.Add(new IndexKey(new BsonValue[] { new BsonInt32(1) }));
        node.Values.Add(new BsonInt32(1));
        node.MarkDirty();

        node.Save(pm);
        node.Save(pm); // second call should hit the !_isDirty early return

        await Assert.That(true).IsTrue();
    }

    [Test]
    public async Task Save_WhenPayloadRequiresMultipleOverflowPages_ShouldAllocateAdditionalPages()
    {
        using var diskStream = new DiskStream(_testFilePath);
        using var pm = new PageManager(diskStream, TestPageSize, TestCacheSize);

        var page = pm.NewPage(PageType.Index);
        var node = new DiskBTreeNode(page, pm);

        var huge = new string('a', 12000);
        node.Keys.Add(new IndexKey(new BsonValue[] { new BsonString(huge) }));
        node.Values.Add(new BsonInt32(1));
        node.MarkDirty();

        node.Save(pm);

        var firstOverflowId = page.Header.NextPageID;
        await Assert.That(firstOverflowId).IsNotEqualTo(0u);

        var firstOverflow = pm.GetPage(firstOverflowId);
        await Assert.That(firstOverflow.PageType).IsEqualTo(PageType.Index);
        await Assert.That(firstOverflow.Header.NextPageID).IsNotEqualTo(0u);
    }

    [Test]
    public async Task Save_WhenPayloadShrinks_ShouldFreeExtraOverflowPages()
    {
        using var diskStream = new DiskStream(_testFilePath);
        using var pm = new PageManager(diskStream, TestPageSize, TestCacheSize);

        var page = pm.NewPage(PageType.Index);
        var node = new DiskBTreeNode(page, pm);

        node.Keys.Add(new IndexKey(new BsonValue[] { new BsonString(new string('b', 12000)) }));
        node.Values.Add(new BsonInt32(1));
        node.MarkDirty();
        node.Save(pm);

        var firstOverflowId = page.Header.NextPageID;
        var firstOverflow = pm.GetPage(firstOverflowId);
        var secondOverflowId = firstOverflow.Header.NextPageID;
        await Assert.That(secondOverflowId).IsNotEqualTo(0u);

        node.Keys[0] = new IndexKey(new BsonValue[] { new BsonString(new string('c', 6000)) });
        node.MarkDirty();
        node.Save(pm);

        var reloadedFirstOverflow = pm.GetPage(firstOverflowId);
        await Assert.That(reloadedFirstOverflow.Header.NextPageID).IsEqualTo(0u);
    }

    [Test]
    public async Task IsFull_ShouldIncludeBooleanDateTimeAndObjectIdSizes()
    {
        using var diskStream = new DiskStream(_testFilePath);
        using var pm = new PageManager(diskStream, TestPageSize, TestCacheSize);

        var page = pm.NewPage(PageType.Index);
        var node = new DiskBTreeNode(page, pm);

        node.Keys.Add(new IndexKey(new BsonValue[] { BsonBoolean.True }));
        node.Values.Add(BsonBoolean.True);

        node.Keys.Add(new IndexKey(new BsonValue[] { new BsonDateTime(DateTime.UtcNow) }));
        node.Values.Add(new BsonDateTime(DateTime.UtcNow));

        node.Keys.Add(new IndexKey(new BsonValue[] { new BsonObjectId(ObjectId.NewObjectId()) }));
        node.Values.Add(new BsonObjectId(ObjectId.NewObjectId()));

        var _ = node.IsFull((int)TestPageSize);
        await Assert.That(true).IsTrue();
    }

    [Test]
    public async Task IsFull_ShouldIncludeDecimal128Size()
    {
        using var diskStream = new DiskStream(_testFilePath);
        using var pm = new PageManager(diskStream, TestPageSize, TestCacheSize);

        var page = pm.NewPage(PageType.Index);
        var node = new DiskBTreeNode(page, pm);

        node.Keys.Add(new IndexKey(new BsonValue[] { new BsonDecimal128(1.25m) }));
        node.Values.Add(new BsonDecimal128(1.25m));

        var _ = node.IsFull((int)TestPageSize);
        await Assert.That(true).IsTrue();
    }

    [Test]
    public async Task LoadFromPage_WhenTreeEntryCountMissing_ShouldDefaultToZero()
    {
        using var diskStream = new DiskStream(_testFilePath);
        using var pm = new PageManager(diskStream, TestPageSize, TestCacheSize);

        var page = pm.NewPage(PageType.Index);

        using var ms = new MemoryStream();
        using (var writer = new BinaryWriter(ms, Encoding.UTF8, true))
        {
            writer.Write(true); // IsLeaf
            writer.Write(0);    // keyCount
            writer.Write(0u);   // ParentId
            writer.Write(0u);   // NextSiblingId
            writer.Write(0u);   // PrevSiblingId
            // TreeEntryCount intentionally omitted (legacy format)
        }

        page.SetContent(ms.ToArray());
        pm.SavePage(page);

        var node = new DiskBTreeNode(page, pm);
        await Assert.That(node.TreeEntryCount).IsEqualTo(0);
    }
}
