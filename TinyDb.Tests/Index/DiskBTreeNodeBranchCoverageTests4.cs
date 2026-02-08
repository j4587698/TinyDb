using System;
using System.Reflection;
using TinyDb.Bson;
using TinyDb.Index;
using TinyDb.Storage;
using TinyDb.Tests.Storage;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Index;

public class DiskBTreeNodeBranchCoverageTests4
{
    [Test]
    public async Task Save_WhenOverflowPageIsNotIndex_ShouldUpdatePageType()
    {
        using var pm = new PageManager(new MockDiskStream(), pageSize: 4096, maxCacheSize: 64);

        var rootPage = pm.NewPage(PageType.Index);
        var node = new DiskBTreeNode(rootPage, pm);

        var overflow = pm.NewPage(PageType.Data);
        rootPage.SetLinks(rootPage.Header.PrevPageID, overflow.PageID);

        node.Keys.Add(new IndexKey(new BsonValue[] { new BsonString(new string('x', 12000)) }));
        node.Values.Add(new BsonInt32(1));
        node.MarkDirty();

        node.Save(pm);

        var overflowReloaded = pm.GetPage(overflow.PageID);
        await Assert.That(overflowReloaded.PageType).IsEqualTo(PageType.Index);
    }

    [Test]
    public async Task LoadFromPage_WhenNoPayload_ShouldReturnEarly()
    {
        using var pm = new PageManager(new MockDiskStream(), pageSize: 4096, maxCacheSize: 64);
        var page = pm.NewPage(PageType.Index);
        var node = new DiskBTreeNode(page, pm);

        var method = typeof(DiskBTreeNode).GetMethod("LoadFromPage", BindingFlags.NonPublic | BindingFlags.Instance);
        await Assert.That(method).IsNotNull();

        method!.Invoke(node, null);
        await Assert.That(node.KeyCount).IsEqualTo(0);
    }

    [Test]
    public async Task GetBsonValueSize_DefaultCase_ShouldReturnFallbackSize()
    {
        using var pm = new PageManager(new MockDiskStream(), pageSize: 4096, maxCacheSize: 64);
        var page = pm.NewPage(PageType.Index);
        var node = new DiskBTreeNode(page, pm);

        var method = typeof(DiskBTreeNode).GetMethod("GetBsonValueSize", BindingFlags.NonPublic | BindingFlags.Instance);
        await Assert.That(method).IsNotNull();

        var size = (int)method!.Invoke(node, new object[] { new BsonArray() })!;
        await Assert.That(size).IsEqualTo(20);
    }

    [Test]
    public async Task GetBsonValueSize_WhenBsonTypeOutOfRange_ShouldReturnFallbackSize()
    {
        using var pm = new PageManager(new MockDiskStream(), pageSize: 4096, maxCacheSize: 64);
        var page = pm.NewPage(PageType.Index);
        var node = new DiskBTreeNode(page, pm);

        var method = typeof(DiskBTreeNode).GetMethod("GetBsonValueSize", BindingFlags.NonPublic | BindingFlags.Instance);
        await Assert.That(method).IsNotNull();

        var size = (int)method!.Invoke(node, new object[] { BsonMaxKey.Value })!;
        await Assert.That(size).IsEqualTo(20);
    }

    [Test]
    public async Task GetBsonValueSize_Double_Int32_Int64_Decimal128_And_Timestamp_ShouldReturnExpectedSizes()
    {
        using var pm = new PageManager(new MockDiskStream(), pageSize: 4096, maxCacheSize: 64);
        var page = pm.NewPage(PageType.Index);
        var node = new DiskBTreeNode(page, pm);

        var method = typeof(DiskBTreeNode).GetMethod("GetBsonValueSize", BindingFlags.NonPublic | BindingFlags.Instance);
        await Assert.That(method).IsNotNull();

        var doubleSize = (int)method!.Invoke(node, new object[] { new BsonDouble(1.0) })!;
        var int32Size = (int)method.Invoke(node, new object[] { new BsonInt32(1) })!;
        var int64Size = (int)method.Invoke(node, new object[] { new BsonInt64(1L) })!;
        var decimal128Size = (int)method.Invoke(node, new object[] { new BsonDecimal128(1m) })!;
        var timestampSize = (int)method.Invoke(node, new object[] { new BsonTimestamp(1, 2) })!;

        await Assert.That(doubleSize).IsEqualTo(8);
        await Assert.That(int32Size).IsEqualTo(4);
        await Assert.That(int64Size).IsEqualTo(8);
        await Assert.That(decimal128Size).IsEqualTo(16);
        await Assert.That(timestampSize).IsEqualTo(20);
    }
}
