using System;
using System.IO;
using TinyDb.Bson;
using TinyDb.Index;
using TinyDb.Storage;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Index;

public sealed class DiskBTreeCoverageTests
{
    [Test]
    public async Task Constructor_WhenMaxKeysNonPositive_ShouldDefaultTo200()
    {
        var path = Path.Combine(Path.GetTempPath(), $"btree_cov_ctor_{Guid.NewGuid():N}.db");

        try
        {
            using var ds = new DiskStream(path);
            using var pm = new PageManager(ds, 4096);

            var tree = DiskBTree.Create(pm, maxKeys: 3);
            var custom = new DiskBTree(pm, tree.RootPageId, maxKeys: 0);

            await Assert.That(custom.RootPageId).IsEqualTo(tree.RootPageId);

            var maxKeysField = typeof(DiskBTree).GetField("_maxKeys", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            await Assert.That(maxKeysField).IsNotNull();
            await Assert.That((int)maxKeysField!.GetValue(custom)!).IsEqualTo(200);
        }
        finally
        {
            try { if (File.Exists(path)) File.Delete(path); } catch { }
        }
    }

    [Test]
    public async Task RootNode_WhenCacheCleared_ShouldRecreateAndCacheNode()
    {
        var path = Path.Combine(Path.GetTempPath(), $"btree_cov_cache_{Guid.NewGuid():N}.db");

        try
        {
            using var ds = new DiskStream(path);
            using var pm = new PageManager(ds, 4096);

            var tree = DiskBTree.Create(pm, maxKeys: 3);

            var rootPage = pm.GetPage(tree.RootPageId);
            rootPage.CachedParsedData = null;

            var node = tree.RootNode;
            await Assert.That(node).IsNotNull();
            await Assert.That(rootPage.CachedParsedData).IsNotNull();
        }
        finally
        {
            try { if (File.Exists(path)) File.Delete(path); } catch { }
        }
    }

    [Test]
    public async Task Delete_DuplicateKey_ShouldTraverseSiblings()
    {
        var path = Path.Combine(Path.GetTempPath(), $"btree_cov_dup_{Guid.NewGuid():N}.db");

        try
        {
            using var ds = new DiskStream(path);
            using var pm = new PageManager(ds, 4096);

            var tree = DiskBTree.Create(pm, maxKeys: 3);
            var key = new IndexKey(new BsonString("k"));

            for (int i = 0; i < 30; i++)
            {
                tree.Insert(key, new BsonInt32(i));
            }

            var deleted = tree.Delete(key, new BsonInt32(0));
            await Assert.That(deleted).IsTrue();
        }
        finally
        {
            try { if (File.Exists(path)) File.Delete(path); } catch { }
        }
    }

    [Test]
    public async Task Delete_WhenKeyGreaterThanAll_ShouldBreakAtLastSibling()
    {
        var path = Path.Combine(Path.GetTempPath(), $"btree_cov_last_{Guid.NewGuid():N}.db");

        try
        {
            using var ds = new DiskStream(path);
            using var pm = new PageManager(ds, 4096);

            var tree = DiskBTree.Create(pm, maxKeys: 3);
            var key = new IndexKey(new BsonString("k"));

            for (int i = 0; i < 20; i++)
            {
                tree.Insert(key, new BsonInt32(i));
            }

            var missingKey = new IndexKey(new BsonString("z"));
            var deleted = tree.Delete(missingKey, new BsonInt32(123));
            await Assert.That(deleted).IsFalse();
        }
        finally
        {
            try { if (File.Exists(path)) File.Delete(path); } catch { }
        }
    }

    [Test]
    public async Task LockWrapperMethods_ShouldWork()
    {
        var path = Path.Combine(Path.GetTempPath(), $"btree_cov_lock_{Guid.NewGuid():N}.db");

        try
        {
            using var ds = new DiskStream(path);
            using var pm = new PageManager(ds, 4096);
            using var tree = DiskBTree.Create(pm, maxKeys: 3);

            tree.EnterReadLock();
            tree.ExitReadLock();

            tree.EnterWriteLock();
            tree.ExitWriteLock();

            await Assert.That(tree.RootPageId).IsGreaterThan(0u);
        }
        finally
        {
            try { if (File.Exists(path)) File.Delete(path); } catch { }
        }
    }

    [Test]
    public async Task Contains_DuplicateKeyAcrossSiblings_ShouldTraversePrevSiblings()
    {
        var path = Path.Combine(Path.GetTempPath(), $"btree_cov_contains_dup_{Guid.NewGuid():N}.db");

        try
        {
            using var ds = new DiskStream(path);
            using var pm = new PageManager(ds, 4096);
            using var tree = DiskBTree.Create(pm, maxKeys: 2);

            var key = new IndexKey(new BsonString("k"));
            for (int i = 0; i < 40; i++)
            {
                tree.Insert(key, new BsonInt32(i));
            }

            await Assert.That(tree.Contains(key)).IsTrue();
            await Assert.That(tree.Contains(key, new BsonInt32(25))).IsTrue();
        }
        finally
        {
            try { if (File.Exists(path)) File.Delete(path); } catch { }
        }
    }

    [Test]
    public async Task Contains_KeyValue_ShouldTraverseNextSibling_WhenKeyBetweenNodes()
    {
        var path = Path.Combine(Path.GetTempPath(), $"btree_cov_contains_next_{Guid.NewGuid():N}.db");

        try
        {
            using var ds = new DiskStream(path);
            using var pm = new PageManager(ds, 4096);
            using var tree = DiskBTree.Create(pm, maxKeys: 2);

            foreach (var k in new[] { "a", "c", "e", "g" })
            {
                tree.Insert(new IndexKey(new BsonString(k)), new BsonInt32(1));
            }

            var missingBetween = new IndexKey(new BsonString("b"));
            await Assert.That(tree.Contains(missingBetween, new BsonInt32(1))).IsFalse();
        }
        finally
        {
            try { if (File.Exists(path)) File.Delete(path); } catch { }
        }
    }

    [Test]
    public async Task Contains_KeyOnly_ShouldTraverseNextSibling_WhenKeyBetweenNodes()
    {
        var path = Path.Combine(Path.GetTempPath(), $"btree_cov_contains_next_key_{Guid.NewGuid():N}.db");

        try
        {
            using var ds = new DiskStream(path);
            using var pm = new PageManager(ds, 4096);
            using var tree = DiskBTree.Create(pm, maxKeys: 2);

            foreach (var k in new[] { "a", "c", "e", "g" })
            {
                tree.Insert(new IndexKey(new BsonString(k)), new BsonInt32(1));
            }

            var missingBetween = new IndexKey(new BsonString("b"));
            await Assert.That(tree.Contains(missingBetween)).IsFalse();
        }
        finally
        {
            try { if (File.Exists(path)) File.Delete(path); } catch { }
        }
    }

    [Test]
    public async Task Insert_AfterDispose_ShouldThrowObjectDisposedException()
    {
        var path = Path.Combine(Path.GetTempPath(), $"btree_cov_disposed_{Guid.NewGuid():N}.db");

        try
        {
            using var ds = new DiskStream(path);
            using var pm = new PageManager(ds, 4096);
            var tree = DiskBTree.Create(pm, maxKeys: 3);
            tree.Dispose();

            await Assert.That(() => tree.Insert(new IndexKey(new BsonString("k")), new BsonInt32(1)))
                .Throws<ObjectDisposedException>();
        }
        finally
        {
            try { if (File.Exists(path)) File.Delete(path); } catch { }
        }
    }
}
