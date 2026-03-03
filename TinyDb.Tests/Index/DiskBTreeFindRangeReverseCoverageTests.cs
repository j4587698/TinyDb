using System;
using System.IO;
using System.Linq;
using TinyDb.Bson;
using TinyDb.Core;
using TinyDb.Index;
using TinyDb.Storage;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Index;

public sealed class DiskBTreeFindRangeReverseCoverageTests
{
    [Test]
    public async Task FindRangeReverse_ShouldReturnDescendingAcrossLeafSiblings()
    {
        var path = Path.Combine(Path.GetTempPath(), $"btree_find_range_reverse_{Guid.NewGuid():N}.db");

        try
        {
            using var ds = new DiskStream(path);
            using var pm = new PageManager(ds, 4096);
            using var tree = DiskBTree.Create(pm, maxKeys: 2);

            for (int i = 1; i <= 20; i++)
            {
                tree.Insert(new IndexKey(new BsonInt32(i)), new BsonInt32(i));
            }

            var result = tree
                .FindRangeReverse(new IndexKey(new BsonInt32(5)), new IndexKey(new BsonInt32(17)), true, true)
                .Cast<BsonInt32>()
                .Select(x => x.Value)
                .ToArray();

            var expected = Enumerable.Range(5, 13).Reverse().ToArray();
            await Assert.That(result.SequenceEqual(expected)).IsTrue();
        }
        finally
        {
            try { if (File.Exists(path)) File.Delete(path); } catch { }
        }
    }

    [Test]
    public async Task FindRangeReverse_ShouldRespectExclusiveBoundaries()
    {
        var path = Path.Combine(Path.GetTempPath(), $"btree_find_range_reverse_exclusive_{Guid.NewGuid():N}.db");

        try
        {
            using var ds = new DiskStream(path);
            using var pm = new PageManager(ds, 4096);
            using var tree = DiskBTree.Create(pm, maxKeys: 2);

            for (int i = 1; i <= 20; i++)
            {
                tree.Insert(new IndexKey(new BsonInt32(i)), new BsonInt32(i));
            }

            var result = tree
                .FindRangeReverse(new IndexKey(new BsonInt32(5)), new IndexKey(new BsonInt32(10)), false, false)
                .Cast<BsonInt32>()
                .Select(x => x.Value)
                .ToArray();

            await Assert.That(result.SequenceEqual(new[] { 9, 8, 7, 6 })).IsTrue();
        }
        finally
        {
            try { if (File.Exists(path)) File.Delete(path); } catch { }
        }
    }

    [Test]
    public async Task GetAllReverse_ShouldReturnDescendingOrder()
    {
        var path = Path.Combine(Path.GetTempPath(), $"btree_get_all_reverse_{Guid.NewGuid():N}.db");

        try
        {
            using var ds = new DiskStream(path);
            using var pm = new PageManager(ds, 4096);
            using var tree = DiskBTree.Create(pm, maxKeys: 2);

            for (int i = 1; i <= 8; i++)
            {
                tree.Insert(new IndexKey(new BsonInt32(i)), new BsonInt32(i));
            }

            var values = tree.GetAllReverse()
                .Cast<BsonInt32>()
                .Select(x => x.Value)
                .ToArray();

            await Assert.That(values.SequenceEqual(Enumerable.Range(1, 8).Reverse())).IsTrue();
        }
        finally
        {
            try { if (File.Exists(path)) File.Delete(path); } catch { }
        }
    }

    [Test]
    public async Task BTreeIndex_ReverseIterators_ShouldEnumerate()
    {
        var path = Path.Combine(Path.GetTempPath(), $"btree_index_reverse_iter_{Guid.NewGuid():N}.db");

        try
        {
            using var engine = new TinyDbEngine(path, new TinyDbOptions { EnableJournaling = false });
            var col = engine.GetBsonCollection("it");
            engine.EnsureIndex("it", "score", "idx_score", unique: false);

            for (int i = 1; i <= 10; i++)
            {
                col.Insert(new BsonDocument().Set("_id", i).Set("score", i));
            }

            var idx = engine.GetIndexManager("it").GetIndex("idx_score");
            await Assert.That(idx).IsNotNull();

            var allReverse = idx!.GetAllReverse().Cast<BsonInt32>().Select(x => x.Value).ToArray();
            await Assert.That(allReverse.Length).IsEqualTo(10);
            await Assert.That(allReverse.Min()).IsEqualTo(1);
            await Assert.That(allReverse.Max()).IsEqualTo(10);
            await Assert.That(allReverse.Distinct().Count()).IsEqualTo(10);

            var rangeReverse = idx.FindRangeReverse(
                    new IndexKey(new BsonInt32(3)),
                    new IndexKey(new BsonInt32(7)),
                    includeStart: true,
                    includeEnd: true)
                .Cast<BsonInt32>()
                .Select(x => x.Value)
                .ToArray();

            await Assert.That(rangeReverse.SequenceEqual(new[] { 7, 6, 5, 4, 3 })).IsTrue();
        }
        finally
        {
            try { if (File.Exists(path)) File.Delete(path); } catch { }
            var wal = Path.Combine(Path.GetDirectoryName(path)!, $"{Path.GetFileNameWithoutExtension(path)}-wal.db");
            try { if (File.Exists(wal)) File.Delete(wal); } catch { }
        }
    }
}
