using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using TinyDb.Bson;
using TinyDb.Index;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Index;

public class BTreeIndexExtendedTests
{
    private BTreeIndex _index = null!;

    [Before(Test)]
    public void Setup()
    {
        _index = new BTreeIndex("test_index", new[] { "Field1" }, false, 4);
    }

    [After(Test)]
    public void Cleanup()
    {
        _index?.Dispose();
    }

    [Test]
    public async Task Insert_Should_Store_DocumentIds()
    {
        var key = new IndexKey(new BsonValue[] { new BsonString("key") });
        var id1 = ObjectId.NewObjectId();
        var id2 = ObjectId.NewObjectId();

        await Assert.That(_index.Insert(key, id1)).IsTrue();
        await Assert.That(_index.Insert(key, id2)).IsTrue();

        var storedIds = _index.Find(key)
            .OfType<BsonObjectId>()
            .Select(x => x.Value)
            .ToList();

        await Assert.That(storedIds).Contains(id1);
        await Assert.That(storedIds).Contains(id2);
    }

    [Test]
    public async Task Delete_Should_Remove_DocumentId()
    {
        var key = new IndexKey(new BsonValue[] { new BsonString("key") });
        var id = ObjectId.NewObjectId();

        _index.Insert(key, id);
        var removed = _index.Delete(key, id);

        await Assert.That(removed).IsTrue();
        var storedIds = _index.Find(key)
            .OfType<BsonObjectId>()
            .Select(x => x.Value)
            .ToList();
        await Assert.That(storedIds).IsEmpty();
    }

    [Test]
    public async Task FindRange_Should_Return_Expected_Documents()
    {
        var keys = new[]
        {
            new IndexKey(new BsonValue[] { new BsonString("a") }),
            new IndexKey(new BsonValue[] { new BsonString("b") }),
            new IndexKey(new BsonValue[] { new BsonString("c") })
        };

        var ids = keys.Select(_ => ObjectId.NewObjectId()).ToArray();

        for (var i = 0; i < keys.Length; i++)
        {
            _index.Insert(keys[i], ids[i]);
        }

        var rangeIds = _index.FindRange(
            new IndexKey(new BsonValue[] { new BsonString("b") }),
            new IndexKey(new BsonValue[] { new BsonString("c") }))
            .OfType<BsonObjectId>()
            .Select(x => x.Value)
            .ToList();

        await Assert.That(rangeIds).Contains(ids[1]);
        await Assert.That(rangeIds).Contains(ids[2]);
    }

    [Test]
    public async Task Clear_Should_Reset_Index()
    {
        var key = new IndexKey(new BsonValue[] { new BsonString("key") });
        _index.Insert(key, ObjectId.NewObjectId());

        _index.Clear();

        await Assert.That(_index.EntryCount).IsEqualTo(0);
        await Assert.That(_index.Find(key)).IsEmpty();
    }

    [Test]
    public async Task CompositeKeys_ShouldMaintainLexicographicalOrderInRange()
    {
        using var compositeIndex = new BTreeIndex("composite_index", new[] { "Category", "Sku" }, false, 4);

        var entries = new (IndexKey Key, BsonValue DocumentId)[]
        {
            (new IndexKey(new BsonValue[] { new BsonString("A"), new BsonString("001") }), new BsonString("A-001")),
            (new IndexKey(new BsonValue[] { new BsonString("A"), new BsonString("002") }), new BsonString("A-002")),
            (new IndexKey(new BsonValue[] { new BsonString("B"), new BsonString("001") }), new BsonString("B-001")),
            (new IndexKey(new BsonValue[] { new BsonString("B"), new BsonString("003") }), new BsonString("B-003")),
            (new IndexKey(new BsonValue[] { new BsonString("C"), new BsonString("001") }), new BsonString("C-001"))
        };

        foreach (var (key, documentId) in entries)
        {
            await Assert.That(compositeIndex.Insert(key, documentId)).IsTrue();
        }

        var rangeResults = compositeIndex.FindRange(
                new IndexKey(new BsonValue[] { new BsonString("A"), new BsonString("002") }),
                new IndexKey(new BsonValue[] { new BsonString("B"), new BsonString("003") }))
            .OfType<BsonString>()
            .Select(v => v.Value)
            .ToList();

        await Assert.That(rangeResults.Count).IsEqualTo(3);
        await Assert.That(rangeResults[0]).IsEqualTo("A-002");
        await Assert.That(rangeResults[1]).IsEqualTo("B-001");
        await Assert.That(rangeResults[2]).IsEqualTo("B-003");
    }

    [Test]
    public async Task FindRange_ShouldTraverseMultipleLeafNodes()
    {
        var insertedIds = new List<string>();
        for (var i = 1; i <= 12; i++)
        {
            var key = new IndexKey(new BsonValue[] { new BsonInt32(i) });
            var documentId = new BsonString($"doc_{i}");
            _index.Insert(key, documentId);
            insertedIds.Add(documentId.Value);
        }

        var results = _index.FindRange(
                new IndexKey(new BsonValue[] { new BsonInt32(3) }),
                new IndexKey(new BsonValue[] { new BsonInt32(9) }))
            .OfType<BsonString>()
            .Select(v => v.Value)
            .ToList();

        var expected = insertedIds.Where(id =>
        {
            var number = int.Parse(id.Split('_')[1], CultureInfo.InvariantCulture);
            return number >= 3 && number <= 9;
        }).ToList();

        await Assert.That(results.Count).IsEqualTo(expected.Count);
        for (var i = 0; i < expected.Count; i++)
        {
            await Assert.That(results[i]).IsEqualTo(expected[i]);
        }
    }
}
