using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using SimpleDb.Bson;
using SimpleDb.Index;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace SimpleDb.Tests.Index;

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
}
