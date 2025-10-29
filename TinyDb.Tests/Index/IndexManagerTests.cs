using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using TinyDb.Bson;
using TinyDb.Index;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Index;

public class IndexManagerTests
{
    private IndexManager _manager = null!;

    [Before(Test)]
    public void Setup()
    {
        _manager = new IndexManager("test_collection");
    }

    [After(Test)]
    public void Cleanup()
    {
        _manager?.Dispose();
    }

    [Test]
    public async Task CreateIndex_Should_Add_Index()
    {
        var created = _manager.CreateIndex("idx_name", new[] { "Name" });

        await Assert.That(created).IsTrue();
        await Assert.That(_manager.IndexExists("idx_name")).IsTrue();

        var index = _manager.GetIndex("idx_name");
        await Assert.That(index).IsNotNull();
        await Assert.That(index!.Fields).Contains("Name");
    }

    [Test]
    public async Task GetBestIndex_Should_Return_Best_Match()
    {
        _manager.CreateIndex("idx_name", new[] { "Name" });
        _manager.CreateIndex("idx_name_age", new[] { "Name", "Age" });
        _manager.CreateIndex("idx_age", new[] { "Age" });

        var best = _manager.GetBestIndex(new[] { "Name", "Age" });

        await Assert.That(best).IsNotNull();
        await Assert.That(best!.Name).IsEqualTo("idx_name_age");
    }

    [Test]
    public async Task InsertDocument_Should_Update_All_Indexes()
    {
        _manager.CreateIndex("idx_name", new[] { "Name" });
        _manager.CreateIndex("idx_age", new[] { "Age" });

        var documentId = ObjectId.NewObjectId();
        var document = CreateDocument(
            ("_id", (BsonValue)documentId),
            ("Name", new BsonString("Alice")),
            ("Age", new BsonInt32(28)));

        _manager.InsertDocument(document, documentId);

        var nameIndex = _manager.GetIndex("idx_name");
        var ageIndex = _manager.GetIndex("idx_age");

        await Assert.That(nameIndex).IsNotNull();
        await Assert.That(ageIndex).IsNotNull();
        await Assert.That(nameIndex!.Find(new IndexKey(new BsonValue[] { new BsonString("Alice") })))
            .Contains(new BsonObjectId(documentId));
        await Assert.That(ageIndex!.Find(new IndexKey(new BsonValue[] { new BsonInt32(28) })))
            .Contains(new BsonObjectId(documentId));
    }

    [Test]
    public async Task ClearAllIndexes_Should_Reset_Entries()
    {
        _manager.CreateIndex("idx_field", new[] { "Field1" });
        var documentId = ObjectId.NewObjectId();
        var document = CreateDocument(
            ("_id", (BsonValue)documentId),
            ("Field1", new BsonString("value")));
        _manager.InsertDocument(document, documentId);

        _manager.ClearAllIndexes();

        var index = _manager.GetIndex("idx_field");
        await Assert.That(index).IsNotNull();
        await Assert.That(index!.EntryCount).IsEqualTo(0);
    }

    private static BsonDocument CreateDocument(params (string Key, BsonValue Value)[] fields)
    {
        var elements = new Dictionary<string, BsonValue>(StringComparer.OrdinalIgnoreCase);
        foreach (var (key, value) in fields)
        {
            elements[key] = value;
        }

        return new BsonDocument(elements);
    }
}
