using TinyDb.Index;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Index;

public class IndexManagerTests : IDisposable
{
    private readonly string _testCollectionName = "test_collection";
    private readonly IndexManager _manager;

    public IndexManagerTests()
    {
        _manager = new IndexManager(_testCollectionName);
    }

    public void Dispose()
    {
        _manager.Dispose();
    }

    [Test]
    public async Task CreateIndex_Should_Create_New_Index()
    {
        var result = _manager.CreateIndex("idx_name", new[] { "Name" });
        
        await Assert.That(result).IsTrue();
        await Assert.That(_manager.IndexCount).IsEqualTo(1);
        await Assert.That(_manager.IndexExists("idx_name")).IsTrue();
        await Assert.That(_manager.GetIndex("idx_name")).IsNotNull();
    }

    [Test]
    public async Task CreateIndex_Duplicate_Should_Return_False()
    {
        _manager.CreateIndex("idx_name", new[] { "Name" });
        var result = _manager.CreateIndex("idx_name", new[] { "Name" });
        
        await Assert.That(result).IsFalse();
        await Assert.That(_manager.IndexCount).IsEqualTo(1);
    }

    [Test]
    public async Task DropIndex_Should_Remove_Index()
    {
        _manager.CreateIndex("idx_name", new[] { "Name" });
        var result = _manager.DropIndex("idx_name");
        
        await Assert.That(result).IsTrue();
        await Assert.That(_manager.IndexCount).IsEqualTo(0);
        await Assert.That(_manager.IndexExists("idx_name")).IsFalse();
    }

    [Test]
    public async Task DropIndex_NonExistent_Should_Return_False()
    {
        var result = _manager.DropIndex("non_existent");
        await Assert.That(result).IsFalse();
    }

    [Test]
    public async Task GetBestIndex_Should_Return_Most_Matching_Index()
    {
        _manager.CreateIndex("idx_name", new[] { "Name" });
        _manager.CreateIndex("idx_name_age", new[] { "Name", "Age" });

        var best = _manager.GetBestIndex(new[] { "Name", "Age" });
        
        await Assert.That(best).IsNotNull();
        await Assert.That(best!.Name).IsEqualTo("idx_name_age");
    }

    [Test]
    public async Task InsertDocument_Should_Update_Indexes()
    {
        _manager.CreateIndex("idx_age", new[] { "Age" });
        var doc = new BsonDocument().Set("age", 25).Set("_id", 1);
        
        _manager.InsertDocument(doc, 1);
        
        var index = _manager.GetIndex("idx_age");
        var ids = index!.Find(new IndexKey(25));
        
        await Assert.That(ids).Count().IsEqualTo(1);
        // Note: BTreeIndex.Insert takes BsonValue for docId? 
        // IndexManager.InsertDocument passes documentId directly.
    }

    [Test]
    public async Task DeleteDocument_Should_Update_Indexes()
    {
        _manager.CreateIndex("idx_age", new[] { "Age" });
        var doc = new BsonDocument().Set("age", 25).Set("_id", 1);
        _manager.InsertDocument(doc, 1);
        
        _manager.DeleteDocument(doc, 1);
        
        var index = _manager.GetIndex("idx_age");
        var ids = index!.Find(new IndexKey(25));
        
        await Assert.That(ids).IsEmpty();
    }

    [Test]
    public async Task UpdateDocument_Should_Update_Indexes()
    {
        _manager.CreateIndex("idx_age", new[] { "Age" });
        var oldDoc = new BsonDocument().Set("age", 25).Set("_id", 1);
        var newDoc = new BsonDocument().Set("age", 30).Set("_id", 1);
        
        _manager.InsertDocument(oldDoc, 1);
        _manager.UpdateDocument(oldDoc, newDoc, 1);
        
        var index = _manager.GetIndex("idx_age");
        var oldIds = index!.Find(new IndexKey(25));
        var newIds = index!.Find(new IndexKey(30));
        
        await Assert.That(oldIds).IsEmpty();
        await Assert.That(newIds).Count().IsEqualTo(1);
    }
}
