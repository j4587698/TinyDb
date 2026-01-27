using TinyDb.Core;
using TinyDb.Attributes;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Collections;

public class DocumentCollectionTests : IDisposable
{
    private readonly string _testDbPath;
    private readonly TinyDbEngine _engine;

    public DocumentCollectionTests()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"coll_test_{Guid.NewGuid():N}.db");
        _engine = new TinyDbEngine(_testDbPath);
    }

    public void Dispose()
    {
        _engine.Dispose();
        try { if (File.Exists(_testDbPath)) File.Delete(_testDbPath); } catch { }
    }

    [Test]
    public async Task Upsert_Should_Insert_If_New()
    {
        var collection = _engine.GetCollection<DocumentCollectionItem>();
        var item = new DocumentCollectionItem { Id = 1, Name = "New" };

        var result = collection.Upsert(item);

        await Assert.That(result.UpdateType).IsEqualTo(TinyDb.Collections.UpdateType.Insert);
        await Assert.That(result.Count).IsEqualTo(1);
        await Assert.That(collection.Count()).IsEqualTo(1);
    }

    [Test]
    public async Task Upsert_Should_Update_If_Exists()
    {
        var collection = _engine.GetCollection<DocumentCollectionItem>();
        var item = new DocumentCollectionItem { Id = 1, Name = "Old" };
        collection.Insert(item);

        item.Name = "Updated";
        var result = collection.Upsert(item);

        await Assert.That(result.UpdateType).IsEqualTo(TinyDb.Collections.UpdateType.Update);
        await Assert.That(result.Count).IsEqualTo(1);

        var loaded = collection.FindById(1);
        await Assert.That(loaded!.Name).IsEqualTo("Updated");
    }

    [Test]
    public async Task DeleteMany_Should_Delete_Matching_Documents()
    {
        var collection = _engine.GetCollection<DocumentCollectionItem>();
        collection.Insert(new DocumentCollectionItem { Id = 1, Name = "A" });
        collection.Insert(new DocumentCollectionItem { Id = 2, Name = "A" });
        collection.Insert(new DocumentCollectionItem { Id = 3, Name = "B" });

        var deleted = collection.DeleteMany(x => x.Name == "A");

        await Assert.That(deleted).IsEqualTo(2);
        await Assert.That(collection.Count()).IsEqualTo(1);
        await Assert.That(collection.FindById(3)).IsNotNull();
    }

    [Test]
    public async Task Exists_Should_Return_True_If_Match_Found()
    {
        var collection = _engine.GetCollection<DocumentCollectionItem>();
        collection.Insert(new DocumentCollectionItem { Id = 1, Name = "Target" });

        await Assert.That(collection.Exists(x => x.Name == "Target")).IsTrue();
        await Assert.That(collection.Exists(x => x.Name == "NonExistent")).IsFalse();
    }

    [Test]
    public async Task Update_Batch_Should_Update_All()
    {
        var collection = _engine.GetCollection<DocumentCollectionItem>();
        var items = new[]
        {
            new DocumentCollectionItem { Id = 1, Name = "1" },
            new DocumentCollectionItem { Id = 2, Name = "2" }
        };
        collection.Insert(items);

        items[0].Name = "1_U";
        items[1].Name = "2_U";

        var updated = collection.Update(items);
        await Assert.That(updated).IsEqualTo(2);

        await Assert.That(collection.FindById(1)!.Name).IsEqualTo("1_U");
    }
}

[Entity("Items")]
public class DocumentCollectionItem
{
    public int Id { get; set; }
    public string Name { get; set; } = "";
}
