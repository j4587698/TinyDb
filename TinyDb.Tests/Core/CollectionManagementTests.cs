using TinyDb.Core;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Core;

public class CollectionManagementTests : IDisposable
{
    private readonly string _testDbPath;
    private readonly TinyDbEngine _engine;

    public CollectionManagementTests()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"col_mgmt_{Guid.NewGuid():N}.db");
        _engine = new TinyDbEngine(_testDbPath);
    }

    public void Dispose()
    {
        _engine.Dispose();
        try { if (File.Exists(_testDbPath)) File.Delete(_testDbPath); } catch { }
    }

    public class TestItem
    {
        public int Id { get; set; }
        public string Val { get; set; } = "";
    }

    [Test]
    public async Task DropCollection_Should_Remove_Collection_And_Data()
    {
        var colName = "drop_test";
        var col = _engine.GetCollection<TestItem>(colName);
        col.Insert(new TestItem { Id = 1, Val = "A" });
        
        await Assert.That(_engine.CollectionExists(colName)).IsTrue();
        
        var dropped = _engine.DropCollection(colName);
        _engine.Flush();
        
        await Assert.That(dropped).IsTrue();
        await Assert.That(_engine.CollectionExists(colName)).IsFalse();
        
        // Re-create
        var col2 = _engine.GetCollection<TestItem>(colName);
        await Assert.That(col2.Count()).IsEqualTo(0);
    }

    [Test]
    public async Task DropCollection_NonExistent_Should_Return_False()
    {
        await Assert.That(_engine.DropCollection("non_existent")).IsFalse();
    }

    [Test]
    public async Task GetCollectionNames_Should_Return_All_Names()
    {
        _engine.GetCollection<TestItem>("col1");
        _engine.GetCollection<TestItem>("col2");
        
        var names = _engine.GetCollectionNames().ToList();
        await Assert.That(names).Contains("col1");
        await Assert.That(names).Contains("col2");
    }
}
