using System;
using System.IO;
using System.Linq;
using TinyDb.Core;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Core;

[NotInParallel]
public class EngineAdvancedTests
{
    private string _dbFile = null!;

    [Before(Test)]
    public void Setup()
    {
        _dbFile = Path.Combine(Path.GetTempPath(), $"eng_adv_{Guid.NewGuid():N}.db");
    }

    [After(Test)]
    public void Cleanup()
    {
        if (File.Exists(_dbFile)) File.Delete(_dbFile);
    }

    [Test]
    public async Task Engine_DropCollection_ShouldWork()
    {
        using (var engine = new TinyDbEngine(_dbFile))
        {
            var col = engine.GetCollection<BsonDocument>("test");
            col.Insert(new BsonDocument().Set("a", 1));
            
            await Assert.That(engine.CollectionExists("test")).IsTrue();
            
            engine.DropCollection("test");
            await Assert.That(engine.CollectionExists("test")).IsFalse();
        }
    }

    [Test]
    public async Task Engine_GetStatistics_ShouldWork()
    {
        using var engine = new TinyDbEngine(_dbFile);
        var stats = engine.GetStatistics();
        await Assert.That(stats).IsNotNull();
        await Assert.That(stats.DatabaseName).IsEqualTo("TinyDb");
    }
}
