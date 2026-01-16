using System;
using System.IO;
using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TinyDb.Bson;
using TinyDb.Core;
using TinyDb.Index;
using TinyDb.Collections;

namespace TinyDb.Tests;

public class CoverageSprintFinal : IDisposable
{
    private readonly string _dbPath;

    public CoverageSprintFinal()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"final_sprint_{Guid.NewGuid():N}.db");
    }

    public void Dispose()
    {
        if (File.Exists(_dbPath)) try { File.Delete(_dbPath); } catch { }
    }

    [Test]
    public async Task BsonTypes_Full_Coverage()
    {
        // BsonNull
        IConvertible n = BsonNull.Value;
        await Assert.That(n.ToBoolean(null)).IsFalse();
        await Assert.That(n.ToType(typeof(string), null)).IsNull();
        
        // BsonObjectId
        var oid = ObjectId.NewObjectId();
        var boid = new BsonObjectId(oid);
        IConvertible coid = boid;
        await Assert.That(coid.ToString(null)).IsEqualTo(oid.ToString());
        
        // BsonDateTime
        var now = new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var bdt = new BsonDateTime(now);
        IConvertible cdt = bdt;
        await Assert.That(cdt.ToDateTime(null)).IsEqualTo(now);
    }

    [Test]
    public async Task BsonDocument_Advanced_Coverage()
    {
        var doc = new BsonDocument();
        doc = doc.Set("a", (BsonValue)1).Set("b", (BsonValue)2);
        
        // ToDictionary returns RawValue (int for BsonInt32)
        var dict = doc.ToDictionary();
        object valA = dict["a"];
        await Assert.That(valA).IsEqualTo(1);
        
        // Immutable ops check - use generic IDictionary<string, BsonValue>
        IDictionary<string, BsonValue> genericDict = (IDictionary<string, BsonValue>)doc;
        await Assert.ThrowsAsync<NotSupportedException>(async () => { genericDict.Add("c", (BsonValue)3); await Task.CompletedTask; });
    }

    [Test]
    public async Task TransactionManager_Limit_Coverage()
    {
        var options = new TinyDbOptions { MaxTransactions = 2 };
        using var engine = new TinyDbEngine(_dbPath, options);
        
        var t1 = engine.BeginTransaction();
        var t2 = engine.BeginTransaction();
        
        await Assert.ThrowsAsync<InvalidOperationException>(async () => 
        {
            engine.BeginTransaction();
            await Task.CompletedTask;
        });
        
        t1.Dispose();
        t2.Dispose();
    }

    [Test]
    public async Task Engine_Drop_And_Names_Coverage()
    {
        using var engine = new TinyDbEngine(_dbPath);
        // Explicitly register a collection name
        engine.GetCollectionWithName<BsonDocument>("col1");
        
        var names = engine.GetCollectionNames().ToList();
        await Assert.That(names.Contains("col1")).IsTrue();
        
        engine.DropCollection("col1");
        // names are refreshed on GetCollectionNames call
        await Assert.That(engine.GetCollectionNames().Contains("col1")).IsFalse();
    }
}
