using System;
using System.Linq.Expressions;
using TinyDb.Bson;
using TinyDb.Core;
using TinyDb.Query;
using TinyDb.Serialization;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Core;

public class InPlaceScanTests
{
    [Test]
    public async Task LocateAndEvaluate_Double_ShouldWork()
    {
        var doc = new BsonDocument()
            .Set("Name", "Test")
            .Set("Salary", 30000.0);
        var bytes = BsonSerializer.SerializeDocument(doc);

        var nameBytes = System.Text.Encoding.UTF8.GetBytes("Salary");
        
        bool found = BsonScanner.TryLocateField(bytes.AsSpan(), nameBytes, out int offset, out BsonType type);
        
        await Assert.That(found).IsTrue();
        await Assert.That(type).IsEqualTo(BsonType.Double);

        bool result = BinaryPredicateEvaluator.Evaluate(bytes.AsSpan(), offset, type, ExpressionType.GreaterThanOrEqual, 30000);
        await Assert.That(result).IsTrue();
    }

    [Test]
    public async Task LocateAndEvaluate_Decimal128_ShouldWork()
    {
        var doc = new BsonDocument()
            .Set("Amount", 123.456m);
        var bytes = BsonSerializer.SerializeDocument(doc);

        var nameBytes = System.Text.Encoding.UTF8.GetBytes("Amount");
        
        bool found = BsonScanner.TryLocateField(bytes.AsSpan(), nameBytes, out int offset, out BsonType type);
        
        await Assert.That(found).IsTrue();
        await Assert.That(type).IsEqualTo(BsonType.Decimal128);

        bool result = BinaryPredicateEvaluator.Evaluate(bytes.AsSpan(), offset, type, ExpressionType.GreaterThan, 100m);
        await Assert.That(result).IsTrue();
    }

    [Test]
    public async Task LocateAndEvaluate_DateTime_ShouldWork()
    {
        var created = new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var doc = new BsonDocument()
            .Set("Created", created);
        var bytes = BsonSerializer.SerializeDocument(doc);

        var nameBytes = System.Text.Encoding.UTF8.GetBytes("Created");

        bool found = BsonScanner.TryLocateField(bytes.AsSpan(), nameBytes, out int offset, out BsonType type);

        await Assert.That(found).IsTrue();
        await Assert.That(type).IsEqualTo(BsonType.DateTime);

        bool result = BinaryPredicateEvaluator.Evaluate(bytes.AsSpan(), offset, type, ExpressionType.Equal, created);
        await Assert.That(result).IsTrue();
    }

    [Test]
    public async Task LocateAndEvaluate_ObjectId_ShouldWork()
    {
        var oid = ObjectId.NewObjectId();
        var doc = new BsonDocument()
            .Set("Oid", oid);
        var bytes = BsonSerializer.SerializeDocument(doc);

        var nameBytes = System.Text.Encoding.UTF8.GetBytes("Oid");

        bool found = BsonScanner.TryLocateField(bytes.AsSpan(), nameBytes, out int offset, out BsonType type);

        await Assert.That(found).IsTrue();
        await Assert.That(type).IsEqualTo(BsonType.ObjectId);

        bool result = BinaryPredicateEvaluator.Evaluate(bytes.AsSpan(), offset, type, ExpressionType.Equal, oid);
        await Assert.That(result).IsTrue();
    }

    [Test]
    public async Task LocateAndEvaluate_Timestamp_ShouldWork()
    {
        var ts = new BsonTimestamp(123, 456);
        var doc = new BsonDocument()
            .Set("Ts", ts);
        var bytes = BsonSerializer.SerializeDocument(doc);

        var nameBytes = System.Text.Encoding.UTF8.GetBytes("Ts");

        bool found = BsonScanner.TryLocateField(bytes.AsSpan(), nameBytes, out int offset, out BsonType type);

        await Assert.That(found).IsTrue();
        await Assert.That(type).IsEqualTo(BsonType.Timestamp);

        bool result = BinaryPredicateEvaluator.Evaluate(bytes.AsSpan(), offset, type, ExpressionType.Equal, ts);
        await Assert.That(result).IsTrue();
    }
}
