using System;
using System.Collections.Generic;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Bson;

public class BsonScalarAdditionalCoverageTests
{
    [Test]
    public async Task BsonDouble_CompareTo_Equals_And_ToType_ShouldCoverBranches()
    {
        var val = new BsonDouble(1.5);

        await Assert.That(val.CompareTo(null)).IsEqualTo(1);
        await Assert.That(val.CompareTo(new BsonDouble(1.5))).IsEqualTo(0);
        await Assert.That(val.CompareTo(new BsonInt32(2))).IsLessThan(0);
        await Assert.That(val.CompareTo(new BsonString("x"))).IsNotEqualTo(0);

        await Assert.That(val.Equals(new BsonDouble(1.5))).IsTrue();
        await Assert.That(val.Equals(new BsonInt32(1))).IsFalse();

        await Assert.That(val.ToType(typeof(double), null)).IsEqualTo(1.5);
        await Assert.That(val.ToType(typeof(BsonDouble), null)).IsEqualTo(val);
        await Assert.That(val.ToType(typeof(object), null)).IsEqualTo(val);
        await Assert.That(val.ToType(typeof(string), null)).IsEqualTo("1.5");
        await Assert.That(val.ToType(typeof(int), null)).IsEqualTo(2);
    }

    [Test]
    public async Task BsonInt32_ImplicitConversion_ToDateTime_And_ToType_ShouldCoverBranches()
    {
        var bson = new BsonInt32(123);

        int value = bson;
        await Assert.That(value).IsEqualTo(123);

        await Assert.That(() => bson.ToDateTime(null)).Throws<InvalidCastException>();

        await Assert.That(bson.ToType(typeof(int), null)).IsEqualTo(123);
        await Assert.That(bson.ToType(typeof(BsonInt32), null)).IsEqualTo(bson);
        await Assert.That(bson.ToType(typeof(object), null)).IsEqualTo(bson);
        await Assert.That(bson.ToType(typeof(string), null)).IsEqualTo("123");
        await Assert.That(bson.ToType(typeof(double), null)).IsEqualTo(123d);
    }

    [Test]
    public async Task BsonObjectId_CompareTo_And_ToType_ShouldCoverBranches()
    {
        var oid = ObjectId.NewObjectId();
        var val = new BsonObjectId(oid);

        await Assert.That(val.CompareTo(null)).IsEqualTo(1);
        await Assert.That(val.CompareTo(new BsonObjectId(oid))).IsEqualTo(0);
        await Assert.That(val.CompareTo(new BsonInt32(1))).IsNotEqualTo(0);

        await Assert.That(val.ToType(typeof(string), null)).IsEqualTo(oid.ToString());
        await Assert.That(() => val.ToType(typeof(int), null)).Throws<FormatException>();
    }

    [Test]
    public async Task BsonDateTime_ToType_ShouldCoverBranches()
    {
        var now = new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Utc);
        var val = new BsonDateTime(now);

        await Assert.That(val.ToType(typeof(string), null)).IsEqualTo(val.ToString());
        await Assert.That(() => val.ToType(typeof(Guid), null)).Throws<InvalidCastException>();
    }

    [Test]
    public async Task BsonJavaScript_Ctor_CompareTo_And_Equals_ShouldCoverBranches()
    {
        await Assert.That(() => new BsonJavaScript(null!)).ThrowsExactly<ArgumentNullException>();

        var js = new BsonJavaScript("code");

        await Assert.That(js.CompareTo((BsonValue?)null)).IsEqualTo(1);
        await Assert.That(js.CompareTo(new BsonJavaScript("code"))).IsEqualTo(0);
        await Assert.That(js.CompareTo(new BsonString("x"))).IsNotEqualTo(0);

        BsonValue asValue = js;
        await Assert.That(asValue.Equals((BsonValue?)new BsonJavaScript("code"))).IsTrue();
    }

    [Test]
    public async Task BsonDocument_ICollectionAdd_And_Equals_ShouldCoverBranches()
    {
        var doc = new BsonDocument();

        var collection = (ICollection<KeyValuePair<string, BsonValue>>)doc;
        await Assert.That(() => collection.Add(new KeyValuePair<string, BsonValue>("a", 1)))
            .Throws<NotSupportedException>();

        await Assert.That(doc.Equals(new BsonInt32(1))).IsFalse();
    }
}

