using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Bson;

public class BsonValueConversionTests
{
    [Test]
    public async Task BsonInt32_Conversions_Should_Work()
    {
        BsonValue val = new BsonInt32(42);
        
        await Assert.That(val.As<int>()).IsEqualTo(42);
        await Assert.That(val.ToBoolean(null)).IsTrue();
        await Assert.That(val.ToDouble(null)).IsEqualTo(42.0);
        await Assert.That(val.ToDecimal(null)).IsEqualTo(42m);
        await Assert.That(val.ToString()).IsEqualTo("42");
        await Assert.That(val.ToInt64(null)).IsEqualTo(42L);
    }

    [Test]
    public async Task BsonString_Conversions_Should_Work()
    {
        BsonValue val = new BsonString("42");
        
        await Assert.That(val.ToInt32(null)).IsEqualTo(42);
        await Assert.That(val.ToDouble(null)).IsEqualTo(42.0);
        
        BsonValue boolVal = new BsonString("true");
        await Assert.That(boolVal.ToBoolean(null)).IsTrue();
        
        BsonValue invalidInt = new BsonString("invalid");
        await Assert.That(() => invalidInt.ToInt32(null)).Throws<FormatException>();
    }

    [Test]
    public async Task BsonNull_Conversions_Should_Return_Defaults()
    {
        BsonValue val = BsonNull.Value;
        
        await Assert.That(val.IsNull).IsTrue();
        await Assert.That(val.ToBoolean(null)).IsFalse();
        await Assert.That(val.ToInt32(null)).IsEqualTo(0);
        await Assert.That(val.ToDouble(null)).IsEqualTo(0.0);
        await Assert.That(val.ToString(null)).IsEqualTo("null");
        
        // ToType
        await Assert.That(val.ToType(typeof(int), null)).IsEqualTo(0);
        await Assert.That(val.ToType(typeof(string), null)).IsNull();
    }

    [Test]
    public async Task BsonObjectId_Conversions_Should_Work()
    {
        var oid = ObjectId.NewObjectId();
        BsonValue val = new BsonObjectId(oid);
        
        await Assert.That(val.ToString()).IsEqualTo(oid.ToString());
        
        // CompareTo
        var val2 = new BsonObjectId(oid);
        await Assert.That(val.CompareTo(val2)).IsEqualTo(0);
        
        // Equals
        await Assert.That(val.Equals(val2)).IsTrue();
    }
}
