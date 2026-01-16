using TinyDb.Bson;
using System.Globalization;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Bson;

public class BsonValueInstanceTests
{
    [Test]
    public async Task BsonString_Instance_Methods()
    {
        var val = new BsonString("123");
        await Assert.That(val.ToDouble(CultureInfo.InvariantCulture)).IsEqualTo(123.0);
        await Assert.That(val.ToDecimal(CultureInfo.InvariantCulture)).IsEqualTo(123m);
        await Assert.That(val.ToInt32(CultureInfo.InvariantCulture)).IsEqualTo(123);
        await Assert.That(val.GetTypeCode()).IsEqualTo(TypeCode.String);
    }

    [Test]
    public async Task BsonInt32_Instance_Methods()
    {
        var val = new BsonInt32(1);
        await Assert.That(val.ToBoolean(null)).IsTrue();
        await Assert.That(val.ToByte(null)).IsEqualTo((byte)1);
        await Assert.That(val.ToChar(null)).IsEqualTo((char)1);
        await Assert.That(val.GetTypeCode()).IsEqualTo(TypeCode.Int32);
    }

    [Test]
    public async Task BsonBoolean_Instance_Methods()
    {
        var val = new BsonBoolean(true);
        await Assert.That(val.ToBoolean(null)).IsTrue();
        await Assert.That(val.ToString(null)).IsEqualTo("true");
        await Assert.That(val.ToByte(null)).IsEqualTo((byte)1);
    }
}
