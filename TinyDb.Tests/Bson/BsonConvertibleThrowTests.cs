using System;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Bson;

public class BsonConvertibleThrowTests
{
    [Test]
    public async Task BsonJavaScript_IConvertible_FullCoverage()
    {
        var js = new BsonJavaScript("code");
        var c = (IConvertible)js;
        
        await Assert.That(c.ToBoolean(null)).IsTrue();
        await Assert.That(c.ToString(null)).IsEqualTo("code");
        await Assert.That(c.GetTypeCode()).IsEqualTo(TypeCode.String);
        await Assert.That(c.ToType(typeof(string), null)).IsEqualTo("code");

        await Assert.That(() => c.ToByte(null)).Throws<InvalidCastException>();
        await Assert.That(() => c.ToChar(null)).Throws<InvalidCastException>();
        await Assert.That(() => c.ToDateTime(null)).Throws<InvalidCastException>();
        await Assert.That(() => c.ToDecimal(null)).Throws<InvalidCastException>();
        await Assert.That(() => c.ToDouble(null)).Throws<InvalidCastException>();
        await Assert.That(() => c.ToInt16(null)).Throws<InvalidCastException>();
        await Assert.That(() => c.ToInt32(null)).Throws<InvalidCastException>();
        await Assert.That(() => c.ToInt64(null)).Throws<InvalidCastException>();
        await Assert.That(() => c.ToSByte(null)).Throws<InvalidCastException>();
        await Assert.That(() => c.ToSingle(null)).Throws<InvalidCastException>();
        await Assert.That(() => c.ToUInt16(null)).Throws<InvalidCastException>();
        await Assert.That(() => c.ToUInt32(null)).Throws<InvalidCastException>();
        await Assert.That(() => c.ToUInt64(null)).Throws<InvalidCastException>();
        await Assert.That(() => c.ToType(typeof(int), null)).Throws<InvalidCastException>();
    }

    [Test]
    public async Task BsonSymbol_IConvertible_FullCoverage()
    {
        var sym = new BsonSymbol("sym");
        var c = (IConvertible)sym;
        
        await Assert.That(c.ToBoolean(null)).IsTrue();
        await Assert.That(c.ToString(null)).IsEqualTo("sym");
        await Assert.That(c.GetTypeCode()).IsEqualTo(TypeCode.String);

        await Assert.That(() => c.ToByte(null)).Throws<InvalidCastException>();
        await Assert.That(() => c.ToChar(null)).Throws<InvalidCastException>();
        await Assert.That(() => c.ToDateTime(null)).Throws<InvalidCastException>();
        await Assert.That(() => c.ToDecimal(null)).Throws<InvalidCastException>();
        await Assert.That(() => c.ToDouble(null)).Throws<InvalidCastException>();
        await Assert.That(() => c.ToInt16(null)).Throws<InvalidCastException>();
        await Assert.That(() => c.ToInt32(null)).Throws<InvalidCastException>();
        await Assert.That(() => c.ToInt64(null)).Throws<InvalidCastException>();
        await Assert.That(() => c.ToSByte(null)).Throws<InvalidCastException>();
        await Assert.That(() => c.ToSingle(null)).Throws<InvalidCastException>();
        await Assert.That(() => c.ToUInt16(null)).Throws<InvalidCastException>();
        await Assert.That(() => c.ToUInt32(null)).Throws<InvalidCastException>();
        await Assert.That(() => c.ToUInt64(null)).Throws<InvalidCastException>();
    }
}
