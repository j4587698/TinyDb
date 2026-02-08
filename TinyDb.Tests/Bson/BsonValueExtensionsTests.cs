using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Bson;

public class BsonValueExtensionsTests
{
    [Test]
    public async Task ToInt32_Should_Work()
    {
        BsonValue val = new BsonInt32(42);
        await Assert.That(val.ToInt32()).IsEqualTo(42);
        await Assert.That(() => ((BsonValue)null!).ToInt32()).Throws<ArgumentNullException>();
    }

    [Test]
    public async Task ToInt64_Should_Work()
    {
        BsonValue val = new BsonInt64(42L);
        await Assert.That(val.ToInt64()).IsEqualTo(42L);
        await Assert.That(() => ((BsonValue)null!).ToInt64()).Throws<ArgumentNullException>();
    }

    [Test]
    public async Task ToBoolean_Should_Work()
    {
        BsonValue val = new BsonBoolean(true);
        await Assert.That(val.ToBoolean()).IsTrue();
        await Assert.That(() => ((BsonValue)null!).ToBoolean()).Throws<ArgumentNullException>();
    }

    [Test]
    public async Task ToDouble_Should_Work()
    {
        BsonValue val = new BsonDouble(42.0);
        await Assert.That(val.ToDouble()).IsEqualTo(42.0);
        await Assert.That(() => ((BsonValue)null!).ToDouble()).Throws<ArgumentNullException>();
    }

    [Test]
    public async Task ToString_Should_Work()
    {
        BsonValue val = new BsonString("test");
        await Assert.That(val.ToString(null)).IsEqualTo("test");
        // Extension method is shadowed by BsonValue.ToString(IFormatProvider?) abstract method.
        // So calling it on null instance throws NullReferenceException before entering extension method.
        await Assert.That(() => ((BsonValue)null!).ToString(null)).Throws<NullReferenceException>();
    }

    [Test]
    public async Task Extension_ToString_Should_Work_WhenCalledStatically()
    {
        BsonValue val = new BsonString("test");
        await Assert.That(BsonValueExtensions.ToString(val, null)).IsEqualTo("test");
        await Assert.That(() => BsonValueExtensions.ToString(null!, null)).Throws<ArgumentNullException>();
    }

    [Test]
    public async Task ToDateTime_Should_Work()
    {
        var now = DateTime.UtcNow;
        BsonValue val = new BsonDateTime(now);
        await Assert.That(val.ToDateTime()).IsEqualTo(now);
        await Assert.That(() => ((BsonValue)null!).ToDateTime()).Throws<ArgumentNullException>();
    }

    [Test]
    public async Task ToDecimal_Should_Work()
    {
        BsonValue val = new BsonDecimal128(42m);
        await Assert.That(val.ToDecimal()).IsEqualTo(42m);
        await Assert.That(() => ((BsonValue)null!).ToDecimal()).Throws<ArgumentNullException>();
    }

    [Test]
    public async Task ToSingle_Should_Work()
    {
        BsonValue val = new BsonDouble(42.0);
        await Assert.That(val.ToSingle()).IsEqualTo(42.0f);
        await Assert.That(() => ((BsonValue)null!).ToSingle()).Throws<ArgumentNullException>();
    }

    [Test]
    public async Task ToInt16_Should_Work()
    {
        BsonValue val = new BsonInt32(42);
        await Assert.That(val.ToInt16()).IsEqualTo((short)42);
        await Assert.That(() => ((BsonValue)null!).ToInt16()).Throws<ArgumentNullException>();
    }

    [Test]
    public async Task ToByte_Should_Work()
    {
        BsonValue val = new BsonInt32(42);
        await Assert.That(val.ToByte()).IsEqualTo((byte)42);
        await Assert.That(() => ((BsonValue)null!).ToByte()).Throws<ArgumentNullException>();
    }

    [Test]
    public async Task ToSByte_Should_Work()
    {
        BsonValue val = new BsonInt32(42);
        await Assert.That(val.ToSByte()).IsEqualTo((sbyte)42);
        await Assert.That(() => ((BsonValue)null!).ToSByte()).Throws<ArgumentNullException>();
    }

    [Test]
    public async Task ToChar_Should_Work()
    {
        BsonValue val = new BsonString("A");
        await Assert.That(val.ToChar()).IsEqualTo('A');
        await Assert.That(() => ((BsonValue)null!).ToChar()).Throws<ArgumentNullException>();
    }

    [Test]
    public async Task ToUInt64_Should_Work()
    {
        BsonValue val = new BsonInt64(42L);
        await Assert.That(val.ToUInt64()).IsEqualTo(42UL);
        await Assert.That(() => ((BsonValue)null!).ToUInt64()).Throws<ArgumentNullException>();
    }

    [Test]
    public async Task ToUInt16_Should_Work()
    {
        BsonValue val = new BsonInt32(42);
        await Assert.That(val.ToUInt16()).IsEqualTo((ushort)42);
        await Assert.That(() => ((BsonValue)null!).ToUInt16()).Throws<ArgumentNullException>();
    }

    [Test]
    public async Task ToUInt32_Should_Work()
    {
        BsonValue val = new BsonInt32(42);
        await Assert.That(val.ToUInt32()).IsEqualTo(42U);
        await Assert.That(() => ((BsonValue)null!).ToUInt32()).Throws<ArgumentNullException>();
    }
}
