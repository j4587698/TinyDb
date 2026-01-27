using System;
using System.Globalization;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Bson;

/// <summary>
/// Comprehensive tests for BsonDateTime to improve coverage
/// </summary>
public class BsonDateTimeFullTests
{
    #region Constructor and Basic Properties

    [Test]
    public async Task Constructor_ShouldSetValue()
    {
        var dt = new DateTime(2024, 1, 15, 10, 30, 45, DateTimeKind.Utc);
        var bsonDt = new BsonDateTime(dt);

        await Assert.That(bsonDt.Value).IsEqualTo(dt);
        await Assert.That(bsonDt.BsonType).IsEqualTo(BsonType.DateTime);
        await Assert.That(bsonDt.RawValue).IsEqualTo(dt);
    }

    [Test]
    public async Task ImplicitConversion_FromDateTime_ShouldWork()
    {
        var dt = new DateTime(2024, 1, 15);
        BsonDateTime bsonDt = dt;

        await Assert.That(bsonDt.Value).IsEqualTo(dt);
    }

    [Test]
    public async Task ImplicitConversion_ToDateTime_ShouldWork()
    {
        var bsonDt = new BsonDateTime(new DateTime(2024, 1, 15));
        DateTime dt = bsonDt;

        await Assert.That(dt).IsEqualTo(bsonDt.Value);
    }

    #endregion

    #region CompareTo Tests

    [Test]
    public async Task CompareTo_SameValue_ShouldReturnZero()
    {
        var dt = new DateTime(2024, 1, 15);
        var bsonDt1 = new BsonDateTime(dt);
        var bsonDt2 = new BsonDateTime(dt);

        await Assert.That(bsonDt1.CompareTo(bsonDt2)).IsEqualTo(0);
    }

    [Test]
    public async Task CompareTo_EarlierDate_ShouldReturnNegative()
    {
        var bsonDt1 = new BsonDateTime(new DateTime(2024, 1, 1));
        var bsonDt2 = new BsonDateTime(new DateTime(2024, 12, 31));

        await Assert.That(bsonDt1.CompareTo(bsonDt2)).IsLessThan(0);
    }

    [Test]
    public async Task CompareTo_LaterDate_ShouldReturnPositive()
    {
        var bsonDt1 = new BsonDateTime(new DateTime(2024, 12, 31));
        var bsonDt2 = new BsonDateTime(new DateTime(2024, 1, 1));

        await Assert.That(bsonDt1.CompareTo(bsonDt2)).IsGreaterThan(0);
    }

    [Test]
    public async Task CompareTo_Null_ShouldReturn1()
    {
        var bsonDt = new BsonDateTime(DateTime.Now);
        await Assert.That(bsonDt.CompareTo(null)).IsEqualTo(1);
    }

    [Test]
    public async Task CompareTo_DifferentBsonType_ShouldCompareByType()
    {
        var bsonDt = new BsonDateTime(DateTime.Now);
        var bsonStr = new BsonString("test");

        var result = bsonDt.CompareTo(bsonStr);
        await Assert.That(result).IsNotEqualTo(0);
    }

    #endregion

    #region Equals Tests

    [Test]
    public async Task Equals_SameValue_ShouldReturnTrue()
    {
        var dt = new DateTime(2024, 1, 15, 10, 30, 45);
        var bsonDt1 = new BsonDateTime(dt);
        var bsonDt2 = new BsonDateTime(dt);

        await Assert.That(bsonDt1.Equals(bsonDt2)).IsTrue();
    }

    [Test]
    public async Task Equals_DifferentValue_ShouldReturnFalse()
    {
        var bsonDt1 = new BsonDateTime(new DateTime(2024, 1, 1));
        var bsonDt2 = new BsonDateTime(new DateTime(2024, 1, 2));

        await Assert.That(bsonDt1.Equals(bsonDt2)).IsFalse();
    }

    [Test]
    public async Task Equals_DifferentType_ShouldReturnFalse()
    {
        var bsonDt = new BsonDateTime(DateTime.Now);
        var bsonStr = new BsonString("test");

        await Assert.That(bsonDt.Equals(bsonStr)).IsFalse();
    }

    #endregion

    #region GetHashCode Tests

    [Test]
    public async Task GetHashCode_SameValue_ShouldBeSame()
    {
        var dt = new DateTime(2024, 1, 15);
        var bsonDt1 = new BsonDateTime(dt);
        var bsonDt2 = new BsonDateTime(dt);

        await Assert.That(bsonDt1.GetHashCode()).IsEqualTo(bsonDt2.GetHashCode());
    }

    #endregion

    #region ToString Tests

    [Test]
    public async Task ToString_ShouldReturnIsoFormat()
    {
        var dt = new DateTime(2024, 1, 15, 10, 30, 45, 123, DateTimeKind.Utc);
        var bsonDt = new BsonDateTime(dt);

        var str = bsonDt.ToString();
        await Assert.That(str).IsEqualTo("2024-01-15T10:30:45.123Z");
    }

    #endregion

    #region IConvertible Tests

    [Test]
    public async Task GetTypeCode_ShouldReturnDateTime()
    {
        var bsonDt = new BsonDateTime(DateTime.Now);
        await Assert.That(bsonDt.GetTypeCode()).IsEqualTo(TypeCode.DateTime);
    }

    [Test]
    public async Task ToDateTime_ShouldReturnValue()
    {
        var dt = new DateTime(2024, 1, 15);
        var bsonDt = new BsonDateTime(dt);

        await Assert.That(bsonDt.ToDateTime(null)).IsEqualTo(dt);
    }

    [Test]
    public async Task ToInt64_ShouldReturnTicks()
    {
        var dt = new DateTime(2024, 1, 15);
        var bsonDt = new BsonDateTime(dt);

        await Assert.That(bsonDt.ToInt64(null)).IsEqualTo(dt.Ticks);
    }

    [Test]
    public async Task ToStringWithProvider_ShouldReturnIsoFormat()
    {
        var dt = new DateTime(2024, 1, 15, 10, 30, 45, 123, DateTimeKind.Utc);
        var bsonDt = new BsonDateTime(dt);

        await Assert.That(bsonDt.ToString(CultureInfo.InvariantCulture)).IsEqualTo("2024-01-15T10:30:45.123Z");
    }

    [Test]
    public async Task ToType_DateTime_ShouldReturnValue()
    {
        var dt = new DateTime(2024, 1, 15);
        var bsonDt = new BsonDateTime(dt);

        var result = bsonDt.ToType(typeof(DateTime), null);
        await Assert.That(result).IsEqualTo(dt);
    }

    [Test]
    public async Task ToType_BsonDateTime_ShouldReturnSelf()
    {
        var bsonDt = new BsonDateTime(DateTime.Now);
        var result = bsonDt.ToType(typeof(BsonDateTime), null);
        await Assert.That(result).IsEqualTo(bsonDt);
    }

    [Test]
    public async Task ToType_Object_ShouldReturnSelf()
    {
        var bsonDt = new BsonDateTime(DateTime.Now);
        var result = bsonDt.ToType(typeof(object), null);
        await Assert.That(result).IsEqualTo(bsonDt);
    }

    [Test]
    public async Task ToType_String_ShouldReturnString()
    {
        var dt = new DateTime(2024, 1, 15, 10, 30, 45, 123, DateTimeKind.Utc);
        var bsonDt = new BsonDateTime(dt);
        var result = bsonDt.ToType(typeof(string), null);
        await Assert.That(result).IsEqualTo("2024-01-15T10:30:45.123Z");
    }

    [Test]
    public async Task ToType_Long_ShouldConvert()
    {
        var dt = new DateTime(2024, 1, 15);
        var bsonDt = new BsonDateTime(dt);
        // ToInt64 returns Ticks for DateTime
        var result = bsonDt.ToInt64(null);
        await Assert.That(result).IsEqualTo(dt.Ticks);
    }

    // Note: ToBoolean, ToByte, ToChar, ToDecimal, ToDouble, ToInt16, ToInt32, ToSByte, ToSingle
    // ToUInt16, ToUInt32, ToUInt64 all use Convert.To* which throws InvalidCastException for DateTime

    [Test]
    public async Task ToBoolean_ShouldThrow()
    {
        var bsonDt = new BsonDateTime(DateTime.Now);
        await Assert.That(() => bsonDt.ToBoolean(null)).Throws<InvalidCastException>();
    }

    [Test]
    public async Task ToByte_ShouldThrow()
    {
        var bsonDt = new BsonDateTime(DateTime.Now);
        await Assert.That(() => bsonDt.ToByte(null)).Throws<InvalidCastException>();
    }

    [Test]
    public async Task ToChar_ShouldThrow()
    {
        var bsonDt = new BsonDateTime(DateTime.Now);
        await Assert.That(() => bsonDt.ToChar(null)).Throws<InvalidCastException>();
    }

    [Test]
    public async Task ToDecimal_ShouldThrow()
    {
        var bsonDt = new BsonDateTime(DateTime.Now);
        await Assert.That(() => bsonDt.ToDecimal(null)).Throws<InvalidCastException>();
    }

    [Test]
    public async Task ToDouble_ShouldThrow()
    {
        var bsonDt = new BsonDateTime(DateTime.Now);
        await Assert.That(() => bsonDt.ToDouble(null)).Throws<InvalidCastException>();
    }

    [Test]
    public async Task ToInt16_ShouldThrow()
    {
        var bsonDt = new BsonDateTime(DateTime.Now);
        await Assert.That(() => bsonDt.ToInt16(null)).Throws<InvalidCastException>();
    }

    [Test]
    public async Task ToInt32_ShouldThrow()
    {
        var bsonDt = new BsonDateTime(DateTime.Now);
        await Assert.That(() => bsonDt.ToInt32(null)).Throws<InvalidCastException>();
    }

    [Test]
    public async Task ToSByte_ShouldThrow()
    {
        var bsonDt = new BsonDateTime(DateTime.Now);
        await Assert.That(() => bsonDt.ToSByte(null)).Throws<InvalidCastException>();
    }

    [Test]
    public async Task ToSingle_ShouldThrow()
    {
        var bsonDt = new BsonDateTime(DateTime.Now);
        await Assert.That(() => bsonDt.ToSingle(null)).Throws<InvalidCastException>();
    }

    [Test]
    public async Task ToUInt16_ShouldThrow()
    {
        var bsonDt = new BsonDateTime(DateTime.Now);
        await Assert.That(() => bsonDt.ToUInt16(null)).Throws<InvalidCastException>();
    }

    [Test]
    public async Task ToUInt32_ShouldThrow()
    {
        var bsonDt = new BsonDateTime(DateTime.Now);
        await Assert.That(() => bsonDt.ToUInt32(null)).Throws<InvalidCastException>();
    }

    [Test]
    public async Task ToUInt64_ShouldThrow()
    {
        var bsonDt = new BsonDateTime(DateTime.Now);
        await Assert.That(() => bsonDt.ToUInt64(null)).Throws<InvalidCastException>();
    }

    #endregion
}
