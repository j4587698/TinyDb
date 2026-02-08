using System;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Bson;

/// <summary>
/// Complete tests for BsonNull class, covering all ToType branches
/// </summary>
public class BsonNullCompleteTests
{
    #region Basic Properties
    
    [Test]
    public async Task BsonType_ShouldBeNull()
    {
        await Assert.That(BsonNull.Value.BsonType).IsEqualTo(BsonType.Null);
    }

    [Test]
    public async Task RawValue_ShouldBeNull()
    {
        await Assert.That(BsonNull.Value.RawValue).IsNull();
    }

    [Test]
    public async Task IsNull_ShouldBeTrue()
    {
        await Assert.That(BsonNull.Value.IsNull).IsTrue();
    }

    [Test]
    public async Task Value_ShouldBeSingleton()
    {
        var n1 = BsonNull.Value;
        var n2 = BsonNull.Value;
        await Assert.That(ReferenceEquals(n1, n2)).IsTrue();
    }
    
    #endregion

    #region CompareTo Tests
    
    [Test]
    public async Task CompareTo_Null_ShouldReturnZero()
    {
        await Assert.That(BsonNull.Value.CompareTo(null)).IsEqualTo(0);
    }

    [Test]
    public async Task CompareTo_BsonNull_ShouldReturnZero()
    {
        await Assert.That(BsonNull.Value.CompareTo(BsonNull.Value)).IsEqualTo(0);
    }

    [Test]
    public async Task CompareTo_NonNull_ShouldReturnNegative()
    {
        await Assert.That(BsonNull.Value.CompareTo(new BsonInt32(1))).IsLessThan(0);
        await Assert.That(BsonNull.Value.CompareTo(new BsonString("test"))).IsLessThan(0);
        await Assert.That(BsonNull.Value.CompareTo(new BsonBoolean(false))).IsLessThan(0);
    }
    
    #endregion

    #region Equals Tests
    
    [Test]
    public async Task Equals_Null_ShouldReturnTrue()
    {
        await Assert.That(BsonNull.Value.Equals(null)).IsTrue();
    }

    [Test]
    public async Task Equals_BsonNull_ShouldReturnTrue()
    {
        await Assert.That(BsonNull.Value.Equals(BsonNull.Value)).IsTrue();
    }

    [Test]
    public async Task Equals_NonNull_ShouldReturnFalse()
    {
        await Assert.That(BsonNull.Value.Equals(new BsonInt32(0))).IsFalse();
    }
    
    #endregion

    #region GetHashCode and ToString
    
    [Test]
    public async Task GetHashCode_ShouldReturnZero()
    {
        await Assert.That(BsonNull.Value.GetHashCode()).IsEqualTo(0);
    }

    [Test]
    public async Task ToString_ShouldReturnNull()
    {
        await Assert.That(BsonNull.Value.ToString()).IsEqualTo("null");
    }
    
    #endregion

    #region IConvertible Basic Methods
    
    [Test]
    public async Task GetTypeCode_ShouldReturnEmpty()
    {
        await Assert.That(BsonNull.Value.GetTypeCode()).IsEqualTo(TypeCode.Empty);
    }

    [Test]
    public async Task ToBoolean_ShouldReturnFalse()
    {
        await Assert.That(BsonNull.Value.ToBoolean(null)).IsFalse();
    }

    [Test]
    public async Task ToByte_ShouldReturnZero()
    {
        await Assert.That(BsonNull.Value.ToByte(null)).IsEqualTo((byte)0);
    }

    [Test]
    public async Task ToChar_ShouldReturnNullChar()
    {
        await Assert.That(BsonNull.Value.ToChar(null)).IsEqualTo('\0');
    }

    [Test]
    public async Task ToDateTime_ShouldReturnDefault()
    {
        await Assert.That(BsonNull.Value.ToDateTime(null)).IsEqualTo(default(DateTime));
    }

    [Test]
    public async Task ToDecimal_ShouldReturnZero()
    {
        await Assert.That(BsonNull.Value.ToDecimal(null)).IsEqualTo(0m);
    }

    [Test]
    public async Task ToDouble_ShouldReturnZero()
    {
        await Assert.That(BsonNull.Value.ToDouble(null)).IsEqualTo(0.0);
    }

    [Test]
    public async Task ToInt16_ShouldReturnZero()
    {
        await Assert.That(BsonNull.Value.ToInt16(null)).IsEqualTo((short)0);
    }

    [Test]
    public async Task ToInt32_ShouldReturnZero()
    {
        await Assert.That(BsonNull.Value.ToInt32(null)).IsEqualTo(0);
    }

    [Test]
    public async Task ToInt64_ShouldReturnZero()
    {
        await Assert.That(BsonNull.Value.ToInt64(null)).IsEqualTo(0L);
    }

    [Test]
    public async Task ToSByte_ShouldReturnZero()
    {
        await Assert.That(BsonNull.Value.ToSByte(null)).IsEqualTo((sbyte)0);
    }

    [Test]
    public async Task ToSingle_ShouldReturnZero()
    {
        await Assert.That(BsonNull.Value.ToSingle(null)).IsEqualTo(0.0f);
    }

    [Test]
    public async Task ToString_WithProvider_ShouldReturnNull()
    {
        await Assert.That(BsonNull.Value.ToString(null)).IsEqualTo("null");
    }

    [Test]
    public async Task ToUInt16_ShouldReturnZero()
    {
        await Assert.That(BsonNull.Value.ToUInt16(null)).IsEqualTo((ushort)0);
    }

    [Test]
    public async Task ToUInt32_ShouldReturnZero()
    {
        await Assert.That(BsonNull.Value.ToUInt32(null)).IsEqualTo(0u);
    }

    [Test]
    public async Task ToUInt64_ShouldReturnZero()
    {
        await Assert.That(BsonNull.Value.ToUInt64(null)).IsEqualTo(0UL);
    }
    
    #endregion

    #region ToType - All Branch Coverage
    
    [Test]
    public async Task ToType_Object_ShouldReturnNull()
    {
        var result = BsonNull.Value.ToType(typeof(object), null);
        await Assert.That(result).IsNull();
    }

    [Test]
    public async Task ToType_BsonNull_ShouldReturnSelf()
    {
        var result = BsonNull.Value.ToType(typeof(BsonNull), null);
        await Assert.That(result).IsEqualTo(BsonNull.Value);
    }

    [Test]
    public async Task ToType_NullableInt_ShouldReturnNull()
    {
        var result = BsonNull.Value.ToType(typeof(int?), null);
        await Assert.That(result).IsNull();
    }

    [Test]
    public async Task ToType_NullableLong_ShouldReturnNull()
    {
        var result = BsonNull.Value.ToType(typeof(long?), null);
        await Assert.That(result).IsNull();
    }

    [Test]
    public async Task ToType_NullableDecimal_ShouldReturnNull()
    {
        var result = BsonNull.Value.ToType(typeof(decimal?), null);
        await Assert.That(result).IsNull();
    }

    [Test]
    public async Task ToType_String_ShouldReturnNull()
    {
        var result = BsonNull.Value.ToType(typeof(string), null);
        await Assert.That(result).IsNull();
    }

    [Test]
    public async Task ToType_ReferenceType_ShouldReturnNull()
    {
        var result = BsonNull.Value.ToType(typeof(int[]), null);
        await Assert.That(result).IsNull();
    }

    [Test]
    public async Task ToType_Enum_ShouldReturnDefault()
    {
        var result = BsonNull.Value.ToType(typeof(DayOfWeek), null);
        await Assert.That(result).IsEqualTo(DayOfWeek.Sunday);
    }

    [Test]
    public async Task ToType_CustomEnum_ShouldReturnDefault()
    {
        var result = BsonNull.Value.ToType(typeof(TestEnum), null);
        await Assert.That(result).IsEqualTo(TestEnum.Default);
    }

    [Test]
    public async Task ToType_Guid_ShouldReturnEmpty()
    {
        var result = BsonNull.Value.ToType(typeof(Guid), null);
        await Assert.That(result).IsEqualTo(Guid.Empty);
    }

    [Test]
    public async Task ToType_ObjectId_ShouldReturnEmpty()
    {
        var result = BsonNull.Value.ToType(typeof(ObjectId), null);
        await Assert.That(result).IsEqualTo(ObjectId.Empty);
    }

    [Test]
    public async Task ToType_DateTime_ShouldReturnDefault()
    {
        var result = BsonNull.Value.ToType(typeof(DateTime), null);
        await Assert.That(result).IsEqualTo(default(DateTime));
    }

    [Test]
    public async Task ToType_DateTimeOffset_ShouldReturnDefault()
    {
        var result = BsonNull.Value.ToType(typeof(DateTimeOffset), null);
        await Assert.That(result).IsEqualTo(default(DateTimeOffset));
    }

    [Test]
    public async Task ToType_TimeSpan_ShouldReturnDefault()
    {
        var result = BsonNull.Value.ToType(typeof(TimeSpan), null);
        await Assert.That(result).IsEqualTo(default(TimeSpan));
    }

    // TypeCode switch branches
    [Test]
    public async Task ToType_Boolean_ShouldReturnFalse()
    {
        var result = BsonNull.Value.ToType(typeof(bool), null);
        await Assert.That(result).IsEqualTo(false);
    }

    [Test]
    public async Task ToType_Byte_ShouldReturnZero()
    {
        var result = BsonNull.Value.ToType(typeof(byte), null);
        await Assert.That(result).IsEqualTo((byte)0);
    }

    [Test]
    public async Task ToType_SByte_ShouldReturnZero()
    {
        var result = BsonNull.Value.ToType(typeof(sbyte), null);
        await Assert.That(result).IsEqualTo((sbyte)0);
    }

    [Test]
    public async Task ToType_Int16_ShouldReturnZero()
    {
        var result = BsonNull.Value.ToType(typeof(short), null);
        await Assert.That(result).IsEqualTo((short)0);
    }

    [Test]
    public async Task ToType_UInt16_ShouldReturnZero()
    {
        var result = BsonNull.Value.ToType(typeof(ushort), null);
        await Assert.That(result).IsEqualTo((ushort)0);
    }

    [Test]
    public async Task ToType_Int32_ShouldReturnZero()
    {
        var result = BsonNull.Value.ToType(typeof(int), null);
        await Assert.That(result).IsEqualTo(0);
    }

    [Test]
    public async Task ToType_UInt32_ShouldReturnZero()
    {
        var result = BsonNull.Value.ToType(typeof(uint), null);
        await Assert.That(result).IsEqualTo(0u);
    }

    [Test]
    public async Task ToType_Int64_ShouldReturnZero()
    {
        var result = BsonNull.Value.ToType(typeof(long), null);
        await Assert.That(result).IsEqualTo(0L);
    }

    [Test]
    public async Task ToType_UInt64_ShouldReturnZero()
    {
        var result = BsonNull.Value.ToType(typeof(ulong), null);
        await Assert.That(result).IsEqualTo(0UL);
    }

    [Test]
    public async Task ToType_Single_ShouldReturnZero()
    {
        var result = BsonNull.Value.ToType(typeof(float), null);
        await Assert.That(result).IsEqualTo(0f);
    }

    [Test]
    public async Task ToType_Double_ShouldReturnZero()
    {
        var result = BsonNull.Value.ToType(typeof(double), null);
        await Assert.That(result).IsEqualTo(0d);
    }

    [Test]
    public async Task ToType_Decimal_ShouldReturnZero()
    {
        var result = BsonNull.Value.ToType(typeof(decimal), null);
        await Assert.That(result).IsEqualTo(0m);
    }

    [Test]
    public async Task ToType_Char_ShouldReturnNullChar()
    {
        var result = BsonNull.Value.ToType(typeof(char), null);
        await Assert.That(result).IsEqualTo('\0');
    }

    [Test]
    public async Task ToType_UnsupportedValueType_ShouldThrow()
    {
        // DBNull is a value type that's not in the switch cases
        // Actually, let's use a struct that won't be in the TypeCode switch
        await Assert.That(() => BsonNull.Value.ToType(typeof(TestStruct), null))
            .Throws<NotSupportedException>();
    }
    
    #endregion
    
    // Test helpers
    private enum TestEnum
    {
        Default = 0,
        First = 1,
        Second = 2
    }

    private struct TestStruct
    {
    }
}
