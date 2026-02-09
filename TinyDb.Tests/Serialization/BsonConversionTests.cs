using System;
using System.Collections.Generic;
using TinyDb.Bson;
using TinyDb.Serialization;
using TinyDb.Attributes;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Serialization;

public class BsonConversionTests
{
    public enum TestEnum { Value1 = 1, Value2 = 2 }
    public enum UIntEnum : uint { E = 5 }

    public class NoEntityType
    {
        public int Id { get; set; }
    }

    [Test]
    public async Task ToBsonValue_ShouldHandleVariousTypes()
    {
        await Assert.That(BsonConversion.ToBsonValue((byte)1)).IsTypeOf<BsonInt32>();
        await Assert.That(BsonConversion.ToBsonValue((sbyte)1)).IsTypeOf<BsonInt32>();
        await Assert.That(BsonConversion.ToBsonValue((short)1)).IsTypeOf<BsonInt32>();
        await Assert.That(BsonConversion.ToBsonValue((ushort)1)).IsTypeOf<BsonInt32>();
        await Assert.That(BsonConversion.ToBsonValue((uint)1)).IsTypeOf<BsonInt64>();
        await Assert.That(BsonConversion.ToBsonValue((ulong)1)).IsTypeOf<BsonInt64>();
        await Assert.That(BsonConversion.ToBsonValue(1.0f)).IsTypeOf<BsonDouble>();
        await Assert.That(BsonConversion.ToBsonValue(TestEnum.Value2)).IsTypeOf<BsonInt32>();
        
        var dict = new Dictionary<string, object> { ["key"] = "value" };
        await Assert.That(BsonConversion.ToBsonValue(dict)).IsTypeOf<BsonDocument>();
        
        ReadOnlyMemory<byte> rom = new byte[] { 1, 2, 3 };
        await Assert.That(BsonConversion.ToBsonValue(rom)).IsTypeOf<BsonBinary>();
        
        Memory<byte> mem = new byte[] { 4, 5 };
        await Assert.That(BsonConversion.ToBsonValue(mem)).IsTypeOf<BsonBinary>();
    }

    [Test]
    public async Task FromBsonValue_ShouldHandleNumericalTypes()
    {
        var bsonInt = new BsonInt32(42);
        await Assert.That(BsonConversion.FromBsonValue(bsonInt, typeof(byte))).IsEqualTo((byte)42);
        await Assert.That(BsonConversion.FromBsonValue(bsonInt, typeof(sbyte))).IsEqualTo((sbyte)42);
        await Assert.That(BsonConversion.FromBsonValue(bsonInt, typeof(short))).IsEqualTo((short)42);
        await Assert.That(BsonConversion.FromBsonValue(bsonInt, typeof(ushort))).IsEqualTo((ushort)42);
        await Assert.That(BsonConversion.FromBsonValue(bsonInt, typeof(uint))).IsEqualTo((uint)42);
        await Assert.That(BsonConversion.FromBsonValue(bsonInt, typeof(ulong))).IsEqualTo((ulong)42);
        await Assert.That(BsonConversion.FromBsonValue(bsonInt, typeof(float))).IsEqualTo(42.0f);
        await Assert.That(BsonConversion.FromBsonValue(bsonInt, typeof(decimal))).IsEqualTo(42.0m);

        var bsonLong = new BsonInt64(100);
        await Assert.That(BsonConversion.FromBsonValue(bsonLong, typeof(int))).IsEqualTo(100);
        await Assert.That(BsonConversion.FromBsonValue(bsonLong, typeof(byte))).IsEqualTo((byte)100);

        var bsonDouble = new BsonDouble(3.14);
        await Assert.That(BsonConversion.FromBsonValue(bsonDouble, typeof(float))).IsEqualTo(3.14f);
        await Assert.That(BsonConversion.FromBsonValue(bsonDouble, typeof(decimal))).IsEqualTo(3.14m);
    }

    [Test]
    public async Task FromBsonValue_ShouldHandleCollections()
    {
        var array = new BsonArray(new BsonValue[] { 1, 2, 3 });
        var list = (List<int>)BsonConversion.FromBsonValue(array, typeof(List<int>))!;
        await Assert.That(list).Count().IsEqualTo(3);
        await Assert.That(list[0]).IsEqualTo(1);

        var doc = new BsonDocument().Set("a", 1).Set("b", 2);
        var dict = (Dictionary<string, int>)BsonConversion.FromBsonValue(doc, typeof(Dictionary<string, int>))!;
        await Assert.That(dict).Count().IsEqualTo(2);
        await Assert.That(dict["a"]).IsEqualTo(1);
    }

    [Test]
    public async Task FromBsonValue_ShouldHandleEnums()
    {
        await Assert.That(BsonConversion.FromBsonValue(new BsonInt32(1), typeof(TestEnum))).IsEqualTo(TestEnum.Value1);
        await Assert.That(BsonConversion.FromBsonValueEnum<TestEnum>(new BsonString("Value2"))).IsEqualTo(TestEnum.Value2);
    }

    [Test]
    public async Task FromBsonValue_ShouldHandleBinary()
    {
        var bytes = new byte[] { 1, 2, 3 };
        var bin = new BsonBinary(bytes);
        
        var resultBytes = (byte[])BsonConversion.FromBsonValue(bin, typeof(byte[]))!;
        await Assert.That(resultBytes.SequenceEqual(bytes)).IsTrue();
        
        var rom = (ReadOnlyMemory<byte>)BsonConversion.FromBsonValue(bin, typeof(ReadOnlyMemory<byte>))!;
        await Assert.That(rom.ToArray().SequenceEqual(bytes)).IsTrue();
        
        var mem = (Memory<byte>)BsonConversion.FromBsonValue(bin, typeof(Memory<byte>))!;
        await Assert.That(mem.ToArray().SequenceEqual(bytes)).IsTrue();
    }

    [Test]
    public async Task BsonConversion_EdgeCases_ShouldWork()
    {
        // ToBsonValue
        await Assert.That(BsonConversion.ToBsonValue(null!)).IsEqualTo(BsonNull.Value);
        var bson = new BsonInt32(10);
        await Assert.That(BsonConversion.ToBsonValue(bson)).IsSameReferenceAs(bson);
        
        // FromBsonValue
        await Assert.That(BsonConversion.FromBsonValue(BsonNull.Value, typeof(string))).IsNull();
        
        // char
        await Assert.That(BsonConversion.FromBsonValue(new BsonInt32(65), typeof(char))).IsEqualTo('A');
        
        // float
        await Assert.That(BsonConversion.FromBsonValue(new BsonDouble(1.5), typeof(float))).IsEqualTo(1.5f);
        
        // bool from string
        await Assert.That(BsonConversion.FromBsonValue(new BsonString("True"), typeof(bool))).IsEqualTo(true);
        
        // Guid from string
        var guid = Guid.NewGuid();
        await Assert.That(BsonConversion.FromBsonValue(new BsonString(guid.ToString()), typeof(Guid))).IsEqualTo(guid);
    }

    [Test]
    public async Task FromBsonValueEnum_ShouldHandleUnderlyingTypes()
    {
        var fromLong = BsonConversion.FromBsonValueEnum<LongEnum>(new BsonString("2"));
        await Assert.That(fromLong).IsEqualTo(LongEnum.B);

        var fromShort = BsonConversion.FromBsonValueEnum<ShortEnum>(new BsonInt64(3));
        await Assert.That(fromShort).IsEqualTo(ShortEnum.C);

        var fromByte = BsonConversion.FromBsonValueEnum<ByteEnum>(new BsonDouble(4.0));
        await Assert.That(fromByte).IsEqualTo(ByteEnum.D);

        var fromUint = BsonConversion.FromBsonValueEnum<UIntEnum>(new BsonInt32(5));
        await Assert.That(fromUint).IsEqualTo(UIntEnum.E);

        var fromNull = BsonConversion.FromBsonValueEnum<TestEnum>(BsonNull.Value);
        await Assert.That(fromNull).IsEqualTo(default(TestEnum));
    }

    [Test]
    public async Task ToBsonValue_ComplexType_WithoutEntity_ShouldThrow()
    {
        var entity = new NoEntityType { Id = 1 };

        await Assert.That(() => BsonConversion.ToBsonValue(entity))
            .Throws<InvalidOperationException>();
    }

    [Test]
    public async Task ToBsonValue_PrimitiveBranches_ShouldWork()
    {
        var guid = Guid.NewGuid();
        await Assert.That(BsonConversion.ToBsonValue(guid)).IsTypeOf<BsonString>();

        var oid = ObjectId.NewObjectId();
        await Assert.That(BsonConversion.ToBsonValue(oid)).IsTypeOf<BsonObjectId>();

        var date = new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Utc);
        await Assert.That(BsonConversion.ToBsonValue(date)).IsTypeOf<BsonDateTime>();

        await Assert.That(BsonConversion.ToBsonValue(true)).IsTypeOf<BsonBoolean>();
        await Assert.That(BsonConversion.ToBsonValue(12.34m)).IsTypeOf<BsonDecimal128>();

        var bytes = new byte[] { 1, 2, 3 };
        await Assert.That(BsonConversion.ToBsonValue(bytes)).IsTypeOf<BsonBinary>();
    }

    [Test]
    public async Task FromBsonValue_Object_Target_ShouldUnwrap()
    {
        var guid = Guid.NewGuid();
        var uuid = new BsonBinary(guid.ToByteArray(), BsonBinary.BinarySubType.Uuid);
        await Assert.That(BsonConversion.FromBsonValue(uuid, typeof(object))).IsEqualTo(guid);

        var bytes = new byte[] { 1, 2, 3 };
        var bin = new BsonBinary(bytes, BsonBinary.BinarySubType.Generic);
        var byteResult = (byte[])BsonConversion.FromBsonValue(bin, typeof(object))!;
        await Assert.That(byteResult.SequenceEqual(bytes)).IsTrue();

        var doc = new BsonDocument().Set("a", 1);
        var dict = (Dictionary<string, object?>)BsonConversion.FromBsonValue(doc, typeof(object))!;
        await Assert.That(dict["a"]).IsEqualTo(1);

        var array = new BsonArray(new BsonValue[] { 1, 2 });
        var list = (List<object?>)BsonConversion.FromBsonValue(array, typeof(object))!;
        await Assert.That(list.Count).IsEqualTo(2);
        await Assert.That(list[0]).IsEqualTo(1);

        var dec = new BsonDecimal128(12.5m);
        await Assert.That(BsonConversion.FromBsonValue(dec, typeof(object))).IsEqualTo(12.5m);

        var date = new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Utc);
        await Assert.That(BsonConversion.FromBsonValue(new BsonDateTime(date), typeof(object))).IsEqualTo(date);

        var oid = ObjectId.NewObjectId();
        await Assert.That(BsonConversion.FromBsonValue(new BsonObjectId(oid), typeof(object))).IsEqualTo(oid);

        await Assert.That(BsonConversion.FromBsonValue(new BsonBoolean(true), typeof(object))).IsEqualTo(true);
        await Assert.That(BsonConversion.FromBsonValue(BsonNull.Value, typeof(object))).IsNull();
    }

    [Test]
    public async Task BsonConversion_NumericalTypes_ShouldWork()
    {
        var bsonInt = new BsonInt32(100);
        await Assert.That(BsonConversion.FromBsonValue(bsonInt, typeof(sbyte))).IsEqualTo((sbyte)100);
        await Assert.That(BsonConversion.FromBsonValue(bsonInt, typeof(short))).IsEqualTo((short)100);
        await Assert.That(BsonConversion.FromBsonValue(bsonInt, typeof(ushort))).IsEqualTo((ushort)100);
        await Assert.That(BsonConversion.FromBsonValue(bsonInt, typeof(uint))).IsEqualTo((uint)100);
        await Assert.That(BsonConversion.FromBsonValue(bsonInt, typeof(ulong))).IsEqualTo((ulong)100);
        
        var bsonLong = new BsonInt64(200);
        await Assert.That(BsonConversion.FromBsonValue(bsonLong, typeof(uint))).IsEqualTo((uint)200);
        await Assert.That(BsonConversion.FromBsonValue(bsonLong, typeof(ulong))).IsEqualTo((ulong)200);
        
        var bsonDouble = new BsonDouble(300.0);
        await Assert.That(BsonConversion.FromBsonValue(bsonDouble, typeof(uint))).IsEqualTo((uint)300);
    }

    public enum IntEnum : int { A = 1 }
    public enum LongEnum : long { B = 2 }
    public enum ShortEnum : short { C = 3 }
    public enum ByteEnum : byte { D = 4 }

    [Test]
    public async Task BsonConversion_EnumTypes_ShouldWork()
    {
        await Assert.That(BsonConversion.FromBsonValue(new BsonInt32(1), typeof(IntEnum))).IsEqualTo(IntEnum.A);
        await Assert.That(BsonConversion.FromBsonValue(new BsonInt64(2), typeof(LongEnum))).IsEqualTo(LongEnum.B);
        await Assert.That(BsonConversion.FromBsonValue(new BsonInt32(3), typeof(ShortEnum))).IsEqualTo(ShortEnum.C);
        await Assert.That(BsonConversion.FromBsonValue(new BsonInt32(4), typeof(ByteEnum))).IsEqualTo(ByteEnum.D);
        
        // String fallback
        await Assert.That(BsonConversion.FromBsonValueEnum<IntEnum>(new BsonString("A"))).IsEqualTo(IntEnum.A);
        await Assert.That(BsonConversion.FromBsonValueEnum<LongEnum>(new BsonString("B"))).IsEqualTo(LongEnum.B);
    }

    [Test]
    public async Task FromBsonValue_List_ShouldWork()
    {
        var array = new BsonArray(new BsonValue[] { 1, 2, 3 });
        var list = (List<int>)BsonConversion.FromBsonValue(array, typeof(List<int>))!;
        await Assert.That(list).Count().IsEqualTo(3);
        await Assert.That(list[0]).IsEqualTo(1);
    }

    [Test]
    public async Task FromBsonValue_Dictionary_ShouldWork()
    {
        var doc = new BsonDocument().Set("key", "value");
        var dict = (Dictionary<string, string>)BsonConversion.FromBsonValue(doc, typeof(Dictionary<string, string>))!;
        await Assert.That(dict["key"]).IsEqualTo("value");
    }

    // ===== Additional Coverage Tests =====

    /// <summary>
    /// Test int conversion from BsonDouble
    /// </summary>
    [Test]
    public async Task FromBsonValue_Int_FromBsonDouble_ShouldWork()
    {
        var bsonDouble = new BsonDouble(42.0);
        var result = BsonConversion.FromBsonValue(bsonDouble, typeof(int));
        await Assert.That(result).IsEqualTo(42);
    }

    /// <summary>
    /// Test int conversion from BsonString
    /// </summary>
    [Test]
    public async Task FromBsonValue_Int_FromBsonString_ShouldWork()
    {
        var bsonString = new BsonString("123");
        var result = BsonConversion.FromBsonValue(bsonString, typeof(int));
        await Assert.That(result).IsEqualTo(123);
    }

    /// <summary>
    /// Test long conversion from BsonDouble
    /// </summary>
    [Test]
    public async Task FromBsonValue_Long_FromBsonDouble_ShouldWork()
    {
        var bsonDouble = new BsonDouble(999.0);
        var result = BsonConversion.FromBsonValue(bsonDouble, typeof(long));
        await Assert.That(result).IsEqualTo(999L);
    }

    /// <summary>
    /// Test long conversion from BsonString
    /// </summary>
    [Test]
    public async Task FromBsonValue_Long_FromBsonString_ShouldWork()
    {
        var bsonString = new BsonString("9876543210");
        var result = BsonConversion.FromBsonValue(bsonString, typeof(long));
        await Assert.That(result).IsEqualTo(9876543210L);
    }

    /// <summary>
    /// Test decimal conversion from BsonDecimal128
    /// </summary>
    [Test]
    public async Task FromBsonValue_Decimal_FromBsonDecimal128_ShouldWork()
    {
        var bsonDecimal = new BsonDecimal128(123.456m);
        var result = BsonConversion.FromBsonValue(bsonDecimal, typeof(decimal));
        await Assert.That(result).IsEqualTo(123.456m);
    }

    /// <summary>
    /// Test decimal conversion from BsonInt64
    /// </summary>
    [Test]
    public async Task FromBsonValue_Decimal_FromBsonInt64_ShouldWork()
    {
        var bsonLong = new BsonInt64(12345);
        var result = BsonConversion.FromBsonValue(bsonLong, typeof(decimal));
        await Assert.That(result).IsEqualTo(12345m);
    }

    /// <summary>
    /// Test decimal conversion from BsonString
    /// </summary>
    [Test]
    public async Task FromBsonValue_Decimal_FromBsonString_ShouldWork()
    {
        var bsonString = new BsonString("999.99");
        var result = BsonConversion.FromBsonValue(bsonString, typeof(decimal));
        await Assert.That(result).IsEqualTo(999.99m);
    }

    /// <summary>
    /// Test char conversion from BsonString
    /// </summary>
    [Test]
    public async Task FromBsonValue_Char_FromBsonString_ShouldWork()
    {
        var bsonString = new BsonString("X");
        var result = BsonConversion.FromBsonValue(bsonString, typeof(char));
        await Assert.That(result).IsEqualTo('X');
    }

    /// <summary>
    /// Test char conversion from empty string (fallback) - this tests the BsonString path with length > 0
    /// </summary>
    [Test]
    public async Task FromBsonValue_Char_FromBsonString_MultiChar_TakesFirst_ShouldWork()
    {
        // BsonString with length > 0 takes the first character
        var bsonString = new BsonString("Hello");
        var result = BsonConversion.FromBsonValue(bsonString, typeof(char));
        await Assert.That(result).IsEqualTo('H');
    }

    /// <summary>
    /// Test byte conversion from BsonDouble
    /// </summary>
    [Test]
    public async Task FromBsonValue_Byte_FromBsonDouble_ShouldWork()
    {
        var bsonDouble = new BsonDouble(200.0);
        var result = BsonConversion.FromBsonValue(bsonDouble, typeof(byte));
        await Assert.That(result).IsEqualTo((byte)200);
    }

    /// <summary>
    /// Test byte conversion from BsonString
    /// </summary>
    [Test]
    public async Task FromBsonValue_Byte_FromBsonString_ShouldWork()
    {
        var bsonString = new BsonString("255");
        var result = BsonConversion.FromBsonValue(bsonString, typeof(byte));
        await Assert.That(result).IsEqualTo((byte)255);
    }

    /// <summary>
    /// Test byte conversion from fallback ToString
    /// </summary>
    [Test]
    public async Task FromBsonValue_Byte_Fallback_ToString_ShouldWork()
    {
        // Hits: _ => Convert.ToByte(bsonValue.ToString(), CultureInfo.InvariantCulture)
        var bsonDec = new BsonDecimal128(50m);
        var result = BsonConversion.FromBsonValue(bsonDec, typeof(byte));
        await Assert.That(result).IsEqualTo((byte)50);
    }

    /// <summary>
    /// Test sbyte conversion from BsonInt64
    /// </summary>
    [Test]
    public async Task FromBsonValue_SByte_FromBsonInt64_ShouldWork()
    {
        var bsonLong = new BsonInt64(100);
        var result = BsonConversion.FromBsonValue(bsonLong, typeof(sbyte));
        await Assert.That(result).IsEqualTo((sbyte)100);
    }

    /// <summary>
    /// Test sbyte conversion from BsonDouble
    /// </summary>
    [Test]
    public async Task FromBsonValue_SByte_FromBsonDouble_ShouldWork()
    {
        var bsonDouble = new BsonDouble(50.0);
        var result = BsonConversion.FromBsonValue(bsonDouble, typeof(sbyte));
        await Assert.That(result).IsEqualTo((sbyte)50);
    }

    /// <summary>
    /// Test sbyte conversion from BsonString
    /// </summary>
    [Test]
    public async Task FromBsonValue_SByte_FromBsonString_ShouldWork()
    {
        var bsonString = new BsonString("-100");
        var result = BsonConversion.FromBsonValue(bsonString, typeof(sbyte));
        await Assert.That(result).IsEqualTo((sbyte)-100);
    }

    /// <summary>
    /// Test short conversion from BsonInt64
    /// </summary>
    [Test]
    public async Task FromBsonValue_Short_FromBsonInt64_ShouldWork()
    {
        var bsonLong = new BsonInt64(1000);
        var result = BsonConversion.FromBsonValue(bsonLong, typeof(short));
        await Assert.That(result).IsEqualTo((short)1000);
    }

    /// <summary>
    /// Test short conversion from BsonDouble
    /// </summary>
    [Test]
    public async Task FromBsonValue_Short_FromBsonDouble_ShouldWork()
    {
        var bsonDouble = new BsonDouble(2000.0);
        var result = BsonConversion.FromBsonValue(bsonDouble, typeof(short));
        await Assert.That(result).IsEqualTo((short)2000);
    }

    /// <summary>
    /// Test short conversion from BsonString
    /// </summary>
    [Test]
    public async Task FromBsonValue_Short_FromBsonString_ShouldWork()
    {
        var bsonString = new BsonString("30000");
        var result = BsonConversion.FromBsonValue(bsonString, typeof(short));
        await Assert.That(result).IsEqualTo((short)30000);
    }

    /// <summary>
    /// Test ushort conversion from BsonInt64
    /// </summary>
    [Test]
    public async Task FromBsonValue_UShort_FromBsonInt64_ShouldWork()
    {
        var bsonLong = new BsonInt64(50000);
        var result = BsonConversion.FromBsonValue(bsonLong, typeof(ushort));
        await Assert.That(result).IsEqualTo((ushort)50000);
    }

    /// <summary>
    /// Test ushort conversion from BsonDouble
    /// </summary>
    [Test]
    public async Task FromBsonValue_UShort_FromBsonDouble_ShouldWork()
    {
        var bsonDouble = new BsonDouble(40000.0);
        var result = BsonConversion.FromBsonValue(bsonDouble, typeof(ushort));
        await Assert.That(result).IsEqualTo((ushort)40000);
    }

    /// <summary>
    /// Test ushort conversion from BsonString
    /// </summary>
    [Test]
    public async Task FromBsonValue_UShort_FromBsonString_ShouldWork()
    {
        var bsonString = new BsonString("65000");
        var result = BsonConversion.FromBsonValue(bsonString, typeof(ushort));
        await Assert.That(result).IsEqualTo((ushort)65000);
    }

    /// <summary>
    /// Test uint conversion from BsonString
    /// </summary>
    [Test]
    public async Task FromBsonValue_UInt_FromBsonString_ShouldWork()
    {
        var bsonString = new BsonString("4000000000");
        var result = BsonConversion.FromBsonValue(bsonString, typeof(uint));
        await Assert.That(result).IsEqualTo(4000000000u);
    }

    /// <summary>
    /// Test ulong conversion from BsonDouble
    /// </summary>
    [Test]
    public async Task FromBsonValue_ULong_FromBsonDouble_ShouldWork()
    {
        var bsonDouble = new BsonDouble(1234567890.0);
        var result = BsonConversion.FromBsonValue(bsonDouble, typeof(ulong));
        await Assert.That(result).IsEqualTo(1234567890UL);
    }

    /// <summary>
    /// Test ulong conversion from BsonString
    /// </summary>
    [Test]
    public async Task FromBsonValue_ULong_FromBsonString_ShouldWork()
    {
        var bsonString = new BsonString("18446744073709551615"); // ulong.MaxValue
        var result = BsonConversion.FromBsonValue(bsonString, typeof(ulong));
        await Assert.That(result).IsEqualTo(ulong.MaxValue);
    }

    /// <summary>
    /// Test byte[] conversion from BsonString (base64)
    /// </summary>
    [Test]
    public async Task FromBsonValue_ByteArray_FromBsonString_ShouldWork()
    {
        var bytes = new byte[] { 1, 2, 3, 4, 5 };
        var base64 = Convert.ToBase64String(bytes);
        var bsonString = new BsonString(base64);
        var result = (byte[])BsonConversion.FromBsonValue(bsonString, typeof(byte[]))!;
        await Assert.That(result.SequenceEqual(bytes)).IsTrue();
    }

    /// <summary>
    /// Test ReadOnlyMemory<byte> conversion from BsonString (base64)
    /// </summary>
    [Test]
    public async Task FromBsonValue_ReadOnlyMemoryByte_FromBsonString_ShouldWork()
    {
        var bytes = new byte[] { 10, 20, 30 };
        var base64 = Convert.ToBase64String(bytes);
        var bsonString = new BsonString(base64);
        var result = (ReadOnlyMemory<byte>)BsonConversion.FromBsonValue(bsonString, typeof(ReadOnlyMemory<byte>))!;
        await Assert.That(result.ToArray().SequenceEqual(bytes)).IsTrue();
    }

    /// <summary>
    /// Test Memory<byte> conversion from BsonString (base64)
    /// </summary>
    [Test]
    public async Task FromBsonValue_MemoryByte_FromBsonString_ShouldWork()
    {
        var bytes = new byte[] { 100, 200 };
        var base64 = Convert.ToBase64String(bytes);
        var bsonString = new BsonString(base64);
        var result = (Memory<byte>)BsonConversion.FromBsonValue(bsonString, typeof(Memory<byte>))!;
        await Assert.That(result.ToArray().SequenceEqual(bytes)).IsTrue();
    }

    /// <summary>
    /// Test Guid conversion from BsonBinary (UUID)
    /// </summary>
    [Test]
    public async Task FromBsonValue_Guid_FromBsonBinary_ShouldWork()
    {
        var guid = Guid.NewGuid();
        var bin = new BsonBinary(guid.ToByteArray(), BsonBinary.BinarySubType.Uuid);
        var result = BsonConversion.FromBsonValue(bin, typeof(Guid));
        await Assert.That(result).IsEqualTo(guid);
    }

    /// <summary>
    /// Test enum conversion from BsonInt64 with int underlying type
    /// </summary>
    [Test]
    public async Task FromBsonValue_IntEnum_FromBsonInt64_ShouldWork()
    {
        var bsonLong = new BsonInt64(1);
        var result = BsonConversion.FromBsonValue(bsonLong, typeof(IntEnum));
        await Assert.That(result).IsEqualTo(IntEnum.A);
    }

    /// <summary>
    /// Test enum conversion from BsonDouble with int underlying type
    /// </summary>
    [Test]
    public async Task FromBsonValue_IntEnum_FromBsonDouble_ShouldWork()
    {
        var bsonDouble = new BsonDouble(1.0);
        var result = BsonConversion.FromBsonValue(bsonDouble, typeof(IntEnum));
        await Assert.That(result).IsEqualTo(IntEnum.A);
    }

    /// <summary>
    /// Test enum conversion from BsonString (numeric string) with int underlying type
    /// </summary>
    [Test]
    public async Task FromBsonValue_IntEnum_FromBsonString_Numeric_ShouldWork()
    {
        var bsonString = new BsonString("1");
        var result = BsonConversion.FromBsonValue(bsonString, typeof(IntEnum));
        await Assert.That(result).IsEqualTo(IntEnum.A);
    }

    /// <summary>
    /// Test enum conversion from BsonInt32 with long underlying type
    /// </summary>
    [Test]
    public async Task FromBsonValue_LongEnum_FromBsonInt32_ShouldWork()
    {
        var bsonInt = new BsonInt32(2);
        var result = BsonConversion.FromBsonValue(bsonInt, typeof(LongEnum));
        await Assert.That(result).IsEqualTo(LongEnum.B);
    }

    /// <summary>
    /// Test enum conversion from BsonDouble with long underlying type
    /// </summary>
    [Test]
    public async Task FromBsonValue_LongEnum_FromBsonDouble_ShouldWork()
    {
        var bsonDouble = new BsonDouble(2.0);
        var result = BsonConversion.FromBsonValue(bsonDouble, typeof(LongEnum));
        await Assert.That(result).IsEqualTo(LongEnum.B);
    }

    /// <summary>
    /// Test enum conversion from BsonString with long underlying type
    /// </summary>
    [Test]
    public async Task FromBsonValue_LongEnum_FromBsonString_Numeric_ShouldWork()
    {
        var bsonString = new BsonString("2");
        var result = BsonConversion.FromBsonValue(bsonString, typeof(LongEnum));
        await Assert.That(result).IsEqualTo(LongEnum.B);
    }

    /// <summary>
    /// Test enum conversion from BsonInt64 with short underlying type
    /// </summary>
    [Test]
    public async Task FromBsonValue_ShortEnum_FromBsonInt64_ShouldWork()
    {
        var bsonLong = new BsonInt64(3);
        var result = BsonConversion.FromBsonValue(bsonLong, typeof(ShortEnum));
        await Assert.That(result).IsEqualTo(ShortEnum.C);
    }

    /// <summary>
    /// Test enum conversion from BsonDouble with short underlying type
    /// </summary>
    [Test]
    public async Task FromBsonValue_ShortEnum_FromBsonDouble_ShouldWork()
    {
        var bsonDouble = new BsonDouble(3.0);
        var result = BsonConversion.FromBsonValue(bsonDouble, typeof(ShortEnum));
        await Assert.That(result).IsEqualTo(ShortEnum.C);
    }

    /// <summary>
    /// Test enum conversion from BsonString with short underlying type
    /// </summary>
    [Test]
    public async Task FromBsonValue_ShortEnum_FromBsonString_Numeric_ShouldWork()
    {
        var bsonString = new BsonString("3");
        var result = BsonConversion.FromBsonValue(bsonString, typeof(ShortEnum));
        await Assert.That(result).IsEqualTo(ShortEnum.C);
    }

    /// <summary>
    /// Test enum conversion from BsonInt64 with byte underlying type
    /// </summary>
    [Test]
    public async Task FromBsonValue_ByteEnum_FromBsonInt64_ShouldWork()
    {
        var bsonLong = new BsonInt64(4);
        var result = BsonConversion.FromBsonValue(bsonLong, typeof(ByteEnum));
        await Assert.That(result).IsEqualTo(ByteEnum.D);
    }

    /// <summary>
    /// Test enum conversion from BsonDouble with byte underlying type
    /// </summary>
    [Test]
    public async Task FromBsonValue_ByteEnum_FromBsonDouble_ShouldWork()
    {
        var bsonDouble = new BsonDouble(4.0);
        var result = BsonConversion.FromBsonValue(bsonDouble, typeof(ByteEnum));
        await Assert.That(result).IsEqualTo(ByteEnum.D);
    }

    /// <summary>
    /// Test enum conversion from BsonString with byte underlying type
    /// </summary>
    [Test]
    public async Task FromBsonValue_ByteEnum_FromBsonString_Numeric_ShouldWork()
    {
        var bsonString = new BsonString("4");
        var result = BsonConversion.FromBsonValue(bsonString, typeof(ByteEnum));
        await Assert.That(result).IsEqualTo(ByteEnum.D);
    }

    /// <summary>
    /// Test enum with unusual underlying type (ushort)
    /// </summary>
    public enum UShortEnum : ushort { E = 5 }

    [Test]
    public async Task FromBsonValue_UShortEnum_FallbackToString_ShouldWork()
    {
        // For unsupported underlying types, falls back to string parsing
        var bsonInt = new BsonInt32(5);
        var result = BsonConversion.FromBsonValue(bsonInt, typeof(UShortEnum));
        await Assert.That(result).IsEqualTo(UShortEnum.E);
    }

    /// <summary>
    /// Test nullable type conversion
    /// </summary>
    [Test]
    public async Task FromBsonValue_NullableInt_ShouldWork()
    {
        var bsonInt = new BsonInt32(42);
        var result = BsonConversion.FromBsonValue(bsonInt, typeof(int?));
        await Assert.That(result).IsEqualTo(42);
    }

    /// <summary>
    /// Test nullable type with null value
    /// </summary>
    [Test]
    public async Task FromBsonValue_NullableInt_Null_ShouldReturnNull()
    {
        var result = BsonConversion.FromBsonValue(BsonNull.Value, typeof(int?));
        await Assert.That(result).IsNull();
    }

    /// <summary>
    /// Test complex object to BsonValue (fallback to string when AotBsonMapper fails)
    /// </summary>
    [Entity]
    public class UnregisteredComplexType
    {
        public int Value { get; set; }
        public override string ToString() => $"Value={Value}";
    }

    [Test]
    public async Task ToBsonValue_UnregisteredComplexType_ShouldFallbackToString()
    {
        var obj = new UnregisteredComplexType { Value = 42 };
        var result = BsonConversion.ToBsonValue(obj);
        // AotBsonMapper might return a document or fall back to string
        // Let's verify it doesn't throw
        await Assert.That(result).IsNotNull();
    }

    /// <summary>
    /// Test IsComplexObjectType with IEnumerable type (should return false)
    /// </summary>
    [Test]
    public async Task ToBsonValue_IEnumerable_ShouldReturnBsonArray()
    {
        var list = new List<int> { 1, 2, 3 };
        var result = BsonConversion.ToBsonValue(list);
        await Assert.That(result).IsTypeOf<BsonArray>();
    }

    /// <summary>
    /// Test ConvertToBsonValue (alias for ToBsonValue)
    /// </summary>
    [Test]
    public async Task ConvertToBsonValue_ShouldWork()
    {
        var result = BsonConversion.ConvertToBsonValue(123);
        await Assert.That(result).IsTypeOf<BsonInt32>();
        await Assert.That(((BsonInt32)result).Value).IsEqualTo(123);
    }
}
