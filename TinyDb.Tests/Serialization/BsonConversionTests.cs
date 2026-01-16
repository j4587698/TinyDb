using System;
using System.Collections.Generic;
using TinyDb.Bson;
using TinyDb.Serialization;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Serialization;

public class BsonConversionTests
{
    public enum TestEnum { Value1 = 1, Value2 = 2 }

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
        await Assert.That(BsonConversion.FromBsonValue(new BsonString("Value2"), typeof(TestEnum))).IsEqualTo(TestEnum.Value2);
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
        await Assert.That(BsonConversion.FromBsonValue(new BsonString("A"), typeof(IntEnum))).IsEqualTo(IntEnum.A);
        await Assert.That(BsonConversion.FromBsonValue(new BsonString("B"), typeof(LongEnum))).IsEqualTo(LongEnum.B);
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
}
