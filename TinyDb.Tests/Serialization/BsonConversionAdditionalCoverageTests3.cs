using System;
using System.Collections.Generic;
using TinyDb.Bson;
using TinyDb.Serialization;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Serialization;

public class BsonConversionAdditionalCoverageTests3
{
    private enum IntEnum : int { A = 1 }
    private enum LongEnum : long { A = 1 }
    private enum ShortEnum : short { A = 1 }
    private enum ByteEnum : byte { A = 1 }

    [Test]
    public async Task FromBsonValue_Object_DefaultArm_ShouldReturnRawValue()
    {
        var value = BsonConversion.FromBsonValue(new BsonSymbol("sym"), typeof(object));
        await Assert.That(value).IsTypeOf<string>();
        await Assert.That((string)value!).IsEqualTo("sym");
    }

    [Test]
    public async Task FromBsonValue_Primitives_DefaultNumericParsingArms_ShouldWork()
    {
        var symbol = new BsonSymbol("1");

        await Assert.That(BsonConversion.FromBsonValue(symbol, typeof(sbyte))).IsEqualTo((sbyte)1);
        await Assert.That(BsonConversion.FromBsonValue(symbol, typeof(short))).IsEqualTo((short)1);
        await Assert.That(BsonConversion.FromBsonValue(symbol, typeof(ushort))).IsEqualTo((ushort)1);
        await Assert.That(BsonConversion.FromBsonValue(symbol, typeof(uint))).IsEqualTo((uint)1);
        await Assert.That(BsonConversion.FromBsonValue(symbol, typeof(ulong))).IsEqualTo((ulong)1);
    }

    [Test]
    public async Task FromBsonValue_Long_FromBsonInt64_ShouldReturnLong()
    {
        await Assert.That(BsonConversion.FromBsonValue(new BsonInt64(5), typeof(long))).IsEqualTo(5L);
    }

    [Test]
    public async Task FromBsonValue_Char_EmptyString_ShouldUseFallbackAndThrow()
    {
        await Assert.That(() => BsonConversion.FromBsonValue(new BsonString(string.Empty), typeof(char)))
            .Throws<FormatException>();
    }

    [Test]
    public async Task FromBsonValue_ObjectId_FromString_ShouldParse()
    {
        var oid = ObjectId.NewObjectId();
        var parsed = (ObjectId)BsonConversion.FromBsonValue(new BsonString(oid.ToString()), typeof(ObjectId))!;
        await Assert.That(parsed).IsEqualTo(oid);
    }

    [Test]
    public async Task FromBsonValue_ByteArray_Memory_InvalidType_ShouldThrow()
    {
        await Assert.That(() => BsonConversion.FromBsonValue(new BsonInt32(1), typeof(byte[])))
            .Throws<InvalidOperationException>();
        await Assert.That(() => BsonConversion.FromBsonValue(new BsonInt32(1), typeof(ReadOnlyMemory<byte>)))
            .Throws<InvalidOperationException>();
        await Assert.That(() => BsonConversion.FromBsonValue(new BsonInt32(1), typeof(Memory<byte>)))
            .Throws<InvalidOperationException>();
    }

    [Test]
    public async Task FromBsonValue_Enum_DefaultFallbackArms_ShouldWork()
    {
        var symbol = new BsonSymbol("1");

        await Assert.That((IntEnum)BsonConversion.FromBsonValue(symbol, typeof(IntEnum))!).IsEqualTo(IntEnum.A);
        await Assert.That((LongEnum)BsonConversion.FromBsonValue(symbol, typeof(LongEnum))!).IsEqualTo(LongEnum.A);
        await Assert.That((ShortEnum)BsonConversion.FromBsonValue(symbol, typeof(ShortEnum))!).IsEqualTo(ShortEnum.A);
        await Assert.That((ByteEnum)BsonConversion.FromBsonValue(symbol, typeof(ByteEnum))!).IsEqualTo(ByteEnum.A);
    }

    [Test]
    public async Task FromBsonValue_IsComplexObjectType_EnumerableAndBsonValueGuards_ShouldNotTreatAsComplex()
    {
        var value = BsonConversion.FromBsonValue(new BsonArray(new BsonValue[] { 1, 2 }), typeof(HashSet<int>));
        await Assert.That(value).IsTypeOf<string>();
        await Assert.That(((string)value!).Length).IsGreaterThan(0);

        var value2 = BsonConversion.FromBsonValue(new BsonInt32(5), typeof(BsonInt32));
        await Assert.That(value2).IsTypeOf<BsonInt32>();
        await Assert.That(((BsonInt32)value2!).Value).IsEqualTo(5);
    }
}
