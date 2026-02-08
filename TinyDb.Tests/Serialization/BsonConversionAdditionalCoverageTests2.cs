using System;
using System.Collections.Generic;
using TinyDb.Bson;
using TinyDb.Serialization;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Serialization;

public class BsonConversionAdditionalCoverageTests2
{
    public enum IntEnum : int { A = 1 }
    public enum LongEnum : long { A = 1 }
    public enum ShortEnum : short { A = 1 }
    public enum ByteEnum : byte { A = 1 }

    public enum KeyEnum : int { A = 1, B = 2 }

    private sealed class PlainPoco
    {
        public int Id { get; set; }
    }

    private sealed class AdapterPoco
    {
        public int Id { get; set; }
    }

    [Test]
    public async Task FromBsonValueEnum_ShouldCoverNumericArms_And_DefaultFallback()
    {
        await Assert.That(BsonConversion.FromBsonValueEnum<IntEnum>(new BsonInt32(1))).IsEqualTo(IntEnum.A);
        await Assert.That(BsonConversion.FromBsonValueEnum<IntEnum>(new BsonInt64(1))).IsEqualTo(IntEnum.A);
        await Assert.That(BsonConversion.FromBsonValueEnum<IntEnum>(new BsonDouble(1.0))).IsEqualTo(IntEnum.A);
        await Assert.That(BsonConversion.FromBsonValueEnum<IntEnum>(new BsonString("1"))).IsEqualTo(IntEnum.A);
        await Assert.That(BsonConversion.FromBsonValueEnum<IntEnum>(new BsonDecimal128(1m))).IsEqualTo(IntEnum.A);

        await Assert.That(BsonConversion.FromBsonValueEnum<LongEnum>(new BsonInt64(1))).IsEqualTo(LongEnum.A);
        await Assert.That(BsonConversion.FromBsonValueEnum<LongEnum>(new BsonInt32(1))).IsEqualTo(LongEnum.A);
        await Assert.That(BsonConversion.FromBsonValueEnum<LongEnum>(new BsonDouble(1.0))).IsEqualTo(LongEnum.A);
        await Assert.That(BsonConversion.FromBsonValueEnum<LongEnum>(new BsonString("1"))).IsEqualTo(LongEnum.A);
        await Assert.That(BsonConversion.FromBsonValueEnum<LongEnum>(new BsonDecimal128(1m))).IsEqualTo(LongEnum.A);

        await Assert.That(BsonConversion.FromBsonValueEnum<ShortEnum>(new BsonInt32(1))).IsEqualTo(ShortEnum.A);
        await Assert.That(BsonConversion.FromBsonValueEnum<ShortEnum>(new BsonInt64(1))).IsEqualTo(ShortEnum.A);
        await Assert.That(BsonConversion.FromBsonValueEnum<ShortEnum>(new BsonDouble(1.0))).IsEqualTo(ShortEnum.A);
        await Assert.That(BsonConversion.FromBsonValueEnum<ShortEnum>(new BsonString("1"))).IsEqualTo(ShortEnum.A);
        await Assert.That(BsonConversion.FromBsonValueEnum<ShortEnum>(new BsonDecimal128(1m))).IsEqualTo(ShortEnum.A);

        await Assert.That(BsonConversion.FromBsonValueEnum<ByteEnum>(new BsonInt32(1))).IsEqualTo(ByteEnum.A);
        await Assert.That(BsonConversion.FromBsonValueEnum<ByteEnum>(new BsonInt64(1))).IsEqualTo(ByteEnum.A);
        await Assert.That(BsonConversion.FromBsonValueEnum<ByteEnum>(new BsonDouble(1.0))).IsEqualTo(ByteEnum.A);
        await Assert.That(BsonConversion.FromBsonValueEnum<ByteEnum>(new BsonString("1"))).IsEqualTo(ByteEnum.A);
        await Assert.That(BsonConversion.FromBsonValueEnum<ByteEnum>(new BsonDecimal128(1m))).IsEqualTo(ByteEnum.A);
    }

    [Test]
    public async Task FromBsonValue_Array_ShouldConvertElements()
    {
        var bsonArray = new BsonArray(new BsonValue[] { 1, 2, 3 });

        var ints = (int[])BsonConversion.FromBsonValue(bsonArray, typeof(int[]))!;
        await Assert.That(ints.Length).IsEqualTo(3);
        await Assert.That(ints[0]).IsEqualTo(1);
        await Assert.That(ints[2]).IsEqualTo(3);

        var strArray = new BsonArray(new BsonValue[] { "a", "b" });
        var strings = (string[])BsonConversion.FromBsonValue(strArray, typeof(string[]))!;
        await Assert.That(strings.Length).IsEqualTo(2);
        await Assert.That(strings[0]).IsEqualTo("a");
    }

    [Test]
    public async Task FromBsonValue_Dictionary_WithEnumKey_ShouldParseKeys()
    {
        var doc = new BsonDocument().Set("A", 1).Set("B", 2);
        var dict = (Dictionary<KeyEnum, int>)BsonConversion.FromBsonValue(doc, typeof(Dictionary<KeyEnum, int>))!;

        await Assert.That(dict[KeyEnum.A]).IsEqualTo(1);
        await Assert.That(dict[KeyEnum.B]).IsEqualTo(2);
    }

    [Test]
    public async Task FromBsonValue_ComplexType_FromDocumentValue_WithoutAdapter_ShouldThrow()
    {
        var docValue = new BsonDocumentValue(new BsonDocument().Set("id", 1));

        await Assert.That(() => BsonConversion.FromBsonValue(docValue, typeof(PlainPoco)))
            .Throws<InvalidOperationException>();
    }

    [Test]
    public async Task FromBsonValue_ComplexType_FromBsonDocument_WithRegisteredAdapter_ShouldUseAdapter()
    {
        var adapter = new AotEntityAdapter<AdapterPoco>(
            toDocument: _ => new BsonDocument(),
            fromDocument: document =>
            {
                var poco = new AdapterPoco();
                if (document.TryGetValue("id", out var idValue))
                {
                    poco.Id = idValue.ToInt32(null);
                }
                return poco;
            },
            getId: e => BsonInt32.FromValue(e.Id),
            setId: (e, id) => e.Id = id.ToInt32(null),
            hasValidId: e => e.Id != 0,
            getPropertyValue: (e, name) => name == nameof(AdapterPoco.Id) ? e.Id : null);

        AotHelperRegistry.Register(adapter);

        var doc = new BsonDocument().Set("id", 123);
        var obj = (AdapterPoco)BsonConversion.FromBsonValue(doc, typeof(AdapterPoco))!;
        await Assert.That(obj.Id).IsEqualTo(123);
    }

    [Test]
    public async Task ToBsonValue_ObjectType_ShouldThrowNotSupported()
    {
        await Assert.That(() => BsonConversion.ToBsonValue(new object()))
            .Throws<NotSupportedException>();
    }

    [Test]
    public async Task ToBsonValue_Long_ShouldCreateInt64()
    {
        var bson = BsonConversion.ToBsonValue(123L);
        await Assert.That(bson).IsTypeOf<BsonInt64>();
        await Assert.That(((BsonInt64)bson).Value).IsEqualTo(123L);
    }

    [Test]
    public async Task FromBsonValue_Object_Int64_ShouldReturnLong()
    {
        var value = BsonConversion.FromBsonValue(new BsonInt64(5), typeof(object));
        await Assert.That(value).IsTypeOf<long>();
        await Assert.That((long)value!).IsEqualTo(5L);
    }
}
