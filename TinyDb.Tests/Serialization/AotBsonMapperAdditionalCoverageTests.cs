using System;
using System.Collections.Generic;
using TinyDb.Bson;
using TinyDb.Serialization;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Serialization;

public class AotBsonMapperAdditionalCoverageTests
{
    private sealed class NonEntityComplex
    {
        public int Id { get; set; }
    }

    private sealed class EntityWithNullableId
    {
        public string? Id { get; set; }
    }

    [Test]
    public async Task ConvertValue_BsonNull_ToValueType_ShouldReturnDefault()
    {
        await Assert.That(AotBsonMapper.ConvertValue(BsonNull.Value, typeof(int))).IsEqualTo(0);
    }

    [Test]
    public async Task ConvertValue_DictionaryType_ShouldConvert()
    {
        var doc = new BsonDocument().Set("a", 1);
        var result = (Dictionary<string, int>)AotBsonMapper.ConvertValue(doc, typeof(Dictionary<string, int>))!;
        await Assert.That(result["a"]).IsEqualTo(1);
    }

    [Test]
    public async Task ConvertValue_IntArray_FromBsonArray_ShouldConvert()
    {
        var arr = new BsonArray().AddValue(1).AddValue(2);
        var result = (int[])AotBsonMapper.ConvertValue(arr, typeof(int[]))!;

        await Assert.That(result.Length).IsEqualTo(2);
        await Assert.That(result[0]).IsEqualTo(1);
        await Assert.That(result[1]).IsEqualTo(2);
    }

    [Test]
    public async Task ConvertValue_ByteArray_FromBsonArray_ShouldThrow()
    {
        var arr = new BsonArray().AddValue(1).AddValue(2).AddValue(3);
        await Assert.That(() => AotBsonMapper.ConvertValue(arr, typeof(byte[]))).Throws<InvalidOperationException>();
    }

    [Test]
    public async Task ConvertValue_ByteArray_FromUnsupportedBsonType_ShouldThrow()
    {
        await Assert.That(() => AotBsonMapper.ConvertValue(new BsonInt32(1), typeof(byte[]))).Throws<InvalidOperationException>();
    }

    [Test]
    public async Task ConvertValue_ObjectId_FromUnsupportedBsonType_ShouldThrow()
    {
        await Assert.That(() => AotBsonMapper.ConvertValue(new BsonDocument(), typeof(ObjectId))).Throws<ArgumentException>();
    }

    [Test]
    public async Task ConvertValue_ComplexType_WithoutAdapter_ShouldFallback()
    {
        var doc = new BsonDocument().Set("_id", 1).Set("id", 1);
        var converted = (NonEntityComplex)AotBsonMapper.ConvertValue(doc, typeof(NonEntityComplex))!;
        await Assert.That(converted.Id).IsEqualTo(1);
    }

    [Test]
    public async Task ConvertValue_ObjectType_ShouldUnwrapBsonBinary()
    {
        var guid = Guid.NewGuid();
        var uuidBin = new BsonBinary(guid.ToByteArray(), BsonBinary.BinarySubType.Uuid);
        await Assert.That(AotBsonMapper.ConvertValue(uuidBin, typeof(object))).IsEqualTo(guid);

        var rawBytes = new byte[] { 1, 2, 3 };
        var rawBin = new BsonBinary(rawBytes);
        var bytes = (byte[])AotBsonMapper.ConvertValue(rawBin, typeof(object))!;
        await Assert.That(bytes.Length).IsEqualTo(3);
        await Assert.That(bytes[0]).IsEqualTo((byte)1);
        await Assert.That(bytes[1]).IsEqualTo((byte)2);
        await Assert.That(bytes[2]).IsEqualTo((byte)3);
    }

    [Test]
    public async Task GetId_WhenIdValueIsNull_ShouldReturnBsonNull()
    {
        var entity = new EntityWithNullableId { Id = null };
        var id = AotBsonMapper.GetId(entity);

        await Assert.That(id.IsNull).IsTrue();
    }

    [Test]
    public async Task ConvertValue_ArrayList_FromBsonArray_ShouldConvert()
    {
        var arr = new BsonArray().AddValue(1).AddValue("a");
        var result = (System.Collections.ArrayList)AotBsonMapper.ConvertValue(arr, typeof(System.Collections.ArrayList))!;

        await Assert.That(result.Count).IsEqualTo(2);
        await Assert.That(result[0]).IsEqualTo(1);
        await Assert.That(result[1]).IsEqualTo("a");
    }
}
