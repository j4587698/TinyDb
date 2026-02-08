using System;
using System.Linq;
using System.Reflection;
using TinyDb.Bson;
using TinyDb.Serialization;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Serialization;

public class BsonConversionBranchCoverageTests4
{
    [Test]
    public async Task FromBsonValue_Object_WithBsonNull_ShouldReturnNull()
    {
        var converted = BsonConversion.FromBsonValue(BsonNull.Value, typeof(object));
        await Assert.That(converted).IsNull();
    }

    [Test]
    public async Task FromBsonValue_Object_WithBsonBinary_ShouldUnwrapGuidOrBytes()
    {
        var guid = Guid.NewGuid();
        var uuid = new BsonBinary(guid.ToByteArray(), BsonBinary.BinarySubType.Uuid);
        var uuidValue = BsonConversion.FromBsonValue(uuid, typeof(object));
        await Assert.That(uuidValue).IsTypeOf<Guid>();
        await Assert.That((Guid)uuidValue!).IsEqualTo(guid);

        var bytes = new byte[] { 1, 2, 3 };
        var generic = new BsonBinary(bytes, BsonBinary.BinarySubType.Generic);
        var genericValue = BsonConversion.FromBsonValue(generic, typeof(object));
        await Assert.That(genericValue).IsTypeOf<byte[]>();
        await Assert.That(((byte[])genericValue!).SequenceEqual(bytes)).IsTrue();
    }

    [Test]
    public async Task FromBsonValue_Guid_FromNonStringNonBinary_ShouldParseFallback()
    {
        var guid = Guid.NewGuid();
        var symbol = new BsonSymbol(guid.ToString());

        var converted = (Guid)BsonConversion.FromBsonValue(symbol, typeof(Guid))!;
        await Assert.That(converted).IsEqualTo(guid);
    }

    [Test]
    public async Task FromBsonValue_Guid_FromBsonString_ShouldParse()
    {
        var guid = Guid.NewGuid();
        var bson = new BsonString(guid.ToString());

        var converted = (Guid)BsonConversion.FromBsonValue(bson, typeof(Guid))!;

        await Assert.That(converted).IsEqualTo(guid);
    }

    [Test]
    public async Task FromBsonValue_Guid_FromBsonBinaryLength16_ShouldReturnGuid()
    {
        var guid = Guid.NewGuid();
        var bin = new BsonBinary(guid.ToByteArray(), BsonBinary.BinarySubType.Generic);

        var converted = (Guid)BsonConversion.FromBsonValue(bin, typeof(Guid))!;

        await Assert.That(converted).IsEqualTo(guid);
    }

    [Test]
    public async Task FromBsonValue_Char_ShouldHandleIntAndStringBranches()
    {
        var fromInt = (char)BsonConversion.FromBsonValue(new BsonInt32(65), typeof(char))!;
        await Assert.That(fromInt).IsEqualTo('A');

        var fromString = (char)BsonConversion.FromBsonValue(new BsonString("Z"), typeof(char))!;
        await Assert.That(fromString).IsEqualTo('Z');

        var fromSymbol = (char)BsonConversion.FromBsonValue(new BsonSymbol("Q"), typeof(char))!;
        await Assert.That(fromSymbol).IsEqualTo('Q');

        await Assert.That(() => BsonConversion.FromBsonValue(new BsonString(string.Empty), typeof(char)))
            .Throws<FormatException>();
    }

    [Test]
    public async Task FromBsonValue_ObjectId_FromBsonObjectId_ShouldReturnValue()
    {
        var oid = ObjectId.NewObjectId();
        var converted = (ObjectId)BsonConversion.FromBsonValue(new BsonObjectId(oid), typeof(ObjectId))!;
        await Assert.That(converted).IsEqualTo(oid);
    }

    [Test]
    public async Task IsComplexObjectType_Interface_ShouldReturnFalse()
    {
        var method = typeof(BsonConversion).GetMethod("IsComplexObjectType", BindingFlags.NonPublic | BindingFlags.Static);
        await Assert.That(method).IsNotNull();

        var isComplex = (bool)method!.Invoke(null, new object[] { typeof(IDisposable) })!;
        await Assert.That(isComplex).IsFalse();
    }
}
