using System;
using System.Buffers.Binary;
using System.Reflection;
using TinyDb.Bson;
using TinyDb.Serialization;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Serialization;

public sealed class BsonScannerIsKnownTypeCoverageTests
{
    private delegate bool IsKnownTypeDelegate(BsonType type);

    private static readonly IsKnownTypeDelegate IsKnownType = CreateIsKnownTypeDelegate();

    [Test]
    public async Task TryGetValue_KnownComplexTypeMalformed_ShouldReturnBsonNull()
    {
        var bytes = new byte[12];
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(0, 4), bytes.Length);
        bytes[4] = (byte)BsonType.Document;
        bytes[5] = (byte)'x';
        bytes[6] = 0;
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(7, 4), 100);
        bytes[11] = 0;

        var found = BsonScanner.TryGetValue(bytes, "x", out var value);
        await Assert.That(found).IsTrue();
        await Assert.That(value).IsNotNull();
        await Assert.That(value!.IsNull).IsTrue();
    }

    [Test]
    public async Task IsKnownType_ShouldCoverAllSwitchArms()
    {
        var known = new[]
        {
            BsonType.Double,
            BsonType.String,
            BsonType.Document,
            BsonType.Array,
            BsonType.Binary,
            BsonType.Undefined,
            BsonType.ObjectId,
            BsonType.Boolean,
            BsonType.DateTime,
            BsonType.Null,
            BsonType.RegularExpression,
            BsonType.JavaScript,
            BsonType.Symbol,
            BsonType.JavaScriptWithScope,
            BsonType.Int32,
            BsonType.Timestamp,
            BsonType.Int64,
            BsonType.Decimal128,
            BsonType.MinKey,
            BsonType.MaxKey,
            BsonType.End
        };

        foreach (var type in known)
        {
            await Assert.That(IsKnownType(type)).IsTrue();
        }

        await Assert.That(IsKnownType((BsonType)0x20)).IsFalse();
    }

    private static IsKnownTypeDelegate CreateIsKnownTypeDelegate()
    {
        var method = typeof(BsonScanner).GetMethod("IsKnownType", BindingFlags.NonPublic | BindingFlags.Static)
            ?? throw new MissingMethodException(typeof(BsonScanner).FullName, "IsKnownType");
        return (IsKnownTypeDelegate)method.CreateDelegate(typeof(IsKnownTypeDelegate));
    }
}
