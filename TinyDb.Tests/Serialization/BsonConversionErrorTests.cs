using System;
using TinyDb.Bson;
using TinyDb.Serialization;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Serialization;

public class BsonConversionErrorTests
{
    [Test]
    public async Task FromBsonValue_Overflow_ShouldThrow()
    {
        // Int64 to Int32 overflow
        var bigLong = new BsonInt64(long.MaxValue);
        await Assert.That(() => BsonConversion.FromBsonValue(bigLong, typeof(int))).Throws<OverflowException>();
        
        // Double to Byte overflow
        var bigDouble = new BsonDouble(1000.0);
        await Assert.That(() => BsonConversion.FromBsonValue(bigDouble, typeof(byte))).Throws<OverflowException>();
    }

    [Test]
    public async Task FromBsonValue_InvalidFormat_ShouldThrow()
    {
        // String to Int32 invalid format
        var badStr = new BsonString("not-a-number");
        await Assert.That(() => BsonConversion.FromBsonValue(badStr, typeof(int))).Throws<FormatException>();
        
        // String to Guid invalid format
        await Assert.That(() => BsonConversion.FromBsonValue(badStr, typeof(Guid))).Throws<FormatException>();
    }

    [Test]
    public async Task FromBsonValue_UnsupportedTargetType_ShouldReturnString()
    {
        var bson = new BsonInt32(42);
        // If type is not handled in switch, it returns ToString()
        var result = BsonConversion.FromBsonValue(bson, typeof(System.Drawing.Point));
        await Assert.That(result).IsEqualTo("42");
    }
}
