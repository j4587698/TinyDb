using System;
using TinyDb.Bson;
using TinyDb.Serialization;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Serialization;

public class BsonConversionBranchCoverageTests5
{
    [Test]
    public async Task FromBsonValue_Guid_FromBsonBinary_InvalidLength_Generic_ShouldThrowFormatException()
    {
        var bin = new BsonBinary(new byte[15], BsonBinary.BinarySubType.Generic);
        await Assert.That(() => BsonConversion.FromBsonValue(bin, typeof(Guid)))
            .Throws<FormatException>();
    }

    [Test]
    public async Task FromBsonValue_Guid_FromBsonBinary_InvalidLength_Uuid_ShouldThrowArgumentException()
    {
        var bin = new BsonBinary(new byte[15], BsonBinary.BinarySubType.Uuid);
        await Assert.That(() => BsonConversion.FromBsonValue(bin, typeof(Guid)))
            .Throws<ArgumentException>();
    }

    [Test]
    public async Task FromBsonValue_Guid_FromBsonBinary_InvalidLength_UuidLegacy_ShouldThrowArgumentException()
    {
        var bin = new BsonBinary(new byte[15], BsonBinary.BinarySubType.UuidLegacy);
        await Assert.That(() => BsonConversion.FromBsonValue(bin, typeof(Guid)))
            .Throws<ArgumentException>();
    }

    [Test]
    public async Task FromBsonValue_Char_FromEmptyString_ShouldThrowFormatException()
    {
        var str = new BsonString("");
        await Assert.That(() => BsonConversion.FromBsonValue(str, typeof(char)))
            .Throws<FormatException>();
    }
}
