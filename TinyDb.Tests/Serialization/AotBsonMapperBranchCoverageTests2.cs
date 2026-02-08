using System;
using System.Collections.Generic;
using System.Reflection;
using TinyDb.Bson;
using TinyDb.Serialization;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Serialization;

public class AotBsonMapperBranchCoverageTests2
{
    private sealed class NoMatchingCtorCollection
    {
        public NoMatchingCtorCollection(string _) { }
    }

    [Test]
    public async Task ConvertValue_Guid_FromBsonBinary_Uuid_ShouldWork()
    {
        var guid = Guid.NewGuid();
        var bin = new BsonBinary(guid.ToByteArray(), BsonBinary.BinarySubType.Uuid);
        var converted = (Guid)AotBsonMapper.ConvertValue(bin, typeof(Guid))!;
        await Assert.That(converted).IsEqualTo(guid);
    }

    [Test]
    public async Task ConvertValue_Guid_FromBsonBinary_NonUuidSubtype_ShouldThrowFormatException()
    {
        var bin = new BsonBinary(Guid.NewGuid().ToByteArray(), BsonBinary.BinarySubType.Generic);
        await Assert.That(() => AotBsonMapper.ConvertValue(bin, typeof(Guid)))
            .Throws<FormatException>();
    }

    [Test]
    public async Task TryWrapWithTargetCollection_WhenNoMatchingCtor_ShouldReturnNull()
    {
        var method = typeof(AotBsonMapper).GetMethod("TryWrapWithTargetCollection", BindingFlags.NonPublic | BindingFlags.Static);
        await Assert.That(method).IsNotNull();

        var source = new List<int> { 1, 2, 3 };
        var wrapped = method!.Invoke(null, new object[] { typeof(NoMatchingCtorCollection), source });
        await Assert.That(wrapped).IsNull();
    }
}

