using System;
using System.Collections.Generic;
using TinyDb.Bson;
using TinyDb.Serialization;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Serialization;

public sealed class AotBsonMapperOpenGenericCoverageTests
{
    [Test]
    public async Task ConvertValue_OpenGenericCollectionType_ShouldThrowInvalidOperation()
    {
        var bsonArray = new BsonArray(new BsonValue[] { new BsonInt32(1) });

        await Assert.That(() => AotBsonMapper.ConvertValue(bsonArray, typeof(IEnumerable<>)))
            .Throws<InvalidOperationException>();
    }
}
