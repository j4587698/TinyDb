using System;
using System.Threading.Tasks;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Bson;

public class ImmutableTests
{
    [Test]
    public async Task BsonArray_IsImmutable()
    {
        var arr = new BsonArray();
        await Assert.That(() => arr.Add(1)).Throws<NotSupportedException>();
        await Assert.That(() => arr.Insert(0, 1)).Throws<NotSupportedException>();
        await Assert.That(() => arr.Clear()).Throws<NotSupportedException>();
        await Assert.That(() => arr.Remove(1)).Throws<NotSupportedException>();
        await Assert.That(() => arr.RemoveAt(0)).Throws<NotSupportedException>();
        await Assert.That(() => arr[0] = 1).Throws<NotSupportedException>();
    }

    [Test]
    public async Task BsonDocument_IsImmutable()
    {
        var doc = new BsonDocument();
        await Assert.That(() => doc.Add("a", 1)).Throws<NotSupportedException>();
        await Assert.That(() => doc.Add(new System.Collections.Generic.KeyValuePair<string, BsonValue>("a", 1))).Throws<NotSupportedException>();
        await Assert.That(() => doc.Clear()).Throws<NotSupportedException>();
        await Assert.That(() => doc.Remove("a")).Throws<NotSupportedException>();
        await Assert.That(() => doc.Remove(new System.Collections.Generic.KeyValuePair<string, BsonValue>("a", 1))).Throws<NotSupportedException>();
        await Assert.That(() => doc["a"] = 1).Throws<NotSupportedException>();
    }
}
