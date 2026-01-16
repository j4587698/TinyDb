using System.Collections.Generic;
using System.Threading.Tasks;
using TinyDb.Serialization;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Serialization;

public class ComplexObj
{
    public int Id { get; set; }
    public string[] List { get; set; } = Array.Empty<string>();
    public Dictionary<string, int> Dict { get; set; } = new();
    public ComplexObj? Nested { get; set; }
    public object? NullProp { get; set; }
}

public class BsonMapperCoverageTests
{
    [Test]
    public async Task Map_ComplexObject_ShouldWork()
    {
        var obj = new ComplexObj
        {
            Id = 1,
            List = new[] { "a", "b" },
            Dict = { { "k1", 1 } },
            Nested = new ComplexObj { Id = 2 }
        };
        
        var doc = BsonMapper.ToDocument(obj);
        await Assert.That(doc).IsNotNull();
        await Assert.That(doc["_id"].ToInt32(null)).IsEqualTo(1);
        
        // Debug info: check if List exists
        // await Assert.That(doc.ContainsKey("List")).IsTrue(); 
        
        // await Assert.That(((TinyDb.Bson.BsonArray)doc["List"]).Count).IsEqualTo(2);
        
        var mappedBack = BsonMapper.ToObject<ComplexObj>(doc);
        await Assert.That(mappedBack).IsNotNull();
        await Assert.That(mappedBack!.Id).IsEqualTo(1);
        await Assert.That(mappedBack.List).Count().IsEqualTo(2);
        await Assert.That(mappedBack.Dict["k1"]).IsEqualTo(1);
        await Assert.That(mappedBack.Nested!.Id).IsEqualTo(2);
        await Assert.That(mappedBack.NullProp).IsNull();
    }
}
