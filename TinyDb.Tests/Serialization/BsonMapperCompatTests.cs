using TinyDb.Bson;
using TinyDb.Serialization;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Serialization;

public class BsonMapperCompatTests
{
    public class Simple { public int Id { get; set; } public string Name { get; set; } = ""; }

    [Test]
    public async Task BsonMapper_Delegation_ShouldWork()
    {
        var entity = new Simple { Id = 1, Name = "test" };
        
        var doc = BsonMapper.ToDocument(entity);
        await Assert.That(doc["_id"].ToInt32()).IsEqualTo(1);
        
        var restored = BsonMapper.ToObject<Simple>(doc);
        await Assert.That(restored!.Name).IsEqualTo("test");
        
        var id = BsonMapper.GetId(entity);
        await Assert.That(id.ToInt32()).IsEqualTo(1);
        
        BsonMapper.SetId(entity, 2);
        await Assert.That(entity.Id).IsEqualTo(2);
        
        var bsonVal = BsonMapper.ConvertToBsonValue("hello");
        await Assert.That(bsonVal.ToString()).IsEqualTo("hello");
        
        var fromBson = BsonMapper.ConvertFromBsonValue(new BsonInt32(123), typeof(int));
        await Assert.That(fromBson).IsEqualTo(123);
    }
}
