using TinyDb.Serialization;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Serialization;

public class AotMapperTypeTests
{
    [Test]
    public async Task Mapping_With_Guid_And_DateTime_Should_Work()
    {
        var entity = new TypeEntity
        {
            Id = Guid.NewGuid(),
            Time = DateTime.UtcNow,
            NullableTime = null,
            Price = 9.99m
        };

        var doc = AotBsonMapper.ToDocument(entity);
        var replayed = AotBsonMapper.FromDocument<TypeEntity>(doc);

        await Assert.That(replayed.Id).IsEqualTo(entity.Id);
        // BSON DateTime precision is milliseconds
        await Assert.That((replayed.Time - entity.Time).TotalMilliseconds).IsLessThan(1.0);
        await Assert.That(replayed.NullableTime).IsNull();
        await Assert.That(replayed.Price).IsEqualTo(9.99m);
    }

    public class TypeEntity
    {
        public Guid Id { get; set; }
        public DateTime Time { get; set; }
        public DateTime? NullableTime { get; set; }
        public decimal Price { get; set; }
    }
}
