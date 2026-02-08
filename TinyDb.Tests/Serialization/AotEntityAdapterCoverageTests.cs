using System.Collections;
using TinyDb.Bson;
using TinyDb.Serialization;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Serialization;

public class AotEntityAdapterCoverageTests
{
    public class AdapterEntity
    {
        public int Id { get; set; }
    }

    [Test]
    public async Task CreateArray_And_List_ShouldWork()
    {
        var adapter = CreateAdapter();

        var array = adapter.CreateArray(2);
        await Assert.That(array.Length).IsEqualTo(2);

        var list = adapter.CreateList();
        await Assert.That(list).IsAssignableTo<IList>();
        await Assert.That(list.Count).IsEqualTo(0);
    }

    [Test]
    public async Task CreateListFrom_ShouldFilterItems()
    {
        var adapter = CreateAdapter();
        var items = new object[]
        {
            new AdapterEntity { Id = 1 },
            null!,
            "ignored"
        };

        var list = adapter.CreateListFrom(items);
        await Assert.That(list.Count).IsEqualTo(2);
        await Assert.That(list[0]).IsTypeOf<AdapterEntity>();
        await Assert.That(list[1]).IsNull();
    }

    private static AotEntityAdapter<AdapterEntity> CreateAdapter()
    {
        return new AotEntityAdapter<AdapterEntity>(
            entity => new BsonDocument().Set("_id", entity.Id),
            document => new AdapterEntity { Id = document["_id"].ToInt32(null) },
            entity => new BsonInt32(entity.Id),
            (entity, id) => entity.Id = id.ToInt32(null),
            entity => entity.Id != 0,
            (entity, name) => name == "Id" ? entity.Id : null);
    }
}
