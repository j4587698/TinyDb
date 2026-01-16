using TinyDb.Bson;
using TinyDb.Serialization;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Serialization;

public class AotIdAccessorCoverageTests
{
    public class NoIdClass { public string Name { get; set; } = ""; }
    public class IntIdClass { public int Id { get; set; } }
    public class LongIdClass { public long Id { get; set; } }
    public class GuidIdClass { public Guid Id { get; set; } }
    public class StringIdClass { public string Id { get; set; } = ""; }
    public class ObjectIdClass { public ObjectId Id { get; set; } }

    [Test]
    public async Task GetId_NoIdProperty_ShouldReturnNull()
    {
        var entity = new NoIdClass { Name = "test" };
        var id = AotIdAccessor<NoIdClass>.GetId(entity);
        await Assert.That(id).IsEqualTo(BsonNull.Value);
    }

    [Test]
    public async Task SetId_NoIdProperty_ShouldNoOp()
    {
        var entity = new NoIdClass { Name = "test" };
        AotIdAccessor<NoIdClass>.SetId(entity, 123);
        await Assert.That(entity.Name).IsEqualTo("test");
    }

    [Test]
    public async Task HasValidId_VariousTypes_ShouldWork()
    {
        await Assert.That(AotIdAccessor<IntIdClass>.HasValidId(new IntIdClass { Id = 0 })).IsFalse();
        await Assert.That(AotIdAccessor<IntIdClass>.HasValidId(new IntIdClass { Id = 1 })).IsTrue();

        await Assert.That(AotIdAccessor<LongIdClass>.HasValidId(new LongIdClass { Id = 0 })).IsFalse();
        await Assert.That(AotIdAccessor<LongIdClass>.HasValidId(new LongIdClass { Id = 1 })).IsTrue();

        await Assert.That(AotIdAccessor<GuidIdClass>.HasValidId(new GuidIdClass { Id = Guid.Empty })).IsFalse();
        await Assert.That(AotIdAccessor<GuidIdClass>.HasValidId(new GuidIdClass { Id = Guid.NewGuid() })).IsTrue();

        await Assert.That(AotIdAccessor<StringIdClass>.HasValidId(new StringIdClass { Id = "" })).IsFalse();
        await Assert.That(AotIdAccessor<StringIdClass>.HasValidId(new StringIdClass { Id = " " })).IsFalse();
        await Assert.That(AotIdAccessor<StringIdClass>.HasValidId(new StringIdClass { Id = "id" })).IsTrue();

        await Assert.That(AotIdAccessor<ObjectIdClass>.HasValidId(new ObjectIdClass { Id = ObjectId.Empty })).IsFalse();
        await Assert.That(AotIdAccessor<ObjectIdClass>.HasValidId(new ObjectIdClass { Id = ObjectId.NewObjectId() })).IsTrue();
        
        await Assert.That(AotIdAccessor<NoIdClass>.HasValidId(new NoIdClass())).IsFalse();
        await Assert.That(AotIdAccessor<IntIdClass>.HasValidId(null!)).IsFalse();
    }

    [Test]
    public async Task SetId_Conversion_Coverage()
    {
        var guid = Guid.NewGuid();
        var guidEntity = new GuidIdClass();
        AotIdAccessor<GuidIdClass>.SetId(guidEntity, guid.ToString()); // String to Guid
        await Assert.That(guidEntity.Id).IsEqualTo(guid);

        var oid = ObjectId.NewObjectId();
        var oidEntity = new ObjectIdClass();
        AotIdAccessor<ObjectIdClass>.SetId(oidEntity, oid.ToString()); // String to ObjectId
        await Assert.That(oidEntity.Id).IsEqualTo(oid);
        
        var strEntity = new StringIdClass();
        AotIdAccessor<StringIdClass>.SetId(strEntity, 123); // Int to String
        await Assert.That(strEntity.Id).IsEqualTo("123");
    }
}
