using TinyDb.IdGeneration;
using TinyDb.Bson;
using TinyDb.Attributes;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.IdGeneration;

public class IdGenerationCoverageTests
{
    public enum IdEnum { None, Some }
    public class EnumIdClass { [IdGeneration(IdGenerationStrategy.IdentityInt)] public IdEnum Id { get; set; } }
    public class GuidIdClass { [IdGeneration(IdGenerationStrategy.GuidV4)] public Guid Id { get; set; } }
    public class StringIdClass { [IdGeneration(IdGenerationStrategy.ObjectId)] public string Id { get; set; } = ""; }

    [Test]
    public async Task ConvertGeneratedId_Coverage()
    {
        // Internal method, test through public API or reflection
        var method = typeof(IdGenerationHelper<GuidIdClass>).GetMethod("ConvertGeneratedId", 
            System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.NonPublic);

        // Guid from bytes
        var guid = Guid.NewGuid();
        var bytes = guid.ToByteArray();
        var res1 = method!.Invoke(null, new object[] { new BsonBinary(bytes), typeof(Guid) });
        await Assert.That(res1).IsEqualTo(guid);

        // ObjectId from string
        var oid = ObjectId.NewObjectId();
        var res2 = method!.Invoke(null, new object[] { new BsonString(oid.ToString()), typeof(ObjectId) });
        await Assert.That(res2).IsEqualTo(oid);

        // Enum conversion
        var res3 = method!.Invoke(null, new object[] { new BsonString("Some"), typeof(IdEnum) });
        await Assert.That(res3).IsEqualTo(IdEnum.Some);
        
        // Null raw value
        var res4 = method!.Invoke(null, new object[] { BsonNull.Value, typeof(string) });
        await Assert.That(res4).IsNull();
    }

    [Test]
    public async Task IdentityGenerator_UnsupportedType_ShouldThrow()
    {
        var gen = new IdentityGenerator();
        var prop = typeof(EnumIdClass).GetProperty("Id")!;
        await Assert.That(() => gen.GenerateId(typeof(EnumIdClass), prop)).Throws<InvalidOperationException>();
    }

    [Test]
    public async Task ShouldGenerateId_Coverage()
    {
        var entity = new GuidIdClass { Id = Guid.NewGuid() };
        // Already has valid ID
        await Assert.That(IdGenerationHelper<GuidIdClass>.ShouldGenerateId(entity)).IsFalse();
        
        entity.Id = Guid.Empty;
        await Assert.That(IdGenerationHelper<GuidIdClass>.ShouldGenerateId(entity)).IsTrue();
    }
}
