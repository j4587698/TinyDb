using TinyDb.Attributes;
using TinyDb.Bson;
using TinyDb.Serialization;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Regression;

public class AotReflectionRegressionTests
{
    [Test]
    public async Task AotIdAccessor_ShouldExposeGeneratedIdMetadata()
    {
        var found = AotIdAccessor<AotGeneratedIntIdDocument>.TryGetIdInfo(out var name, out var type);

        await Assert.That(found).IsTrue();
        await Assert.That(name).IsEqualTo(nameof(AotGeneratedIntIdDocument.Id));
        await Assert.That(type).IsEqualTo(typeof(int));
    }

    [Test]
    public async Task AotIdAccessor_GenerateIdIfNeeded_ShouldLeaveIdentityIdsForEngine()
    {
        var entity = new AotGeneratedIntIdDocument();

        var generated = AotIdAccessor<AotGeneratedIntIdDocument>.GenerateIdIfNeeded(entity);

        await Assert.That(generated).IsFalse();
        await Assert.That(entity.Id).IsEqualTo(0);
    }

    [Test]
    public async Task BsonConversion_GenericAotPath_ShouldConvertNestedScalarList()
    {
        var array = new BsonArray(new BsonValue[] { new BsonInt32(1), new BsonInt32(2) });

        var values = BsonConversion.FromBsonValue<List<int>>(array);

        await Assert.That(values).IsNotNull();
        await Assert.That(values!.Count).IsEqualTo(2);
        await Assert.That(values[0]).IsEqualTo(1);
        await Assert.That(values[1]).IsEqualTo(2);
    }
}

[Entity]
public sealed class AotGeneratedIntIdDocument
{
    public int Id { get; set; }
    public string Name { get; set; } = "";
}
