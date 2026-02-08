using System.Linq;
using TinyDb.Attributes;
using TinyDb.Serialization;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Serialization;

public class EntityMetadataAdditionalCoverageTests
{
    private sealed class MetadataSample
    {
        public int Id { get; set; }

        public int WriteOnly
        {
            set { }
        }

        public static int StaticValue { get; set; }

        [BsonIgnore]
        public int Ignored { get; set; }
    }

    [Entity(IdProperty = nameof(CustomKey))]
    internal sealed class EntityWithCustomId
    {
        public int CustomKey { get; set; }
        public int Other { get; set; }
    }

    [Entity(IdProperty = "Missing")]
    internal sealed class EntityWithMissingIdProperty
    {
        public int Id { get; set; }
    }

    [Test]
    public async Task EntityMetadata_Properties_ShouldFilterStaticWriteOnlyAndIgnored()
    {
        var properties = EntityMetadata<MetadataSample>.Properties;

        await Assert.That(properties.Any(p => p.Name == nameof(MetadataSample.Id))).IsTrue();
        await Assert.That(properties.Any(p => p.Name == nameof(MetadataSample.WriteOnly))).IsFalse();
        await Assert.That(properties.Any(p => p.Name == nameof(MetadataSample.StaticValue))).IsFalse();
        await Assert.That(properties.Any(p => p.Name == nameof(MetadataSample.Ignored))).IsFalse();
    }

    [Test]
    public async Task EntityMetadata_IdProperty_WhenSpecifiedByEntityAttribute_ShouldUseSpecifiedProperty()
    {
        var idProperty = EntityMetadata<EntityWithCustomId>.IdProperty;

        await Assert.That(idProperty).IsNotNull();
        await Assert.That(idProperty!.Name).IsEqualTo(nameof(EntityWithCustomId.CustomKey));
    }

    [Test]
    public async Task EntityMetadata_IdProperty_WhenSpecifiedPropertyMissing_ShouldFallbackToNamedId()
    {
        var idProperty = EntityMetadata<EntityWithMissingIdProperty>.IdProperty;

        await Assert.That(idProperty).IsNotNull();
        await Assert.That(idProperty!.Name).IsEqualTo(nameof(EntityWithMissingIdProperty.Id));
    }
}
