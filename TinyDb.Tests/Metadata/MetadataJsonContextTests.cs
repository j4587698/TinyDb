using System.Collections.Generic;
using System.Threading.Tasks;
using TinyDb.Metadata;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Metadata;

public class MetadataJsonContextTests
{
    [Test]
    public async Task MetadataJsonContext_ShouldHaveDefaultInstance()
    {
        var context = MetadataJsonContext.Default;
        await Assert.That(context).IsNotNull();
        
        // Access generated property
        await Assert.That(context.ListPropertyMetadata).IsNotNull();
    }

    [Test]
    public async Task MetadataJsonContext_Serialization_ShouldWork()
    {
        var list = new List<TinyDb.Metadata.PropertyMetadata>
        {
            new TinyDb.Metadata.PropertyMetadata { PropertyName = "P1", DisplayName = "D1" }
        };
        
        var json = System.Text.Json.JsonSerializer.Serialize(list, MetadataJsonContext.Default.ListPropertyMetadata);
        await Assert.That(json).Contains("P1");
        
        var deserialized = System.Text.Json.JsonSerializer.Deserialize(json, MetadataJsonContext.Default.ListPropertyMetadata);
        await Assert.That(deserialized).IsNotNull();
        await Assert.That(deserialized![0].PropertyName).IsEqualTo("P1");
    }
}