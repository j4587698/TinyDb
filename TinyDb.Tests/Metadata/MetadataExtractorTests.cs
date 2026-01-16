using TinyDb.Metadata;
using TinyDb.Attributes;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Metadata;

public class MetadataExtractorTests
{
    [Test]
    public async Task ExtractEntityMetadata_Should_Work_For_Plain_Class()
    {
        var meta = MetadataExtractor.ExtractEntityMetadata(typeof(PlainClass));
        await Assert.That(meta.TypeName).IsEqualTo(typeof(PlainClass).FullName);
        await Assert.That(meta.DisplayName).IsEqualTo("PlainClass");
        await Assert.That(meta.Description).IsNull();
        await Assert.That(meta.Properties.Count).IsEqualTo(1);
    }

    [Test]
    public async Task ExtractEntityMetadata_Should_Work_For_Annotated_Class()
    {
        var meta = MetadataExtractor.ExtractEntityMetadata(typeof(AnnotatedClass));
        await Assert.That(meta.DisplayName).IsEqualTo("MyClass");
        await Assert.That(meta.Description).IsEqualTo("Desc");
        
        var prop = meta.Properties.First(p => p.PropertyName == "Prop");
        await Assert.That(prop.DisplayName).IsEqualTo("MyProp");
        await Assert.That(prop.Description).IsEqualTo("PropDesc");
        await Assert.That(prop.Order).IsEqualTo(10);
        await Assert.That(prop.Required).IsTrue();
    }

    [Test]
    public async Task ExtractEntityMetadata_Should_Throw_On_Null()
    {
        await Assert.That(() => MetadataExtractor.ExtractEntityMetadata(null!)).Throws<ArgumentNullException>();
    }

    public class PlainClass
    {
        public int Id { get; set; }
    }

    [EntityMetadata("MyClass", Description = "Desc")]
    public class AnnotatedClass
    {
        [PropertyMetadata("MyProp", Description = "PropDesc", Order = 10, Required = true)]
        public string Prop { get; set; } = "";
    }
}
