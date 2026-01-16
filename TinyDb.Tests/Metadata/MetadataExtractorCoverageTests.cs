using TinyDb.Metadata;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Metadata;

public class MetadataExtractorCoverageTests
{
    [EntityMetadata(DisplayName = "Custom Name", Description = "Custom Description")]
    public class AnnotatedClass
    {
        [PropertyMetadata("Age Prop", Description = "Age desc", Order = 10, Required = true)]
        public int Age { get; set; }
        
        public string Name { get; set; } = "";
    }

    public class PlainClass
    {
        public int Id { get; set; }
    }

    [Test]
    public async Task Extract_AnnotatedClass_ShouldWork()
    {
        var meta = MetadataExtractor.ExtractEntityMetadata(typeof(AnnotatedClass));
        
        await Assert.That(meta.DisplayName).IsEqualTo("Custom Name");
        await Assert.That(meta.Description).IsEqualTo("Custom Description");
        
        var ageProp = meta.Properties.First(p => p.PropertyName == "Age");
        await Assert.That(ageProp.DisplayName).IsEqualTo("Age Prop");
        await Assert.That(ageProp.Description).IsEqualTo("Age desc");
        await Assert.That(ageProp.Order).IsEqualTo(10);
        await Assert.That(ageProp.Required).IsTrue();
        
        var nameProp = meta.Properties.First(p => p.PropertyName == "Name");
        await Assert.That(nameProp.DisplayName).IsEqualTo("Name");
        await Assert.That(nameProp.Required).IsFalse();
    }

    [EntityMetadata] // One empty
    [EntityMetadata(DisplayName = "Real Name")]
    public class MultiAnnotatedClass
    {
        public int Id { get; set; }
    }

    public class PartialAnnotatedClass
    {
        [PropertyMetadata("P1")]
        public string Prop1 { get; set; } = "";
        
        public string Prop2 { get; set; } = "";
    }

    [EntityMetadata(Description = "")] // One empty desc
    [EntityMetadata(Description = "Real Description")]
    public class MultiDescClass
    {
        public int Id { get; set; }
    }

    public class PartialPropertyClass
    {
        public int ReadWrite { get; set; }
        public int GetOnly { get; } = 1;
        public int SetOnly { set { } }
    }

    [Test]
    public async Task Extract_PartialPropertyClass_ShouldOnlyIncludeReadWrite()
    {
        var meta = MetadataExtractor.ExtractEntityMetadata(typeof(PartialPropertyClass));
        await Assert.That(meta.Properties.Count).IsEqualTo(1);
        await Assert.That(meta.Properties[0].PropertyName).IsEqualTo("ReadWrite");
    }

    public class GenericClass<T>
    {
        public T? Data { get; set; }
    }

    [Test]
    public async Task Extract_GenericClass_ShouldWork()
    {
        var meta = MetadataExtractor.ExtractEntityMetadata(typeof(GenericClass<int>));
        await Assert.That(meta.Properties.Count).IsEqualTo(1);
        await Assert.That(meta.Properties[0].PropertyName).IsEqualTo("Data");
    }

    [Test]
    public async Task Extract_OpenGenericClass_ShouldWork()
    {
        // For open generic types, properties might have null FullName
        var meta = MetadataExtractor.ExtractEntityMetadata(typeof(GenericClass<>));
        await Assert.That(meta.Properties.Count).IsEqualTo(1);
        // PropertyType.FullName will likely be null here
    }

    [Test]
    public async Task Extract_MultiAnnotatedClass_ShouldUseFirstNonEmpty()
    {
        var meta = MetadataExtractor.ExtractEntityMetadata(typeof(MultiAnnotatedClass));
        // It should pick "Real Name"
        await Assert.That(meta.DisplayName).IsEqualTo("Real Name");
    }

    [Test]
    public async Task Extract_MultiDescClass_ShouldUseFirstNonEmpty()
    {
        var meta = MetadataExtractor.ExtractEntityMetadata(typeof(MultiDescClass));
        await Assert.That(meta.Description).IsEqualTo("Real Description");
    }

    [Test]
    public async Task Extract_PartialAnnotatedClass_ShouldWork()
    {
        var meta = MetadataExtractor.ExtractEntityMetadata(typeof(PartialAnnotatedClass));
        
        var p1 = meta.Properties.First(p => p.PropertyName == "Prop1");
        await Assert.That(p1.DisplayName).IsEqualTo("P1");
        
        var p2 = meta.Properties.First(p => p.PropertyName == "Prop2");
        await Assert.That(p2.DisplayName).IsEqualTo("Prop2");
    }

    [Test]
    public async Task Extract_InvalidArguments_ShouldThrow()
    {
        await Assert.That(() => MetadataExtractor.ExtractEntityMetadata(null!)).Throws<ArgumentNullException>();
        await Assert.That(() => MetadataExtractor.ExtractPropertyMetadata(null!)).Throws<ArgumentNullException>();
    }
}
