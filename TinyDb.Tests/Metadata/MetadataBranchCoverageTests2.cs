using System;
using System.Reflection;
using TinyDb.Core;
using TinyDb.Metadata;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Metadata;

public class MetadataBranchCoverageTests2
{
    private sealed class GenericHolder<T>
    {
        public int Value { get; set; }
    }

    [Test]
    public async Task MetadataAttributes_NullDisplayName_ShouldThrow()
    {
        await Assert.That(() => new EntityMetadataAttribute(null!)).Throws<ArgumentNullException>();
        await Assert.That(() => new PropertyMetadataAttribute(null!)).Throws<ArgumentNullException>();
    }

    [Test]
    public async Task MetadataExtractor_WhenTypeFullNameNull_ShouldFallbackToName()
    {
        var typeParam = typeof(GenericHolder<>).GetGenericArguments()[0];

        var metaParam = MetadataExtractor.ExtractEntityMetadata(typeParam);
        await Assert.That(metaParam.TypeName).IsEqualTo(typeParam.Name);

        var metaClosed = MetadataExtractor.ExtractEntityMetadata(typeof(GenericHolder<int>));
        await Assert.That(metaClosed.TypeName).Contains(nameof(GenericHolder<int>));
    }

    [Test]
    public async Task MetadataManager_Ctor_NullEngine_ShouldThrow()
    {
        await Assert.That(() => new MetadataManager(null!)).Throws<ArgumentNullException>();
    }

    [Test]
    public async Task MetadataManager_GetMetadataCollectionName_ShouldFallbackToTypeName_WhenFullNameNull()
    {
        var method = typeof(MetadataManager).GetMethod("GetMetadataCollectionName", BindingFlags.NonPublic | BindingFlags.Static);
        await Assert.That(method).IsNotNull();

        var fullName = (string)method!.Invoke(null, new object[] { typeof(GenericHolder<int>) })!;
        await Assert.That(fullName).Contains(typeof(GenericHolder<int>).FullName!);

        var typeParam = typeof(GenericHolder<>).GetGenericArguments()[0];
        var fallback = (string)method.Invoke(null, new object[] { typeParam })!;
        await Assert.That(fallback).Contains(typeParam.Name);
    }
}

