using System;
using System.IO;
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

        await Assert.That(MetadataExtractor.GetEntityTypeName(typeParam)).IsEqualTo(typeParam.Name);

        var metaClosed = MetadataExtractor.ExtractEntityMetadata(typeof(GenericHolder<int>));
        await Assert.That(metaClosed.TypeName).Contains(nameof(GenericHolder<int>));
    }

    [Test]
    public async Task MetadataManager_Ctor_NullEngine_ShouldThrow()
    {
        await Assert.That(() => new MetadataManager(null!)).Throws<ArgumentNullException>();
    }

    [Test]
    public async Task MetadataManager_SaveMetadata_ShouldRequire_TableName()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"metadata_guard_{Guid.NewGuid():N}.db");
        try
        {
            using var engine = new TinyDbEngine(dbPath, new TinyDbOptions { EnableJournaling = false });
            var manager = new MetadataManager(engine);

            await Assert.That(() => manager.SaveMetadata(new MetadataDocument())).Throws<ArgumentException>();
        }
        finally
        {
            try { if (File.Exists(dbPath)) File.Delete(dbPath); } catch { }
        }
    }
}
