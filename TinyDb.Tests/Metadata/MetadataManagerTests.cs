using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using TinyDb.Core;
using TinyDb.Metadata;
using TinyDb.Attributes;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Metadata;

public class MetadataManagerTests
{
    private string _testDbPath = null!;
    private TinyDbEngine _engine = null!;

    [Before(Test)]
    public void Setup()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"metadata_db_{Guid.NewGuid():N}.db");
        if (File.Exists(_testDbPath))
        {
            File.Delete(_testDbPath);
        }

        _engine = new TinyDbEngine(_testDbPath);
    }

    [After(Test)]
    public void Cleanup()
    {
        _engine?.Dispose();
        if (File.Exists(_testDbPath))
        {
            File.Delete(_testDbPath);
        }
    }

    [Test]
    public async Task Save_Metadata_Should_Preserve_Special_Characters()
    {
        var manager = new MetadataManager(_engine);

        manager.SaveEntityMetadata(typeof(SpecialMetadataEntity));
        var metadata = manager.GetEntityMetadata(typeof(SpecialMetadataEntity));

        await Assert.That(metadata).IsNotNull();
        await Assert.That(metadata!.DisplayName).IsEqualTo("复杂实体");
        await Assert.That(metadata.Description).IsEqualTo("描述包含;分号和|竖线");
        await Assert.That(metadata.Properties.Count).IsEqualTo(3);

        var specialProperty = metadata.Properties.First(prop => prop.PropertyName == nameof(SpecialMetadataEntity.SpecialField));
        await Assert.That(specialProperty.DisplayName).IsEqualTo("特殊字段|字段");
        await Assert.That(specialProperty.Description).IsEqualTo("包含;分号|竖线");
        await Assert.That(specialProperty.Required).IsTrue();
    }

    [Test]
    public async Task Save_Metadata_Should_Update_Existing_Record()
    {
        var manager = new MetadataManager(_engine);
        var collectionName = $"__metadata_{typeof(SpecialMetadataEntity).FullName}";
        var collection = _engine.GetCollectionWithName<MetadataDocument>(collectionName);

        manager.SaveEntityMetadata(typeof(SpecialMetadataEntity));
        var initialDoc = collection.FindAll().Single();
        var initialUpdatedAt = initialDoc.UpdatedAt;
        var initialCreatedAt = initialDoc.CreatedAt;

        await Task.Delay(50);

        manager.SaveEntityMetadata(typeof(SpecialMetadataEntity));
        var documents = collection.FindAll().ToList();

        await Assert.That(documents.Count).IsEqualTo(1);

        var updatedDoc = documents.Single();
        await Assert.That(updatedDoc.CreatedAt).IsEqualTo(initialCreatedAt);
        await Assert.That(updatedDoc.UpdatedAt).IsGreaterThan(initialUpdatedAt);
    }

    [EntityMetadata("复杂实体", Description = "描述包含;分号和|竖线")]
    private class SpecialMetadataEntity
    {
        [Id]
        [PropertyMetadata("标识符", Order = 1)]
        public int Id { get; set; }

        [PropertyMetadata("特殊字段|字段", Description = "包含;分号|竖线", Order = 2, Required = true)]
        public string SpecialField { get; set; } = string.Empty;

        [PropertyMetadata("普通字段", Description = "普通描述", Order = 3)]
        public string NormalField { get; set; } = string.Empty;
    }
}
