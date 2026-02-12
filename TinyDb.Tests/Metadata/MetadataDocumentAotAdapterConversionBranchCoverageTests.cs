using System;
using System.Linq;
using TinyDb.Bson;
using TinyDb.Metadata;
using TinyDb.Serialization;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Metadata;

public class MetadataDocumentAotAdapterConversionBranchCoverageTests
{
    [Test]
    public async Task FromDocument_WithStringEncodedDateTime_ShouldFallbackToDefaults()
    {
        var entity = new MetadataDocument
        {
            TableName = "C",
            TypeName = "T",
            DisplayName = "D",
            Description = "Desc",
            CreatedAt = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc),
            UpdatedAt = new DateTime(2024, 1, 2, 0, 0, 0, DateTimeKind.Utc)
        };

        var doc = AotBsonMapper.ToDocument(entity);

        var modified = doc;
        foreach (var kv in doc)
        {
            modified = kv.Value switch
            {
                // 仅将 DateTime 写成字符串格式以覆盖对应分支；_id 仍保持为 BsonObjectId（更贴近真实存储）
                BsonDateTime dt => modified.Set(kv.Key, new BsonString(dt.Value.ToString("O"))),
                _ => modified
            };
        }

        var roundtrip = AotBsonMapper.FromDocument<MetadataDocument>(modified);
        await Assert.That(roundtrip.TableName).IsEqualTo(entity.TableName);

        // 当前 AOT 适配器仅处理 BsonDateTime；当收到字符串时应回退到属性默认值（DateTime.Now）
        await Assert.That(roundtrip.CreatedAt).IsNotEqualTo(entity.CreatedAt);
        await Assert.That(roundtrip.UpdatedAt).IsNotEqualTo(entity.UpdatedAt);

        var now = DateTime.Now;
        await Assert.That(Math.Abs((roundtrip.CreatedAt - now).TotalMinutes)).IsLessThan(5.0);
        await Assert.That(Math.Abs((roundtrip.UpdatedAt - now).TotalMinutes)).IsLessThan(5.0);
    }

    [Test]
    public async Task FromDocument_ShouldHandleNullValues()
    {
        var entity = new MetadataDocument
        {
            TableName = "C",
            TypeName = "T",
            DisplayName = "D",
            Description = "Desc",
            CreatedAt = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc),
            UpdatedAt = new DateTime(2024, 1, 2, 0, 0, 0, DateTimeKind.Utc)
        };

        var doc = AotBsonMapper.ToDocument(entity);
        var modified = doc;
        foreach (var key in doc.Keys.ToList())
        {
            modified = modified.Set(key, BsonNull.Value);
        }

        var roundtrip = AotBsonMapper.FromDocument<MetadataDocument>(modified);
        await Assert.That(roundtrip).IsNotNull();
    }
}
