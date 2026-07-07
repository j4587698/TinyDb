using System.IO;
using TinyDb.Attributes;
using TinyDb.Bson;
using TinyDb.Core;
using TinyDb.IdGeneration;
using TinyDb.Index;
using TinyDb.Metadata;
using TinyDb.Query;
using TinyDb.Serialization;
using TinyDb.Storage;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using QueryConstantExpression = TinyDb.Query.ConstantExpression;

namespace TinyDb.Tests.Regression;

[NotInParallel]
public sealed class ImplementationFixRegressionTests : IDisposable
{
    private readonly string _directory;

    public ImplementationFixRegressionTests()
    {
        _directory = Path.Combine(Path.GetTempPath(), "TinyDbImplementationFixes", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_directory);
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_directory)) Directory.Delete(_directory, recursive: true);
        }
        catch
        {
        }
    }

    [Test]
    public async Task IndexedGuidPredicate_ShouldUseBinaryBsonKey()
    {
        var path = Path.Combine(_directory, "guid-query.db");
        var externalId = Guid.NewGuid();

        using var engine = new TinyDbEngine(path);
        var collection = engine.GetCollection<GuidIndexedDocument>("guid_docs");
        engine.EnsureIndex("guid_docs", "ExternalId", "idx_external_id", unique: false);

        collection.Insert(new GuidIndexedDocument { Id = 1, ExternalId = externalId, Name = "match" });
        collection.Insert(new GuidIndexedDocument { Id = 2, ExternalId = Guid.NewGuid(), Name = "miss" });

        var matches = collection.Find(x => x.ExternalId == externalId).ToList();

        await Assert.That(BsonConversion.ToBsonValue(externalId)).IsTypeOf<BsonBinary>();
        await Assert.That(matches.Select(x => x.Id).SequenceEqual(new[] { 1 })).IsTrue();
    }

    [Test]
    public async Task GuidV7Generator_ShouldReturnBinaryForGuidIds()
    {
        var generator = new GuidV7Generator();
        var idProperty = typeof(GuidGeneratedIdDocument).GetProperty(nameof(GuidGeneratedIdDocument.Id))!;

        var generated = generator.GenerateId(typeof(GuidGeneratedIdDocument), idProperty);

        await Assert.That(generated).IsTypeOf<BsonBinary>();
        await Assert.That(new Guid(((BsonBinary)generated).Bytes)).IsNotEqualTo(Guid.Empty);
    }

    [Test]
    public async Task CompositeRangeIndex_ShouldStillApplyTrailingPredicatePostFilter()
    {
        var path = Path.Combine(_directory, "composite-post-filter.db");

        using var engine = new TinyDbEngine(path);
        var collection = engine.GetCollection<CompositeIndexedDocument>("composite_docs");
        engine.GetIndexManager("composite_docs").CreateIndex("idx_ab", new[] { "A", "B" }, unique: false);

        collection.Insert(new CompositeIndexedDocument { Id = 1, A = 2, B = 0 });
        collection.Insert(new CompositeIndexedDocument { Id = 2, A = 2, B = 1 });
        collection.Insert(new CompositeIndexedDocument { Id = 3, A = 3, B = 0 });
        collection.Insert(new CompositeIndexedDocument { Id = 4, A = 3, B = 1 });

        var matches = collection.Find(x => x.A > 1 && x.B == 1).OrderBy(x => x.Id).ToList();

        await Assert.That(matches.Select(x => x.Id).SequenceEqual(new[] { 2, 4 })).IsTrue();
    }

    [Test]
    public async Task BsonSerializer_ShouldPreserveEmptyPayloadCompatibility()
    {
        await Assert.That(BsonSerializer.Deserialize(Array.Empty<byte>()).IsNull).IsTrue();
        await Assert.That(BsonSerializer.DeserializeDocument(Array.Empty<byte>()).Count).IsEqualTo(0);
        await Assert.That(BsonSerializer.DeserializeDocument(ReadOnlyMemory<byte>.Empty).Count).IsEqualTo(0);
        await Assert.That(BsonSerializer.DeserializeArray(Array.Empty<byte>()).Count).IsEqualTo(0);
    }

    [Test]
    public async Task BsonScanner_ShouldRejectDeclaredDocumentLengthOutsideBuffer()
    {
        var malformed = new byte[]
        {
            100, 0, 0, 0,
            (byte)BsonType.Int32,
            (byte)'x', 0,
            1, 0, 0, 0,
            0
        };

        await Assert.That(BsonScanner.TryGetValue(malformed, "x", out _)).IsFalse();
        await Assert.That(BsonScanner.TryLocateField(malformed, "x"u8, out _, out _)).IsFalse();
    }

    [Test]
    public async Task ExpressionEvaluator_StringFunctions_ShouldRespectStringComparison()
    {
        var doc = new BsonDocument();

        var contains = ExpressionEvaluator.EvaluateValue(
            new FunctionExpression(
                "Contains",
                new QueryConstantExpression("Hello"),
                new QueryExpression[] { new QueryConstantExpression("he"), new QueryConstantExpression(StringComparison.OrdinalIgnoreCase) }),
            doc);

        var startsWith = ExpressionEvaluator.EvaluateValue(
            new FunctionExpression(
                "StartsWith",
                new QueryConstantExpression("Hello"),
                new QueryExpression[] { new QueryConstantExpression("he"), new QueryConstantExpression(StringComparison.OrdinalIgnoreCase) }),
            doc);

        var endsWith = ExpressionEvaluator.EvaluateValue(
            new FunctionExpression(
                "EndsWith",
                new QueryConstantExpression("Hello"),
                new QueryExpression[] { new QueryConstantExpression("LO"), new QueryConstantExpression(StringComparison.OrdinalIgnoreCase) }),
            doc);

        await Assert.That(contains).IsEqualTo(true);
        await Assert.That(startsWith).IsEqualTo(true);
        await Assert.That(endsWith).IsEqualTo(true);
    }

    [Test]
    public async Task ExpressionEvaluator_InternalCaches_ShouldStayBounded()
    {
        var doc = new BsonDocument();
        var entity = new CacheProbe();

        for (var i = 0; i < 3000; i++)
        {
            ExpressionEvaluator.EvaluateValue(new TinyDb.Query.MemberExpression("Field" + i, null), doc);
        }

        for (var i = 0; i < 5000; i++)
        {
            ExpressionEvaluator.EvaluateValue(new TinyDb.Query.MemberExpression("Missing" + i, null), entity);
        }

        var cacheCounts = ExpressionEvaluator.GetCacheCounts();
        await Assert.That(cacheCounts.CamelCaseNames).IsLessThanOrEqualTo(2048);
        await Assert.That(cacheCounts.Properties).IsLessThanOrEqualTo(4096);
    }

    [Test]
    public async Task MetadataDocument_ShouldRoundTripPasswordMetadata()
    {
        var metadata = new EntityMetadata
        {
            TypeName = "Sample",
            CollectionName = "samples",
            DisplayName = "Samples",
            Properties =
            [
                new TinyDb.Metadata.PropertyMetadata
                {
                    PropertyName = "Password",
                    PropertyType = "System.String",
                    DisplayName = "Password",
                    Password = new PasswordMetadata
                    {
                        IsPassword = true,
                        RequiredStrength = PasswordStrength.Strong,
                        MinLength = 12,
                        MaxLength = 64,
                        Hint = "Use a phrase",
                        AllowToggle = false
                    }
                }
            ]
        };

        var roundTripped = MetadataDocument.FromEntityMetadata(metadata).ToEntityMetadata();
        var password = roundTripped.Properties.Single().Password;

        await Assert.That(password).IsNotNull();
        await Assert.That(password!.IsPassword).IsTrue();
        await Assert.That(password.RequiredStrength).IsEqualTo(PasswordStrength.Strong);
        await Assert.That(password.MinLength).IsEqualTo(12);
        await Assert.That(password.MaxLength).IsEqualTo(64);
        await Assert.That(password.Hint).IsEqualTo("Use a phrase");
        await Assert.That(password.AllowToggle).IsFalse();
    }

    [Test]
    public async Task SourceGeneratedMetadataRegistry_ShouldPreserveMetadataAttributes()
    {
        var metadata = MetadataExtractor.ExtractEntityMetadata(typeof(SourceGeneratedMetadataDocument));
        var name = metadata.Properties.Single(p => p.PropertyName == nameof(SourceGeneratedMetadataDocument.Name));

        await Assert.That(metadata.CollectionName).IsEqualTo("sg_metadata_docs");
        await Assert.That(metadata.DisplayName).IsEqualTo("Generated Metadata");
        await Assert.That(metadata.Description).IsEqualTo("Generated description");
        await Assert.That(name.DisplayName).IsEqualTo("Display Name");
        await Assert.That(name.Description).IsEqualTo("Property description");
        await Assert.That(name.Order).IsEqualTo(7);
        await Assert.That(name.Required).IsTrue();
    }

    [Test]
    public async Task MetadataRegistry_Register_ShouldReplaceExistingMetadata()
    {
        var first = new EntityMetadata
        {
            TypeName = "First",
            CollectionName = "first",
            DisplayName = "First"
        };
        var second = new EntityMetadata
        {
            TypeName = "Second",
            CollectionName = "second",
            DisplayName = "Second"
        };

        MetadataRegistry.Register(typeof(MetadataRegistryOverrideProbe), first);
        MetadataRegistry.Register(typeof(MetadataRegistryOverrideProbe), second);

        await Assert.That(MetadataRegistry.TryGet(typeof(MetadataRegistryOverrideProbe), out var resolved)).IsTrue();
        await Assert.That(object.ReferenceEquals(resolved, second)).IsTrue();
    }

    [Test]
    public async Task TinyDbOptions_ShouldRejectPageSizeThatCannotFitUShortFreeBytes()
    {
        var options = new TinyDbOptions { PageSize = 131072 };

        await Assert.That(() => options.Validate()).Throws<ArgumentException>();
    }

    [Test]
    public async Task TransactionManager_DisposeAsync_ShouldDisposeWithoutBlockingCallers()
    {
        var path = Path.Combine(_directory, "transaction-manager-dispose-async.db");
        using var engine = new TinyDbEngine(path);
        var manager = new TransactionManager(engine, maxTransactions: 2, transactionTimeout: TimeSpan.FromSeconds(5));

        manager.BeginTransaction();

        await manager.DisposeAsync();
        await manager.DisposeAsync();

        await Assert.That(() => manager.BeginTransaction()).Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task DiskBTreeValidate_ShouldAllowDuplicateAdjacentKeys()
    {
        var path = Path.Combine(_directory, "btree-duplicate-keys.db");
        using var disk = new DiskStream(path);
        using var pageManager = new PageManager(disk, pageSize: 4096, maxCacheSize: 16);
        using var tree = DiskBTree.Create(pageManager, maxKeys: 3);
        var duplicateKey = new IndexKey(new BsonInt32(5));

        tree.Insert(duplicateKey, new BsonInt32(1));
        tree.Insert(duplicateKey, new BsonInt32(2));

        await Assert.That(tree.Validate()).IsTrue();
    }

    [Entity("guid_docs")]
    public partial class GuidIndexedDocument
    {
        public int Id { get; set; }
        public Guid ExternalId { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    [Entity("guid_generated_docs")]
    public partial class GuidGeneratedIdDocument
    {
        [IdGeneration(IdGenerationStrategy.GuidV7)]
        public Guid Id { get; set; }
    }

    [Entity("composite_docs")]
    public partial class CompositeIndexedDocument
    {
        public int Id { get; set; }
        public int A { get; set; }
        public int B { get; set; }
    }

    [Entity("sg_metadata_docs")]
    [EntityMetadata("Generated Metadata", Description = "Generated description")]
    public partial class SourceGeneratedMetadataDocument
    {
        public int Id { get; set; }

        [PropertyMetadata("Display Name", Description = "Property description", Order = 7, Required = true)]
        public string Name { get; set; } = string.Empty;
    }

    public sealed class CacheProbe
    {
        public string Existing { get; set; } = string.Empty;
    }

    public sealed class MetadataRegistryOverrideProbe
    {
    }

}
