using System.Reflection;
using TinyDb.Attributes;
using TinyDb.Bson;
using TinyDb.Core;
using TinyDb.Serialization;
using TinyDb.Storage;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Regression;

[NotInParallel]
public sealed class DeepReviewReportRegressionTests : IDisposable
{
    private readonly string _directory;

    public DeepReviewReportRegressionTests()
    {
        _directory = Path.Combine(Path.GetTempPath(), "TinyDbDeepReviewReport", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_directory);
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_directory))
            {
                Directory.Delete(_directory, recursive: true);
            }
        }
        catch
        {
        }
    }

    [Test]
    public async Task IndexScan_WithCoveredPredicate_ShouldFilterPendingTransactionOverlay()
    {
        const string collectionName = "deep_review_overlay_items";
        using var engine = new TinyDbEngine(Path.Combine(_directory, "overlay.db"));
        var collection = engine.GetCollection<IndexOverlayRegressionItem>(collectionName);

        collection.Insert(new IndexOverlayRegressionItem { Id = 1, Score = 10, Name = "match" });
        collection.Insert(new IndexOverlayRegressionItem { Id = 2, Score = 20, Name = "other" });
        engine.EnsureIndex(collectionName, nameof(IndexOverlayRegressionItem.Score), "score_idx");

        using var transaction = engine.BeginTransaction();
        collection.Insert(new IndexOverlayRegressionItem { Id = 3, Score = 30, Name = "pending-other" });

        var results = collection.Find(static item => item.Score == 10).ToList();

        await Assert.That(results.Select(static item => item.Id).SequenceEqual(new[] { 1 })).IsTrue();
    }

    [Test]
    public async Task SourceGenerator_ShouldPreserveEmptyStringAsBsonString()
    {
        var document = AotBsonMapper.ToDocument(new EmptyStringRegressionEntity
        {
            Id = 1,
            Name = string.Empty
        });

        await Assert.That(document["name"]).IsTypeOf<BsonString>();
        await Assert.That(((BsonString)document["name"]).Value).IsEqualTo(string.Empty);
    }

    [Test]
    public async Task SourceGenerator_ShouldSerializeInitOnlyProperties()
    {
        var document = AotBsonMapper.ToDocument(new InitOnlyRegressionEntity
        {
            Id = 1,
            Code = "init-value"
        });

        await Assert.That(document["code"].ToString()).IsEqualTo("init-value");
    }

    [Test]
    public async Task SourceGenerator_ShouldDeserializeInitOnlyPropertiesOnDependentTypes()
    {
        var document = AotBsonMapper.ToDocument(new NestedInitOnlyRegressionEntity
        {
            Id = 1,
            Details = new NestedInitOnlyRegressionValue { Code = "nested-init" }
        });

        var restored = AotBsonMapper.FromDocument<NestedInitOnlyRegressionEntity>(document);

        await Assert.That(restored).IsNotNull();
        await Assert.That(restored!.Details.Code).IsEqualTo("nested-init");
    }

    [Test]
    public async Task SourceGenerator_ShouldCompileNestedEntityInsideGenericType()
    {
        await Assert.That(typeof(GenericOuterRegression<int>.InnerEntity).Name).IsEqualTo(nameof(GenericOuterRegression<int>.InnerEntity));
    }

    [Test]
    public async Task AotPrimitiveConversion_ShouldSupportCharTimeSpanAndDateTimeOffset()
    {
        var timeSpan = TimeSpan.FromMinutes(12) + TimeSpan.FromTicks(34);
        var dateTimeOffset = new DateTimeOffset(2026, 7, 8, 9, 10, 11, TimeSpan.FromHours(8));
        var dateTimeOffsetText = dateTimeOffset.ToString("O", System.Globalization.CultureInfo.InvariantCulture);

        await Assert.That(AotBsonMapper.ConvertValue(new BsonString("Z"), typeof(char))).IsEqualTo('Z');
        await Assert.That(AotBsonMapper.ConvertValue(new BsonInt64(timeSpan.Ticks), typeof(TimeSpan))).IsEqualTo(timeSpan);
        await Assert.That(AotBsonMapper.ConvertValue(new BsonString(dateTimeOffsetText), typeof(DateTimeOffset))).IsEqualTo(dateTimeOffset);

        await Assert.That(BsonConversion.ToBsonValue(timeSpan)).IsTypeOf<BsonInt64>();
        await Assert.That(BsonConversion.FromBsonValue<TimeSpan>(new BsonInt64(timeSpan.Ticks))).IsEqualTo(timeSpan);
        await Assert.That(BsonConversion.FromBsonValue<DateTimeOffset>(new BsonString(dateTimeOffsetText))).IsEqualTo(dateTimeOffset);
    }

    [Test]
    public async Task PageManager_Eviction_ShouldNotDisposeExistingUnpinnedReference()
    {
        using var disk = new DiskStream(Path.Combine(_directory, "page-eviction.db"));
        using var pageManager = new PageManager(disk, pageSize: 4096, maxCacheSize: 2);

        var first = pageManager.NewPage(PageType.Data);
        first.WriteData(0, new byte[] { 42 });
        pageManager.SavePage(first);

        var second = pageManager.NewPage(PageType.Data);
        pageManager.SavePage(second);

        var third = pageManager.NewPage(PageType.Data);
        pageManager.SavePage(third);

        await Assert.That(() => first.ReadData(0, 1)).ThrowsNothing();
        await Assert.That(first.ReadData(0, 1).SequenceEqual(new byte[] { 42 })).IsTrue();
    }

    [Test]
    public async Task SingleDocumentInsert_ShouldWaitForCollectionCommitGate()
    {
        const string collectionName = "deep_review_gate_items";
        using var engine = new TinyDbEngine(Path.Combine(_directory, "commit-gate.db"));
        var collection = engine.GetCollection<CommitGateRegressionItem>(collectionName);
        using var gate = EnterCollectionCommitGate(engine, collectionName);

        using var started = new ManualResetEventSlim();
        var insertTask = Task.Run(() =>
        {
            started.Set();
            collection.Insert(new CommitGateRegressionItem { Id = 1, Name = "blocked" });
        });

        await Task.Run(() => started.Wait()).WaitAsync(TimeSpan.FromSeconds(2));
        var completedWhileGateHeld = await Task.WhenAny(insertTask, Task.Delay(TimeSpan.FromMilliseconds(200)));
        await Assert.That(completedWhileGateHeld == insertTask).IsFalse();

        gate.Dispose();
        await insertTask.WaitAsync(TimeSpan.FromSeconds(2));
        await Assert.That(collection.FindById(1)).IsNotNull();
    }

    [Test]
    public async Task FindAll_ShouldWaitForCollectionCommitGate()
    {
        const string collectionName = "deep_review_read_gate_items";
        using var engine = new TinyDbEngine(Path.Combine(_directory, "read-commit-gate.db"));
        var collection = engine.GetCollection<CommitGateRegressionItem>(collectionName);
        collection.Insert(new CommitGateRegressionItem { Id = 1, Name = "existing" });

        using var gate = EnterCollectionCommitGate(engine, collectionName);
        using var started = new ManualResetEventSlim();
        var readTask = Task.Run(() =>
        {
            started.Set();
            return collection.FindAll().ToList();
        });

        await Task.Run(() => started.Wait()).WaitAsync(TimeSpan.FromSeconds(2));
        var completedWhileGateHeld = await Task.WhenAny(readTask, Task.Delay(TimeSpan.FromMilliseconds(200)));
        await Assert.That(completedWhileGateHeld == readTask).IsFalse();

        gate.Dispose();
        var results = await readTask.WaitAsync(TimeSpan.FromSeconds(2));
        await Assert.That(results.Count).IsEqualTo(1);
        await Assert.That(results[0].Name).IsEqualTo("existing");
    }

    [Test]
    public async Task SingleDocumentInsert_ShouldNotWaitForSharedCollectionWriteGate()
    {
        const string collectionName = "deep_review_shared_gate_items";
        using var engine = new TinyDbEngine(Path.Combine(_directory, "shared-gate.db"));
        var collection = engine.GetCollection<CommitGateRegressionItem>(collectionName);
        using var gate = EnterCollectionWriteGate(engine, collectionName);

        var insertTask = Task.Run(() => collection.Insert(new CommitGateRegressionItem { Id = 1, Name = "shared" }));

        await insertTask.WaitAsync(TimeSpan.FromSeconds(2));
        await Assert.That(collection.FindById(1)).IsNotNull();
    }

    private static IDisposable EnterCollectionCommitGate(TinyDbEngine engine, string collectionName)
    {
        return EnterCollectionGate(engine, "EnterCollectionCommitGates", collectionName);
    }

    private static IDisposable EnterCollectionWriteGate(TinyDbEngine engine, string collectionName)
    {
        return EnterCollectionGate(engine, "EnterCollectionWriteGates", collectionName);
    }

    private static IDisposable EnterCollectionGate(TinyDbEngine engine, string methodName, string collectionName)
    {
        var method = typeof(TinyDbEngine).GetMethod(
            methodName,
            BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new MissingMethodException(typeof(TinyDbEngine).FullName, methodName);

        return (IDisposable)method.Invoke(engine, new object[] { new[] { collectionName } })!;
    }

    [Entity("deep_review_overlay_items")]
    public sealed class IndexOverlayRegressionItem
    {
        public int Id { get; set; }
        public int Score { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    [Entity("deep_review_empty_string_items")]
    public sealed class EmptyStringRegressionEntity
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    [Entity("deep_review_init_only_items")]
    public sealed class InitOnlyRegressionEntity
    {
        public int Id { get; set; }
        public string Code { get; init; } = string.Empty;
    }

    [Entity("deep_review_nested_init_only_items")]
    public sealed class NestedInitOnlyRegressionEntity
    {
        public int Id { get; set; }
        public NestedInitOnlyRegressionValue Details { get; set; } = new();
    }

    public sealed class NestedInitOnlyRegressionValue
    {
        public string Code { get; init; } = string.Empty;
    }

    public sealed class GenericOuterRegression<T>
    {
        [Entity("deep_review_generic_outer_inner_items")]
        public sealed class InnerEntity
        {
            public int Id { get; set; }
            public string Name { get; set; } = string.Empty;
        }
    }

    [Entity("deep_review_gate_items")]
    public sealed class CommitGateRegressionItem
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}
