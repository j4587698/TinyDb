using TinyDb.Attributes;
using TinyDb.Core;
using TinyDb.Query;
using TinyDb.Serialization;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Serialization;

public sealed class PrimaryConstructorRecordTests : IDisposable
{
    private readonly string _dbPath;
    private readonly TinyDbEngine _engine;

    public PrimaryConstructorRecordTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"primary_record_{Guid.NewGuid():N}.db");
        _engine = new TinyDbEngine(_dbPath);
    }

    public void Dispose()
    {
        _engine.Dispose();
        try { if (File.Exists(_dbPath)) File.Delete(_dbPath); } catch { }
    }

    [Test]
    public async Task AotMapper_ShouldRoundTrip_PrimaryConstructorRecord()
    {
        var source = new PrimaryRecordEntity(1, "Alice", 30) { Note = "first" };

        var document = AotBsonMapper.ToDocument(source);
        var roundtrip = AotBsonMapper.FromDocument<PrimaryRecordEntity>(document);

        await Assert.That(roundtrip).IsEqualTo(source);
        await Assert.That(document["_id"].ToInt32(null)).IsEqualTo(1);
    }

    [Test]
    public async Task Collection_ShouldSupport_PrimaryConstructorRecord()
    {
        var collection = _engine.GetCollection<PrimaryRecordEntity>("primary_records");

        collection.Insert(new PrimaryRecordEntity(1, "Alice", 30) { Note = "first" });

        var found = collection.FindById(1);

        await Assert.That(found).IsNotNull();
        await Assert.That(found!.Name).IsEqualTo("Alice");
        await Assert.That(found.Note).IsEqualTo("first");
    }

    [Test]
    public async Task Collection_ShouldReject_PrimaryConstructorRecord_WithDefaultImmutableId()
    {
        var collection = _engine.GetCollection<PrimaryRecordEntity>("primary_records_invalid_id");

        await Assert.That(() => collection.Insert(new PrimaryRecordEntity(0, "Alice", 30)))
            .Throws<InvalidOperationException>();
    }

    [Test]
    public async Task SqlUpdate_ShouldDeserialize_PrimaryConstructorRecord_WithMutableRemainder()
    {
        var collection = _engine.GetCollection<PrimaryRecordEntity>("primary_records_sql");
        collection.Insert(new PrimaryRecordEntity(1, "Alice", 30) { Note = "first" });

        var result = collection.Execute(
            "update primary_records_sql set Note = @note where Id = @id",
            QueryParams.Create(("id", 1), ("note", "updated")));

        var updated = collection.FindById(1);

        await Assert.That(result.AffectedRows).IsEqualTo(1);
        await Assert.That(updated).IsNotNull();
        await Assert.That(updated!.Name).IsEqualTo("Alice");
        await Assert.That(updated.Note).IsEqualTo("updated");
    }

    [Test]
    public async Task AotMapper_ShouldRoundTrip_PrimaryConstructorRecord_WithComplexConstructorParameters()
    {
        var source = new ComplexPrimaryRecordEntity(
            7,
            new ComplexRecordChild("main", new ComplexRecordDetail("M1")),
            new List<ComplexRecordChild>
            {
                new("child", new ComplexRecordDetail("C1"))
            },
            new Dictionary<string, ComplexRecordChild>
            {
                ["main"] = new("dict", new ComplexRecordDetail("D1"))
            });

        var document = AotBsonMapper.ToDocument(source);
        var roundtrip = AotBsonMapper.FromDocument<ComplexPrimaryRecordEntity>(document);

        await Assert.That(roundtrip.Id).IsEqualTo(7);
        await Assert.That(roundtrip.Child.Code).IsEqualTo("main");
        await Assert.That(roundtrip.Child.Detail.Lot).IsEqualTo("M1");
        await Assert.That(roundtrip.Children[0].Detail.Lot).IsEqualTo("C1");
        await Assert.That(roundtrip.ChildByCode["main"].Detail.Lot).IsEqualTo("D1");
    }

    [Entity("primary_records")]
    public sealed record PrimaryRecordEntity([property: Id] int Id, string Name, int Age)
    {
        public string? Note { get; set; }
    }

    [Entity("complex_primary_records")]
    public sealed record ComplexPrimaryRecordEntity(
        [property: Id] int Id,
        ComplexRecordChild Child,
        List<ComplexRecordChild> Children,
        Dictionary<string, ComplexRecordChild> ChildByCode);

    public sealed record ComplexRecordChild(string Code, ComplexRecordDetail Detail);

    public sealed record ComplexRecordDetail(string Lot);
}
