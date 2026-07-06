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

    [Entity("primary_records")]
    public sealed record PrimaryRecordEntity([property: Id] int Id, string Name, int Age)
    {
        public string? Note { get; set; }
    }
}
