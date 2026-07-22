using TinyDb.Bson;
using TinyDb.Core;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Core;

public sealed class TinyDbEngineDirectConnectionTests : IDisposable
{
    private readonly string _directory = Path.Combine(
        Path.GetTempPath(),
        "TinyDbTests",
        Guid.NewGuid().ToString("N"));

    private string DatabasePath => Path.Combine(_directory, "direct-connection.db");

    public TinyDbEngineDirectConnectionTests()
    {
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
    public async Task DirectConnection_ShouldRejectSecondWriterAndAllowReopenAfterDispose()
    {
        var options = new TinyDbOptions { EnableJournaling = false };
        using (var first = new TinyDbEngine(DatabasePath, options))
        {
            first.GetBsonCollection("items").Insert(
                new BsonDocument().Set("_id", ObjectId.NewObjectId()));

            await Assert.That(() => new TinyDbEngine(DatabasePath, options)).Throws<IOException>();
        }

        using var reopened = new TinyDbEngine(DatabasePath, options);
        var documents = reopened.GetBsonCollection("items").FindAll().ToList();
        await Assert.That(documents.Count).IsEqualTo(1);
    }

    [Test]
    public async Task DirectConnection_ShouldAllowConcurrentReadHandle()
    {
        var options = new TinyDbOptions { EnableJournaling = false };
        using var engine = new TinyDbEngine(DatabasePath, options);

        using var reader = new FileStream(
            DatabasePath,
            FileMode.Open,
            FileAccess.Read,
            FileShare.ReadWrite);

        await Assert.That(reader.Length).IsGreaterThan(0L);
    }
}
