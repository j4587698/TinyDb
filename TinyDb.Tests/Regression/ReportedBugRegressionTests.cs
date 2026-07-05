using System.Buffers.Binary;
using System.Linq.Expressions;
using System.Reflection;
using TinyDb.Bson;
using TinyDb.Query;
using TinyDb.Storage;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using QueryBinaryExpression = TinyDb.Query.BinaryExpression;
using QueryConstantExpression = TinyDb.Query.ConstantExpression;
using QueryMemberExpression = TinyDb.Query.MemberExpression;

namespace TinyDb.Tests.Regression;

public sealed class ReportedBugRegressionTests : IDisposable
{
    private readonly string _testDirectory;

    public ReportedBugRegressionTests()
    {
        _testDirectory = Path.Combine(Path.GetTempPath(), "TinyDbReportedBugTests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_testDirectory);
    }

    public void Dispose()
    {
        if (Directory.Exists(_testDirectory))
        {
            try
            {
                Directory.Delete(_testDirectory, recursive: true);
            }
            catch
            {
                // Best-effort cleanup.
            }
        }
    }

    [Test]
    public async Task ExpressionEvaluator_AndAlso_WithMissingMember_ShouldReturnFalse()
    {
        var expression = new QueryBinaryExpression(
            ExpressionType.AndAlso,
            new QueryMemberExpression("missing"),
            new QueryConstantExpression(true));

        var result = ExpressionEvaluator.Evaluate(expression, new BsonDocument());

        await Assert.That(result).IsFalse();
    }

    [Test]
    public async Task ExpressionEvaluator_Contains_ShouldUseQueryValueComparer()
    {
        var values = new BsonArray(new BsonValue[] { new BsonInt32(5) });
        var expression = new FunctionExpression(
            "Contains",
            new QueryConstantExpression(values),
            new QueryExpression[] { new QueryConstantExpression(5L) });

        var result = ExpressionEvaluator.EvaluateValue(expression, new BsonDocument());

        await Assert.That(result).IsEqualTo(true);
    }

    [Test]
    public async Task ExpressionParser_TryCompare_ShouldTreatShortAndIntAsComparableNumbers()
    {
        var method = typeof(ExpressionParser).GetMethod("TryCompare", BindingFlags.NonPublic | BindingFlags.Static)!;
        var args = new object?[] { (short)5, 5, 0 };

        var compared = (bool)method.Invoke(null, args)!;

        await Assert.That(compared).IsTrue();
        await Assert.That((int)args[2]!).IsEqualTo(0);
    }

    [Test]
    public async Task BsonValueComparer_ShouldCompareDocumentsAndArraysStructurally()
    {
        var left = new BsonDocument()
            .Set("a", new BsonInt32(5))
            .Set("b", new BsonArray(new BsonValue[] { new BsonInt32(1) }));

        var right = new BsonDocument()
            .Set("b", new BsonArray(new BsonValue[] { new BsonInt64(1) }))
            .Set("a", new BsonInt64(5));

        await Assert.That(BsonValueComparer.Compare(left, right)).IsEqualTo(0);
        await Assert.That(BsonValueComparer.GetHashCode(left)).IsEqualTo(BsonValueComparer.GetHashCode(right));
    }

    [Test]
    public async Task BsonArrayAndDocument_Equals_ShouldBeSymmetricWithWrappers()
    {
        var array = new BsonArray(new BsonValue[] { new BsonInt32(1) });
        var arrayValue = new BsonArrayValue(array);
        var document = new BsonDocument().Set("x", array);
        var documentValue = new BsonDocumentValue(document);

        await Assert.That(array.Equals(arrayValue)).IsTrue();
        await Assert.That(arrayValue.Equals(array)).IsTrue();
        await Assert.That(document.Equals(documentValue)).IsTrue();
        await Assert.That(documentValue.Equals(document)).IsTrue();
    }

    [Test]
    public async Task PageManager_ClearCache_ShouldNotDisposePinnedPages()
    {
        var dbPath = Path.Combine(_testDirectory, "pages.db");
        using var diskStream = new DiskStream(dbPath);
        using var pageManager = new PageManager(diskStream, pageSize: 4096, maxCacheSize: 16);

        var pinnedPage = pageManager.NewPage(PageType.Data);
        pageManager.SavePage(pinnedPage);
        pinnedPage.Pin();

        try
        {
            pageManager.ClearCache();
            await Assert.That(() => pinnedPage.WriteData(0, new byte[] { 1 })).ThrowsNothing();
        }
        finally
        {
            pinnedPage.Unpin();
        }

    }

    [Test]
    public async Task WriteAheadLog_ReplayAsync_ShouldTruncateIncompleteTailRecord()
    {
        var dbPath = Path.Combine(_testDirectory, "wal-tail.db");
        var walPath = GetWalPath(dbPath);

        using (var wal = new WriteAheadLog(dbPath, pageSize: 4096, enabled: true))
        {
            var page = new Page(1, 4096, PageType.Data);
            page.WriteData(0, new byte[] { 42 });
            await wal.AppendPageAsync(page);
        }

        var partialHeader = new byte[13];
        partialHeader[0] = 0x1;
        BinaryPrimitives.WriteUInt32LittleEndian(partialHeader.AsSpan(1, 4), 2);
        BinaryPrimitives.WriteInt32LittleEndian(partialHeader.AsSpan(5, 4), 4096);

        await using (var stream = new FileStream(walPath, FileMode.Append, FileAccess.Write, FileShare.Read))
        {
            await stream.WriteAsync(partialHeader);
        }

        var lengthWithPartialRecord = new FileInfo(walPath).Length;
        var replayedPages = new List<uint>();

        using (var wal = new WriteAheadLog(dbPath, pageSize: 4096, enabled: true))
        {
            await wal.ReplayAsync((pageId, _) =>
            {
                replayedPages.Add(pageId);
                return Task.CompletedTask;
            });
        }

        await Assert.That(replayedPages.SequenceEqual(new[] { 1u })).IsTrue();
        await Assert.That(new FileInfo(walPath).Length).IsLessThan(lengthWithPartialRecord);
    }

    private static string GetWalPath(string dbPath)
    {
        var directory = Path.GetDirectoryName(dbPath)!;
        var name = Path.GetFileNameWithoutExtension(dbPath);
        var extension = Path.GetExtension(dbPath).TrimStart('.');
        return Path.Combine(directory, $"{name}-wal.{extension}");
    }
}
