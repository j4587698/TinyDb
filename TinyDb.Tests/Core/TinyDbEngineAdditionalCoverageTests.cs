using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using TinyDb.Bson;
using TinyDb.Core;
using TinyDb.Storage;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Core;

public class TinyDbEngineAdditionalCoverageTests
{
    [Test]
    public async Task FindAll_ShouldSkipNonDataPages_And_EmptyDataPages()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"eng_cov_{Guid.NewGuid():N}.db");
        using var engine = new TinyDbEngine(dbPath);

        var colName = "Docs";
        var col = engine.GetCollection<BsonDocument>(colName);
        _ = col.Insert(new BsonDocument().Set("name", "a"));

        var state = GetCollectionState(engine, colName);
        var ownedPages = state.OwnedPages;

        var pageManager = (PageManager)typeof(TinyDbEngine)
            .GetField("_pageManager", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(engine)!;

        var emptyDataPage = pageManager.NewPage(PageType.Data);
        pageManager.SavePage(emptyDataPage, forceFlush: true);
        ownedPages.TryAdd(emptyDataPage.PageID, 0);

        var nonDataPage = pageManager.NewPage(PageType.Index);
        pageManager.SavePage(nonDataPage, forceFlush: true);
        ownedPages.TryAdd(nonDataPage.PageID, 0);

        var docs = InvokeFindAll(engine, colName).ToList();
        await Assert.That(docs.Count).IsEqualTo(1);

        try { if (File.Exists(dbPath)) File.Delete(dbPath); } catch { }
    }

    [Test]
    public async Task Insert_WithNullCollectionField_ShouldNotTreatAsCorrectCollection()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"eng_cov_{Guid.NewGuid():N}.db");
        using var engine = new TinyDbEngine(dbPath);

        var colName = "Docs";
        var col = engine.GetCollection<BsonDocument>(colName);

        var id = ObjectId.NewObjectId();
        var doc = new BsonDocument(new Dictionary<string, BsonValue>
        {
            { "_id", id },
            { "_collection", null! },
            { "name", "a" }
        });

        var insertedId = col.Insert(doc);
        await Assert.That(insertedId.ToString()).IsEqualTo(id.ToString());

        try { if (File.Exists(dbPath)) File.Delete(dbPath); } catch { }
    }

    [Test]
    public async Task FindAll_WithTransactionOperationMissingDocumentId_ShouldMergeWithoutThrow()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"eng_cov_{Guid.NewGuid():N}.db");
        using var engine = new TinyDbEngine(dbPath);

        var colName = "Docs";
        var col = engine.GetCollection<BsonDocument>(colName);
        var id = col.Insert(new BsonDocument().Set("name", "a"));

        var tx = engine.BeginTransaction();
        try
        {
            var operations = GetTransactionOperations(tx);
            operations.Add(new TransactionOperation(
                TransactionOperationType.CreateIndex,
                colName,
                documentId: null,
                indexName: "idx_name",
                indexFields: new[] { "name" },
                indexUnique: false));

            operations.Add(new TransactionOperation(
                TransactionOperationType.Delete,
                colName,
                documentId: id));

            var docs = InvokeFindAll(engine, colName).ToList();
            await Assert.That(docs.Count).IsEqualTo(0);
        }
        finally
        {
            tx.Dispose();
            try { if (File.Exists(dbPath)) File.Delete(dbPath); } catch { }
        }
    }

    [Test]
    public async Task NormalizeInterval_WhenInfinite_ShouldReturnZero()
    {
        var normalize = typeof(TinyDbEngine).GetMethod("NormalizeInterval", BindingFlags.NonPublic | BindingFlags.Static);
        await Assert.That(normalize).IsNotNull();

        var zero = (TimeSpan)normalize!.Invoke(null, new object[] { Timeout.InfiniteTimeSpan })!;
        await Assert.That(zero).IsEqualTo(TimeSpan.Zero);

        var value = (TimeSpan)normalize.Invoke(null, new object[] { TimeSpan.FromMilliseconds(1) })!;
        await Assert.That(value).IsEqualTo(TimeSpan.FromMilliseconds(1));
    }

    [Test]
    public async Task ThrowIfDisposed_AfterDispose_ShouldThrowObjectDisposedException()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"eng_cov_{Guid.NewGuid():N}.db");
        var engine = new TinyDbEngine(dbPath);
        engine.Dispose();

        await Assert.That(() => engine.BeginTransaction()).ThrowsExactly<ObjectDisposedException>();

        try { if (File.Exists(dbPath)) File.Delete(dbPath); } catch { }
    }

    [Test]
    public async Task EnsureInitialized_WhenNotInitialized_ShouldThrowInvalidOperationException()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"eng_cov_{Guid.NewGuid():N}.db");
        using var engine = new TinyDbEngine(dbPath);

        typeof(TinyDbEngine)
            .GetField("_isInitialized", BindingFlags.Instance | BindingFlags.NonPublic)!
            .SetValue(engine, false);

        await Assert.That(() => engine.GetCollectionNames().ToList()).ThrowsExactly<InvalidOperationException>();

        try { if (File.Exists(dbPath)) File.Delete(dbPath); } catch { }
    }

    private static CollectionState GetCollectionState(TinyDbEngine engine, string collectionName)
    {
        var method = typeof(TinyDbEngine).GetMethod("GetCollectionState", BindingFlags.Instance | BindingFlags.NonPublic);
        return (CollectionState)method!.Invoke(engine, new object[] { collectionName })!;
    }

    private static IEnumerable<BsonDocument> InvokeFindAll(TinyDbEngine engine, string collectionName)
    {
        var method = typeof(TinyDbEngine).GetMethod("FindAll", BindingFlags.Instance | BindingFlags.NonPublic);
        return (IEnumerable<BsonDocument>)method!.Invoke(engine, new object[] { collectionName })!;
    }

    private static List<TransactionOperation> GetTransactionOperations(ITransaction tx)
    {
        return ((Transaction)tx).Operations;
    }
}
