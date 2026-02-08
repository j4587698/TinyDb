using System;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using System.Reflection;
using TinyDb.Bson;
using TinyDb.Core;
using TinyDb.Query;
using TinyDb.Serialization;
using TinyDb.Storage;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Query;

public class QueryExecutorCollectionTagBranchCoverageTests
{
    [Test]
    public async Task Execute_FullTableScan_ShouldAllowDocumentsWithoutCollectionTag()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"tinydb_qexec_tag_{Guid.NewGuid():N}.db");
        const string collectionName = "tag_cov";

        try
        {
            using var engine = new TinyDbEngine(dbPath, new TinyDbOptions { EnableJournaling = false });
            var col = engine.GetCollection<BsonDocument>(collectionName);

            col.Insert(new BsonDocument().Set("x", 1));

            var getCollectionState = typeof(TinyDbEngine).GetMethod("GetCollectionState", BindingFlags.NonPublic | BindingFlags.Instance);
            await Assert.That(getCollectionState).IsNotNull();

            var state = (CollectionState)getCollectionState!.Invoke(engine, new object[] { collectionName })!;
            var ownedPages = state.OwnedPages;

            var pmField = typeof(TinyDbEngine).GetField("_pageManager", BindingFlags.NonPublic | BindingFlags.Instance);
            await Assert.That(pmField).IsNotNull();
            var pm = (PageManager)pmField!.GetValue(engine)!;

            uint dataPageId = 0;
            foreach (var pageId in ownedPages.Keys)
            {
                var page = pm.GetPage(pageId);
                if (page.PageType == PageType.Data)
                {
                    dataPageId = pageId;
                    break;
                }
            }

            await Assert.That(dataPageId).IsNotEqualTo(0u);

            var rawDoc = new BsonDocument()
                .Set("_id", 999)
                .Set("x", 2);

            var bytes = BsonSerializer.SerializeDocument(rawDoc);
            var targetPage = pm.GetPage(dataPageId);
            targetPage.Append(bytes);
            pm.SavePage(targetPage, forceFlush: false);

            var executor = new QueryExecutor(engine);
            var docs = executor.Execute<BsonDocument>(collectionName).ToList();

            await Assert.That(docs.Any(d => !d.ContainsKey("_collection"))).IsTrue();
        }
        finally
        {
            try { if (File.Exists(dbPath)) File.Delete(dbPath); } catch { }
        }
    }
}
