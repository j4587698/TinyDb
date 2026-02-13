using System;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using TinyDb.Bson;
using TinyDb.Core;
using TinyDb.Serialization;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Core;

public sealed class PredicatePushdownLargeDocumentTests
{
    [Test]
    public async Task FindAllRawWithPredicateInfo_ShouldNotDropLargeDocumentsOnMissingFields()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"pushdown_large_{Guid.NewGuid():N}.db");

        try
        {
            using var engine = new TinyDbEngine(dbPath, new TinyDbOptions { EnableJournaling = false });
            const string collection = "col";

            engine.InsertDocumentInternal(collection, new BsonDocument()
                .Set("age", 20)
                .Set("payload", "small"));

            engine.InsertDocumentInternal(collection, new BsonDocument()
                .Set("age", 20)
                .Set("payload", new string('x', 10000)));

            var predicate = new ScanPredicate(
                fieldNameBytes: System.Text.Encoding.UTF8.GetBytes("age"),
                alternateFieldNameBytes: null,
                secondAlternateFieldNameBytes: null,
                targetValue: 10,
                op: ExpressionType.GreaterThan);

            var results = engine.FindAllRawWithPredicateInfo(collection, new[] { predicate }).ToList();

            await Assert.That(results.Count).IsEqualTo(2);

            int largeCount = 0;
            int smallCount = 0;
            bool sawLargeWithPostFilter = false;
            bool sawSmallWithoutPostFilter = false;

            foreach (var r in results)
            {
                var doc = BsonSerializer.DeserializeDocument(r.Slice);
                bool isLarge = doc.TryGetValue("_isLargeDocument", out var v) && v.ToBoolean(null);
                if (isLarge)
                {
                    largeCount++;
                    if (r.RequiresPostFilter) sawLargeWithPostFilter = true;
                }
                else
                {
                    smallCount++;
                    if (!r.RequiresPostFilter) sawSmallWithoutPostFilter = true;
                }
            }

            await Assert.That(largeCount).IsEqualTo(1);
            await Assert.That(smallCount).IsEqualTo(1);
            await Assert.That(sawLargeWithPostFilter).IsTrue();
            await Assert.That(sawSmallWithoutPostFilter).IsTrue();
        }
        finally
        {
            try { File.Delete(dbPath); } catch { }
        }
    }
}
