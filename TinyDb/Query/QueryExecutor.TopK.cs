using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using TinyDb.Bson;
using TinyDb.Core;
using TinyDb.Index;
using TinyDb.Serialization;

namespace TinyDb.Query;

public sealed partial class QueryExecutor
{
    internal IEnumerable<T> ExecuteTopKScan<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(
        string collectionName,
        QueryShape<T> shape,
        out QueryPushdownInfo pushdown)
        where T : class
    {
        var sort = shape.Sort;
        var skip = Math.Max(shape.Skip.GetValueOrDefault(), 0);
        var take = shape.Take ?? 0;

        var kLong = (long)skip + take;
        if (kLong <= 0)
        {
            pushdown = CreatePushdownInfo(
                shape,
                orderPushedCount: shape.Sort.Count,
                skipPushedCount: skip > 0 ? 1 : 0,
                takePushedCount: 1);
            return Enumerable.Empty<T>();
        }

        if (kLong > int.MaxValue)
        {
            throw new NotSupportedException("Skip + Take is too large.");
        }

        var k = (int)kLong;

        var queryExpression = ParsePredicate(shape.Predicate);

        var predicates = new List<ScanPredicate>();
        bool fullyPushed = CollectPredicates(queryExpression, predicates);
        var pushDownPredicates = predicates.Count > 0 ? predicates.ToArray() : null;

        pushdown = CreatePushdownInfo(
            shape,
            orderPushedCount: shape.Sort.Count,
            skipPushedCount: skip > 0 ? 1 : 0,
            takePushedCount: 1);

        var sortFields = QuerySortKeyReader.CreateSortFields(sort);

        var collectionFieldNameBytes = Encoding.UTF8.GetBytes("_collection");
        var collectionNameBytes = Encoding.UTF8.GetBytes(collectionName);
        var isLargeDocumentFieldNameBytes = Encoding.UTF8.GetBytes("_isLargeDocument");

        return Iterator();

        IEnumerable<T> Iterator()
        {
            var heap = new List<TopKRow>(Math.Min(k, 256));
            long sequence = 0;

            var tx = _engine.GetCurrentTransaction();
            var txOverlay = tx != null ? QueryTransactionOverlay.Build(tx, collectionName) : null;

            foreach (var result in _engine.FindAllRawWithPredicateInfo(collectionName, pushDownPredicates))
            {
                var slice = result.Slice;
                var span = slice.Span;

                if (!MatchesCollection(span, collectionFieldNameBytes, collectionNameBytes))
                {
                    continue;
                }

                if (txOverlay != null)
                {
                    if (QuerySortKeyReader.TryReadBsonValue(span, SortFieldBytes.Id, out var idValue))
                    {
                        if (txOverlay.TryGetValue(idValue, out var txDoc))
                        {
                            txOverlay.Remove(idValue);
                            if (txDoc == null) continue;
                            ConsiderDocument(txDoc, requiresPostFilter: true);
                            continue;
                        }
                    }
                }

                ConsiderSlice(slice, result.RequiresPostFilter);
            }

            if (txOverlay != null)
            {
                foreach (var doc in txOverlay.Values)
                {
                    if (doc == null) continue;
                    ConsiderDocument(doc, requiresPostFilter: true);
                }
            }

            TopKHeap.SortBestFirst(heap, sort);

            var start = Math.Min(skip, heap.Count);
            var end = Math.Min(start + take, heap.Count);

            for (int i = start; i < end; i++)
            {
                var row = heap[i];
                var doc = row.TransactionDocument ?? _engine.FindById(collectionName, row.Id);
                if (doc == null) continue;

                var entity = AotBsonMapper.FromDocument<T>(doc);
                if (entity != null) yield return entity;
            }

            void ConsiderDocument(BsonDocument doc, bool requiresPostFilter)
            {
                bool match;
                if (queryExpression == null)
                {
                    match = true;
                }
                else
                {
                    match = fullyPushed
                        ? (!requiresPostFilter || ExpressionEvaluator.Evaluate(queryExpression, doc))
                        : ExpressionEvaluator.Evaluate(queryExpression, doc);
                }

                if (!match) return;

                var seq = sequence++;

                var keys = new SortKey[sort.Count];
                for (int i = 0; i < sort.Count; i++)
                {
                    var bsonValue = QuerySortKeyReader.TryGetSortValue(doc, sort[i].FieldName);
                    keys[i] = SortKey.FromBsonValue(bsonValue);
                }

                if (!doc.TryGetValue("_id", out var id) || id == null) return;

                var row = new TopKRow(id, keys, seq, doc);
                ConsiderRow(row);
            }

            void ConsiderSlice(ReadOnlyMemory<byte> slice, bool requiresPostFilter)
            {
                var span = slice.Span;

                BsonDocument? doc = null;
                bool match;

                if (queryExpression == null)
                {
                    if (IsLargeDocumentStub(span))
                    {
                        doc = DeserializeDocumentOrThrow(slice);
                        doc = _engine.ResolveLargeDocument(doc);
                    }

                    match = true;
                }
                else if (fullyPushed && !requiresPostFilter && !IsLargeDocumentStub(span))
                {
                    match = true;
                }
                else
                {
                    doc = DeserializeDocumentOrThrow(slice);
                    doc = _engine.ResolveLargeDocument(doc);

                    match = fullyPushed
                        ? (!requiresPostFilter || ExpressionEvaluator.Evaluate(queryExpression, doc))
                        : ExpressionEvaluator.Evaluate(queryExpression, doc);
                }

                if (!match) return;

                var seq = sequence++;

                if (heap.Count == k)
                {
                    var worst = heap[0];
                    var cmp = doc != null
                        ? QuerySortKeyReader.CompareDocumentToRow(doc, worst, seq, sort)
                        : QuerySortKeyReader.CompareSliceToRow(span, sortFields, worst, seq, sort);

                    if (cmp >= 0) return;
                }

                var keys = doc != null
                    ? QuerySortKeyReader.MaterializeKeysFromDocument(doc, sort)
                    : QuerySortKeyReader.MaterializeKeysFromSlice(span, sortFields, sort);

                if (!QuerySortKeyReader.TryReadBsonValue(span, SortFieldBytes.Id, out var idValue))
                {
                    return;
                }

                var rowDocument = doc;
                if (rowDocument == null)
                {
                    rowDocument = DeserializeDocumentOrThrow(slice);
                    rowDocument = _engine.ResolveLargeDocument(rowDocument);
                }

                var row = new TopKRow(idValue, keys, seq, rowDocument);
                ConsiderRow(row);
            }

            void ConsiderRow(TopKRow row)
            {
                TopKHeap.AddOrReplaceWorst(heap, row, k, sort);
            }

            bool IsLargeDocumentStub(ReadOnlySpan<byte> span)
            {
                if (!BsonScanner.TryLocateField(span, isLargeDocumentFieldNameBytes, out var offset, out var type)) return false;
                return type == BsonType.Boolean && offset >= 0 && offset < span.Length && span[offset] != 0;
            }
        }
    }

    private IAsyncEnumerable<T> ExecuteTopKScanAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(
        string collectionName,
        QueryShape<T> shape,
        out QueryPushdownInfo pushdown,
        CancellationToken cancellationToken)
        where T : class
    {
        var sort = shape.Sort;
        var skip = Math.Max(shape.Skip.GetValueOrDefault(), 0);
        var take = shape.Take ?? 0;

        var kLong = (long)skip + take;
        if (kLong <= 0)
        {
            pushdown = CreatePushdownInfo(
                shape,
                orderPushedCount: shape.Sort.Count,
                skipPushedCount: skip > 0 ? 1 : 0,
                takePushedCount: 1);
            return AsyncEmpty<T>();
        }

        if (kLong > int.MaxValue)
        {
            throw new NotSupportedException("Skip + Take is too large.");
        }

        var k = (int)kLong;

        var queryExpression = ParsePredicate(shape.Predicate);

        var predicates = new List<ScanPredicate>();
        bool fullyPushed = CollectPredicates(queryExpression, predicates);
        var pushDownPredicates = predicates.Count > 0 ? predicates.ToArray() : null;

        pushdown = CreatePushdownInfo(
            shape,
            orderPushedCount: shape.Sort.Count,
            skipPushedCount: skip > 0 ? 1 : 0,
            takePushedCount: 1);

        return Iterator(queryExpression, fullyPushed, pushDownPredicates, cancellationToken);

        async IAsyncEnumerable<T> Iterator(
            QueryExpression? queryExpr,
            bool allPredicatesPushed,
            ScanPredicate[]? scanPredicates,
            [EnumeratorCancellation] CancellationToken ct)
        {
            var heap = new List<TopKRow>(Math.Min(k, 256));
            long sequence = 0;

            var tx = _engine.GetCurrentTransaction();
            var txOverlay = tx != null ? QueryTransactionOverlay.Build(tx, collectionName) : null;

            await foreach (var result in _engine.FindAllRawWithPredicateInfoAsync(collectionName, scanPredicates, ct).ConfigureAwait(false))
            {
                ct.ThrowIfCancellationRequested();

                var doc = DeserializeDocumentOrThrow(result.Slice);
                if (doc.TryGetValue("_collection", out var c) && c.ToString() != collectionName)
                {
                    continue;
                }

                doc = await _engine.ResolveLargeDocumentAsync(doc, ct).ConfigureAwait(false);
                var requiresPostFilter = result.RequiresPostFilter;

                if (txOverlay != null && doc.TryGetValue("_id", out var idValue))
                {
                    if (txOverlay.TryGetValue(idValue, out var txDoc))
                    {
                        txOverlay.Remove(idValue);
                        if (txDoc == null) continue;
                        ConsiderDocument(txDoc, requiresPostFilter: true);
                        continue;
                    }
                }

                ConsiderDocument(doc, requiresPostFilter);
            }

            if (txOverlay != null)
            {
                foreach (var doc in txOverlay.Values)
                {
                    ct.ThrowIfCancellationRequested();
                    if (doc == null) continue;
                    ConsiderDocument(doc, requiresPostFilter: true);
                }
            }

            TopKHeap.SortBestFirst(heap, sort);

            var start = Math.Min(skip, heap.Count);
            var end = Math.Min(start + take, heap.Count);

            for (int i = start; i < end; i++)
            {
                ct.ThrowIfCancellationRequested();

                var row = heap[i];
                if (row.TransactionDocument == null) continue;

                var entity = AotBsonMapper.FromDocument<T>(row.TransactionDocument);
                if (entity != null) yield return entity;
            }

            void ConsiderDocument(BsonDocument doc, bool requiresPostFilter)
            {
                bool match;
                if (queryExpr == null)
                {
                    match = true;
                }
                else
                {
                    match = allPredicatesPushed
                        ? (!requiresPostFilter || ExpressionEvaluator.Evaluate(queryExpr, doc))
                        : ExpressionEvaluator.Evaluate(queryExpr, doc);
                }

                if (!match) return;
                if (!doc.TryGetValue("_id", out var id) || id == null) return;

                var seq = sequence++;
                var row = new TopKRow(id, QuerySortKeyReader.MaterializeKeysFromDocument(doc, sort), seq, doc);

                TopKHeap.AddOrReplaceWorst(heap, row, k, sort);
            }
        }
    }

    internal static bool MatchesCollection(ReadOnlySpan<byte> document, byte[] collectionFieldNameBytes, byte[] collectionNameBytes)
    {
        if (!BsonScanner.TryLocateField(document, collectionFieldNameBytes, out int valueOffset, out var type))
        {
            return true;
        }

        if (type != BsonType.String) return false;
        if (valueOffset < 0 || valueOffset + 4 > document.Length) return false;

        var len = BinaryPrimitives.ReadInt32LittleEndian(document.Slice(valueOffset, 4));
        if (len <= 0) return false;

        var bytesLen = len - 1;
        var start = valueOffset + 4;
        if (start < 0 || start + bytesLen > document.Length) return false;

        return document.Slice(start, bytesLen).SequenceEqual(collectionNameBytes);
    }

    private static bool IsLargeDocumentStub(ReadOnlySpan<byte> document, byte[] isLargeDocumentFieldNameBytes)
    {
        if (!BsonScanner.TryLocateField(document, isLargeDocumentFieldNameBytes, out var offset, out var type)) return false;
        return type == BsonType.Boolean && offset >= 0 && offset < document.Length && document[offset] != 0;
    }

}
