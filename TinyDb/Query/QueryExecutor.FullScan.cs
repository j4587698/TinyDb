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
    internal IEnumerable<T> ExecuteFullTableScan<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(string collectionName, Expression<Func<T, bool>>? expression = null)
        where T : class
    {
        var queryExpression = ParsePredicate(expression);

        return ExecuteFullTableScanQuery<T>(collectionName, queryExpression);
    }

    private IEnumerable<T> ExecuteFullTableScanQuery<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(
        string collectionName,
        QueryExpression? queryExpression)
        where T : class
    {
        var predicates = new List<ScanPredicate>();
        bool fullyPushed = CollectPredicates(queryExpression, predicates);
        var pushDownPredicates = predicates.Count > 0 ? predicates.ToArray() : null;
        var collectionFieldNameBytes = Encoding.UTF8.GetBytes("_collection");
        var collectionNameBytes = Encoding.UTF8.GetBytes(collectionName);

        return Iterator();

        IEnumerable<T> Iterator()
        {
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

                var requiresPostFilter = result.RequiresPostFilter;
                BsonDocument doc;

                if (txOverlay != null && QuerySortKeyReader.TryReadBsonValue(span, SortFieldBytes.Id, out var rawIdValue))
                {
                    if (txOverlay.TryGetValue(rawIdValue, out var txDoc))
                    {
                        txOverlay.Remove(rawIdValue);
                        if (txDoc == null) continue;
                        doc = txDoc;
                        requiresPostFilter = true;
                    }
                    else
                    {
                        doc = DeserializeDocumentOrThrow(slice);
                        doc = _engine.ResolveLargeDocument(doc);
                    }
                }
                else
                {
                    doc = DeserializeDocumentOrThrow(slice);
                    doc = _engine.ResolveLargeDocument(doc);

                    if (txOverlay != null)
                    {
                        var idValue = doc["_id"];
                        if (txOverlay.TryGetValue(idValue, out var txDoc))
                        {
                            txOverlay.Remove(idValue);
                            if (txDoc == null) continue;
                            doc = txDoc;
                            requiresPostFilter = true;
                        }
                    }
                }

                if (queryExpression != null)
                {
                    var matched = fullyPushed
                        ? (!requiresPostFilter || ExpressionEvaluator.Evaluate(queryExpression, doc))
                        : ExpressionEvaluator.Evaluate(queryExpression, doc);
                    if (!matched) continue;
                }

                var entity = AotBsonMapper.FromDocument<T>(doc);
                if (entity != null) yield return entity;
            }

            if (txOverlay != null)
            {
                foreach (var txDoc in txOverlay.Values)
                {
                    if (txDoc == null) continue;
                    if (queryExpression != null && !ExpressionEvaluator.Evaluate(queryExpression, txDoc)) continue;

                    var entity = AotBsonMapper.FromDocument<T>(txDoc);
                    if (entity != null) yield return entity;
                }
            }
        }
    }

    private async IAsyncEnumerable<T> ExecuteFullTableScanAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(
        string collectionName,
        Expression<Func<T, bool>>? expression = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
        where T : class
    {
        var queryExpression = ParsePredicate(expression);

        var predicates = new List<ScanPredicate>();
        bool fullyPushed = CollectPredicates(queryExpression, predicates);
        var pushDownPredicates = predicates.Count > 0 ? predicates.ToArray() : null;
        var collectionFieldNameBytes = Encoding.UTF8.GetBytes("_collection");
        var collectionNameBytes = Encoding.UTF8.GetBytes(collectionName);

        var tx = _engine.GetCurrentTransaction();
        var txOverlay = tx != null ? QueryTransactionOverlay.Build(tx, collectionName) : null;

        await foreach (var result in _engine.FindAllRawWithPredicateInfoAsync(collectionName, pushDownPredicates, cancellationToken).ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();

            var slice = result.Slice;
            var span = slice.Span;
            if (!MatchesCollection(span, collectionFieldNameBytes, collectionNameBytes))
            {
                continue;
            }

            var requiresPostFilter = result.RequiresPostFilter;
            BsonDocument doc;

            if (txOverlay != null && QuerySortKeyReader.TryReadBsonValue(span, SortFieldBytes.Id, out var rawIdValue))
            {
                if (txOverlay.TryGetValue(rawIdValue, out var txDoc))
                {
                    txOverlay.Remove(rawIdValue);
                    if (txDoc == null) continue;
                    doc = txDoc;
                    requiresPostFilter = true;
                }
                else
                {
                    doc = DeserializeDocumentOrThrow(slice);
                    doc = await _engine.ResolveLargeDocumentAsync(doc, cancellationToken).ConfigureAwait(false);
                }
            }
            else
            {
                doc = DeserializeDocumentOrThrow(slice);
                doc = await _engine.ResolveLargeDocumentAsync(doc, cancellationToken).ConfigureAwait(false);

                if (txOverlay != null)
                {
                    var idValue = doc["_id"];
                    if (txOverlay.TryGetValue(idValue, out var txDoc))
                    {
                        txOverlay.Remove(idValue);
                        if (txDoc == null) continue;
                        doc = txDoc;
                        requiresPostFilter = true;
                    }
                }
            }

            if (queryExpression != null)
            {
                var matched = fullyPushed
                    ? (!requiresPostFilter || ExpressionEvaluator.Evaluate(queryExpression, doc))
                    : ExpressionEvaluator.Evaluate(queryExpression, doc);
                if (!matched) continue;
            }

            var entity = AotBsonMapper.FromDocument<T>(doc);
            if (entity != null) yield return entity;
        }

        if (txOverlay != null)
        {
            foreach (var txDoc in txOverlay.Values)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (txDoc == null) continue;
                if (queryExpression != null && !ExpressionEvaluator.Evaluate(queryExpression, txDoc)) continue;

                var entity = AotBsonMapper.FromDocument<T>(txDoc);
                if (entity != null) yield return entity;
            }
        }
    }
}
