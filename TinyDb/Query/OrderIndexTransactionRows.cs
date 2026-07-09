using System.Collections.Generic;
using TinyDb.Bson;
using TinyDb.Index;

namespace TinyDb.Query;

internal readonly struct OrderIndexRow
{
    public BsonValue Id { get; }
    public IndexKey Key { get; }
    public BsonDocument Document { get; }

    public OrderIndexRow(BsonValue id, IndexKey key, BsonDocument document)
    {
        Id = id;
        Key = key;
        Document = document;
    }
}

internal static class OrderIndexTransactionRows
{
    public static List<OrderIndexRow>? FromOverlay(
        Dictionary<BsonValue, BsonDocument?>? overlay,
        BTreeIndex index,
        QueryExpression? queryExpression,
        bool descending)
    {
        if (overlay == null || overlay.Count == 0) return null;

        var rows = new List<OrderIndexRow>(overlay.Count);
        foreach (var (id, doc) in overlay)
        {
            if (doc == null) continue;
            if (queryExpression != null && !ExpressionEvaluator.Evaluate(queryExpression, doc)) continue;

            rows.Add(new OrderIndexRow(id, BuildKey(index, doc), doc));
        }

        if (rows.Count == 0) return null;

        rows.Sort((a, b) => Compare(a, b, descending));
        return rows;
    }

    public static IndexKey BuildKey(BTreeIndex index, BsonDocument doc)
    {
        var fields = index.Fields;

        if (fields.Count == 1)
        {
            return doc.TryGetValue(fields[0], out var v) && v != null ? IndexKey.Create(v) : IndexKey.Create(BsonNull.Value);
        }

        var values = new BsonValue[fields.Count];
        for (int i = 0; i < fields.Count; i++)
        {
            values[i] = doc.TryGetValue(fields[i], out var v) && v != null ? v : BsonNull.Value;
        }

        return new IndexKey(values);
    }

    public static int Compare(in OrderIndexRow a, in OrderIndexRow b, bool descending)
    {
        var cmp = a.Key.CompareTo(b.Key);
        if (cmp != 0) return descending ? -cmp : cmp;

        cmp = a.Id.CompareTo(b.Id);
        return descending ? -cmp : cmp;
    }

    public static int CompareToBase(in OrderIndexRow txRow, IndexKey baseKey, BsonValue baseId, bool descending)
    {
        var cmp = txRow.Key.CompareTo(baseKey);
        if (cmp != 0) return descending ? -cmp : cmp;

        return 0;
    }
}
