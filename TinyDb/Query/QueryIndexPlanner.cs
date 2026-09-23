using TinyDb.Bson;
using TinyDb.Index;
using TinyDb.Serialization;

namespace TinyDb.Query;

internal static class QueryIndexPlanner
{
    public static BsonValue? ExtractPrimaryKeyValue(QueryExpression queryExpression)
    {
        if (queryExpression is not BinaryExpression binaryExpr)
        {
            return null;
        }

        if (binaryExpr.NodeType == System.Linq.Expressions.ExpressionType.AndAlso)
        {
            var leftResult = ExtractPrimaryKeyValue(binaryExpr.Left);
            return leftResult ?? ExtractPrimaryKeyValue(binaryExpr.Right);
        }

        if (binaryExpr.NodeType != System.Linq.Expressions.ExpressionType.Equal)
        {
            return null;
        }

        string? fieldName = null;
        BsonValue? value = null;
        var left = QueryPredicateAnalyzer.UnwrapConvert(binaryExpr.Left);
        var right = QueryPredicateAnalyzer.UnwrapConvert(binaryExpr.Right);

        // 只有直接访问查询参数的成员才可能是根文档主键。
        // 缺少这个守卫时 x.Owner.Id == 5 会被误判为根文档 _id == 5。
        if (left is MemberExpression { IsRootMember: true } leftMember)
        {
            fieldName = leftMember.StorageName;
            value = QueryPredicateAnalyzer.ExtractConstantValue(right);
        }
        else if (right is MemberExpression { IsRootMember: true } rightMember)
        {
            fieldName = rightMember.StorageName;
            value = QueryPredicateAnalyzer.ExtractConstantValue(left);
        }

        return IsPrimaryKeyField(fieldName) ? value : null;
    }

    public static IndexScanKey CreatePrimaryKeyScanKey(BsonValue value)
    {
        return new IndexScanKey
        {
            FieldName = BsonFieldName.Id,
            Value = value,
            ComparisonType = ComparisonType.Equal
        };
    }

    public static IndexStatistics? SelectBestIndex(
        IEnumerable<IndexStatistics> availableIndexes,
        IReadOnlyDictionary<string, QueryFieldComparison> comparisons)
    {
        IndexStatistics? bestIndex = null;
        var bestScore = 0;

        foreach (var indexStat in availableIndexes)
        {
            var matchScore = CalculateIndexMatchScore(indexStat, comparisons);
            if (matchScore <= 0)
            {
                continue;
            }

            if (bestIndex == null ||
                matchScore > bestScore ||
                matchScore == bestScore && indexStat.EntryCount < bestIndex.EntryCount)
            {
                bestIndex = indexStat;
                bestScore = matchScore;
            }
        }

        return bestIndex;
    }

    public static int CalculateIndexMatchScore(
        IndexStatistics indexStat,
        IReadOnlyDictionary<string, QueryFieldComparison> comparisons)
    {
        if (indexStat.Fields.Length == 1)
        {
            if (!TryGetComparisonForIndexField(comparisons, indexStat.Fields[0], out _))
            {
                return 0;
            }

            return indexStat.IsUnique ? 15 : 10;
        }

        if (indexStat.Fields.Length <= 1)
        {
            return 0;
        }

        var score = 0;
        var matchedFields = 0;
        for (var i = 0; i < indexStat.Fields.Length; i++)
        {
            if (!TryGetComparisonForIndexField(comparisons, indexStat.Fields[i], out _))
            {
                break;
            }

            matchedFields++;
            score += 10;
        }

        return matchedFields == 0
            ? 0
            : score + matchedFields * 2;
    }

    public static List<IndexScanKey> ExtractIndexScanKeys(
        IndexStatistics indexStat,
        IReadOnlyDictionary<string, QueryFieldComparison> comparisons)
    {
        var scanKeys = new List<IndexScanKey>();

        for (var i = 0; i < indexStat.Fields.Length; i++)
        {
            var fieldName = indexStat.Fields[i];
            if (!TryGetComparisonForIndexField(comparisons, fieldName, out var comparison))
            {
                break;
            }

            scanKeys.Add(comparison.ToIndexScanKey(fieldName));
        }

        return scanKeys;
    }

    public static bool IsUniqueEqualitySeek(IndexStatistics indexStat, IReadOnlyList<IndexScanKey> scanKeys)
    {
        if (!indexStat.IsUnique || scanKeys.Count != indexStat.Fields.Length)
        {
            return false;
        }

        for (var i = 0; i < scanKeys.Count; i++)
        {
            if (scanKeys[i].ComparisonType != ComparisonType.Equal)
            {
                return false;
            }
        }

        return true;
    }

    private static bool IsPrimaryKeyField(string? fieldName)
    {
        // fieldName 已经过 BsonFieldName.ForMember 解析，主键一定是保留名 _id。
        return string.Equals(fieldName, BsonFieldName.Id, StringComparison.Ordinal);
    }

    private static bool TryGetComparisonForIndexField(
        IReadOnlyDictionary<string, QueryFieldComparison> comparisons,
        string indexFieldName,
        out QueryFieldComparison comparison)
    {
        // comparisons 的键已经是存储字段名；IndexManager 创建索引时也会把字段名归一化为 camelCase。
        // 这里再按 camelCase 兜底一次，以兼容未归一化的索引元数据。
        return comparisons.TryGetValue(indexFieldName, out comparison!) ||
               comparisons.TryGetValue(BsonFieldName.ToCamelCase(indexFieldName), out comparison!);
    }
}
