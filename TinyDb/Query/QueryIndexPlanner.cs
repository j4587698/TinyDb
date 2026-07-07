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

        if (left is MemberExpression leftMember)
        {
            fieldName = leftMember.MemberName;
            value = QueryPredicateAnalyzer.ExtractConstantValue(right);
        }
        else if (right is MemberExpression rightMember)
        {
            fieldName = rightMember.MemberName;
            value = QueryPredicateAnalyzer.ExtractConstantValue(left);
        }

        return IsPrimaryKeyField(fieldName) ? value : null;
    }

    public static IndexScanKey CreatePrimaryKeyScanKey(BsonValue value)
    {
        return new IndexScanKey
        {
            FieldName = "_id",
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
        return fieldName != null &&
               (string.Equals(fieldName, "_id", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(fieldName, "Id", StringComparison.OrdinalIgnoreCase));
    }

    private static bool TryGetComparisonForIndexField(
        IReadOnlyDictionary<string, QueryFieldComparison> comparisons,
        string indexFieldName,
        out QueryFieldComparison comparison)
    {
        if (comparisons.TryGetValue(indexFieldName, out comparison!))
        {
            return true;
        }

        QueryFieldComparison? matched = null;
        foreach (var candidate in comparisons)
        {
            if (!string.Equals(BsonFieldName.ToCamelCase(candidate.Key), indexFieldName, StringComparison.Ordinal))
            {
                continue;
            }

            if (matched != null)
            {
                comparison = null!;
                return false;
            }

            matched = candidate.Value;
        }

        if (matched == null)
        {
            comparison = null!;
            return false;
        }

        comparison = matched;
        return true;
    }
}
