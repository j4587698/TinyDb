using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using TinyDb.Bson;
using TinyDb.Core;
using TinyDb.Index;
using TinyDb.Serialization;

namespace TinyDb.Query;

/// <summary>
/// æŸ¥è¯¢ä¼˜åŒ–å™¨ - è´Ÿè´£é€‰æ‹©æœ€ä¼˜ç´¢å¼•å’Œæ‰§è¡Œè®¡åˆ’
/// </summary>
public sealed class QueryOptimizer
{
    private const int MaxIndexUnionClauses = 16;
    private readonly TinyDbEngine _engine;
    private readonly ExpressionParser _expressionParser;

    /// <summary>
    /// åˆå§‹åŒ–æŸ¥è¯¢ä¼˜åŒ–å™¨
    /// </summary>
    /// <param name="engine">æ•°æ®åº“å¼•æ“Ž</param>
    public QueryOptimizer(TinyDbEngine engine)
    {
        _engine = engine ?? throw new ArgumentNullException(nameof(engine));
        _expressionParser = new ExpressionParser();
    }

    /// <summary>
    /// ä¸ºæŸ¥è¯¢åˆ›å»ºæœ€ä¼˜æ‰§è¡Œè®¡åˆ’
    /// </summary>
    /// <typeparam name="T">æ–‡æ¡£ç±»åž‹</typeparam>
    /// <param name="collectionName">é›†åˆåç§°</param>
    /// <param name="expression">æŸ¥è¯¢è¡¨è¾¾å¼</param>
    /// <returns>æŸ¥è¯¢æ‰§è¡Œè®¡åˆ’</returns>
    public QueryExecutionPlan CreateExecutionPlan<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(
        string collectionName, Expression<Func<T, bool>>? expression = null,
        bool planningMetadataOnly = false)
        where T : class, new()
    {
        var plan = new QueryExecutionPlan
        {
            CollectionName = collectionName,
            OriginalExpression = expression
        };

        // å¦‚æžœæ²¡æœ‰æŸ¥è¯¢æ¡ä»¶ï¼Œä½¿ç”¨å…¨è¡¨æ‰«æ
        if (expression == null)
        {
            plan.Strategy = QueryExecutionStrategy.FullTableScan;
            return plan;
        }

        // è§£æžæŸ¥è¯¢è¡¨è¾¾å¼
        QueryExpression? queryExpression = null;
        try
        {
            queryExpression = _expressionParser.Parse(expression);
            plan.QueryExpression = queryExpression;
        }
        catch (Exception ex) when (ex is NotSupportedException or InvalidOperationException or ArgumentException)
        {
            // è§£æžå¤±è´¥ï¼ˆä¾‹å¦‚åŒ…å«ä¸æ”¯æŒçš„èŠ‚ç‚¹ï¼‰ï¼Œå›žé€€åˆ°å…¨è¡¨æ‰«æ
            plan.Strategy = QueryExecutionStrategy.FullTableScan;
            return plan;
        }

        // ä¼˜åŒ–ï¼šæ£€æŸ¥æ˜¯å¦ä¸ºä¸»é”®æŸ¥è¯¢
        if (TryCreateIndexUnionPlan(collectionName, queryExpression, expression, planningMetadataOnly, out var unionPlan))
        {
            return unionPlan;
        }

        var primaryKeyValue = ExtractPrimaryKeyValue(queryExpression);
        if (primaryKeyValue != null)
        {
            plan.Strategy = QueryExecutionStrategy.PrimaryKeyLookup;
            // å°†ä¸»é”®å€¼å­˜å‚¨åœ¨ ScanKeys ä¸­ä»¥ä¾¿ Executor ä½¿ç”¨
            plan.IndexScanKeys = new List<IndexScanKey> 
            { 
                new IndexScanKey 
                { 
                    FieldName = "_id", 
                    Value = primaryKeyValue, 
                    ComparisonType = ComparisonType.Equal 
                } 
            };
            return plan;
        }

        // èŽ·å–é›†åˆçš„ç´¢å¼•ç®¡ç†å™¨
        var indexManager = _engine.GetIndexManager(collectionName);
        // åˆ†æžå¯ç”¨çš„ç´¢å¼•
        var availableIndexes = planningMetadataOnly
            ? indexManager.GetPlanningStatistics()
            : indexManager.GetAllStatistics();

        // å°è¯•æ‰¾åˆ°æœ€é€‚åˆçš„ç´¢å¼•
        var comparisons = QueryPredicateAnalyzer.ExtractComparisonMap(queryExpression);
        var bestIndex = SelectBestIndex(availableIndexes, comparisons);
        if (bestIndex != null)
        {
            plan.Strategy = QueryExecutionStrategy.IndexScan;
            plan.UseIndex = bestIndex;
            plan.IndexScanKeys = ExtractIndexScanKeys(bestIndex, comparisons);

            // ä¼˜åŒ–ï¼šå¦‚æžœæ˜¯å”¯ä¸€ç´¢å¼•ä¸”æŸ¥è¯¢è¦†ç›–äº†æ‰€æœ‰å­—æ®µä¸”å‡ä¸ºç­‰å€¼åŒ¹é…ï¼Œå‡çº§ä¸º IndexSeek
            if (bestIndex.IsUnique && 
                plan.IndexScanKeys.Count == bestIndex.Fields.Length && 
                plan.IndexScanKeys.All(k => k.ComparisonType == ComparisonType.Equal))
            {
                plan.Strategy = QueryExecutionStrategy.IndexSeek;
            }
        }
        else
        {
            plan.Strategy = QueryExecutionStrategy.FullTableScan;
        }

        return plan;
    }

    internal QueryExecutionPlan CreateExecutionPlan(
        string collectionName,
        QueryExpression? queryExpression,
        bool planningMetadataOnly = false)
    {
        var plan = new QueryExecutionPlan
        {
            CollectionName = collectionName,
            QueryExpression = queryExpression
        };

        if (queryExpression == null)
        {
            plan.Strategy = QueryExecutionStrategy.FullTableScan;
            return plan;
        }

        if (TryCreateIndexUnionPlan(collectionName, queryExpression, originalExpression: null, planningMetadataOnly, out var unionPlan))
        {
            return unionPlan;
        }

        var primaryKeyValue = ExtractPrimaryKeyValue(queryExpression);
        if (primaryKeyValue != null)
        {
            plan.Strategy = QueryExecutionStrategy.PrimaryKeyLookup;
            plan.IndexScanKeys = new List<IndexScanKey>
            {
                new IndexScanKey
                {
                    FieldName = "_id",
                    Value = primaryKeyValue,
                    ComparisonType = ComparisonType.Equal
                }
            };
            return plan;
        }

        var indexManager = _engine.GetIndexManager(collectionName);
        var availableIndexes = planningMetadataOnly
            ? indexManager.GetPlanningStatistics()
            : indexManager.GetAllStatistics();

        var comparisons = QueryPredicateAnalyzer.ExtractComparisonMap(queryExpression);
        var bestIndex = SelectBestIndex(availableIndexes, comparisons);
        if (bestIndex != null)
        {
            plan.Strategy = QueryExecutionStrategy.IndexScan;
            plan.UseIndex = bestIndex;
            plan.IndexScanKeys = ExtractIndexScanKeys(bestIndex, comparisons);

            if (bestIndex.IsUnique &&
                plan.IndexScanKeys.Count == bestIndex.Fields.Length &&
                plan.IndexScanKeys.All(k => k.ComparisonType == ComparisonType.Equal))
            {
                plan.Strategy = QueryExecutionStrategy.IndexSeek;
            }
        }
        else
        {
            plan.Strategy = QueryExecutionStrategy.FullTableScan;
        }

        return plan;
    }

    /// <summary>
    /// å°è¯•ä»ŽæŸ¥è¯¢è¡¨è¾¾å¼ä¸­æå–ä¸»é”®å€¼
    /// </summary>
    /// <param name="queryExpression">æŸ¥è¯¢è¡¨è¾¾å¼</param>
    /// <returns>ä¸»é”®å€¼ï¼Œå¦‚æžœä¸æ˜¯ä¸»é”®æŸ¥è¯¢åˆ™è¿”å›žnull</returns>
    private bool TryCreateIndexUnionPlan(
        string collectionName,
        QueryExpression queryExpression,
        LambdaExpression? originalExpression,
        bool planningMetadataOnly,
        [NotNullWhen(true)] out QueryExecutionPlan? unionPlan)
    {
        unionPlan = null;

        if (!QueryPredicateAnalyzer.TryBuildDisjunctiveClauses(queryExpression, MaxIndexUnionClauses, out var clauses))
        {
            return false;
        }

        var branchPlans = new List<QueryExecutionPlan>(clauses.Count);
        foreach (var clause in clauses)
        {
            var branchPlan = CreateSingleBranchPlan(collectionName, clause, planningMetadataOnly);
            if (branchPlan.Strategy == QueryExecutionStrategy.FullTableScan)
            {
                return false;
            }

            branchPlans.Add(branchPlan);
        }

        unionPlan = new QueryExecutionPlan
        {
            CollectionName = collectionName,
            OriginalExpression = originalExpression,
            QueryExpression = queryExpression,
            Strategy = QueryExecutionStrategy.IndexUnion,
            BranchPlans = branchPlans
        };
        return true;
    }

    private QueryExecutionPlan CreateSingleBranchPlan(
        string collectionName,
        QueryExpression queryExpression,
        bool planningMetadataOnly)
    {
        var plan = new QueryExecutionPlan
        {
            CollectionName = collectionName,
            QueryExpression = queryExpression
        };

        var primaryKeyValue = ExtractPrimaryKeyValue(queryExpression);
        if (primaryKeyValue != null)
        {
            plan.Strategy = QueryExecutionStrategy.PrimaryKeyLookup;
            plan.IndexScanKeys = new List<IndexScanKey>
            {
                new()
                {
                    FieldName = "_id",
                    Value = primaryKeyValue,
                    ComparisonType = ComparisonType.Equal
                }
            };
            return plan;
        }

        var indexManager = _engine.GetIndexManager(collectionName);
        var availableIndexes = planningMetadataOnly
            ? indexManager.GetPlanningStatistics()
            : indexManager.GetAllStatistics();

        var comparisons = QueryPredicateAnalyzer.ExtractComparisonMap(queryExpression);
        var bestIndex = SelectBestIndex(availableIndexes, comparisons);
        if (bestIndex == null)
        {
            plan.Strategy = QueryExecutionStrategy.FullTableScan;
            return plan;
        }

        plan.Strategy = QueryExecutionStrategy.IndexScan;
        plan.UseIndex = bestIndex;
        plan.IndexScanKeys = ExtractIndexScanKeys(bestIndex, comparisons);

        if (bestIndex.IsUnique &&
            plan.IndexScanKeys.Count == bestIndex.Fields.Length &&
            plan.IndexScanKeys.All(k => k.ComparisonType == ComparisonType.Equal))
        {
            plan.Strategy = QueryExecutionStrategy.IndexSeek;
        }

        return plan;
    }

    private static BsonValue? ExtractPrimaryKeyValue(QueryExpression queryExpression)
    {
        if (queryExpression is BinaryExpression binaryExpr)
        {
            // å¦‚æžœæ˜¯ AND è¡¨è¾¾å¼ï¼Œé€’å½’æ£€æŸ¥å·¦å³å­æ ‘
            if (binaryExpr.NodeType == System.Linq.Expressions.ExpressionType.AndAlso)
            {
                var leftResult = ExtractPrimaryKeyValue(binaryExpr.Left);
                if (leftResult != null) return leftResult;
                
                return ExtractPrimaryKeyValue(binaryExpr.Right);
            }

            // å¿…é¡»æ˜¯ç­‰å€¼æ¯”è¾ƒ
            if (binaryExpr.NodeType != System.Linq.Expressions.ExpressionType.Equal)
                return null;

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

            // æ£€æŸ¥å­—æ®µåæ˜¯å¦ä¸º _id æˆ– Id (ä¸åŒºåˆ†å¤§å°å†™)
            if (fieldName != null && (string.Equals(fieldName, "_id", StringComparison.OrdinalIgnoreCase) || string.Equals(fieldName, "Id", StringComparison.OrdinalIgnoreCase)))
            {
                return value;
            }
        }

        return null;
    }

    /// <summary>
    /// é€‰æ‹©æœ€é€‚åˆçš„ç´¢å¼•
    /// </summary>
    /// <param name="queryExpression">æŸ¥è¯¢è¡¨è¾¾å¼</param>
    /// <param name="availableIndexes">å¯ç”¨ç´¢å¼•åˆ—è¡¨</param>
    /// <returns>æœ€é€‚åˆçš„ç´¢å¼•ï¼Œå¦‚æžœæ²¡æœ‰åˆé€‚çš„åˆ™è¿”å›žnull</returns>
    private static IndexStatistics? SelectBestIndex(
        IEnumerable<IndexStatistics> availableIndexes,
        IReadOnlyDictionary<string, QueryFieldComparison> comparisons)
    {
        IndexStatistics? bestIndex = null;
        var bestScore = 0;

        // åˆ†æžæ¯ä¸ªç´¢å¼•ä¸ŽæŸ¥è¯¢çš„åŒ¹é…åº¦
        foreach (var indexStat in availableIndexes)
        {
            var matchScore = CalculateIndexMatchScoreCore(indexStat, comparisons);
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

    /// <summary>
    /// è®¡ç®—ç´¢å¼•ä¸ŽæŸ¥è¯¢è¡¨è¾¾å¼çš„åŒ¹é…åˆ†æ•°
    /// </summary>
    /// <param name="queryExpression">æŸ¥è¯¢è¡¨è¾¾å¼</param>
    /// <param name="indexStat">ç´¢å¼•ç»Ÿè®¡</param>
    /// <returns>åŒ¹é…åˆ†æ•°ï¼ˆ0è¡¨ç¤ºä¸åŒ¹é…ï¼‰</returns>
    private static int CalculateIndexMatchScoreCore(
        IndexStatistics indexStat,
        IReadOnlyDictionary<string, QueryFieldComparison> comparisons)
    {
        var score = 0;
        // å•å­—æ®µç´¢å¼•åŒ¹é…
        if (indexStat.Fields.Length == 1)
        {
            if (TryGetComparisonForIndexField(comparisons, indexStat.Fields[0], out _))
            {
                score = 10; // å•å­—æ®µç²¾ç¡®åŒ¹é…
                if (indexStat.IsUnique)
                {
                    score += 5; // å”¯ä¸€ç´¢å¼•åŠ åˆ†
                }
            }
        }
        // å¤åˆç´¢å¼•åŒ¹é…
        else if (indexStat.Fields.Length > 1)
        {
            var matchedFields = 0;
            for (int i = 0; i < indexStat.Fields.Length; i++)
            {
                if (TryGetComparisonForIndexField(comparisons, indexStat.Fields[i], out _))
                {
                    matchedFields++;
                    score += 10; // å‰ç¼€åŒ¹é…æ¯ä¸ªå­—æ®µåŠ åˆ†
                }
                else
                {
                    break; // å¤åˆç´¢å¼•å¿…é¡»å‰ç¼€åŒ¹é…
                }
            }

            if (matchedFields > 0)
            {
                score += matchedFields * 2; // å¤åˆç´¢å¼•é¢å¤–åŠ åˆ†
            }
        }

        return score;
    }

    private static int CalculateIndexMatchScore(QueryExpression queryExpression, IndexStatistics indexStat)
    {
        return CalculateIndexMatchScoreCore(indexStat, QueryPredicateAnalyzer.ExtractComparisonMap(queryExpression));
    }

    /// <summary>
    /// ä»ŽæŸ¥è¯¢è¡¨è¾¾å¼ä¸­æå–æŸ¥è¯¢å­—æ®µ
    /// </summary>
    /// <param name="queryExpression">æŸ¥è¯¢è¡¨è¾¾å¼</param>
    /// <returns>æŸ¥è¯¢å­—æ®µåˆ—è¡¨</returns>
    private static IEnumerable<string> ExtractQueryFields(QueryExpression queryExpression)
    {
        if (queryExpression is BinaryExpression binaryExpr)
        {
            if (binaryExpr.Left is MemberExpression leftMember)
            {
                yield return leftMember.MemberName;
            }
            else if (binaryExpr.Right is MemberExpression rightMember)
            {
                yield return rightMember.MemberName;
            }
        }

        // é€’å½’å¤„ç†åµŒå¥—è¡¨è¾¾å¼
        if (queryExpression is BinaryExpression nestedBinary)
        {
            foreach (var field in ExtractQueryFields(nestedBinary.Left))
                yield return field;
            foreach (var field in ExtractQueryFields(nestedBinary.Right))
                yield return field;
        }
    }

    /// <summary>
    /// ä»ŽæŸ¥è¯¢è¡¨è¾¾å¼ä¸­æå–ç´¢å¼•æ‰«æé”®
    /// </summary>
    /// <param name="queryExpression">æŸ¥è¯¢è¡¨è¾¾å¼</param>
    /// <param name="indexStat">ç´¢å¼•ç»Ÿè®¡</param>
    /// <returns>ç´¢å¼•æ‰«æé”®åˆ—è¡¨</returns>
    private static List<IndexScanKey> ExtractIndexScanKeys(
        IndexStatistics indexStat,
        IReadOnlyDictionary<string, QueryFieldComparison> comparisons)
    {
        var scanKeys = new List<IndexScanKey>();

        for (int i = 0; i < indexStat.Fields.Length; i++)
        {
            var fieldName = indexStat.Fields[i];

            if (TryGetComparisonForIndexField(comparisons, fieldName, out var comparison))
            {
                scanKeys.Add(comparison.ToIndexScanKey(fieldName));
            }
            else
            {
                break;
            }
        }

        return scanKeys;
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

/// <summary>
/// æŸ¥è¯¢æ‰§è¡Œè®¡åˆ’
/// </summary>
public sealed class QueryExecutionPlan
{
    /// <summary>
    /// é›†åˆåç§°
    /// </summary>
    public string CollectionName { get; set; } = string.Empty;

    /// <summary>
    /// åŽŸå§‹æŸ¥è¯¢è¡¨è¾¾å¼
    /// </summary>
    public LambdaExpression? OriginalExpression { get; set; }

    /// <summary>
    /// è§£æžåŽçš„æŸ¥è¯¢è¡¨è¾¾å¼
    /// </summary>
    public QueryExpression? QueryExpression { get; set; }

    /// <summary>
    /// æ‰§è¡Œç­–ç•¥
    /// </summary>
    public QueryExecutionStrategy Strategy { get; set; }

    /// <summary>
    /// ä½¿ç”¨çš„ç´¢å¼•
    /// </summary>
    public IndexStatistics? UseIndex { get; set; }

    /// <summary>
    /// ç´¢å¼•æ‰«æé”®
    /// </summary>
    public List<IndexScanKey> IndexScanKeys { get; set; } = new();

    public List<QueryExecutionPlan> BranchPlans { get; set; } = new();

    /// <summary>
    /// ä¼°ç®—çš„æ‰§è¡Œæˆæœ¬
    /// </summary>
    public double EstimatedCost { get; set; }

    /// <summary>
    /// ä¼°ç®—çš„ç»“æžœæ•°é‡
    /// </summary>
    public long EstimatedResultCount { get; set; }
}

/// <summary>
/// æŸ¥è¯¢æ‰§è¡Œç­–ç•¥
/// </summary>
public enum QueryExecutionStrategy
{
    /// <summary>
    /// å…¨è¡¨æ‰«æ
    /// </summary>
    FullTableScan,

    /// <summary>
    /// ç´¢å¼•æ‰«æ
    /// </summary>
    IndexScan,

    /// <summary>
    /// ç´¢å¼•æŸ¥æ‰¾ï¼ˆä½¿ç”¨å”¯ä¸€ç´¢å¼•ï¼‰
    /// </summary>
    IndexSeek,

    /// <summary>
    /// ä¸»é”®æŸ¥æ‰¾
    /// </summary>
    PrimaryKeyLookup,

    /// <summary>
    /// OR Ã¥Ë†â€ Ã¦â€Â¯Ã§Â´Â¢Ã¥Â¼â€¢Ã¥Â¹Â¶Ã©â€ºâ€ 
    /// </summary>
    IndexUnion
}

/// <summary>
/// ç´¢å¼•æ‰«æé”®
/// </summary>
public sealed class IndexScanKey
{
    /// <summary>
    /// å­—æ®µå
    /// </summary>
    public string FieldName { get; set; } = string.Empty;

    /// <summary>
    /// æ¯”è¾ƒå€¼
    /// </summary>
    public BsonValue Value { get; set; } = BsonNull.Value;

    /// <summary>
    /// èŒƒå›´ä¸‹ç•Œå€¼ï¼Œä»…åœ¨ Range æ¯”è¾ƒæ—¶ä½¿ç”¨
    /// </summary>
    public BsonValue? LowerValue { get; set; }

    /// <summary>
    /// èŒƒå›´ä¸Šç•Œå€¼ï¼Œä»…åœ¨ Range æ¯”è¾ƒæ—¶ä½¿ç”¨
    /// </summary>
    public BsonValue? UpperValue { get; set; }

    /// <summary>
    /// æ˜¯å¦åŒ…å«èŒƒå›´ä¸‹ç•Œ
    /// </summary>
    public bool IncludeLower { get; set; } = true;

    /// <summary>
    /// æ˜¯å¦åŒ…å«èŒƒå›´ä¸Šç•Œ
    /// </summary>
    public bool IncludeUpper { get; set; } = true;

    /// <summary>
    /// æ¯”è¾ƒç±»åž‹
    /// </summary>
    public ComparisonType ComparisonType { get; set; } = ComparisonType.Equal;
}

/// <summary>
/// æ¯”è¾ƒç±»åž‹
/// </summary>
public enum ComparisonType
{
    /// <summary>
    /// ç­‰äºŽ
    /// </summary>
    Equal,

    /// <summary>
    /// ä¸ç­‰äºŽ
    /// </summary>
    NotEqual,

    /// <summary>
    /// å¤§äºŽ
    /// </summary>
    GreaterThan,

    /// <summary>
    /// å¤§äºŽç­‰äºŽ
    /// </summary>
    GreaterThanOrEqual,

    /// <summary>
    /// å°äºŽ
    /// </summary>
    LessThan,

    /// <summary>
    /// å°äºŽç­‰äºŽ
    /// </summary>
    LessThanOrEqual,

    /// <summary>
    /// åŒå­—æ®µèŒƒå›´
    /// </summary>
    Range
}
