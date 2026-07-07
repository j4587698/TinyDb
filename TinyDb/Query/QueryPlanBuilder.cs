using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using TinyDb.Core;
using TinyDb.Index;

namespace TinyDb.Query;

internal sealed class QueryPlanBuilder
{
    private const int MaxIndexUnionClauses = 16;

    private readonly TinyDbEngine _engine;

    public QueryPlanBuilder(TinyDbEngine engine)
    {
        _engine = engine ?? throw new ArgumentNullException(nameof(engine));
    }

    public QueryExecutionPlan CreatePlan(
        string collectionName,
        QueryExpression? queryExpression,
        LambdaExpression? originalExpression,
        bool planningMetadataOnly)
    {
        if (queryExpression == null)
        {
            return QueryExecutionPlan.FullTableScan(collectionName, originalExpression);
        }

        if (TryCreateIndexUnionPlan(
                collectionName,
                queryExpression,
                originalExpression,
                planningMetadataOnly,
                out var unionPlan))
        {
            return unionPlan;
        }

        return CreateSingleBranchPlan(collectionName, queryExpression, originalExpression, planningMetadataOnly);
    }

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
            var branchPlan = CreateSingleBranchPlan(collectionName, clause, originalExpression: null, planningMetadataOnly);
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
        LambdaExpression? originalExpression,
        bool planningMetadataOnly)
    {
        var primaryKeyValue = QueryIndexPlanner.ExtractPrimaryKeyValue(queryExpression);
        if (primaryKeyValue != null)
        {
            return new QueryExecutionPlan
            {
                CollectionName = collectionName,
                OriginalExpression = originalExpression,
                QueryExpression = queryExpression,
                Strategy = QueryExecutionStrategy.PrimaryKeyLookup,
                IndexScanKeys = new List<IndexScanKey>
                {
                    QueryIndexPlanner.CreatePrimaryKeyScanKey(primaryKeyValue)
                }
            };
        }

        var availableIndexes = GetAvailableIndexes(collectionName, planningMetadataOnly);
        var comparisons = QueryPredicateAnalyzer.ExtractComparisonMap(queryExpression);
        var bestIndex = QueryIndexPlanner.SelectBestIndex(availableIndexes, comparisons);
        if (bestIndex == null)
        {
            return QueryExecutionPlan.FullTableScan(collectionName, originalExpression, queryExpression);
        }

        var scanKeys = QueryIndexPlanner.ExtractIndexScanKeys(bestIndex, comparisons);
        return new QueryExecutionPlan
        {
            CollectionName = collectionName,
            OriginalExpression = originalExpression,
            QueryExpression = queryExpression,
            Strategy = QueryIndexPlanner.IsUniqueEqualitySeek(bestIndex, scanKeys)
                ? QueryExecutionStrategy.IndexSeek
                : QueryExecutionStrategy.IndexScan,
            UseIndex = bestIndex,
            IndexScanKeys = scanKeys
        };
    }

    private IEnumerable<IndexStatistics> GetAvailableIndexes(string collectionName, bool planningMetadataOnly)
    {
        var indexManager = _engine.GetIndexManager(collectionName);
        return planningMetadataOnly
            ? indexManager.GetPlanningStatistics()
            : indexManager.GetAllStatistics();
    }
}
