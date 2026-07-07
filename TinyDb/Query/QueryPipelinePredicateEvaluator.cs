using System.Collections;
using LinqExp = System.Linq.Expressions;

namespace TinyDb.Query;

internal static class QueryPipelinePredicateEvaluator
{
    public static IEnumerable Filter(IEnumerable source, LinqExp.LambdaExpression lambda)
    {
        var queryExpr = Parse(lambda);

        foreach (var item in source)
        {
            if (item == null) continue;
            if (Matches(queryExpr, item))
            {
                yield return item;
            }
        }
    }

    public static QueryExpression Parse(LinqExp.LambdaExpression lambda)
    {
        var parser = new ExpressionParser();
        return parser.ParseExpression(lambda.Body);
    }

    public static bool Matches(QueryExpression queryExpr, object item)
    {
        return ExpressionEvaluator.EvaluateValue(queryExpr, item) is bool result && result;
    }
}
