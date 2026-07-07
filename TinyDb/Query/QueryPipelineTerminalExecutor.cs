using System.Collections;
using LinqExp = System.Linq.Expressions;

namespace TinyDb.Query;

internal static class QueryPipelineTerminalExecutor
{
    public static bool IsTerminal(string name) => QueryTerminalMethods.Contains(name);

    public static object? Execute(IEnumerable source, LinqExp.MethodCallExpression methodCall)
    {
        if (TryExecuteAll(source, methodCall, out var allResult))
        {
            return allResult;
        }

        if (methodCall.Arguments.Count == 2 &&
            methodCall.Arguments[1] is LinqExp.UnaryExpression u &&
            u.Operand is LinqExp.LambdaExpression lambda)
        {
            source = QueryPipelinePredicateEvaluator.Filter(source, lambda);
        }

        var typedSource = source.Cast<object>();

        return methodCall.Method.Name switch
        {
            "Count" => typedSource.Count(),
            "LongCount" => typedSource.LongCount(),
            "Any" => typedSource.Any(),
            "First" => typedSource.First(),
            "FirstOrDefault" => typedSource.FirstOrDefault(),
            "Single" => typedSource.Single(),
            "SingleOrDefault" => typedSource.SingleOrDefault(),
            "Last" => typedSource.Last(),
            "LastOrDefault" => typedSource.LastOrDefault(),
            "ElementAt" => typedSource.ElementAt(GetIndex(methodCall)),
            "ElementAtOrDefault" => typedSource.ElementAtOrDefault(GetIndex(methodCall)),
            _ => null
        };
    }

    private static bool TryExecuteAll(IEnumerable source, LinqExp.MethodCallExpression methodCall, out object? result)
    {
        result = null;

        if (methodCall.Method.Name != "All")
        {
            return false;
        }

        if (methodCall.Arguments.Count != 2 ||
            methodCall.Arguments[1] is not LinqExp.UnaryExpression u ||
            u.Operand is not LinqExp.LambdaExpression lambda)
        {
            return false;
        }

        var queryExpr = QueryPipelinePredicateEvaluator.Parse(lambda);

        foreach (var item in source)
        {
            if (item == null) continue;
            if (!QueryPipelinePredicateEvaluator.Matches(queryExpr, item))
            {
                result = false;
                return true;
            }
        }

        result = true;
        return true;
    }

    private static int GetIndex(LinqExp.MethodCallExpression methodCall)
    {
        return (int)((LinqExp.ConstantExpression)methodCall.Arguments[1]).Value!;
    }
}
