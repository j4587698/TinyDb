using System.Collections;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using TinyDb.Bson;
using LinqExp = System.Linq.Expressions;

namespace TinyDb.Query;

internal static partial class QueryPipeline
{
    private static IEnumerable ExecuteWhere(IEnumerable source, LinqExp.MethodCallExpression m)
    {
        var lambda = (LinqExp.LambdaExpression)((LinqExp.UnaryExpression)m.Arguments[1]).Operand;
        return ExecuteWhereLambda(source, lambda);
    }

    private static IEnumerable ExecuteWhereLambda(IEnumerable source, LinqExp.LambdaExpression lambda)
    {
        return QueryPipelinePredicateEvaluator.Filter(source, lambda);
    }

    private static IEnumerable ExecuteSelect(IEnumerable source, LinqExp.LambdaExpression selector)
    {
        var parser = new ExpressionParser();
        var queryExpr = parser.ParseExpression(selector.Body);

        foreach (var item in source)
        {
            yield return QueryPipelineAggregation.ConvertValueToType(ExpressionEvaluator.EvaluateValue(queryExpr, item!), selector.ReturnType);
        }
    }

    private static IEnumerable ExecuteSkip(IEnumerable source, int count)
    {
        var e = source.GetEnumerator();
        while (count > 0 && e.MoveNext()) count--;
        while (e.MoveNext()) yield return e.Current;
    }

    private static IEnumerable ExecuteTake(IEnumerable source, int count)
    {
        var e = source.GetEnumerator();
        while (count > 0 && e.MoveNext())
        {
            yield return e.Current;
            count--;
        }
    }

    private static IEnumerable ExecuteDistinct(IEnumerable source)
    {
        return source.Cast<object>().Distinct();
    }
}
