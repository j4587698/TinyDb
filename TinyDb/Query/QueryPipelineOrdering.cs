using System.Collections;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using LinqExp = System.Linq.Expressions;

namespace TinyDb.Query;

internal static class QueryPipelineOrdering
{
    public static IEnumerable<T> ExecuteGeneric<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(
        IEnumerable<T> source,
        LinqExp.MethodCallExpression methodCall)
        where T : class
    {
        var lambda = (LinqExp.LambdaExpression)((LinqExp.UnaryExpression)methodCall.Arguments[1]).Operand;
        var parser = new ExpressionParser();
        var keyExpr = parser.ParseExpression(lambda.Body);

        Func<T, object> keySelector = item => ExpressionEvaluator.EvaluateValue(keyExpr, item!)!;
        var comparer = QueryObjectComparer.Instance;

        return methodCall.Method.Name switch
        {
            "OrderBy" => source.OrderBy(keySelector, comparer),
            "OrderByDescending" => source.OrderByDescending(keySelector, comparer),
            "ThenBy" when source is IOrderedEnumerable<T> ordered => ordered.ThenBy(keySelector, comparer),
            "ThenByDescending" when source is IOrderedEnumerable<T> ordered => ordered.ThenByDescending(keySelector, comparer),
            _ => source.OrderBy(keySelector, comparer)
        };
    }

    public static IEnumerable Execute(IEnumerable source, LinqExp.MethodCallExpression methodCall)
    {
        var lambda = (LinqExp.LambdaExpression)((LinqExp.UnaryExpression)methodCall.Arguments[1]).Operand;
        var parser = new ExpressionParser();
        var keyExpr = parser.ParseExpression(lambda.Body);

        Func<object, object> keySelector = item => ExpressionEvaluator.EvaluateValue(keyExpr, item!)!;
        var comparer = QueryObjectComparer.Instance;
        var typedSource = source.Cast<object>();

        return methodCall.Method.Name switch
        {
            "OrderBy" => typedSource.OrderBy(keySelector, comparer),
            "OrderByDescending" => typedSource.OrderByDescending(keySelector, comparer),
            "ThenBy" when source is IOrderedEnumerable<object> ordered => ordered.ThenBy(keySelector, comparer),
            "ThenByDescending" when source is IOrderedEnumerable<object> ordered => ordered.ThenByDescending(keySelector, comparer),
            _ => typedSource.OrderBy(keySelector, comparer)
        };
    }
}
