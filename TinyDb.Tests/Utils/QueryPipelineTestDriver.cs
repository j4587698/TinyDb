using System.Collections;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;
using TinyDb.Query;

namespace TinyDb.Tests.Utils;

[UnconditionalSuppressMessage("AOT", "IL3050", Justification = "Test-only reflection driver runs under the normal test host.")]
internal static class QueryPipelineTestDriver
{
    private static readonly MethodInfo ExecuteAotMethod =
        typeof(QueryPipeline).GetMethod("ExecuteAot", BindingFlags.NonPublic | BindingFlags.Static)
        ?? throw new MissingMethodException(typeof(QueryPipeline).FullName, "ExecuteAot");

    private static readonly MethodInfo ExecuteWhereGenericMethod =
        typeof(QueryPipeline).GetMethod("ExecuteWhereGeneric", BindingFlags.NonPublic | BindingFlags.Static)
        ?? throw new MissingMethodException(typeof(QueryPipeline).FullName, "ExecuteWhereGeneric");

    private static readonly MethodInfo ExecuteWhereLambdaMethod =
        typeof(QueryPipeline).GetMethod("ExecuteWhereLambda", BindingFlags.NonPublic | BindingFlags.Static)
        ?? throw new MissingMethodException(typeof(QueryPipeline).FullName, "ExecuteWhereLambda");

    public static object? ExecuteAot<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] TSource>(
        Expression expression,
        IEnumerable<TSource> queryResult,
        Expression? extractedPredicate)
        where TSource : class
    {
        var pushdown = new QueryPushdownInfo
        {
            WherePushedCount = extractedPredicate != null ? int.MaxValue : 0
        };

        return Invoke(() => ExecuteAotMethod
            .MakeGenericMethod(typeof(TSource))
            .Invoke(null, new object[] { expression, queryResult, pushdown }));
    }

    public static IEnumerable<T> ExecuteWhereGeneric<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(
        IEnumerable<T> source,
        MethodCallExpression methodCall)
        where T : class
    {
        return Invoke(() => (IEnumerable<T>)ExecuteWhereGenericMethod
            .MakeGenericMethod(typeof(T))
            .Invoke(null, new object[] { source, methodCall })!)!;
    }

    public static IEnumerable ExecuteWhereLambda(IEnumerable source, LambdaExpression lambda)
    {
        return Invoke(() => (IEnumerable)ExecuteWhereLambdaMethod.Invoke(null, new object[] { source, lambda })!)!;
    }

    public static bool IsTerminal(string name) => QueryPipelineTerminalExecutor.IsTerminal(name);

    private static T? Invoke<T>(Func<T?> action)
    {
        try
        {
            return action();
        }
        catch (TargetInvocationException ex) when (ex.InnerException != null)
        {
            throw ex.InnerException;
        }
    }
}
