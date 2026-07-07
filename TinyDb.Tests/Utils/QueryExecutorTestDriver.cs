using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using TinyDb.Bson;
using TinyDb.Query;

namespace TinyDb.Tests.Utils;

[UnconditionalSuppressMessage("AOT", "IL3050", Justification = "Test-only reflection driver runs under the normal test host.")]
internal static class QueryExecutorTestDriver
{
    private static readonly MethodInfo ExecuteIndexScanMethod =
        typeof(QueryExecutor).GetMethod("ExecuteIndexScan", BindingFlags.NonPublic | BindingFlags.Instance)
        ?? throw new MissingMethodException(typeof(QueryExecutor).FullName, "ExecuteIndexScan");

    private static readonly MethodInfo ExecutePrimaryKeyLookupMethod =
        typeof(QueryExecutor).GetMethod("ExecutePrimaryKeyLookup", BindingFlags.NonPublic | BindingFlags.Instance)
        ?? throw new MissingMethodException(typeof(QueryExecutor).FullName, "ExecutePrimaryKeyLookup");

    private static readonly MethodInfo ExecuteIndexSeekMethod =
        typeof(QueryExecutor).GetMethod("ExecuteIndexSeek", BindingFlags.NonPublic | BindingFlags.Instance)
        ?? throw new MissingMethodException(typeof(QueryExecutor).FullName, "ExecuteIndexSeek");

    public static IEnumerable<BsonDocument> ExecuteIndexScan(QueryExecutor executor, QueryExecutionPlan executionPlan)
    {
        return ExecuteIndexScan<BsonDocument>(executor, executionPlan);
    }

    public static IEnumerable<T> ExecuteIndexScan<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors
            | DynamicallyAccessedMemberTypes.PublicProperties
            | DynamicallyAccessedMemberTypes.PublicFields
            | DynamicallyAccessedMemberTypes.PublicMethods)] T>(
        QueryExecutor executor,
        QueryExecutionPlan executionPlan)
        where T : class
    {
        return Invoke(() => (IEnumerable<T>)ExecuteIndexScanMethod
            .MakeGenericMethod(typeof(T))
            .Invoke(executor, new object[] { executionPlan })!)!;
    }

    public static IEnumerable<T> ExecutePrimaryKeyLookup<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors
            | DynamicallyAccessedMemberTypes.PublicProperties
            | DynamicallyAccessedMemberTypes.PublicFields
            | DynamicallyAccessedMemberTypes.PublicMethods)] T>(
        QueryExecutor executor,
        QueryExecutionPlan executionPlan)
        where T : class
    {
        return Invoke(() => (IEnumerable<T>)ExecutePrimaryKeyLookupMethod
            .MakeGenericMethod(typeof(T))
            .Invoke(executor, new object[] { executionPlan })!)!;
    }

    public static IEnumerable<T> ExecuteIndexSeek<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors
            | DynamicallyAccessedMemberTypes.PublicProperties
            | DynamicallyAccessedMemberTypes.PublicFields
            | DynamicallyAccessedMemberTypes.PublicMethods)] T>(
        QueryExecutor executor,
        QueryExecutionPlan executionPlan)
        where T : class
    {
        return Invoke(() => (IEnumerable<T>)ExecuteIndexSeekMethod
            .MakeGenericMethod(typeof(T))
            .Invoke(executor, new object[] { executionPlan })!)!;
    }

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
