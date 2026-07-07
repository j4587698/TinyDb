using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;
using TinyDb.Query;

namespace TinyDb.Tests.Query;

public class QueryableCountExtensionsTests
{
    private static int _skipCallCount;

    [Test]
    [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "This test intentionally uses EnumerableQuery to exercise IQueryable fallback behavior in the test runtime.")]
    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "This test intentionally uses EnumerableQuery to exercise IQueryable fallback behavior in the test runtime.")]
    public async Task Count_WithUnsupportedPaginationExpression_ShouldExecuteQueryOnce()
    {
        _skipCallCount = 0;
        IQueryable<int> source = new EnumerableQuery<int>(new[] { 1, 2, 3, 4 });
        var getSkip = typeof(QueryableCountExtensionsTests).GetMethod(
            nameof(GetSkip),
            BindingFlags.NonPublic | BindingFlags.Static)!;

        var query = source.Provider.CreateQuery<int>(
            Expression.Call(
                typeof(System.Linq.Queryable),
                nameof(System.Linq.Queryable.Skip),
                new[] { typeof(int) },
                source.Expression,
                Expression.Call(getSkip)));

        var items = QueryableCountExtensions.Count(query, out var totalCount);

        await Assert.That(totalCount).IsEqualTo(3);
        await Assert.That(items.Count).IsEqualTo(3);
        await Assert.That(items[0]).IsEqualTo(2);
        await Assert.That(_skipCallCount).IsEqualTo(1);
    }

    private static int GetSkip()
    {
        _skipCallCount++;
        return 1;
    }
}
