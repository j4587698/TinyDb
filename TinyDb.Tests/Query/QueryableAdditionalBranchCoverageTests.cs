using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using TinyDb.Core;
using TinyDb.Query;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Query;

public class QueryableAdditionalBranchCoverageTests
{
    private sealed class Entity
    {
        public int Id { get; set; }
    }

    [Test]
    public async Task Queryable_GetEnumerator_WhenProviderExecuteReturnsNull_ShouldReturnEmptyEnumerator()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"qry_{Guid.NewGuid():N}.db");
        using var engine = new TinyDbEngine(dbPath);
        var executor = new QueryExecutor(engine);
        var queryable = new Queryable<Entity, Entity>(executor, "col", new NullEnumerableQueryProvider());

        using var enumerator = queryable.GetEnumerator();
        await Assert.That(enumerator.MoveNext()).IsFalse();

        try { if (File.Exists(dbPath)) File.Delete(dbPath); } catch { }
    }

    [Test]
    public async Task ExecuteTerminal_All_WhenPredicateEvaluatesToNonBool_ShouldReturnFalse()
    {
        var executeTerminal = typeof(QueryPipeline).GetMethod("ExecuteTerminal", BindingFlags.NonPublic | BindingFlags.Static);
        await Assert.That(executeTerminal).IsNotNull();

        var items = new[] { new Entity { Id = 1 } };
        Expression<Func<Entity, int>> nonBoolPredicate = x => x.Id;

        var allMethod = typeof(FakeTerminalMethods)
            .GetMethod(nameof(FakeTerminalMethods.All), BindingFlags.Public | BindingFlags.Static)!
            .MakeGenericMethod(typeof(Entity));

        var call = Expression.Call(allMethod, Expression.Constant(items), Expression.Quote(nonBoolPredicate));
        var result = executeTerminal!.Invoke(null, new object[] { items, call });

        await Assert.That((bool)result!).IsFalse();
    }

    private sealed class NullEnumerableQueryProvider : IQueryProvider
    {
        public IQueryable CreateQuery(Expression expression) => throw new NotSupportedException();

        public IQueryable<TElement> CreateQuery<TElement>(Expression expression) => throw new NotSupportedException();

        public object? Execute(Expression expression) => null;

        public TResult Execute<TResult>(Expression expression) => default!;
    }

    private static class FakeTerminalMethods
    {
        public static bool All<T>(IEnumerable<T> source, Expression<Func<T, int>> predicate) => throw new NotSupportedException();
    }
}
