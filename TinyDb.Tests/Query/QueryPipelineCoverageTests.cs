using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using TinyDb.Query;
using TUnit.Core;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Query;

public class QueryPipelineCoverageTests
{
    [Test]
    public async Task QueryOperationResolver_Apply_Coverage()
    {
        var data = Enumerable.Range(0, 10).ToList();
        var query = data.AsQueryable();

        // Test Take
        var exprTake = Expression.Call(
            typeof(Queryable),
            nameof(Queryable.Take),
            new Type[] { typeof(int) },
            query.Expression,
            Expression.Constant(5)
        );
        var resTake = QueryOperationResolver.Apply(exprTake, data) as IEnumerable<int>;
        await Assert.That(resTake!.Count()).IsEqualTo(5);

        // Test Skip
        var exprSkip = Expression.Call(
            typeof(Queryable),
            nameof(Queryable.Skip),
            new Type[] { typeof(int) },
            query.Expression,
            Expression.Constant(5)
        );
        var resSkip = QueryOperationResolver.Apply(exprSkip, data) as IEnumerable<int>;
        await Assert.That(resSkip!.Count()).IsEqualTo(5);
        await Assert.That(resSkip!.First()).IsEqualTo(5);

        // Test Count
        var exprCount = Expression.Call(
            typeof(Queryable),
            nameof(Queryable.Count),
            new Type[] { typeof(int) },
            query.Expression
        );
        var resCount = QueryOperationResolver.Apply(exprCount, data);
        await Assert.That(resCount).IsEqualTo(10);

        // Test LongCount
        var exprLongCount = Expression.Call(
            typeof(Queryable),
            nameof(Queryable.LongCount),
            new Type[] { typeof(int) },
            query.Expression
        );
        var resLongCount = QueryOperationResolver.Apply(exprLongCount, data);
        await Assert.That(resLongCount).IsEqualTo(10L);

        // Test Any
        var exprAny = Expression.Call(
            typeof(Queryable),
            nameof(Queryable.Any),
            new Type[] { typeof(int) },
            query.Expression
        );
        var resAny = QueryOperationResolver.Apply(exprAny, data);
        await Assert.That(resAny).IsEqualTo(true);

        // Test First
        var exprFirst = Expression.Call(
            typeof(Queryable),
            nameof(Queryable.First),
            new Type[] { typeof(int) },
            query.Expression
        );
        var resFirst = QueryOperationResolver.Apply(exprFirst, data);
        await Assert.That(resFirst).IsEqualTo(0);

        // Test FirstOrDefault
        var exprFirstOrDefault = Expression.Call(
            typeof(Queryable),
            nameof(Queryable.FirstOrDefault),
            new Type[] { typeof(int) },
            query.Expression
        );
        var resFirstOrDefault = QueryOperationResolver.Apply(exprFirstOrDefault, data);
        await Assert.That(resFirstOrDefault).IsEqualTo(0);
    }

    [Test]
    public async Task QueryOperationResolver_EdgeCases_Coverage()
    {
        var data = Enumerable.Range(0, 10).ToList();
        var query = data.AsQueryable();

        // Take(0)
        var exprTake0 = Expression.Call(
            typeof(Queryable),
            nameof(Queryable.Take),
            new Type[] { typeof(int) },
            query.Expression,
            Expression.Constant(0)
        );
        var resTake0 = QueryOperationResolver.Apply(exprTake0, data) as IEnumerable<int>;
        await Assert.That(resTake0!.Count()).IsEqualTo(0);

        // Skip(20)
        var exprSkip20 = Expression.Call(
            typeof(Queryable),
            nameof(Queryable.Skip),
            new Type[] { typeof(int) },
            query.Expression,
            Expression.Constant(20)
        );
        var resSkip20 = QueryOperationResolver.Apply(exprSkip20, data) as IEnumerable<int>;
        await Assert.That(resSkip20!.Count()).IsEqualTo(0);
        
        // First on empty
        var emptyData = new List<int>();
        var exprFirst = Expression.Call(
            typeof(Queryable),
            nameof(Queryable.First),
            new Type[] { typeof(int) },
            emptyData.AsQueryable().Expression
        );
        
        try
        {
            QueryOperationResolver.Apply(exprFirst, emptyData);
            Assert.Fail("Should throw InvalidOperationException");
        }
        catch (InvalidOperationException) { }
    }
}
