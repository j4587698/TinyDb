using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using TinyDb.Query;
using TinyDb.Core;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Query;

public class QueryProviderCoverageTests : IDisposable
{
    private readonly TinyDbEngine _engine;
    private readonly string _dbPath;

    public QueryProviderCoverageTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"qp_test_{Guid.NewGuid()}.db");
        _engine = new TinyDbEngine(_dbPath);
    }

    public void Dispose()
    {
        _engine.Dispose();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
    }

    [Test]
    public async Task TypeSystem_GetElementType_ShouldWork()
    {
        // Arrays
        await Assert.That(TypeSystem.GetElementType(typeof(int[]))).IsEqualTo(typeof(int));
        await Assert.That(TypeSystem.GetElementType(typeof(string[]))).IsEqualTo(typeof(string));
        
        // Lists
        await Assert.That(TypeSystem.GetElementType(typeof(List<int>))).IsEqualTo(typeof(int));
        
        // IEnumerable
        await Assert.That(TypeSystem.GetElementType(typeof(IEnumerable<string>))).IsEqualTo(typeof(string));
        
        // String
        await Assert.That(TypeSystem.GetElementType(typeof(string))).IsEqualTo(typeof(string));
        
        // Not Enumerable
        await Assert.That(TypeSystem.GetElementType(typeof(int))).IsEqualTo(typeof(int));
        
        // Custom IEnumerable
        await Assert.That(TypeSystem.GetElementType(typeof(CustomEnumerable))).IsEqualTo(typeof(int));
    }

    private class CustomEnumerable : IEnumerable<int>
    {
        public IEnumerator<int> GetEnumerator() => Enumerable.Empty<int>().GetEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }

    [Test]
    public async Task QueryProvider_CreateQuery_ShouldCreateQueryable()
    {
        // Construct Queryable manually to access Provider directly if needed
        var executor = new QueryExecutor(_engine);
        var q = new Queryable<TestEntity>(executor, "test");
        var provider = q.Provider;
        
        // CreateQuery<TElement>
        var expr = Expression.Constant(q);
        var newQ = provider.CreateQuery<TestEntity>(expr);
        await Assert.That(newQ).IsNotNull();
        await Assert.That(newQ).IsTypeOf<Queryable<TestEntity, TestEntity>>();
        
        // CreateQuery (non-generic)
        var newQObj = provider.CreateQuery(expr);
        await Assert.That(newQObj).IsNotNull();
        await Assert.That(newQObj).IsTypeOf<Queryable<TestEntity, TestEntity>>();
    }
    
    [Test]
    public async Task QueryProvider_Execute_ShouldWork()
    {
        var collection = _engine.GetCollection<TestEntity>("test");
        collection.Insert(new TestEntity { Id = 1, Name = "A" });
        
        var q = collection.Query();
        // Execute<TResult> -> e.g. Count()
        // Expression for Count()
        var call = Expression.Call(
            typeof(System.Linq.Queryable),
            "Count",
            new[] { typeof(TestEntity) },
            q.Expression
        );
        
        var count = q.Provider.Execute<int>(call);
        await Assert.That(count).IsEqualTo(1);
        
        // Execute (non-generic)
        var countObj = q.Provider.Execute(call);
        await Assert.That(countObj).IsEqualTo(1);
    }

    public class TestEntity
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
    }
}