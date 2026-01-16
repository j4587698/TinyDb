using TinyDb.Core;
using TinyDb.Query;
using TinyDb.Attributes;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Query;

public class QueryComparisonTests : IDisposable
{
    private readonly string _testDbPath;
    private readonly TinyDbEngine _engine;
    private readonly QueryExecutor _executor;

    public QueryComparisonTests()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"query_comp_{Guid.NewGuid():N}.db");
        _engine = new TinyDbEngine(_testDbPath);
        _executor = new QueryExecutor(_engine);
    }

    public void Dispose()
    {
        _engine.Dispose();
        try { if (File.Exists(_testDbPath)) File.Delete(_testDbPath); } catch { }
    }

    [Entity("MixedTypes")]
    public class MixedTypeEntity
    {
        public int Id { get; set; }
        public object? Value { get; set; }
    }

    [Test]
    public async Task Compare_Int_And_Double_Should_Work()
    {
        var collection = _engine.GetCollection<MixedTypeEntity>();
        collection.Insert(new MixedTypeEntity { Id = 1, Value = 10 });      // int
        collection.Insert(new MixedTypeEntity { Id = 2, Value = 10.5 });    // double

        // Query: Value > 10.0 (double)
        var results = _executor.Execute<MixedTypeEntity>("MixedTypes", x => (double)x.Value! > 10.0).ToList();
        // This relies on LINQ expression tree. The cast (double)x.Value might fail if x.Value is int in C# runtime, 
        // but QueryExecutor evaluates this.
        // Wait, QueryExecutor `EvaluateExpressionValue` gets the value from entity.
        // If entity.Value is `int(10)`, and expression is `(double)x.Value > 10.0`.
        // The parser might treat `(double)x.Value` as a Convert operation?
        // `ExpressionParser` handles `UnaryExpression` `Convert`.
        // `ParseExpression` calls `ParseUnaryExpression`.
        // `ParseUnaryExpression` for `Convert` returns `ParseExpression(operand)`.
        // So `(double)x.Value` becomes just property access `x.Value`.
        // Then `EvaluateBinaryExpression` calls `Compare(left, right)`.
        // Left is `int(10)`, Right is `double(10.0)`.
        // `Compare` checks `IsNumericType`. Both are.
        // `leftDouble` = 10.0. `rightDouble` = 10.0. Compare -> 0.
        // 10 > 10.0 -> False.
        
        // Value 10.5: `double(10.5)`. Compare(10.5, 10.0) -> 1. True.
        
        await Assert.That(results.Count).IsEqualTo(1);
        await Assert.That(results[0].Id).IsEqualTo(2);
    }

    [Test]
    public async Task Compare_String_And_Number_Should_Fallback_To_String()
    {
        var collection = _engine.GetCollection<MixedTypeEntity>();
        collection.Insert(new MixedTypeEntity { Id = 1, Value = "10" });
        collection.Insert(new MixedTypeEntity { Id = 2, Value = "2" });

        // Query: Value > "10" (String comparison)
        // "2" > "10" is True lexicographically?
        // '2' (50) > '1' (49). Yes.
        
        var results = _executor.Execute<MixedTypeEntity>("MixedTypes", x => (string)x.Value! == "10").ToList();
        await Assert.That(results.Count).IsEqualTo(1);
        await Assert.That(results[0].Id).IsEqualTo(1);
    }

    [Test]
    public async Task Compare_Decimal_Should_Work()
    {
        var collection = _engine.GetCollection<MixedTypeEntity>();
        collection.Insert(new MixedTypeEntity { Id = 1, Value = 100m });
        
        // Query: Value == 100.0 (double)
        // Compare(decimal(100), double(100.0))
        // IsNumericType -> True.
        // left is decimal.
        // leftDecimal = 100. rightDecimal = 100.
        // Compare -> 0.
        
        var results = _executor.Execute<MixedTypeEntity>("MixedTypes", x => (decimal)x.Value! == 100m).ToList();
        await Assert.That(results.Count).IsEqualTo(1);
    }
}
