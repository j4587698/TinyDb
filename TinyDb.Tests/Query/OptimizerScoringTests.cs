using System.Reflection;
using System.Linq.Expressions;
using TinyDb.Core;
using TinyDb.Query;
using TinyDb.Index;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Query;

public class OptimizerScoringTests
{
    [Test]
    public async Task CalculateIndexMatchScore_Should_Score_Correctly()
    {
        var optimizer = new QueryOptimizer(new TinyDbEngine(Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString())));
        var method = typeof(QueryOptimizer).GetMethod("CalculateIndexMatchScore", BindingFlags.NonPublic | BindingFlags.Static);
        
        var queryExpr = new TinyDb.Query.BinaryExpression(System.Linq.Expressions.ExpressionType.Equal, 
            new TinyDb.Query.MemberExpression("Name"), 
            new TinyDb.Query.ConstantExpression("Test"));
            
        var indexStat = new IndexStatistics { Name = "idx", Fields = new[] { "Name" }, IsUnique = true };
        
        var score = (int)method!.Invoke(null, new object[] { queryExpr, indexStat })!;
        await Assert.That(score).IsEqualTo(15); // 10 + 5
        
        var indexStat2 = new IndexStatistics { Name = "idx2", Fields = new[] { "Age" }, IsUnique = false };
        var score2 = (int)method!.Invoke(null, new object[] { queryExpr, indexStat2 })!;
        await Assert.That(score2).IsEqualTo(0);
    }

    [Test]
    public async Task CalculateIndexMatchScore_Composite_Should_Score_Correctly()
    {
        var method = typeof(QueryOptimizer).GetMethod("CalculateIndexMatchScore", BindingFlags.NonPublic | BindingFlags.Static);
        
        // Name == "A" && Age == 10
        var queryExpr = new TinyDb.Query.BinaryExpression(System.Linq.Expressions.ExpressionType.AndAlso,
            new TinyDb.Query.BinaryExpression(System.Linq.Expressions.ExpressionType.Equal, new TinyDb.Query.MemberExpression("Name"), new TinyDb.Query.ConstantExpression("A")),
            new TinyDb.Query.BinaryExpression(System.Linq.Expressions.ExpressionType.Equal, new TinyDb.Query.MemberExpression("Age"), new TinyDb.Query.ConstantExpression(10)));
            
        var indexStat = new IndexStatistics { Name = "idx", Fields = new[] { "Name", "Age" }, IsUnique = false };
        
        var score = (int)method!.Invoke(null, new object[] { queryExpr, indexStat })!;
        // Prefix match: Name (10) + Age (10) + matchedFields * 2 (4) = 24
        await Assert.That(score).IsEqualTo(24);
    }
}