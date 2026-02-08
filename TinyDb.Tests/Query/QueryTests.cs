using System;
using System.IO;
using System.Linq;
using TinyDb.Core;
using TinyDb.Tests.TestEntities;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Query;

[NotInParallel]
public class QueryTests
{
    private string _testFile = null!;
    private TinyDbEngine _engine = null!;

    [Before(Test)]
    public void Setup()
    {
        _testFile = Path.Combine(Path.GetTempPath(), $"query_test_{Guid.NewGuid():N}.db");
        _engine = new TinyDbEngine(_testFile);
        
        // 预填充数据
        var col = _engine.GetCollection<UserWithIntId>();
        for (int i = 1; i <= 100; i++)
        {
            col.Insert(new UserWithIntId { Id = i, Name = $"User_{i}", Age = 20 + (i % 50) });
        }
    }

    [After(Test)]
    public void Cleanup()
    {
        _engine?.Dispose();
        if (File.Exists(_testFile))
        {
            File.Delete(_testFile);
        }
    }

    [Test]
    public async Task Query_FullTableScan_ShouldWork()
    {
        // Act
        var results = _engine.GetCollection<UserWithIntId>().Find(u => u.Age > 60).ToList();

        // Assert
        // i % 50 ranges from 0 to 49. 20 + (i % 50) ranges from 20 to 69.
        // Age > 60 means i % 50 > 40. That is 41, 42, ..., 49 (9 values) per 50 items.
        // For 100 items, there are 18 such values.
        await Assert.That(results).Count().IsEqualTo(18);
        await Assert.That(results.All(u => u.Age > 60)).IsTrue();
    }

    [Test]
    public async Task Query_WithIndexScan_ShouldWork()
    {
        // Arrange
        _engine.EnsureIndex("UserWithIntId", "Age", "age_idx");

        // Act
        var results = _engine.GetCollection<UserWithIntId>().Find(u => u.Age == 25).ToList();

        // Assert
        // 20 + (i % 50) == 25 => i % 50 == 5 => i = 5, 55.
        await Assert.That(results).Count().IsEqualTo(2);
    }

    [Test]
    public async Task Query_StringFunctions_ShouldWork()
    {
        // Act
        var results = _engine.GetCollection<UserWithIntId>().Find(u => u.Name.Contains("User_10")).ToList();

        // Assert
        // User_10, User_100
        await Assert.That(results).Count().IsEqualTo(2);
    }

    [Test]
    public async Task Query_ComplexAnd_ShouldWork()
    {
        // Act
        var results = _engine.GetCollection<UserWithIntId>().Find(u => u.Age > 60 && u.Id < 50).ToList();

        // Assert
        // i % 50 > 40 and i < 50 => i = 41, 42, ..., 49 (9 values)
        await Assert.That(results).Count().IsEqualTo(9);
    }

    [Test]
    public async Task Query_StartsWith_ShouldWork()
    {
        // Act
        var results = _engine.GetCollection<UserWithIntId>().Find(u => u.Name.StartsWith("User_9")).ToList();

        // Assert
        // User_9, User_90, User_91, ..., User_99 (11 values)
        await Assert.That(results).Count().IsEqualTo(11);
    }

    [Test]
    public async Task Query_EndsWith_ShouldWork()
    {
        // Act
        var results = _engine.GetCollection<UserWithIntId>().Find(u => u.Name.EndsWith("_10")).ToList();

        // Assert
        // User_10 only. User_100 ends with "00".
        await Assert.That(results).IsNotNull();
        await Assert.That(results.Count).IsEqualTo(1);
        await Assert.That(results.All(u => u.Name.EndsWith("_10"))).IsTrue();
    }

    [Test]
    public async Task Query_PrimaryKey_With_ExtraCondition_ShouldWork()
    {
        // 这个测试专门用于验证主键查找策略是否会正确应用额外的过滤条件。
        // 之前存在一个潜在 Bug：如果优化器选择了 PrimaryKeyLookup，QueryExecutor 可能会直接返回通过 ID 查到的文档，
        // 而忽略了查询表达式中的其他条件（如 Name == "Wrong"）。
        
        var col = _engine.GetCollection<UserWithIntId>();
        var user = new UserWithIntId { Id = 9999, Name = "TargetUser", Age = 30 };
        col.Insert(user);

        // Case 1: ID 匹配，但 Name 不匹配 -> 应该返回空
        // 优化器应该提取 Id == 9999 使用 PK 索引，然后 Executor 必须校验 Name == "WrongUser"
        var resultNegative = col.Find(u => u.Id == 9999 && u.Name == "WrongUser").ToList();
        await Assert.That(resultNegative).IsEmpty();

        // Case 2: ID 匹配，且 Name 匹配 -> 应该返回结果
        var resultPositive = col.Find(u => u.Id == 9999 && u.Name == "TargetUser").ToList();
        await Assert.That(resultPositive).Count().IsEqualTo(1);
        await Assert.That(resultPositive[0].Name).IsEqualTo("TargetUser");
        
        // Case 3: ID 匹配，Age 条件 (数值比较)
        var resultAgeMismatch = col.Find(u => u.Id == 9999 && u.Age > 100).ToList();
        await Assert.That(resultAgeMismatch).IsEmpty();
        
        var resultAgeMatch = col.Find(u => u.Id == 9999 && u.Age == 30).ToList();
        await Assert.That(resultAgeMatch).Count().IsEqualTo(1);
    }

    [Test]
    public async Task Query_ChainedFunctions_ShouldWork()
    {
        // 测试链式调用：Trim -> Substring -> ToLower
        var col = _engine.GetCollection<UserWithIntId>();
        col.Insert(new UserWithIntId { Id = 8888, Name = "  CHAINED  ", Age = 20 });
        
        var results = col.Find(u => u.Name.Trim().Substring(0, 5).ToLower() == "chain").ToList();
        await Assert.That(results).Count().IsEqualTo(1);
        await Assert.That(results[0].Id).IsEqualTo(8888);
    }

    [Test]
    public async Task Query_MathAndDateTime_ShouldWork()
    {
        var col = _engine.GetCollection<UserWithIntId>();
        // Math.Abs(20 - 25) = 5
        var resultsMath = col.Find(u => Math.Abs(u.Age - 25) <= 5).ToList();
        await Assert.That(resultsMath.Count).IsGreaterThanOrEqualTo(1);

        // DateTime 属性与函数结合 (假设 UserWithIntId 没有 DateTime，我们现场造一个)
        // 实际上我们可以只测试 Math，因为 DateTime 需要实体支持。
    }
}
