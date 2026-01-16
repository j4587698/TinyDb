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
        var results = _engine.GetCollection<UserWithIntId>().Find(u => u.Name.EndsWith("1")).ToList();

        // Assert
        // User_1, User_11, User_21, ..., User_91 (10 values)
        await Assert.That(results).Count().IsEqualTo(10);
    }
}
