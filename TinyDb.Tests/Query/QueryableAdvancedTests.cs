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
public class QueryableAdvancedTests
{
    private string _testFile = null!;
    private TinyDbEngine _engine = null!;

    [Before(Test)]
    public void Setup()
    {
        _testFile = Path.Combine(Path.GetTempPath(), $"queryable_adv_test_{Guid.NewGuid():N}.db");
        _engine = new TinyDbEngine(_testFile);
        
        var col = _engine.GetCollection<UserWithIntId>();
        for (int i = 1; i <= 20; i++)
        {
            col.Insert(new UserWithIntId { Id = i, Name = $"User_{i}", Age = 20 + i });
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
    public async Task Queryable_SkipTake_ShouldWork()
    {
        var col = _engine.GetCollection<UserWithIntId>();
        var results = col.Query().Skip(5).Take(5).ToList();

        await Assert.That(results).Count().IsEqualTo(5);
        // Verify all returned IDs are within the inserted range [1, 20]
        foreach (var user in results)
        {
            await Assert.That(user.Id).IsGreaterThanOrEqualTo(1);
            await Assert.That(user.Id).IsLessThanOrEqualTo(20);
        }
    }

    [Test]
    public async Task Queryable_First_ShouldWork()
    {
        var col = _engine.GetCollection<UserWithIntId>();
        var user = col.Query().Where(u => u.Id == 10).First();

        await Assert.That(user.Id).IsEqualTo(10);
        await Assert.That(user.Name).IsEqualTo("User_10");
    }

    [Test]
    public async Task Queryable_FirstOrDefault_ShouldWork()
    {
        var col = _engine.GetCollection<UserWithIntId>();
        var user = col.Query().Where(u => u.Id == 99).FirstOrDefault();

        await Assert.That(user).IsNull();
    }

    [Test]
    public async Task Queryable_Any_ShouldWork()
    {
        var col = _engine.GetCollection<UserWithIntId>();
        var hasAny = col.Query().Where(u => u.Age > 30).Any();
        var hasNone = col.Query().Where(u => u.Age > 100).Any();

        await Assert.That(hasAny).IsTrue();
        await Assert.That(hasNone).IsFalse();
    }

    [Test]
    public async Task Queryable_Count_ShouldWork()
    {
        var col = _engine.GetCollection<UserWithIntId>();
        var count = col.Query().Where(u => u.Id <= 10).Count();
        var longCount = col.Query().Where(u => u.Id <= 10).LongCount();

        await Assert.That(count).IsEqualTo(10);
        await Assert.That(longCount).IsEqualTo(10L);
    }

    [Test]
    public async Task Queryable_NestedWhere_ShouldWork()
    {
        var col = _engine.GetCollection<UserWithIntId>();
        var results = col.Query()
            .Where(u => u.Id > 10)
            .Where(u => u.Id < 15)
            .ToList();

        await Assert.That(results).Count().IsEqualTo(4); // 11, 12, 13, 14
    }
}
