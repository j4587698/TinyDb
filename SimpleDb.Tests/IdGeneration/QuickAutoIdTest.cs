using SimpleDb.Tests.TestEntities;
using SimpleDb.Core;
using SimpleDb.Demo;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace SimpleDb.Tests.IdGeneration;

/// <summary>
/// 快速自动化ID生成测试
/// </summary>
public class QuickAutoIdTest
{
    [Test]
    public async Task QuickTest_AutoIntIdGeneration()
    {
        // 测试int类型自动生成
        var dbPath = Path.GetTempFileName();
        using var engine = new SimpleDbEngine(dbPath);
        var collection = engine.GetCollection<AutoUser>();

        var user = new AutoUser { Name = "TestUser", Age = 25 };
        collection.Insert(user);

        await Assert.That(user.Id).IsGreaterThan(0);

        engine.Dispose();
        File.Delete(dbPath);
    }

    [Test]
    public async Task QuickTest_AutoGuidGeneration()
    {
        // 测试Guid类型自动生成
        var dbPath = Path.GetTempFileName();
        using var engine = new SimpleDbEngine(dbPath);
        var collection = engine.GetCollection<AutoGuidOrder>();

        var order = new AutoGuidOrder { OrderNumber = "ORD-001", Amount = 100m };
        collection.Insert(order);

        await Assert.That(order.Id).IsNotEqualTo(Guid.Empty);

        engine.Dispose();
        File.Delete(dbPath);
    }
}