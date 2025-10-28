using System.Linq;
using SimpleDb.Tests.TestEntities;
using SimpleDb.IdGeneration;
using SimpleDb.Attributes;
using SimpleDb.Core;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace SimpleDb.Tests.IdGeneration;

/// <summary>
/// ID生成功能测试
/// </summary>
[NotInParallel]
public class IdGenerationTests
{
    private string _testFile = null!;
    private SimpleDbEngine _engine = null!;

    [Before(Test)]
    public void Setup()
    {
        _testFile = Path.GetTempFileName();
        _engine = new SimpleDbEngine(_testFile);
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
    public async Task IdentityInt_ShouldGenerateSequentialIds()
    {
        // Arrange
        var collection = _engine.GetCollection<UserWithIntId>();
        var users = new[]
        {
            new UserWithIntId { Name = "User1", Age = 25 },
            new UserWithIntId { Name = "User2", Age = 30 },
            new UserWithIntId { Name = "User3", Age = 35 }
        };

        // Act
        var ids = users.Select(user =>
        {
            var id = collection.Insert(user);
            return user.Id;
        }).ToList();

        // Assert
        await Assert.That(ids.Count).IsEqualTo(3);
        await Assert.That(ids[0]).IsEqualTo(1);
        await Assert.That(ids[1]).IsEqualTo(2);
        await Assert.That(ids[2]).IsEqualTo(3);

        // 验证ID都是有效值
        foreach (var id in ids)
        {
            await Assert.That(id).IsGreaterThan(0);
        }
    }

    [Test]
    public async Task IdentityLong_ShouldGenerateSequentialIds()
    {
        // Arrange
        var collection = _engine.GetCollection<UserWithLongId>();
        var users = new[]
        {
            new UserWithLongId { Name = "User1", Age = 25 },
            new UserWithLongId { Name = "User2", Age = 30 },
            new UserWithLongId { Name = "User3", Age = 35 }
        };

        // Act
        var ids = users.Select(user =>
        {
            collection.Insert(user);
            return user.Id;
        }).ToList();

        // Assert
        await Assert.That(ids.Count).IsEqualTo(3);
        await Assert.That(ids[0]).IsEqualTo(1L);
        await Assert.That(ids[1]).IsEqualTo(2L);
        await Assert.That(ids[2]).IsEqualTo(3L);

        // 验证ID都是有效值
        foreach (var id in ids)
        {
            await Assert.That(id).IsGreaterThan(0);
        }
    }

    [Test]
    public async Task GuidV7_ShouldGenerateTimeOrderedGuids()
    {
        // Arrange
        var collection = _engine.GetCollection<UserWithGuidV7Id>();
        var user1 = new UserWithGuidV7Id { Name = "User1", Age = 25 };
        var user2 = new UserWithGuidV7Id { Name = "User2", Age = 30 };

        // Act
        collection.Insert(user1);
        await Task.Delay(10); // 确保时间差
        collection.Insert(user2);

        // Assert
        var guid1Parsed = Guid.TryParse(user1.Id, out var guid1);
        var guid2Parsed = Guid.TryParse(user2.Id, out var guid2);

        await Assert.That(guid1Parsed).IsTrue();
        await Assert.That(guid2Parsed).IsTrue();

        // GUID v7 应该是时间排序的
        await Assert.That(guid1).IsNotEqualTo(guid2);

        // 验证GUID格式
        await Assert.That(guid1).IsNotEqualTo(Guid.Empty);
        await Assert.That(guid2).IsNotEqualTo(Guid.Empty);
    }

    [Test]
    public async Task GuidV7_WithGuidType_ShouldGenerateGuids()
    {
        // Arrange
        var collection = _engine.GetCollection<ProductWithGuidId>();
        var product1 = new ProductWithGuidId { Name = "Product1", Price = 10.99m };
        var product2 = new ProductWithGuidId { Name = "Product2", Price = 20.99m };

        // Act
        collection.Insert(product1);
        await Task.Delay(10);
        collection.Insert(product2);

        // Assert
        await Assert.That(product1.Id).IsNotEqualTo(Guid.Empty);
        await Assert.That(product2.Id).IsNotEqualTo(Guid.Empty);
        await Assert.That(product1.Id).IsNotEqualTo(product2.Id);
    }

    [Test]
    public async Task IdGeneratorFactory_ShouldReturnCorrectGenerator()
    {
        // Act & Assert
        var intGenerator = IdGeneratorFactory.GetGenerator(IdGenerationStrategy.IdentityInt);
        await Assert.That(intGenerator).IsAssignableTo<IdentityGenerator>();
        await Assert.That(intGenerator.Supports(typeof(int))).IsTrue();

        var longGenerator = IdGeneratorFactory.GetGenerator(IdGenerationStrategy.IdentityLong);
        await Assert.That(longGenerator).IsAssignableTo<IdentityGenerator>();
        await Assert.That(longGenerator.Supports(typeof(long))).IsTrue();

        var guidV7Generator = IdGeneratorFactory.GetGenerator(IdGenerationStrategy.GuidV7);
        await Assert.That(guidV7Generator).IsAssignableTo<GuidV7Generator>();
        await Assert.That(guidV7Generator.Supports(typeof(string))).IsTrue();
        await Assert.That(guidV7Generator.Supports(typeof(Guid))).IsTrue();

        var guidV4Generator = IdGeneratorFactory.GetGenerator(IdGenerationStrategy.GuidV4);
        await Assert.That(guidV4Generator).IsAssignableTo<GuidV4Generator>();
        await Assert.That(guidV4Generator.Supports(typeof(string))).IsTrue();
        await Assert.That(guidV4Generator.Supports(typeof(Guid))).IsTrue();

        var objectIdGenerator = IdGeneratorFactory.GetGenerator(IdGenerationStrategy.ObjectId);
        await Assert.That(objectIdGenerator).IsAssignableTo<ObjectIdGenerator>();
        await Assert.That(objectIdGenerator.Supports(typeof(SimpleDb.Bson.ObjectId))).IsTrue();
    }

    [Test]
    public async Task IdGenerationHelper_ShouldDetectGenerationStrategy()
    {
        // Act & Assert
        var intStrategy = IdGenerationHelper<UserWithIntId>.GetIdGenerationStrategy();
        await Assert.That(intStrategy).IsEqualTo(IdGenerationStrategy.IdentityInt);

        var longStrategy = IdGenerationHelper<UserWithLongId>.GetIdGenerationStrategy();
        await Assert.That(longStrategy).IsEqualTo(IdGenerationStrategy.IdentityLong);

        var guidV7Strategy = IdGenerationHelper<UserWithGuidV7Id>.GetIdGenerationStrategy();
        await Assert.That(guidV7Strategy).IsEqualTo(IdGenerationStrategy.GuidV7);

        var guidProductStrategy = IdGenerationHelper<ProductWithGuidId>.GetIdGenerationStrategy();
        await Assert.That(guidProductStrategy).IsEqualTo(IdGenerationStrategy.GuidV7);
    }

    [Test]
    public async Task ShouldGenerateId_ShouldWorkCorrectly()
    {
        // Arrange
        var userWithEmptyId = new UserWithIntId { Name = "Test", Age = 25 };
        var userWithExistingId = new UserWithIntId { Id = 999, Name = "Test", Age = 25 };

        // Act & Assert
        await Assert.That(IdGenerationHelper<UserWithIntId>.ShouldGenerateId(userWithEmptyId)).IsTrue();
        await Assert.That(IdGenerationHelper<UserWithIntId>.ShouldGenerateId(userWithExistingId)).IsFalse();
    }
}
