using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using TinyDb.Core;
using TinyDb.Tests.TestEntities;
using TinyDb.IdGeneration;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Core;

/// <summary>
/// 事务ACID属性验证测试
/// 确保事务的原子性、一致性、隔离性、持久性
/// </summary>
[NotInParallel]
public class TransactionACIDTests
{
    private string _testFile = null!;
    private TinyDbEngine _engine = null!;

    [Before(Test)]
    public void Setup()
    {
        // 重置所有ID序列，避免测试间的数据污染
        IdentitySequences.ResetAll();

        _testFile = Path.GetTempFileName();
        _engine = new TinyDbEngine(_testFile);
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

    /// <summary>
    /// 测试事务的原子性 (Atomicity) - 要么全部成功，要么全部失败
    /// </summary>
    [Test]
    public async Task Transaction_ShouldMaintainAtomicity_OnCommit()
    {
        // Arrange
        var collection = _engine.GetCollection<UserWithIntId>();
        var users = new[]
        {
            new UserWithIntId { Name = "User1", Age = 25 },
            new UserWithIntId { Name = "User2", Age = 30 },
            new UserWithIntId { Name = "User3", Age = 35 }
        };

        // Act - 在事务中执行多个操作
        using var transaction = _engine.BeginTransaction();

        var insertedIds = new List<int>();
        foreach (var user in users)
        {
            var id = collection.Insert(user);
            insertedIds.Add(user.Id);
        }

        // 提交事务
        transaction.Commit();

        // Assert - 验证所有操作都已持久化
        await Assert.That(insertedIds).Count().IsEqualTo(3);
        await Assert.That(insertedIds.All(id => id > 0)).IsTrue();

        // 验证数据确实被保存
        foreach (var userId in insertedIds)
        {
            var foundUser = collection.FindById(userId);
            await Assert.That(foundUser).IsNotNull();
            await Assert.That(users.Any(u => u.Id == userId)).IsTrue();
        }

        // 验证集合中的总记录数
        var allUsers = collection.FindAll().ToList();
        await Assert.That(allUsers).Count().IsEqualTo(3);
    }

    /// <summary>
    /// 测试事务的原子性 - 回滚时所有操作都被撤销
    /// </summary>
    [Test]
    public async Task Transaction_ShouldMaintainAtomicity_OnRollback()
    {
        // Arrange
        var collection = _engine.GetCollection<UserWithIntId>();
        var initialCount = collection.FindAll().Count();

        // Act - 在事务中执行操作但回滚
        using var transaction = _engine.BeginTransaction();

        var users = new[]
        {
            new UserWithIntId { Name = "User1", Age = 25 },
            new UserWithIntId { Name = "User2", Age = 30 }
        };

        var insertedIds = new List<int>();
        foreach (var user in users)
        {
            var id = collection.Insert(user);
            insertedIds.Add(user.Id);
        }

        // 验证在事务中数据是可见的
        var countDuringTransaction = collection.FindAll().Count();
        await Assert.That(countDuringTransaction).IsGreaterThan(initialCount);

        // 回滚事务
        transaction.Rollback();

        // Assert - 验证回滚后所有操作都被撤销
        var finalCount = collection.FindAll().Count();
        await Assert.That(finalCount).IsEqualTo(initialCount);

        // 验证插入的数据都不存在
        foreach (var userId in insertedIds)
        {
            var foundUser = collection.FindById(userId);
            await Assert.That(foundUser).IsNull();
        }
    }

    /// <summary>
    /// 测试事务的一致性 (Consistency) - 事务前后数据库保持一致状态
    /// </summary>
    [Test]
    public async Task Transaction_ShouldMaintainConsistency()
    {
        // Arrange
        var collection = _engine.GetCollection<UserWithIntId>();

        // 插入一些初始数据
        var initialUser = new UserWithIntId { Name = "Initial", Age = 20 };
        collection.Insert(initialUser);

        // Act - 在事务中执行保持一致性的操作
        using var transaction = _engine.BeginTransaction();

        var newUser = new UserWithIntId { Name = "NewUser", Age = 25 };
        var newUserId = collection.Insert(newUser);

        // 更新现有用户
        initialUser.Age = 21;
        collection.Update(initialUser);

        transaction.Commit();

        // Assert - 验证数据一致性
        var allUsers = collection.FindAll().OrderBy(u => u.Id).ToList();
        await Assert.That(allUsers).Count().IsEqualTo(2);

        // 验证更新的数据
        var updatedUser = allUsers.FirstOrDefault(u => u.Id == initialUser.Id);
        await Assert.That(updatedUser).IsNotNull();
        await Assert.That(updatedUser.Age).IsEqualTo(21);

        // 验证新插入的数据
        var insertedUser = allUsers.FirstOrDefault(u => u.Id == newUserId);
        await Assert.That(insertedUser).IsNotNull();
        await Assert.That(insertedUser.Name).IsEqualTo("NewUser");
        await Assert.That(insertedUser.Age).IsEqualTo(25);

        // 验证索引一致性（如果有的话）
        var foundByName = collection.FindOne(u => u.Name == "NewUser");
        await Assert.That(foundByName).IsNotNull();
        await Assert.That(foundByName!.Id).IsEqualTo(newUserId.ToInt32(CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// 测试事务的隔离性 (Isolation) - 并发事务之间的隔离
    /// </summary>
    [Test]
    public async Task Transaction_ShouldMaintainIsolation()
    {
        // Arrange
        var collection1 = _engine.GetCollection<UserWithIntId>();
        var collection2 = _engine.GetCollection<UserWithLongId>(); // 不同集合测试隔离

        var exceptions = new List<Exception>();
        var results = new List<string>();

        var insertedIdSource = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        var allowCommitSource = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        // Act - 并发执行事务并验证未提交数据不可见
        var task1 = Task.Run(() =>
        {
            try
            {
                using var transaction = _engine.BeginTransaction();
                var user = new UserWithIntId { Name = "Task1User", Age = 25 };
                collection1.Insert(user);
                insertedIdSource.TrySetResult(user.Id);

                allowCommitSource.Task.Wait();
                transaction.Commit();

                lock (results)
                {
                    results.Add($"Task1:Inserted_{user.Id}");
                }
            }
            catch (Exception ex)
            {
                lock (exceptions)
                {
                    exceptions.Add(ex);
                }
            }
        });

        var task2 = Task.Run(async () =>
        {
            try
            {
                var pendingId = await insertedIdSource.Task;

                using (var readTransaction = _engine.BeginTransaction())
                {
                    var visibleBeforeCommit = collection1.FindById(pendingId);
                    await Assert.That(visibleBeforeCommit).IsNull();
                    readTransaction.Commit();
                }

                using (var transaction = _engine.BeginTransaction())
                {
                    var user = new UserWithLongId { Name = "Task2User", Age = 30 };
                    var id = collection2.Insert(user);
                    transaction.Commit();
                    lock (results)
                    {
                        results.Add($"Task2:Inserted_{id}");
                    }
                }
            }
            catch (Exception ex)
            {
                lock (exceptions)
                {
                    exceptions.Add(ex);
                }
            }
            finally
            {
                allowCommitSource.TrySetResult();
            }
        });

        await Task.WhenAll(task1, task2);

        // Assert - 验证隔离性
        await Assert.That(exceptions).IsEmpty();
        await Assert.That(results).Count().IsEqualTo(2);
        await Assert.That(results.Any(r => r.StartsWith("Task1"))).IsTrue();
        await Assert.That(results.Any(r => r.StartsWith("Task2"))).IsTrue();

        // 验证最终数据完整性
        var users1 = collection1.FindAll().ToList();
        var users2 = collection2.FindAll().ToList();

        await Assert.That(users1).Count().IsEqualTo(1);
        await Assert.That(users2).Count().IsEqualTo(1);
        await Assert.That(users1[0].Name).IsEqualTo("Task1User");
        await Assert.That(users2[0].Name).IsEqualTo("Task2User");
    }

    /// <summary>
    /// 测试事务的持久性 (Durability) - 提交后数据持久存在
    /// </summary>
    [Test]
    [SuppressMessage("TUnit", "TUnit0018", Justification = "测试需模拟引擎重启，显式替换测试级资源引用。")]
    public async Task Transaction_ShouldMaintainDurability()
    {
        // Arrange
        var collection = _engine.GetCollection<UserWithIntId>();
        var user = new UserWithIntId { Name = "DurableUser", Age = 25 };

        // Act - 在事务中插入数据并提交
        using var transaction = _engine.BeginTransaction();
        var userId = collection.Insert(user);
        transaction.Commit();

        // 重新创建引擎实例（模拟重启）
        _engine.Dispose();
        using (var reopenedEngine = new TinyDbEngine(_testFile))
        {
            var newCollection = reopenedEngine.GetCollection<UserWithIntId>();

            // Assert - 验证数据持久性
            var persistedUser = newCollection.FindById(userId);
            await Assert.That(persistedUser).IsNotNull();
            await Assert.That(persistedUser.Name).IsEqualTo("DurableUser");
            await Assert.That(persistedUser.Age).IsEqualTo(25);

            // 验证所有数据都持久化了
            var allUsers = newCollection.FindAll().ToList();
            await Assert.That(allUsers).Count().IsEqualTo(1);
            await Assert.That(allUsers[0].Id == userId).IsTrue();
        }

        // 重新初始化引擎供后续使用
        _engine = new TinyDbEngine(_testFile);
    }

    /// <summary>
    /// 测试嵌套事务的回滚行为
    /// </summary>
    [Test]
    public async Task NestedTransaction_ShouldRollbackCorrectly()
    {
        // Arrange
        var collection = _engine.GetCollection<UserWithIntId>();
        var initialCount = collection.FindAll().Count();

        // Act - 测试嵌套事务
        using var outerTransaction = _engine.BeginTransaction();

        // 外层事务插入数据
        var outerUser = new UserWithIntId { Name = "OuterUser", Age = 25 };
        var outerUserId = collection.Insert(outerUser);

        // 内层事务（如果支持）
        using var innerTransaction = _engine.BeginTransaction();
        var innerUser = new UserWithIntId { Name = "InnerUser", Age = 30 };
        var innerUserId = collection.Insert(innerUser);

        // 回滚内层事务
        innerTransaction.Rollback();

        // 提交外层事务
        outerTransaction.Commit();

        // Assert - 验证嵌套事务的回滚行为
        var finalCount = collection.FindAll().Count();
        await Assert.That(finalCount).IsEqualTo(initialCount + 1); // 只有外层数据被提交

        // 验证外层数据存在
        var outerPersistedUser = collection.FindById(outerUserId);
        await Assert.That(outerPersistedUser).IsNotNull();
        await Assert.That(outerPersistedUser.Name).IsEqualTo("OuterUser");

        // 验证内层数据不存在
        var innerPersistedUser = collection.FindById(innerUserId);
        await Assert.That(innerPersistedUser).IsNull();
    }

    /// <summary>
    /// 测试事务中的异常处理和自动回滚
    /// </summary>
    [Test]
    public async Task Transaction_ShouldRollbackOnException()
    {
        // Arrange
        var collection = _engine.GetCollection<UserWithIntId>();
        var initialCount = collection.FindAll().Count();

        // Act & Assert - 测试异常时的自动回滚
        await Assert.That(() =>
        {
            using var transaction = _engine.BeginTransaction();

            // 插入一些数据
            var user1 = new UserWithIntId { Name = "User1", Age = 25 };
            collection.Insert(user1);

            var user2 = new UserWithIntId { Name = "User2", Age = 30 };
            collection.Insert(user2);

            // 模拟异常
            throw new InvalidOperationException("模拟异常");

        }).Throws<InvalidOperationException>();

        // 验证异常后数据被回滚
        var finalCount = collection.FindAll().Count();
        await Assert.That(finalCount).IsEqualTo(initialCount);

        // 验证没有数据被插入
        var user1Found = collection.FindOne(u => u.Name == "User1");
        var user2Found = collection.FindOne(u => u.Name == "User2");
        await Assert.That(user1Found).IsNull();
        await Assert.That(user2Found).IsNull();
    }

    /// <summary>
    /// 测试长时间运行事务的稳定性
    /// </summary>
    [Test]
    public async Task LongRunningTransaction_ShouldMaintainStability()
    {
        // Arrange
        var collection = _engine.GetCollection<UserWithIntId>();
        const int operationCount = 100;

        // Act - 长时间运行的事务
        using var transaction = _engine.BeginTransaction();
        var insertedIds = new List<int>();

        try
        {
            for (int i = 0; i < operationCount; i++)
            {
                var user = new UserWithIntId
                {
                    Name = $"LongRunningUser_{i}",
                    Age = 20 + (i % 50)
                };
                var id = collection.Insert(user);
                insertedIds.Add(user.Id);

                // 模拟一些处理时间
                if (i % 10 == 0)
                {
                    await Task.Delay(1);
                }
            }

            transaction.Commit();
        }
        catch (Exception)
        {
            transaction.Rollback();
            throw;
        }

        // Assert - 验证长时间运行事务的结果
        await Assert.That(insertedIds).Count().IsEqualTo(operationCount);
        await Assert.That(insertedIds.All(id => id > 0)).IsTrue();

        var finalCount = collection.FindAll().Count();
        await Assert.That(finalCount).IsGreaterThanOrEqualTo(operationCount);

        // 验证数据完整性
        for (int i = 0; i < Math.Min(10, insertedIds.Count); i++)
        {
            var user = collection.FindById(insertedIds[i]);
            await Assert.That(user).IsNotNull();
            await Assert.That(user.Name).StartsWith("LongRunningUser_");
        }
    }

    /// <summary>
    /// 测试事务中的混合操作（增删改）
    /// </summary>
    [Test]
    public async Task Transaction_ShouldHandleMixedOperations()
    {
        // Arrange
        var collection = _engine.GetCollection<UserWithIntId>();

        // 插入初始数据
        var initialUsers = new[]
        {
            new UserWithIntId { Name = "Initial1", Age = 25 },
            new UserWithIntId { Name = "Initial2", Age = 30 }
        };

        foreach (var user in initialUsers)
        {
            collection.Insert(user);
        }

        var initialIds = initialUsers.Select(u => u.Id).ToList();
        var initialCount = collection.FindAll().Count();

        // Act - 在事务中执行混合操作
        using var transaction = _engine.BeginTransaction();

        // 插入新用户
        var newUser = new UserWithIntId { Name = "NewUser", Age = 35 };
        var newUserId = collection.Insert(newUser);

        // 更新现有用户
        var userToUpdate = initialUsers[0];
        userToUpdate.Age = 26;
        userToUpdate.Name = "UpdatedUser";
        collection.Update(userToUpdate);

        // 删除一个用户
        var userToDelete = initialUsers[1];
        collection.Delete(userToDelete.Id);

        transaction.Commit();

        // Assert - 验证混合操作的结果
        var finalCount = collection.FindAll().Count();
        await Assert.That(finalCount).IsEqualTo(initialCount); // 删除一个，增加一个，总数不变

        // 验证新用户存在
        var persistedNewUser = collection.FindById(newUserId);
        await Assert.That(persistedNewUser).IsNotNull();
        await Assert.That(persistedNewUser.Name).IsEqualTo("NewUser");

        // 验证更新的用户
        var persistedUpdatedUser = collection.FindById(userToUpdate.Id);
        await Assert.That(persistedUpdatedUser).IsNotNull();
        await Assert.That(persistedUpdatedUser.Name).IsEqualTo("UpdatedUser");
        await Assert.That(persistedUpdatedUser.Age).IsEqualTo(26);

        // 验证删除的用户不存在
        var persistedDeletedUser = collection.FindById(userToDelete.Id);
        await Assert.That(persistedDeletedUser).IsNull();
    }
}
