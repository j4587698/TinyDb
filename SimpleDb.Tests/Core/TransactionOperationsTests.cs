using SimpleDb.Core;
using SimpleDb.Collections;
using SimpleDb.Bson;
using SimpleDb.Attributes;
using Xunit;

namespace SimpleDb.Tests.Core;

/// <summary>
/// 事务操作测试
/// </summary>
public class TransactionOperationsTests : IDisposable
{
    private readonly SimpleDbEngine _engine;
    private readonly ILiteCollection<TestUser> _users;

    public TransactionOperationsTests()
    {
        // 创建临时数据库
        var testDbFile = Path.GetTempFileName();
        var options = new SimpleDbOptions
        {
            DatabaseName = "TransactionOperationsTestDb",
            PageSize = 4096,
            CacheSize = 100,
            MaxTransactions = 10,
            TransactionTimeout = TimeSpan.FromSeconds(30)
        };

        _engine = new SimpleDbEngine(testDbFile, options);
        _users = _engine.GetCollection<TestUser>("users");
    }

    public void Dispose()
    {
        _engine?.Dispose();
    }

    [Fact]
    public void TransactionInsert_ShouldRollbackOnFailure()
    {
        // Arrange
        var initialCount = _users.Count();

        // Act
        using (var transaction = _engine.BeginTransaction())
        {
            // 插入一些数据
            _users.Insert(new TestUser { Name = "User1", Age = 25 });
            _users.Insert(new TestUser { Name = "User2", Age = 30 });

            // 验证数据已插入（在事务内）
            Assert.Equal(initialCount + 2, _users.Count());

            // 故意不提交，让事务自动回滚
        }

        // Assert - 事务回滚后，数据应该恢复
        Assert.Equal(initialCount, _users.Count());
    }

    [Fact]
    public void TransactionInsert_ShouldCommitOnSuccess()
    {
        // Arrange
        var initialCount = _users.Count();

        // Act
        using (var transaction = _engine.BeginTransaction())
        {
            // 插入一些数据
            _users.Insert(new TestUser { Name = "User1", Age = 25 });
            _users.Insert(new TestUser { Name = "User2", Age = 30 });

            // 提交事务
            transaction.Commit();
        }

        // Assert - 事务提交后，数据应该保留
        Assert.Equal(initialCount + 2, _users.Count());
    }

    [Fact]
    public void TransactionUpdate_ShouldRollbackOnFailure()
    {
        // Arrange
        var user = new TestUser { Name = "TestUser", Age = 25 };
        _users.Insert(user);
        var originalAge = user.Age;

        // Act
        using (var transaction = _engine.BeginTransaction())
        {
            // 更新数据
            user.Age = 30;
            _users.Update(user);

            // 验证数据已更新（在事务内）
            var updatedUser = _users.FindById(user.Id);
            Assert.Equal(30, updatedUser?.Age);

            // 故意不提交，让事务自动回滚
        }

        // Assert - 事务回滚后，数据应该恢复
        var restoredUser = _users.FindById(user.Id);
        Assert.Equal(originalAge, restoredUser?.Age);

        // Cleanup
        _users.Delete(user.Id);
    }

    [Fact]
    public void TransactionUpdate_ShouldCommitOnSuccess()
    {
        // Arrange
        var user = new TestUser { Name = "TestUser", Age = 25 };
        _users.Insert(user);

        // Act
        using (var transaction = _engine.BeginTransaction())
        {
            // 更新数据
            user.Age = 30;
            _users.Update(user);

            // 提交事务
            transaction.Commit();
        }

        // Assert - 事务提交后，数据应该保留
        var updatedUser = _users.FindById(user.Id);
        Assert.Equal(30, updatedUser?.Age);

        // Cleanup
        _users.Delete(user.Id);
    }

    [Fact]
    public void TransactionDelete_ShouldRollbackOnFailure()
    {
        // Arrange
        var user = new TestUser { Name = "TestUser", Age = 25 };
        _users.Insert(user);
        var userId = user.Id;

        // Act
        using (var transaction = _engine.BeginTransaction())
        {
            // 删除数据
            _users.Delete(userId);

            // 验证数据已删除（在事务内）
            var deletedUser = _users.FindById(userId);
            Assert.Null(deletedUser);

            // 故意不提交，让事务自动回滚
        }

        // Assert - 事务回滚后，数据应该恢复
        var restoredUser = _users.FindById(userId);
        Assert.NotNull(restoredUser);
        Assert.Equal("TestUser", restoredUser.Name);

        // Cleanup
        _users.Delete(userId);
    }

    [Fact]
    public void TransactionDelete_ShouldCommitOnSuccess()
    {
        // Arrange
        var user = new TestUser { Name = "TestUser", Age = 25 };
        _users.Insert(user);
        var userId = user.Id;

        // Act
        using (var transaction = _engine.BeginTransaction())
        {
            // 删除数据
            _users.Delete(userId);

            // 提交事务
            transaction.Commit();
        }

        // Assert - 事务提交后，数据应该永久删除
        var deletedUser = _users.FindById(userId);
        Assert.Null(deletedUser);
    }

    [Fact]
    public void TransactionWithSavepoint_ShouldRollbackToSavepoint()
    {
        // Arrange
        var initialCount = _users.Count();

        // Act
        using (var transaction = _engine.BeginTransaction())
        {
            // 插入第一个用户
            _users.Insert(new TestUser { Name = "User1", Age = 25 });
            var savepointId = transaction.CreateSavepoint("after_user1");

            // 插入更多用户
            _users.Insert(new TestUser { Name = "User2", Age = 30 });
            _users.Insert(new TestUser { Name = "User3", Age = 35 });

            // 验证当前状态
            Assert.Equal(initialCount + 3, _users.Count());

            // 回滚到保存点
            transaction.RollbackToSavepoint(savepointId);

            // 验证只保留了第一个用户
            Assert.Equal(initialCount + 1, _users.Count());

            // 提交事务
            transaction.Commit();
        }

        // Assert - 最终应该只有一个用户
        Assert.Equal(initialCount + 1, _users.Count());

        // Cleanup
        var remainingUsers = _users.FindAll().ToList();
        foreach (var user in remainingUsers.Skip(initialCount))
        {
            _users.Delete(user.Id);
        }
    }

    [Fact]
    public void NestedTransactions_ShouldWorkCorrectly()
    {
        // Arrange
        var initialCount = _users.Count();

        // Act
        using (var outerTransaction = _engine.BeginTransaction())
        {
            // 外层事务插入数据
            _users.Insert(new TestUser { Name = "OuterUser", Age = 25 });

            using (var innerTransaction = _engine.BeginTransaction())
            {
                // 内层事务插入更多数据
                _users.Insert(new TestUser { Name = "InnerUser", Age = 30 });

                // 验证内层事务状态
                Assert.Equal(initialCount + 2, _users.Count());

                // 提交内层事务
                innerTransaction.Commit();
            }

            // 验证外层事务状态
            Assert.Equal(initialCount + 2, _users.Count());

            // 提交外层事务
            outerTransaction.Commit();
        }

        // Assert - 所有数据都应该保留
        Assert.Equal(initialCount + 2, _users.Count());

        // Cleanup
        var newUsers = _users.FindAll().ToList().Skip(initialCount);
        foreach (var user in newUsers)
        {
            _users.Delete(user.Id);
        }
    }

    [Fact]
    public void ConcurrentTransactions_ShouldNotInterfere()
    {
        // Arrange
        var initialCount = _users.Count();

        // Act
        var task1 = Task.Run(() =>
        {
            using var transaction = _engine.BeginTransaction();
            _users.Insert(new TestUser { Name = "Task1User", Age = 25 });
            Thread.Sleep(100); // 模拟耗时操作
            transaction.Commit();
        });

        var task2 = Task.Run(() =>
        {
            using var transaction = _engine.BeginTransaction();
            _users.Insert(new TestUser { Name = "Task2User", Age = 30 });
            Thread.Sleep(50); // 模拟耗时操作
            transaction.Commit();
        });

        Task.WaitAll(task1, task2);

        // Assert - 两个事务都应该成功
        Assert.Equal(initialCount + 2, _users.Count());

        // Cleanup
        var task1User = _users.FindOne(u => u.Name == "Task1User");
        var task2User = _users.FindOne(u => u.Name == "Task2User");
        if (task1User != null) _users.Delete(task1User.Id);
        if (task2User != null) _users.Delete(task2User.Id);
    }

    [Fact]
    public void TransactionStatistics_ShouldProvideAccurateInformation()
    {
        // Act
        using (var transaction = _engine.BeginTransaction())
        {
            var stats = _engine.GetTransactionStatistics();

            // Assert
            Assert.Equal(1, stats.ActiveTransactionCount);
            Assert.True(stats.TotalOperations >= 0);
            Assert.True(stats.AverageTransactionAge >= 0);
        }

        // 事务结束后，统计应该更新
        var finalStats = _engine.GetTransactionStatistics();
        Assert.Equal(0, finalStats.ActiveTransactionCount);
    }

    [Entity("users")]
    public class TestUser
    {
        public ObjectId Id { get; set; } = ObjectId.NewObjectId();
        public string Name { get; set; } = "";
        public int Age { get; set; }
        public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    }
}