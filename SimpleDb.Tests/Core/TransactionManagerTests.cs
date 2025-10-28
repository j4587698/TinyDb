using SimpleDb.Core;
using SimpleDb.Bson;
using Xunit;

namespace SimpleDb.Tests.Core;

/// <summary>
/// 事务管理器测试
/// </summary>
public class TransactionManagerTests : IDisposable
{
    private readonly SimpleDbEngine _engine;
    private readonly TransactionManager _manager;

    public TransactionManagerTests()
    {
        // 创建临时数据库
        var testDbFile = Path.GetTempFileName();
        var options = new SimpleDbOptions
        {
            DatabaseName = "TransactionTestDb",
            PageSize = 4096,
            CacheSize = 100,
            MaxTransactions = 10,
            TransactionTimeout = TimeSpan.FromSeconds(30)
        };

        _engine = new SimpleDbEngine(testDbFile, options);
        _manager = new TransactionManager(_engine, options.MaxTransactions, options.TransactionTimeout);
    }

    public void Dispose()
    {
        _manager?.Dispose();
        _engine?.Dispose();
    }

    [Fact]
    public void TransactionManager_Constructor_ShouldInitializeCorrectly()
    {
        // Assert
        Assert.Equal(0, _manager.ActiveTransactionCount);
        Assert.Equal(10, _manager.MaxTransactions);
        Assert.Equal(TimeSpan.FromSeconds(30), _manager.TransactionTimeout);
    }

    [Fact]
    public void BeginTransaction_ShouldCreateNewTransaction()
    {
        // Act
        using var transaction = _manager.BeginTransaction();

        // Assert
        Assert.NotNull(transaction);
        Assert.Equal(TransactionState.Active, transaction.State);
        Assert.Equal(1, _manager.ActiveTransactionCount);
    }

    [Fact]
    public void BeginTransaction_ShouldRespectMaxTransactionLimit()
    {
        // Arrange
        var transactions = new List<ITransaction>();

        // Act - 创建最大数量的事务
        for (int i = 0; i < 10; i++)
        {
            transactions.Add(_manager.BeginTransaction());
        }

        // Assert - 达到最大数量
        Assert.Equal(10, _manager.ActiveTransactionCount);

        // Act & Assert - 超过最大数量应该抛出异常
        Assert.Throws<InvalidOperationException>(() => _manager.BeginTransaction());

        // Cleanup
        foreach (var transaction in transactions)
        {
            transaction.Dispose();
        }
    }

    [Fact]
    public void CommitTransaction_ShouldCommitSuccessfully()
    {
        // Arrange
        using var transaction = _manager.BeginTransaction();

        // Act
        transaction.Commit();

        // Assert
        Assert.Equal(TransactionState.Committed, transaction.State);
        Assert.Equal(0, _manager.ActiveTransactionCount);
    }

    [Fact]
    public void RollbackTransaction_ShouldRollbackSuccessfully()
    {
        // Arrange
        using var transaction = _manager.BeginTransaction();

        // Act
        transaction.Rollback();

        // Assert
        Assert.Equal(TransactionState.RolledBack, transaction.State);
        Assert.Equal(0, _manager.ActiveTransactionCount);
    }

    [Fact]
    public void TransactionSavepoint_ShouldWorkCorrectly()
    {
        // Arrange
        using var transaction = _manager.BeginTransaction();

        // Act - 创建保存点
        var savepointId = transaction.CreateSavepoint("test_savepoint");

        // Assert
        Assert.NotEqual(Guid.Empty, savepointId);

        // Act - 回滚到保存点
        transaction.RollbackToSavepoint(savepointId);

        // Assert - 事务仍然活动
        Assert.Equal(TransactionState.Active, transaction.State);

        // Act - 释放保存点
        transaction.ReleaseSavepoint(savepointId);

        // Assert - 事务仍然活动
        Assert.Equal(TransactionState.Active, transaction.State);
    }

    [Fact]
    public void TransactionDispose_ShouldAutoRollback()
    {
        // Arrange
        var transaction = _manager.BeginTransaction();
        Assert.Equal(1, _manager.ActiveTransactionCount);

        // Act
        transaction.Dispose();

        // Assert
        Assert.Equal(0, _manager.ActiveTransactionCount);
    }

    [Fact]
    public void GetStatistics_ShouldReturnCorrectInformation()
    {
        // Arrange
        using var transaction1 = _manager.BeginTransaction();
        using var transaction2 = _manager.BeginTransaction();

        // Act
        var stats = _manager.GetStatistics();

        // Assert
        Assert.Equal(2, stats.ActiveTransactionCount);
        Assert.Equal(10, stats.MaxTransactions);
        Assert.Equal(TimeSpan.FromSeconds(30), stats.TransactionTimeout);
    }

    [Theory]
    [InlineData(TransactionState.Active, true)]
    [InlineData(TransactionState.Committed, false)]
    [InlineData(TransactionState.RolledBack, false)]
    [InlineData(TransactionState.Failed, false)]
    public void TransactionStateValidation_ShouldWorkCorrectly(TransactionState state, bool canOperate)
    {
        // Arrange - 这个测试需要访问内部状态，我们通过操作来验证
        using var transaction = _manager.BeginTransaction();

        if (state != TransactionState.Active)
        {
            // 通过提交或回滚来改变状态
            if (state == TransactionState.Committed)
                transaction.Commit();
            else if (state == TransactionState.RolledBack)
                transaction.Rollback();
        }

        // Act & Assert
        if (canOperate)
        {
            // 应该能够创建保存点
            Assert.NotEqual(Guid.Empty, transaction.CreateSavepoint("test"));
        }
        else
        {
            // 不应该能够进行操作
            Assert.Throws<InvalidOperationException>(() => transaction.CreateSavepoint("test"));
        }
    }

    [Fact]
    public void MultipleConcurrentTransactions_ShouldWorkCorrectly()
    {
        // Arrange
        var transactions = new List<ITransaction>();

        // Act - 创建多个并发事务
        for (int i = 0; i < 5; i++)
        {
            transactions.Add(_manager.BeginTransaction());
        }

        // Assert
        Assert.Equal(5, _manager.ActiveTransactionCount);

        // 提交一部分事务
        for (int i = 0; i < 2; i++)
        {
            transactions[i].Commit();
        }
        Assert.Equal(3, _manager.ActiveTransactionCount);

        // 回滚剩余事务
        for (int i = 2; i < 5; i++)
        {
            transactions[i].Rollback();
        }
        Assert.Equal(0, _manager.ActiveTransactionCount);
    }
}