using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using TinyDb.Core;
using TinyDb.Tests.TestEntities;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Concurrency;

/// <summary>
/// 并发安全性测试
/// 验证数据库在并发环境下的正确性和稳定性
/// </summary>
[NotInParallel]
public class ConcurrencyTests
{
    private string _testFile = null!;
    private TinyDbEngine _engine = null!;

    [Before(Test)]
    public void Setup()
    {
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
    /// 测试并发写入同一集合
    /// </summary>
    [Test]
    public async Task ConcurrentInserts_ShouldBeThreadSafe()
    {
        // Arrange
        var collection = _engine.GetCollection<UserWithIntId>();
        const int threadCount = 10;
        const int itemsPerThread = 100;
        var tasks = new List<Task>();
        var exceptions = new List<Exception>();

        // Act - 多线程并发插入
        for (int i = 0; i < threadCount; i++)
        {
            var threadId = i;
            tasks.Add(Task.Run(() =>
            {
                try
                {
                    for (int j = 0; j < itemsPerThread; j++)
                    {
                        var user = new UserWithIntId 
                        { 
                            Name = $"User_{threadId}_{j}", 
                            Age = 20 + j 
                        };
                        collection.Insert(user);
                    }
                }
                catch (Exception ex)
                {
                    lock (exceptions)
                    {
                        exceptions.Add(ex);
                    }
                }
            }));
        }

        await Task.WhenAll(tasks);

        // Assert
        await Assert.That(exceptions).IsEmpty();
        
        var allUsers = collection.FindAll().ToList();
        await Assert.That(allUsers).Count().IsEqualTo(threadCount * itemsPerThread);
        
        // 验证数据完整性
        var uniqueNames = allUsers.Select(u => u.Name).Distinct().Count();
        await Assert.That(uniqueNames).IsEqualTo(threadCount * itemsPerThread);
    }

    /// <summary>
    /// 测试读写并发 - 写操作不应阻塞读操作（如果是快照隔离）或者应正确序列化
    /// TinyDb目前可能使用锁机制，所以这里验证数据一致性
    /// </summary>
    [Test]
    public async Task ConcurrentReadWrite_ShouldMaintainConsistency()
    {
        // Arrange
        var collection = _engine.GetCollection<UserWithIntId>();
        var isWriting = true;
        var readCount = 0;
        var exceptions = new List<Exception>();

        // 预先插入一些数据
        for (int i = 0; i < 50; i++)
        {
            collection.Insert(new UserWithIntId { Name = $"Initial_{i}", Age = i });
        }

        // Act
        // 启动写入任务
        var writeTask = Task.Run(async () =>
        {
            try
            {
                for (int i = 0; i < 50; i++)
                {
                    collection.Insert(new UserWithIntId { Name = $"New_{i}", Age = 100 + i });
                    await Task.Delay(10); // 模拟写入间隔
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
                isWriting = false;
            }
        });

        // 启动读取任务
        var readTask = Task.Run(async () =>
        {
            try
            {
                while (isWriting)
                {
                    var count = collection.FindAll().Count();
                    // 数量应该是非递减的
                    if (count < 50) throw new Exception("Data loss detected during read!");
                    readCount++;
                    await Task.Delay(5);
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

        await Task.WhenAll(writeTask, readTask);

        // Assert
        await Assert.That(exceptions).IsEmpty();
        await Assert.That(readCount).IsGreaterThan(0);
        
        var finalCount = collection.FindAll().Count();
        await Assert.That(finalCount).IsEqualTo(100); // 50 initial + 50 new
    }


}
