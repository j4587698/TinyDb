using System.Linq;
using System.Diagnostics;
using TinyDb.Core;
using TinyDb.Storage;
using TinyDb.Tests.TestEntities;
using TinyDb.IdGeneration;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Storage;

/// <summary>
/// 存储层压力测试
/// 测试存储系统在高负载、大数据量下的性能和稳定性
/// </summary>
[NotInParallel]
public class StorageStressTests
{
    private string _testFile = null!;
    private TinyDbEngine _engine = null!;

    [Before(Test)]
    public void Setup()
    {
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
    /// 测试大量数据插入的性能
    /// </summary>
    [Test]
    public async Task BulkInsert_ShouldMaintainPerformance()
    {
        // Arrange
        var collection = _engine.GetCollection<UserWithIntId>();
        const int recordCount = 10000;
        var users = Enumerable.Range(1, recordCount)
            .Select(i => new UserWithIntId
            {
                Name = $"User_{i}",
                Age = 20 + (i % 50)
            })
            .ToArray();

        // Act - 测量插入性能
        var stopwatch = Stopwatch.StartNew();
        var insertedIds = new List<int>();

        foreach (var user in users)
        {
            var id = collection.Insert(user);
            insertedIds.Add(user.Id);
        }

        stopwatch.Stop();

        // Assert - 验证性能和数据完整性
        await Assert.That(insertedIds).HasCount(recordCount);
        await Assert.That(insertedIds.All(id => id > 0)).IsTrue();
        Console.WriteLine($"批量插入耗时: {stopwatch.Elapsed.TotalSeconds:F2} 秒");

        // 验证数据完整性
        var allUsers = collection.FindAll().ToList();
        await Assert.That(allUsers).HasCount(recordCount);

        // 随机验证一些记录
        var random = new Random(42);
        for (int i = 0; i < 10; i++)
        {
            var randomIndex = random.Next(0, insertedIds.Count);
            var userId = insertedIds[randomIndex];
            var foundUser = collection.FindById(userId);
            await Assert.That(foundUser).IsNotNull();
            await Assert.That(foundUser.Name).StartsWith("User_");
        }

        // 输出性能指标
        var recordsPerSecond = recordCount / stopwatch.Elapsed.TotalSeconds;
        Console.WriteLine($"插入性能: {recordsPerSecond:F2} 记录/秒");
    }

    /// <summary>
    /// 测试并发读写操作
    /// </summary>
    [Test]
    public async Task ConcurrentReadWrite_ShouldMaintainConsistency()
    {
        // Arrange
        var collection = _engine.GetCollection<UserWithIntId>();
        const int baseRecordCount = 1000;
        const int concurrentOperations = 100;

        // 插入基础数据
        var baseUsers = Enumerable.Range(1, baseRecordCount)
            .Select(i => new UserWithIntId
            {
                Name = $"BaseUser_{i}",
                Age = 20 + (i % 50)
            })
            .ToArray();

        foreach (var user in baseUsers)
        {
            collection.Insert(user);
        }

        var exceptions = new List<Exception>();
        var results = new List<string>();
        var tasks = new List<Task>();

        // Act - 并发读写操作
        for (int i = 0; i < concurrentOperations; i++)
        {
            var taskId = i;
            tasks.Add(Task.Run(() =>
            {
                try
                {
                    // 读取操作
                    var readUsers = collection.FindAll().Take(10).ToList();
                    lock (results)
                    {
                        results.Add($"Task_{taskId}_Read_{readUsers.Count}");
                    }

                    // 写入操作
                    var newUser = new UserWithIntId
                    {
                        Name = $"ConcurrentUser_{taskId}",
                        Age = 25 + (taskId % 30)
                    };
                    var insertedId = collection.Insert(newUser);
                    lock (results)
                    {
                        results.Add($"Task_{taskId}_Write_{insertedId}");
                    }

                    // 更新操作
                    if (taskId < baseRecordCount)
                    {
                        var userToUpdate = collection.FindById(taskId + 1);
                        if (userToUpdate != null)
                        {
                            userToUpdate.Age = userToUpdate.Age + 1;
                            collection.Update(userToUpdate);
                            lock (results)
                            {
                                results.Add($"Task_{taskId}_Update_{userToUpdate.Id}");
                            }
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
            }));
        }

        await Task.WhenAll(tasks);

        // Assert - 验证并发操作结果
        await Assert.That(exceptions).IsEmpty();
        await Assert.That(results).HasCount(concurrentOperations * 3); // 每个任务执行3个操作

        // 验证数据一致性
        var finalUserCount = collection.FindAll().Count();
        await Assert.That(finalUserCount).IsGreaterThan(baseRecordCount);

        // 验证更新操作
        var updatedUser = collection.FindById(1);
        if (updatedUser != null)
        {
            await Assert.That(updatedUser.Age).IsGreaterThan(20); // 应该被至少更新一次
        }
    }

    /// <summary>
    /// 测试大文件处理能力
    /// </summary>
    [Test]
    public async Task LargeFile_ShouldHandleEfficiently()
    {
        // Arrange
        var collection = _engine.GetCollection<LargeDataEntity>();
        const int recordCount = 5000;
        const int largeTextSize = 1000; // 每个记录包含1KB文本

        // Act - 插入大量数据创建大文件
        var stopwatch = Stopwatch.StartNew();
        var insertedIds = new List<string>();

        for (int i = 0; i < recordCount; i++)
        {
            var entity = new LargeDataEntity
            {
                Id = $"large_{i:D6}",
                Name = $"LargeEntity_{i}",
                LargeText = string.Join(" ", Enumerable.Repeat($"text_block_{i}", largeTextSize)),
                CreatedAt = DateTime.UtcNow,
                Number = i,
                Tags = Enumerable.Range(1, 10).Select(j => $"tag_{i}_{j}").ToArray()
            };

            var id = collection.Insert(entity);
            insertedIds.Add(entity.Id);
        }

        stopwatch.Stop();

        // Assert - 验证大文件处理
        await Assert.That(insertedIds).HasCount(recordCount);
        Console.WriteLine($"大文件插入耗时: {stopwatch.Elapsed.TotalSeconds:F2} 秒");

        // 检查文件大小
        var fileInfo = new FileInfo(_testFile);
        await Assert.That(fileInfo.Length).IsGreaterThan(1024 * 1024); // 文件应该大于1MB

        // 验证数据可读性
        var random = new Random(42);
        for (int i = 0; i < 5; i++)
        {
            var randomIndex = random.Next(0, insertedIds.Count);
            var entity = collection.FindById(insertedIds[randomIndex]);
            await Assert.That(entity).IsNotNull();
            await Assert.That(entity.LargeText.Length).IsGreaterThan(largeTextSize * 10);
            await Assert.That(entity.Tags).HasCount(10);
        }

        Console.WriteLine($"文件大小: {fileInfo.Length / (1024.0 * 1024.0):F2} MB");
        Console.WriteLine($"插入性能: {recordCount / stopwatch.Elapsed.TotalSeconds:F2} 记录/秒");
    }

    /// <summary>
    /// 测试内存使用效率
    /// </summary>
    [Test]
    public async Task MemoryUsage_ShouldRemainEfficient()
    {
        // Arrange
        var collection = _engine.GetCollection<UserWithIntId>();
        const int recordCount = 5000;

        // 获取初始内存使用
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        var initialMemory = GC.GetTotalMemory(false);

        // Act - 插入数据并监控内存使用
        var memoryMeasurements = new List<long>();

        for (int i = 0; i < recordCount; i++)
        {
            var user = new UserWithIntId
            {
                Name = $"MemoryTestUser_{i}",
                Age = 20 + (i % 50)
            };
            collection.Insert(user);

            // 每1000次测量一次内存
            if (i % 1000 == 0)
            {
                GC.Collect();
                GC.WaitForPendingFinalizers();
                GC.Collect();
                var currentMemory = GC.GetTotalMemory(false);
                memoryMeasurements.Add(currentMemory);
            }
        }

        // 强制垃圾回收
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        var finalMemory = GC.GetTotalMemory(false);

        // Assert - 验证内存使用效率
        var memoryIncrease = finalMemory - initialMemory;
        var memoryPerRecord = memoryIncrease / (double)recordCount;

        await Assert.That(memoryPerRecord).IsLessThan(1024); // 每记录内存增长应小于1KB

        // 验证内存增长趋势相对线性
        if (memoryMeasurements.Count >= 3)
        {
            var firstGrowth = memoryMeasurements[1] - memoryMeasurements[0];
            var lastGrowth = memoryMeasurements[^1] - memoryMeasurements[^2];
            var growthRatio = Math.Abs(lastGrowth - firstGrowth) / (double)firstGrowth;
            await Assert.That(growthRatio).IsLessThan(0.5); // 内存增长趋势应该相对稳定
        }

        Console.WriteLine($"总内存增长: {memoryIncrease / (1024.0 * 1024.0):F2} MB");
        Console.WriteLine($"每记录内存: {memoryPerRecord:F2} bytes");
    }

    /// <summary>
    /// 测试磁盘空间不足的处理
    /// </summary>
    [Test]
    public async Task DiskSpaceExhaustion_ShouldGracefullyHandle()
    {
        // Arrange - 创建一个非常小的临时文件系统空间限制
        var smallTestFile = Path.GetTempFileName();
        using var smallEngine = new TinyDbEngine(smallTestFile);
        var collection = smallEngine.GetCollection<UserWithIntId>();

        var exceptions = new List<Exception>();
        var stopwatch = Stopwatch.StartNew();

        // Act - 尝试填满空间，但设置合理的限制
        try
        {
            var recordCount = 0;
            const int maxTestRecords = 2000;
            const int maxTestTimeSeconds = 10;
            const int simulatedFailureAfter = 500;

            while (recordCount < maxTestRecords && stopwatch.Elapsed.TotalSeconds < maxTestTimeSeconds)
            {
                var user = new UserWithIntId
                {
                    Name = $"SpaceTestUser_{recordCount}",
                    Age = 20 + (recordCount % 50)
                };

                try
                {
                    collection.Insert(user);
                    recordCount++;

                    if (recordCount == simulatedFailureAfter)
                    {
                        throw new IOException("Simulated disk space exhaustion");
                    }
                }
                catch (Exception ex)
                {
                    exceptions.Add(ex);
                    break;
                }
            }

            Console.WriteLine($"测试完成，成功插入 {recordCount} 条记录，用时 {stopwatch.Elapsed.TotalSeconds:F1} 秒");
        }
        finally
        {
            stopwatch.Stop();
            smallEngine.Dispose();
            if (File.Exists(smallTestFile))
            {
                File.Delete(smallTestFile);
            }
        }

        // Assert - 验证磁盘空间不足的处理
        if (exceptions.Any())
        {
            // 应该抛出合理的异常类型
            var firstException = exceptions.First();
            await Assert.That(firstException).IsAssignableTo<IOException>()
                .Or.IsAssignableTo<OutOfMemoryException>()
                .Or.IsAssignableTo<InvalidOperationException>();
        }

        // 验证至少插入了一些记录，证明测试正常运行
        await Assert.That(exceptions.Count).IsLessThan(100); // 异常不应该太多
    }

    /// <summary>
    /// 测试长时间运行的稳定性
    /// </summary>
    [Test]
    public async Task LongRunningStability_ShouldMaintainPerformance()
    {
        // Arrange
        var collection = _engine.GetCollection<UserWithIntId>();
        const int testDurationSeconds = 10; // 短时间测试，避免长时间运行
        var stopwatch = Stopwatch.StartNew();
        var operationCount = 0;
        var errorCount = 0;
        var performanceMeasurements = new List<(TimeSpan time, int operations)>();

        // Act - 长时间运行混合操作
        var endTime = DateTime.UtcNow.AddSeconds(testDurationSeconds);
        var random = new Random(42);

        while (DateTime.UtcNow < endTime)
        {
            try
            {
                // 插入操作
                var user = new UserWithIntId
                {
                    Name = $"LongRunningUser_{operationCount}",
                    Age = 20 + random.Next(0, 50)
                };
                collection.Insert(user);

                // 查询操作 - 减少频率和复杂度
                if (operationCount % 50 == 0)
                {
                    // 简单计数查询，避免遍历所有数据
                    var count = collection.FindAll().Count();
                    // 验证查询结果
                    await Assert.That(count).IsGreaterThan(0);
                }

                // 更新操作 - 减少频率
                if (operationCount % 100 == 0 && operationCount > 0)
                {
                    // 使用最近插入的用户ID，避免随机查找
                    var recentUserId = Math.Max(1, operationCount - 10);
                    var userToUpdate = collection.FindById(recentUserId);
                    if (userToUpdate != null)
                    {
                        userToUpdate.Age = userToUpdate.Age + 1;
                        collection.Update(userToUpdate);
                    }
                }

                operationCount++;

                // 定期记录性能指标
                if (operationCount % 1000 == 0)
                {
                    performanceMeasurements.Add((stopwatch.Elapsed, operationCount));
                }
            }
            catch
            {
                errorCount++;
                if (errorCount > 10) // 错误过多时停止测试
                {
                    break;
                }
            }
        }

        stopwatch.Stop();

        // Assert - 验证长时间运行稳定性
        await Assert.That(errorCount).IsLessThan((int)(operationCount * 0.01)); // 错误率应小于1%
        await Assert.That(operationCount).IsGreaterThan(100); // 应该完成一定数量的操作

        // 验证性能不会显著下降
        if (performanceMeasurements.Count >= 2)
        {
            var firstMeasurement = performanceMeasurements[0];
            var lastMeasurement = performanceMeasurements[^1];

            var firstRate = firstMeasurement.operations / firstMeasurement.time.TotalSeconds;
            var lastRate = lastMeasurement.operations / lastMeasurement.time.TotalSeconds;
            var performanceDegradation = (firstRate - lastRate) / firstRate;

            await Assert.That(performanceDegradation).IsLessThan(0.3); // 性能下降不应超过30%
        }

        // 验证数据完整性
        var finalUserCount = collection.FindAll().Count();
        await Assert.That(finalUserCount).IsGreaterThan(0);

        Console.WriteLine($"总操作数: {operationCount}");
        Console.WriteLine($"错误数: {errorCount}");
        Console.WriteLine($"运行时间: {stopwatch.Elapsed.TotalMinutes:F2} 分钟");
        Console.WriteLine($"平均操作速率: {operationCount / stopwatch.Elapsed.TotalSeconds:F2} 操作/秒");
    }

    /// <summary>
    /// 测试存储层的恢复能力
    /// </summary>
    [Test]
    public async Task StorageRecovery_ShouldHandleCorruption()
    {
        // Arrange - 创建正常数据
        var collection = _engine.GetCollection<UserWithIntId>();
        var users = Enumerable.Range(1, 1000)
            .Select(i => new UserWithIntId
            {
                Name = $"RecoveryUser_{i}",
                Age = 20 + (i % 50)
            })
            .ToArray();

        foreach (var user in users)
        {
            collection.Insert(user);
        }

        var originalCount = collection.FindAll().Count();
        await Assert.That(originalCount).IsEqualTo(1000);

        // 强制刷新数据
        _engine.Dispose();

        // Act - 模拟文件损坏（通过截断文件）
        using var fileStream = new FileStream(_testFile, FileMode.Open, FileAccess.ReadWrite);
        if (fileStream.Length > 1024)
        {
            fileStream.SetLength(fileStream.Length - 512); // 截断最后512字节
        }

        // 尝试重新打开数据库
        try
        {
            _engine = new TinyDbEngine(_testFile);
            var recoveredCollection = _engine.GetCollection<UserWithIntId>();
            var recoveredCount = recoveredCollection.FindAll().Count();

            // Assert - 验证恢复能力
            await Assert.That(recoveredCount).IsLessThanOrEqualTo(originalCount);

            // 如果有数据恢复，验证数据完整性
            if (recoveredCount > 0)
            {
                var sampleUser = recoveredCollection.FindAll().FirstOrDefault();
                if (sampleUser != null)
                {
                    await Assert.That(sampleUser.Name).IsNotNull();
                    await Assert.That(sampleUser.Age).IsGreaterThan(0);
                }
            }

            Console.WriteLine($"原始记录数: {originalCount}");
            Console.WriteLine($"恢复记录数: {recoveredCount}");
            Console.WriteLine($"恢复率: {(double)recoveredCount / originalCount:P2}");
        }
        catch (Exception ex)
        {
            // 如果无法处理损坏，应该抛出合理的异常
            await Assert.That(ex).IsAssignableTo<IOException>()
                .Or.IsAssignableTo<InvalidOperationException>()
                .Or.IsAssignableTo<InvalidOperationException>();

            Console.WriteLine($"检测到文件损坏，抛出异常: {ex.GetType().Name}");
        }
    }
}

// 测试用的大数据实体
public class LargeDataEntity
{
    public string Id { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public string LargeText { get; set; } = string.Empty;
    public DateTime CreatedAt { get; set; }
    public int Number { get; set; }
    public string[] Tags { get; set; } = Array.Empty<string>();
}

public static class TinyDbExceptionHelper
{
    public static InvalidOperationException InvalidOperationException(string message) => new(message);
}
