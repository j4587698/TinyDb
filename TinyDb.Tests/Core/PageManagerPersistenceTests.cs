using System.Diagnostics.CodeAnalysis;
using TinyDb.Core;
using TinyDb.Storage;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Core;

[NotInParallel]
public class PageManagerPersistenceTests
{
    private string _testDbPath = null!;

    [Before(Test)]
    public void Setup()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"page_manager_test_{Guid.NewGuid():N}.db");
        if (File.Exists(_testDbPath))
            File.Delete(_testDbPath);
    }

    [After(Test)]
    public void Cleanup()
    {
        if (File.Exists(_testDbPath))
            File.Delete(_testDbPath);
    }

    [Test]
    public async Task PageManager_CalculatePageOffset_Should_Work_Correctly()
    {
        const uint pageSize = 8192;

        using var engine = new TinyDbEngine(_testDbPath, new TinyDbOptions { PageSize = pageSize });

        // 通过反射获取PageManager实例
        var pageManagerProperty = typeof(TinyDbEngine).GetField("_pageManager",
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        var pageManager = pageManagerProperty?.GetValue(engine) as PageManager;

        await Assert.That(pageManager).IsNotNull();

        // 通过反射调用CalculatePageOffset方法
        var calculateOffsetMethod = typeof(PageManager).GetMethod("CalculatePageOffset",
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);

        await Assert.That(calculateOffsetMethod).IsNotNull();

        // 测试页面偏移量计算
        var tests = new[]
        {
            (PageID: 1u, ExpectedOffset: 0L),
            (PageID: 2u, ExpectedOffset: 8192L),
            (PageID: 3u, ExpectedOffset: 16384L),
            (PageID: 10u, ExpectedOffset: 73728L)
        };

        foreach (var (pageId, expectedOffset) in tests)
        {
            var actualOffset = (long)calculateOffsetMethod!.Invoke(pageManager!, new object[] { pageId })!;
            await Assert.That(actualOffset).IsEqualTo(expectedOffset);
        }
    }

    
    [Test]
    public async Task Database_Should_Recover_From_Corrupted_Pages()
    {
        const string collectionName = "recovery_test";

        // 创建数据库并插入数据
        using (var engine = new TinyDbEngine(_testDbPath))
        {
            var collection = engine.GetCollection<TinyDb.Tests.TestEntities.User>(collectionName);
            collection.Insert(new TinyDb.Tests.TestEntities.User
            {
                Name = "Recovery Test User",
                Age = 30,
                Email = "recovery@example.com"
            });

            engine.Flush();
        }

        // 模拟文件损坏（在文件末尾添加一些垃圾数据）
        using (var stream = new FileStream(_testDbPath, FileMode.Append, FileAccess.Write))
        {
            var garbageData = new byte[1024];
            new Random().NextBytes(garbageData);
            stream.Write(garbageData, 0, garbageData.Length);
        }

        // 数据库应该能够从损坏中恢复
        using (var reopenedEngine = new TinyDbEngine(_testDbPath))
        {
            var collections = reopenedEngine.GetCollectionNames().ToList();
            await Assert.That(collections.Contains(collectionName)).IsTrue();

            var collection = reopenedEngine.GetCollection<TinyDb.Tests.TestEntities.User>(collectionName);
            var count = collection.Count();

            // 数据应该仍然可访问
            await Assert.That(count).IsGreaterThan(0);
        }
    }

    [Test]
    public async Task Database_Should_Handle_Multiple_Page_Sizes()
    {
        var pageSizes = new[] { 4096u, 8192u, 16384u };

        foreach (var pageSize in pageSizes)
        {
            // 清理数据库文件
            if (File.Exists(_testDbPath))
                File.Delete(_testDbPath);

            var options = new TinyDbOptions { PageSize = pageSize };

            using (var engine = new TinyDbEngine(_testDbPath, options))
            {
                var collection = engine.GetCollection<TinyDb.Tests.TestEntities.User>("page_size_test");

                // 插入足够的数据以测试页面大小处理
                for (int i = 0; i < 50; i++)
                {
                    collection.Insert(new TinyDb.Tests.TestEntities.User
                    {
                        Name = $"Page Size Test User {i}",
                        Age = 25 + i,
                        Email = $"pagesize{i}@example.com"
                    });
                }

                engine.Flush();
            }

            // 验证数据持久化
            using (var reopenedEngine = new TinyDbEngine(_testDbPath, options))
            {
                var collection = reopenedEngine.GetCollection<TinyDb.Tests.TestEntities.User>("page_size_test");
                var count = collection.Count();
                await Assert.That(count).IsEqualTo(50);

                // 验证页面管理器统计
                var stats = reopenedEngine.GetStatistics();
                await Assert.That(stats.PageSize).IsEqualTo(pageSize);
            }
        }
    }
}