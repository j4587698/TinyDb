using TinyDb.Storage;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Storage;

public class PageManagerTests : IDisposable
{
    private readonly string _testDirectory;
    private readonly string _testFilePath;
    private const uint TestPageSize = 4096;
    private const int TestCacheSize = 100;

    public PageManagerTests()
    {
        _testDirectory = Path.Combine(Path.GetTempPath(), "TinyDbTests", Guid.NewGuid().ToString());
        Directory.CreateDirectory(_testDirectory);
        _testFilePath = Path.Combine(_testDirectory, "test.db");
    }

    public void Dispose()
    {
        if (Directory.Exists(_testDirectory))
        {
            try
            {
                Directory.Delete(_testDirectory, true);
            }
            catch
            {
                // Ignore cleanup errors
            }
        }
    }

    [Test]
    public async Task Constructor_Should_Initialize_With_Correct_Values()
    {
        // Arrange
        using var diskStream = new DiskStream(_testFilePath);

        // Act
        using var pageManager = new PageManager(diskStream, TestPageSize, TestCacheSize);

        // Assert
        await Assert.That(pageManager.PageSize).IsEqualTo(TestPageSize);
        await Assert.That(pageManager.MaxCacheSize).IsEqualTo(TestCacheSize);
        await Assert.That(pageManager.CachedPages).IsEqualTo(0);
        await Assert.That(pageManager.FreePages).IsEqualTo(0);
        await Assert.That(pageManager.TotalPages >= 0).IsTrue();
        await Assert.That(File.Exists(_testFilePath)).IsTrue();
    }

    [Test]
    public async Task Constructor_Should_Throw_ArgumentException_For_Zero_Page_Size()
    {
        // Arrange
        using var diskStream = new DiskStream(_testFilePath);

        // Act & Assert
        await Assert.That(() => new PageManager(diskStream, 0, TestCacheSize)).Throws<ArgumentException>();
    }

    [Test]
    public async Task Constructor_Should_Throw_ArgumentException_For_Negative_Cache_Size()
    {
        // Arrange
        using var diskStream = new DiskStream(_testFilePath);

        // Act & Assert
        await Assert.That(() => new PageManager(diskStream, TestPageSize, -1)).Throws<ArgumentOutOfRangeException>();
    }

    [Test]
    public async Task Constructor_Should_Throw_ArgumentNullException_For_Null_DiskStream()
    {
        // Act & Assert
        await Assert.That(() => new PageManager(null!, TestPageSize, TestCacheSize)).Throws<ArgumentNullException>();
    }

    [Test]
    public async Task GetPage_Should_Create_New_Page_When_Not_Exists()
    {
        // Arrange
        using var diskStream = new DiskStream(_testFilePath);
        using var pageManager = new PageManager(diskStream, TestPageSize, TestCacheSize);

        // Act
        var page = pageManager.GetPage(1);

        // Assert
        await Assert.That(page).IsNotNull();
        await Assert.That(page.PageID).IsEqualTo(1u);
        await Assert.That(page.PageType).IsEqualTo(PageType.Empty);
        await Assert.That(pageManager.CachedPages).IsGreaterThan(0);
    }

    [Test]
    public async Task GetPage_Should_Throw_For_Zero_Page_ID()
    {
        // Arrange
        using var diskStream = new DiskStream(_testFilePath);
        using var pageManager = new PageManager(diskStream, TestPageSize, TestCacheSize);

        // Act & Assert
        await Assert.That(() => pageManager.GetPage(0)).Throws<ArgumentException>();
    }

    [Test]
    public async Task GetPage_Should_Return_Cached_Page_On_Second_Call()
    {
        // Arrange
        using var diskStream = new DiskStream(_testFilePath);
        using var pageManager = new PageManager(diskStream, TestPageSize, TestCacheSize);

        // Act
        var page1 = pageManager.GetPage(1);
        var page2 = pageManager.GetPage(1);

        // Assert
        await Assert.That(ReferenceEquals(page1, page2)).IsTrue(); // Should be same cached instance
    }

    [Test]
    public async Task SavePage_Should_Persist_Page_To_Disk()
    {
        // Arrange
        using var diskStream = new DiskStream(_testFilePath);
        using var pageManager = new PageManager(diskStream, TestPageSize, TestCacheSize);
        var page = pageManager.GetPage(1);
        page.UpdatePageType(PageType.Data);

        // Act
        pageManager.SavePage(page);

        // Assert
        await Assert.That(page.IsDirty).IsFalse();

        // Verify by loading a fresh instance
        var freshPageManager = new PageManager(diskStream, TestPageSize, TestCacheSize);
        var loadedPage = freshPageManager.GetPage(1);
        await Assert.That(loadedPage.PageType).IsEqualTo(PageType.Data);
    }

    [Test]
    public async Task FreePage_Should_Add_Page_To_Free_List()
    {
        // Arrange
        using var diskStream = new DiskStream(_testFilePath);
        using var pageManager = new PageManager(diskStream, TestPageSize, TestCacheSize);
        var page = pageManager.GetPage(1);
        page.UpdatePageType(PageType.Data);
        pageManager.SavePage(page);

        // Act
        pageManager.FreePage(1);

        // Assert
        await Assert.That(pageManager.FreePages).IsGreaterThan(0);

        // Verify page is marked as empty
        var reloadedPage = pageManager.GetPage(1);
        await Assert.That(reloadedPage.PageType).IsEqualTo(PageType.Empty);
    }

    [Test]
    public async Task FreePage_Should_Throw_For_Zero_Page_ID()
    {
        // Arrange
        using var diskStream = new DiskStream(_testFilePath);
        using var pageManager = new PageManager(diskStream, TestPageSize, TestCacheSize);

        // Act & Assert
        await Assert.That(() => pageManager.FreePage(0)).Throws<ArgumentException>();
    }

    [Test]
    public async Task GetPageAsync_Should_Work_Correctly()
    {
        // Arrange
        using var diskStream = new DiskStream(_testFilePath);
        using var pageManager = new PageManager(diskStream, TestPageSize, TestCacheSize);

        // Act
        var page = await pageManager.GetPageAsync(1);

        // Assert
        await Assert.That(page).IsNotNull();
        await Assert.That(page.PageID).IsEqualTo(1u);
        await Assert.That(page.PageType).IsEqualTo(PageType.Empty);
    }

    [Test]
    public async Task SavePageAsync_Should_Work_Correctly()
    {
        // Arrange
        using var diskStream = new DiskStream(_testFilePath);
        using var pageManager = new PageManager(diskStream, TestPageSize, TestCacheSize);
        var page = pageManager.GetPage(1);
        page.UpdatePageType(PageType.Index);

        // Act
        await pageManager.SavePageAsync(page);

        // Assert
        await Assert.That(page.IsDirty).IsFalse();

        // Verify by loading a fresh instance
        var freshPageManager = new PageManager(diskStream, TestPageSize, TestCacheSize);
        var loadedPage = freshPageManager.GetPage(1);
        await Assert.That(loadedPage.PageType).IsEqualTo(PageType.Index);
    }

    [Test]
    public async Task Multiple_Page_Operations_Should_Work()
    {
        // Arrange
        using var diskStream = new DiskStream(_testFilePath);
        using var pageManager = new PageManager(diskStream, TestPageSize, TestCacheSize);

        // Act
        var pages = new List<Page>();
        for (uint i = 1; i <= 5; i++)
        {
            var page = pageManager.GetPage(i);
            page.UpdatePageType(PageType.Data);
            pageManager.SavePage(page);
            pages.Add(page);
        }

        // Assert
        await Assert.That(pages.Count).IsEqualTo(5);
        for (uint i = 0; i < pages.Count; i++)
        {
            await Assert.That(pages[(int)i].PageID).IsEqualTo(i + 1);
            await Assert.That(pages[(int)i].PageType).IsEqualTo(PageType.Data);
        }
    }

    [Test]
    public async Task Cache_Eviction_Should_Work()
    {
        // Arrange
        var smallCacheSize = 3;
        using var diskStream = new DiskStream(_testFilePath);
        using var pageManager = new PageManager(diskStream, TestPageSize, smallCacheSize);

        // Act - Create more pages than cache size
        var pages = new List<Page>();
        for (uint i = 1; i <= 5; i++)
        {
            pages.Add(pageManager.GetPage(i));
        }

        // Assert
        await Assert.That(pageManager.CachedPages).IsLessThanOrEqualTo(smallCacheSize);
    }

    [Test]
    public async Task Dispose_Should_Clean_Up_Resources()
    {
        // Arrange
        using var diskStream = new DiskStream(_testFilePath);
        var pageManager = new PageManager(diskStream, TestPageSize, TestCacheSize);
        pageManager.GetPage(1);

        // Act
        pageManager.Dispose();

        // Assert - Should not throw when accessing properties after dispose
        await Assert.That(() => pageManager.GetPage(1)).Throws<ObjectDisposedException>();
        await Assert.That(() => pageManager.SavePage(new Page(1, (int)TestPageSize, PageType.Data))).Throws<ObjectDisposedException>();
        await Assert.That(() => pageManager.FreePage(1)).Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task Page_Data_Persistence_Should_Work()
    {
        // Arrange
        byte[] testData = new byte[] { 1, 2, 3, 4, 5 };
        {
            using var diskStream = new DiskStream(_testFilePath);
            using var pageManager = new PageManager(diskStream, TestPageSize, TestCacheSize);
            var page = pageManager.GetPage(1);
            page.UpdatePageType(PageType.Collection);

            // Write some data to the page
            page.WriteData(0, testData);
            pageManager.SavePage(page);
        } // 释放所有资源

        // Act - Create a new manager and load the page
        using var newPageManager = new PageManager(new DiskStream(_testFilePath), TestPageSize, TestCacheSize);
        var loadedPage = newPageManager.GetPage(1);

        // Assert
        await Assert.That(loadedPage.PageType).IsEqualTo(PageType.Collection);
        var readData = loadedPage.ReadData(0, 5);
        await Assert.That(readData.SequenceEqual(testData)).IsTrue();
    }

    [Test]
    public async Task Large_Number_Of_Pages_Should_Work()
    {
        // Arrange
        using var diskStream = new DiskStream(_testFilePath);
        using var pageManager = new PageManager(diskStream, TestPageSize, TestCacheSize);

        // Act
        var pageIds = new List<uint>();
        for (uint i = 1; i <= 100; i++)
        {
            var page = pageManager.GetPage(i);
            page.UpdatePageType(PageType.Data);
            pageManager.SavePage(page);
            pageIds.Add(i);
        }

        // Assert
        await Assert.That(pageIds.Count).IsEqualTo(100);

        // Verify all pages can be loaded
        foreach (var pageId in pageIds)
        {
            var page = pageManager.GetPage(pageId);
            await Assert.That(page.PageID).IsEqualTo(pageId);
            await Assert.That(page.PageType).IsEqualTo(PageType.Data);
        }
    }

    [Test]
    public async Task Concurrent_Page_Operations_Should_Work()
    {
        // Arrange
        using var diskStream = new DiskStream(_testFilePath);
        using var pageManager = new PageManager(diskStream, TestPageSize, TestCacheSize);
        var tasks = new List<Task<Page>>();

        // Act - Get pages concurrently
        for (uint i = 1; i <= 10; i++)
        {
            var pageId = i;
            tasks.Add(Task.Run(() => pageManager.GetPage(pageId)));
        }

        var pages = await Task.WhenAll(tasks);

        // Assert
        await Assert.That(pages.Length).IsEqualTo(10);
        var pageIds = pages.Select(p => p.PageID).ToHashSet();
        await Assert.That(pageIds.Count).IsEqualTo(10);
        await Assert.That(pageIds.Contains(1u)).IsTrue();
        await Assert.That(pageIds.Contains(10u)).IsTrue();
    }
}