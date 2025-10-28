using SimpleDb.Storage;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace SimpleDb.Tests.Storage;

public class PageHeaderTests
{
    [Test]
    public async Task Constructor_Should_Initialize_With_Default_Values()
    {
        // Act
        var header = new PageHeader();

        // Assert
        await Assert.That(header.PageType == PageType.Empty).IsTrue();
        await Assert.That(header.PageID == 0u).IsTrue();
        await Assert.That(header.PrevPageID == 0u).IsTrue();
        await Assert.That(header.NextPageID == 0u).IsTrue();
        await Assert.That(header.FreeBytes == 0u).IsTrue();
        await Assert.That(header.ItemCount == 0u).IsTrue();
        await Assert.That(header.Version == 0u).IsTrue();
    }

    [Test]
    public async Task Properties_Should_Be_Settable()
    {
        // Arrange
        var header = new PageHeader();

        // Act
        header.PageType = PageType.Data;
        header.PageID = 42u;
        header.PrevPageID = 10u;
        header.NextPageID = 20u;
        header.FreeBytes = 1000;
        header.ItemCount = 5;
        header.Version = 1u;

        // Assert
        await Assert.That(header.PageType == PageType.Data).IsTrue();
        await Assert.That(header.PageID == 42u).IsTrue();
        await Assert.That(header.PrevPageID == 10u).IsTrue();
        await Assert.That(header.NextPageID == 20u).IsTrue();
        await Assert.That(header.FreeBytes == 1000).IsTrue();
        await Assert.That(header.ItemCount == 5).IsTrue();
        await Assert.That(header.Version == 1u).IsTrue();
    }

    [Test]
    public async Task GetSize_Should_Return_Header_Size()
    {
        // Act & Assert
        await Assert.That(PageHeader.Size == 32).IsTrue();
    }

    [Test]
    public async Task GetDataSize_Should_Calculate_Correctly()
    {
        // Arrange
        var header = new PageHeader();
        var pageSize = 4096;

        // Act
        var dataSize = header.GetDataSize(pageSize);

        // Assert
        await Assert.That(dataSize == pageSize - PageHeader.Size).IsTrue();
    }

    [Test]
    public async Task GetDataSize_With_Different_Page_Sizes()
    {
        // Arrange
        var header = new PageHeader();

        // Act & Assert
        await Assert.That(header.GetDataSize(512) == 471).IsTrue();
        await Assert.That(header.GetDataSize(1024) == 983).IsTrue();
        await Assert.That(header.GetDataSize(8192) == 8151).IsTrue();
    }

    [Test]
    public async Task GetDataSize_With_Zero_Page_Size_Should_Return_Zero()
    {
        // Arrange
        var header = new PageHeader();

        // Act
        var dataSize = header.GetDataSize(0);

        // Assert
        await Assert.That(dataSize == 0).IsTrue();
    }

    [Test]
    public async Task GetDataSize_With_Smaller_Page_Size_Should_Return_Zero()
    {
        // Arrange
        var header = new PageHeader();

        // Act
        var dataSize = header.GetDataSize(16); // Smaller than header size

        // Assert
        await Assert.That(dataSize == 0).IsTrue();
    }

    [Test]
    public async Task Header_Should_Support_All_Page_Types()
    {
        // Arrange
        var header = new PageHeader();
        var pageTypes = new[]
        {
            PageType.Empty,
            PageType.Header,
            PageType.Collection,
            PageType.Data,
            PageType.Index,
            PageType.Journal,
            PageType.Extension
        };

        // Act & Assert
        foreach (var pageType in pageTypes)
        {
            header.PageType = pageType;
            await Assert.That(header.PageType == pageType).IsTrue();
        }
    }

    [Test]
    public async Task Header_Should_Handle_Large_Values()
    {
        // Arrange
        var header = new PageHeader();

        // Act
        header.PageID = uint.MaxValue;
        header.PrevPageID = uint.MaxValue;
        header.NextPageID = uint.MaxValue;
        header.FreeBytes = (ushort)ushort.MaxValue;
        header.ItemCount = (ushort)ushort.MaxValue;
        header.Version = uint.MaxValue;

        // Assert
        await Assert.That(header.PageID == uint.MaxValue).IsTrue();
        await Assert.That(header.PrevPageID == uint.MaxValue).IsTrue();
        await Assert.That(header.NextPageID == uint.MaxValue).IsTrue();
        await Assert.That(header.FreeBytes == ushort.MaxValue).IsTrue();
        await Assert.That(header.ItemCount == ushort.MaxValue).IsTrue();
        await Assert.That(header.Version == uint.MaxValue).IsTrue();
    }

    [Test]
    public async Task Header_Should_Maintain_Value_Integrity()
    {
        // Arrange
        var originalHeader = new PageHeader
        {
            PageType = PageType.Index,
            PageID = 123u,
            PrevPageID = 456u,
            NextPageID = 789u,
            FreeBytes = 500,
            ItemCount = 10,
            Version = 3u
        };

        // Act
        var newHeader = new PageHeader
        {
            PageType = originalHeader.PageType,
            PageID = originalHeader.PageID,
            PrevPageID = originalHeader.PrevPageID,
            NextPageID = originalHeader.NextPageID,
            FreeBytes = originalHeader.FreeBytes,
            ItemCount = originalHeader.ItemCount,
            Version = originalHeader.Version
        };

        // Assert
        await Assert.That(newHeader.PageType == originalHeader.PageType).IsTrue();
        await Assert.That(newHeader.PageID == originalHeader.PageID).IsTrue();
        await Assert.That(newHeader.PrevPageID == originalHeader.PrevPageID).IsTrue();
        await Assert.That(newHeader.NextPageID == originalHeader.NextPageID).IsTrue();
        await Assert.That(newHeader.FreeBytes == originalHeader.FreeBytes).IsTrue();
        await Assert.That(newHeader.ItemCount == originalHeader.ItemCount).IsTrue();
        await Assert.That(newHeader.Version == originalHeader.Version).IsTrue();
    }

    [Test]
    public async Task Header_Should_Be_Consistent_After_Multiple_Operations()
    {
        // Arrange
        var header = new PageHeader();

        // Act - Perform multiple operations
        for (int i = 0; i < 100; i++)
        {
            header.PageType = (PageType)(i % 7);
            header.PageID = (uint)i;
            header.FreeBytes = (ushort)(i * 10);
            header.ItemCount = (ushort)(i % 20);
            header.Version = (uint)(i / 10);

            // Assert after each operation
            await Assert.That(header.PageType == (PageType)(i % 7)).IsTrue();
            await Assert.That(header.PageID == (uint)i).IsTrue();
            await Assert.That(header.FreeBytes == (ushort)(i * 10)).IsTrue();
            await Assert.That(header.ItemCount == (ushort)(i % 20)).IsTrue();
            await Assert.That(header.Version == (uint)(i / 10)).IsTrue();
        }
    }
}