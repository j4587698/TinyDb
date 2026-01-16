using System;
using System.Linq;
using TinyDb.Storage;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Storage;

public class PageTests : IDisposable
{
    private const int TestPageSize = 4096;

    [Test]
    public async Task Constructor_Should_Initialize_With_Correct_Values()
    {
        // Arrange
        var pageId = 42u;
        var pageType = PageType.Data;

        // Act
        using var page = new Page(pageId, TestPageSize, pageType);

        // Assert
        await Assert.That(page.PageID).IsEqualTo(pageId);
        await Assert.That(page.PageType).IsEqualTo(pageType);
        await Assert.That(page.PageSize).IsEqualTo(TestPageSize);
        await Assert.That(page.DataSize).IsEqualTo(TestPageSize - PageHeader.Size);
        await Assert.That(page.IsDirty).IsFalse();
    }

    [Test]
    public async Task Constructor_Should_Create_Default_Header()
    {
        // Act
        using var page = new Page(1u, TestPageSize, PageType.Index);

        // Assert
        await Assert.That(page.Header.PageID).IsEqualTo(1u);
        await Assert.That(page.Header.PageType).IsEqualTo(PageType.Index);
        await Assert.That(page.Header.PrevPageID).IsEqualTo(0u);
        await Assert.That(page.Header.NextPageID).IsEqualTo(0u);
        await Assert.That(page.Header.FreeBytes == page.DataSize).IsTrue();
        await Assert.That(page.Header.ItemCount == 0u).IsTrue();
        await Assert.That(page.Header.Version == 0u).IsTrue();
    }

    [Test]
    public async Task Constructor_Should_Throw_ArgumentException_For_Zero_Page_Size()
    {
        // Act & Assert
        await Assert.That(() => new Page(1u, 0, PageType.Data)).Throws<ArgumentException>();
    }

    [Test]
    public async Task Constructor_Should_Throw_ArgumentException_For_Page_Size_Smaller_Than_Header()
    {
        // Act & Assert
        await Assert.That(() => new Page(1u, 16, PageType.Data)).Throws<ArgumentException>();
    }

    [Test]
    public async Task WriteData_Should_Write_Data_To_Page()
    {
        // Arrange
        using var page = new Page(1u, TestPageSize, PageType.Data);
        var testData = new byte[] { 1, 2, 3, 4, 5 };

        // Act
        page.WriteData(0, testData);

        // Assert
        await Assert.That(page.IsDirty).IsTrue();

        // Verify by reading back
        var readData = page.ReadData(0, testData.Length);
        await Assert.That(readData.SequenceEqual(testData)).IsTrue();
    }

    [Test]
    public async Task WriteData_With_Offset_Should_Write_Correctly()
    {
        // Arrange
        using var page = new Page(1u, TestPageSize, PageType.Data);
        var initialData = new byte[] { 1, 2, 3, 4, 5 };
        var newData = new byte[] { 9, 8, 7 };

        page.WriteData(0, initialData);

        // Act
        page.WriteData(2, newData);

        // Assert
        await Assert.That(page.IsDirty).IsTrue();

        // Verify by reading back
        var readData = page.ReadData(0, 5);
        var expected = new byte[] { 1, 2, 9, 8, 7 };
        await Assert.That(readData.SequenceEqual(expected)).IsTrue();
    }

    [Test]
    public async Task ReadData_Beyond_Data_Should_Return_Empty()
    {
        // Arrange
        using var page = new Page(1u, TestPageSize, PageType.Data);

        // Act - Read beyond data size
        var readData = page.ReadData(4050, 10); // Beyond DataSize (4055)

        // Assert
        await Assert.That(readData.Length).IsEqualTo(0);
    }

    [Test]
    public async Task ReadData_Should_Read_Correct_Number_Of_Bytes()
    {
        // Arrange
        using var page = new Page(1u, TestPageSize, PageType.Data);
        var testData = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 };
        page.WriteData(0, testData);

        // Act
        var readData = page.ReadData(2, 3);

        // Assert
        await Assert.That(readData.Length).IsEqualTo(3);
        await Assert.That(readData[0] == 3).IsTrue();
        await Assert.That(readData[1] == 4).IsTrue();
        await Assert.That(readData[2] == 5).IsTrue();
    }

    [Test]
    public async Task ClearData_Should_Reset_Page_Data()
    {
        // Arrange
        using var page = new Page(1u, TestPageSize, PageType.Data);
        var testData = new byte[] { 1, 2, 3, 4, 5 };
        page.WriteData(0, testData);

        // Act
        page.ClearData();

        // Assert
        await Assert.That(page.IsDirty).IsTrue();

        // Verify data is cleared
        var readData = page.ReadData(0, 5);
        await Assert.That(readData.All(b => b == 0)).IsTrue();
    }

    [Test]
    public async Task ClearData_Should_Reset_Header_To_Defaults()
    {
        // Arrange
        using var page = new Page(42u, TestPageSize, PageType.Index);
        page.Header.PrevPageID = 10;
        page.Header.NextPageID = 20;
        page.Header.FreeBytes = 100;
        page.Header.ItemCount = 5;
        page.Header.Version = 3;

        // Act
        page.ClearData();

        // Assert
        await Assert.That(page.Header.PageID).IsEqualTo(42u); // PageID should remain
        await Assert.That(page.Header.PageType).IsEqualTo(PageType.Empty); // Reset to Empty
        await Assert.That(page.Header.PrevPageID).IsEqualTo(0u);
        await Assert.That(page.Header.NextPageID).IsEqualTo(0u);
        await Assert.That(page.Header.FreeBytes).IsEqualTo((ushort)Math.Min(page.DataSize, ushort.MaxValue));
        await Assert.That(page.Header.ItemCount).IsEqualTo((ushort)0);
        await Assert.That(page.Header.Version).IsEqualTo(4u);
    }

    [Test]
    public async Task MarkClean_Should_Set_IsDirty_To_False()
    {
        // Arrange
        using var page = new Page(1u, TestPageSize, PageType.Data);
        page.WriteData(0, new byte[] { 1 }); // Make it dirty

        // Act
        page.MarkClean();

        // Assert
        await Assert.That(page.IsDirty).IsFalse();
    }

    [Test]
    public async Task UpdatePageType_Should_Change_Page_Type()
    {
        // Arrange
        using var page = new Page(1u, TestPageSize, PageType.Data);

        // Act
        page.UpdatePageType(PageType.Index);

        // Assert
        await Assert.That(page.PageType).IsEqualTo(PageType.Index);
        await Assert.That(page.IsDirty).IsTrue();
    }

    [Test]
    public async Task UpdateHeader_Should_Update_All_Header_Values()
    {
        // Arrange
        using var page = new Page(1u, TestPageSize, PageType.Data);
        var newHeader = new PageHeader
        {
            PageType = PageType.Index,
            PageID = 1u,
            PrevPageID = 10,
            NextPageID = 20,
            FreeBytes = 500,
            ItemCount = 3,
            Version = 2
        };

        // Act
        page.UpdateHeader(newHeader);

        // Assert
        await Assert.That(page.PageType).IsEqualTo(PageType.Index);
        await Assert.That(page.Header.PrevPageID).IsEqualTo(10u);
        await Assert.That(page.Header.NextPageID).IsEqualTo(20u);
        await Assert.That(page.Header.FreeBytes == 500).IsTrue();
        await Assert.That(page.Header.ItemCount == 3).IsTrue();
        await Assert.That(page.Header.Version).IsEqualTo(2u);
    }

    [Test]
    public async Task SetLinks_Should_Update_Link_Values()
    {
        // Arrange
        using var page = new Page(1u, TestPageSize, PageType.Data);

        // Act
        page.SetLinks(10, 20);

        // Assert
        await Assert.That(page.Header.PrevPageID).IsEqualTo(10u);
        await Assert.That(page.Header.NextPageID).IsEqualTo(20u);
    }

    [Test]
    public async Task UpdateStats_Should_Update_Statistics_Values()
    {
        // Arrange
        using var page = new Page(1u, TestPageSize, PageType.Data);

        // Act
        page.UpdateStats(500, 3);

        // Assert
        await Assert.That(page.Header.FreeBytes == 500).IsTrue();
        await Assert.That(page.Header.ItemCount == 3).IsTrue();
    }

    [Test]
    public async Task GetDataSpan_Should_Return_Correct_Span()
    {
        // Arrange
        using var page = new Page(1u, TestPageSize, PageType.Data);
        var testData = new byte[] { 1, 2, 3, 4, 5 };
        page.WriteData(0, testData);

        // Act
        var dataSpan = page.GetDataSpan(0, 5);

        // Assert
        var spanLength = dataSpan.Length;
        var firstByte = dataSpan[0];
        var lastByte = dataSpan[4];

        await Assert.That(spanLength).IsEqualTo(5);
        await Assert.That(firstByte == 1).IsTrue();
        await Assert.That(lastByte == 5).IsTrue();
    }

    [Test]
    public async Task Large_Data_Operations_Should_Work()
    {
        // Arrange
        using var page = new Page(1u, TestPageSize, PageType.Data);
        var largeData = new byte[page.DataSize];
        for (int i = 0; i < largeData.Length; i++)
        {
            largeData[i] = (byte)(i % 256);
        }

        // Act
        page.WriteData(0, largeData);
        var readData = page.ReadData(0, largeData.Length);

        // Assert
        await Assert.That(readData.SequenceEqual(largeData)).IsTrue();
    }

    [Test]
    public async Task Multiple_Write_Operations_Should_Work()
    {
        // Arrange
        using var page = new Page(1u, TestPageSize, PageType.Data);

        // Act
        page.WriteData(0, new byte[] { 1, 2, 3 });
        page.WriteData(3, new byte[] { 4, 5, 6 });
        page.WriteData(6, new byte[] { 7, 8, 9 });

        // Assert
        var readData = page.ReadData(0, 9);
        var expected = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        await Assert.That(readData.SequenceEqual(expected)).IsTrue();
    }

    [Test]
    public async Task Dispose_Should_Clean_Up_Resources()
    {
        // Arrange
        var page = new Page(1u, TestPageSize, PageType.Data);

        // Act
        page.Dispose();

        // Assert - Should not throw when accessing properties after dispose
        await Assert.That(() => page.ReadData(0, 1)).Throws<ObjectDisposedException>();
        await Assert.That(() => page.WriteData(0, new byte[] { 1 })).Throws<ObjectDisposedException>();
        await Assert.That(() => page.ClearData()).Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task Clone_Should_Create_Separate_Instance()
    {
        // Arrange
        using var page = new Page(1u, TestPageSize, PageType.Data);
        page.WriteData(0, new byte[] { 1, 2, 3 });

        // Act
        var clonedPage = page.Clone();

        // Assert
        await Assert.That(!ReferenceEquals(page, clonedPage)).IsTrue();
        await Assert.That(clonedPage.PageID).IsEqualTo(page.PageID);
        await Assert.That(clonedPage.PageType).IsEqualTo(page.PageType);

        // Verify data is the same
        var originalData = page.ReadData(0, 3);
        var clonedData = clonedPage.ReadData(0, 3);
        await Assert.That(originalData.SequenceEqual(clonedData)).IsTrue();

        clonedPage.Dispose();
    }

    public void Dispose()
    {
        // Implementation for test cleanup if needed
    }
}
