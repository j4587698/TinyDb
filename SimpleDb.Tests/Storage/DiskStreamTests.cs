using SimpleDb.Storage;
using System.Linq;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace SimpleDb.Tests.Storage;

public class DiskStreamTests : IDisposable
{
    private readonly string _testDirectory;
    private readonly string _testFilePath;

    public DiskStreamTests()
    {
        _testDirectory = Path.Combine(Path.GetTempPath(), "SimpleDbTests", Guid.NewGuid().ToString());
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
    public async Task Constructor_Should_Create_New_File_When_Not_Exists()
    {
        // Act
        using var diskStream = new DiskStream(_testFilePath);

        // Assert
        await Assert.That(diskStream.FilePath).IsEqualTo(_testFilePath);
        await Assert.That(File.Exists(_testFilePath)).IsTrue();
        await Assert.That(diskStream.Size).IsEqualTo(0);
    }

    [Test]
    public async Task Constructor_Should_Open_Existing_File()
    {
        // Arrange
        File.WriteAllText(_testFilePath, "test content");

        // Act
        using var diskStream = new DiskStream(_testFilePath);

        // Assert
        await Assert.That(diskStream.FilePath).IsEqualTo(_testFilePath);
        await Assert.That(diskStream.Size).IsGreaterThan(0);
    }

    [Test]
    public async Task Constructor_Should_Throw_ArgumentException_For_Empty_Path()
    {
        // Act & Assert
        await Assert.That(() => new DiskStream("")).Throws<ArgumentException>();
    }

    [Test]
    public async Task Constructor_Should_Throw_ArgumentNullException_For_Null_Path()
    {
        // Act & Assert
        await Assert.That(() => new DiskStream(null!)).Throws<ArgumentNullException>();
    }

    [Test]
    public async Task Read_Should_Read_Correct_Number_Of_Bytes()
    {
        // Arrange
        var testData = new byte[] { 1, 2, 3, 4, 5 };
        File.WriteAllBytes(_testFilePath, testData);

        using var diskStream = new DiskStream(_testFilePath);
        var buffer = new byte[5];

        // Act
        var bytesRead = diskStream.Read(buffer, 0, buffer.Length);

        // Assert
        await Assert.That(bytesRead).IsEqualTo(5);
        await Assert.That(buffer.SequenceEqual(testData)).IsTrue();
    }

    [Test]
    public async Task Write_Should_Write_Correct_Number_Of_Bytes()
    {
        // Arrange
        var testData = new byte[] { 1, 2, 3, 4, 5 };

        byte[] fileContent;
        {
            using var diskStream = new DiskStream(_testFilePath);

            // Act
            diskStream.Write(testData, 0, testData.Length);

            // Assert
            await Assert.That(diskStream.Size).IsEqualTo(5);
        } // 释放 disk stream

        // Verify file content
        fileContent = File.ReadAllBytes(_testFilePath);
        await Assert.That(fileContent.SequenceEqual(testData)).IsTrue();
    }

    [Test]
    public async Task Flush_Should_Persist_Data()
    {
        // Arrange
        var testData = new byte[] { 1, 2, 3, 4, 5 };

        {
            using var diskStream = new DiskStream(_testFilePath);
            diskStream.Write(testData, 0, testData.Length);

            // Act
            diskStream.Flush();
        } // Dispose diskStream to release file handle

        // Assert
        var fileContent = File.ReadAllBytes(_testFilePath);
        await Assert.That(fileContent.SequenceEqual(testData)).IsTrue();
    }

    [Test]
    public async Task SetLength_Should_Change_File_Size()
    {
        // Arrange
        var testData = new byte[] { 1, 2, 3, 4, 5 };
        File.WriteAllBytes(_testFilePath, testData);

        byte[] fileContent;
        {
            using var diskStream = new DiskStream(_testFilePath);

            // Act
            diskStream.SetLength(10);

            // Assert
            await Assert.That(diskStream.Size).IsEqualTo(10);
        } // 释放 disk stream

        // Verify file was extended with zeros
        fileContent = File.ReadAllBytes(_testFilePath);
        await Assert.That(fileContent.Length).IsEqualTo(10);
        await Assert.That(fileContent[0] == 1).IsTrue();
        await Assert.That(fileContent[4] == 5).IsTrue();
        await Assert.That(fileContent[5] == 0).IsTrue();
        await Assert.That(fileContent[9] == 0).IsTrue();
    }

    [Test]
    public async Task SetLength_Smaller_Should_Truncate_File()
    {
        // Arrange
        var testData = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 };
        File.WriteAllBytes(_testFilePath, testData);

        byte[] fileContent;
        {
            using var diskStream = new DiskStream(_testFilePath);

            // Act
            diskStream.SetLength(5);

            // Assert
            await Assert.That(diskStream.Size).IsEqualTo(5);
        } // 释放 disk stream

        // Verify file was truncated
        fileContent = File.ReadAllBytes(_testFilePath);
        var expected = new byte[] { 1, 2, 3, 4, 5 };
        await Assert.That(fileContent.SequenceEqual(expected)).IsTrue();
    }

    [Test]
    public async Task Seek_Should_Change_Position()
    {
        // Arrange
        var testData = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 };
        File.WriteAllBytes(_testFilePath, testData);

        using var diskStream = new DiskStream(_testFilePath);

        // Act
        var newPosition = diskStream.Seek(3, SeekOrigin.Begin);

        // Assert
        await Assert.That(newPosition).IsEqualTo(3);
    }

    [Test]
    public async Task LockRegion_Should_Return_Lock_Handle()
    {
        // Arrange
        using var diskStream = new DiskStream(_testFilePath);

        // Act
        var lockHandle = diskStream.LockRegion(0, 100);

        // Assert
        await Assert.That(lockHandle).IsNotNull();

        // Cleanup
        diskStream.UnlockRegion(lockHandle);
    }

    [Test]
    public async Task UnlockRegion_Should_Work_Correctly()
    {
        // Arrange
        using var diskStream = new DiskStream(_testFilePath);
        var lockHandle = diskStream.LockRegion(0, 100);

        // Act & Assert - Should not throw
        diskStream.UnlockRegion(lockHandle);
    }

    [Test]
    public async Task GetStatistics_Should_Return_Correct_Information()
    {
        // Arrange
        File.WriteAllBytes(_testFilePath, new byte[] { 1, 2, 3, 4, 5 });

        using var diskStream = new DiskStream(_testFilePath);

        // Act
        var stats = diskStream.GetStatistics();

        // Assert
        await Assert.That(stats.FilePath).IsEqualTo(_testFilePath);
        await Assert.That(stats.Size).IsEqualTo(5);
        await Assert.That(stats.IsReadable).IsTrue();
        await Assert.That(stats.IsWritable).IsTrue();
        await Assert.That(stats.IsSeekable).IsTrue();
    }

    [Test]
    public async Task ReadPage_Should_Read_Page_Data()
    {
        // Arrange
        var testData = new byte[4096];
        for (int i = 0; i < testData.Length; i++)
        {
            testData[i] = (byte)(i % 256);
        }
        File.WriteAllBytes(_testFilePath, testData);

        using var diskStream = new DiskStream(_testFilePath);

        // Act
        var pageData = diskStream.ReadPage(0, testData.Length);

        // Assert
        await Assert.That(pageData.SequenceEqual(testData)).IsTrue();
    }

    [Test]
    public async Task WritePage_Should_Write_Page_Data()
    {
        // Arrange
        var pageData = new byte[4096];
        for (int i = 0; i < pageData.Length; i++)
        {
            pageData[i] = (byte)(i % 256);
        }

        byte[] fileContent;
        {
            using var diskStream = new DiskStream(_testFilePath);

            // Act
            diskStream.WritePage(0, pageData);

            // Assert
            await Assert.That(diskStream.Size).IsEqualTo(pageData.Length);
        } // 释放 disk stream

        // Verify file content
        fileContent = File.ReadAllBytes(_testFilePath);
        await Assert.That(fileContent.SequenceEqual(pageData)).IsTrue();
    }

    [Test]
    public async Task ReadPageAsync_Should_Work_Correctly()
    {
        // Arrange
        var testData = new byte[4096];
        for (int i = 0; i < testData.Length; i++)
        {
            testData[i] = (byte)(i % 256);
        }
        File.WriteAllBytes(_testFilePath, testData);

        using var diskStream = new DiskStream(_testFilePath);

        // Act
        var pageData = await diskStream.ReadPageAsync(0, testData.Length);

        // Assert
        await Assert.That(pageData.SequenceEqual(testData)).IsTrue();
    }

    [Test]
    public async Task WritePageAsync_Should_Work_Correctly()
    {
        // Arrange
        var pageData = new byte[4096];
        for (int i = 0; i < pageData.Length; i++)
        {
            pageData[i] = (byte)(i % 256);
        }

        byte[] fileContent;
        {
            using var diskStream = new DiskStream(_testFilePath);

            // Act
            await diskStream.WritePageAsync(0, pageData);

            // Assert
            await Assert.That(diskStream.Size).IsEqualTo(pageData.Length);
        } // 释放 disk stream

        // Verify file content
        fileContent = File.ReadAllBytes(_testFilePath);
        await Assert.That(fileContent.SequenceEqual(pageData)).IsTrue();
    }

    [Test]
    public async Task Dispose_Should_Release_File_Handle()
    {
        // Arrange
        using var diskStream = new DiskStream(_testFilePath);

        // Act
        diskStream.Dispose();

        // Assert - Should not throw when accessing properties after dispose
        await Assert.That(() => diskStream.Size).Throws<ObjectDisposedException>();
        await Assert.That(() => diskStream.Read(new byte[1], 0, 1)).Throws<ObjectDisposedException>();
        await Assert.That(() => diskStream.Write(new byte[1], 0, 1)).Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task Large_File_Operations_Should_Work()
    {
        // Arrange
        var largeData = new byte[1024 * 1024]; // 1MB
        for (int i = 0; i < largeData.Length; i++)
        {
            largeData[i] = (byte)(i % 256);
        }

        using var diskStream = new DiskStream(_testFilePath);

        // Act
        var startPosition = diskStream.Seek(0, SeekOrigin.Begin);
        diskStream.Write(largeData, 0, largeData.Length);
        diskStream.Flush();

        // Read back
        diskStream.Seek(startPosition, SeekOrigin.Begin);
        var readBuffer = new byte[largeData.Length];
        var totalRead = 0;
        while (totalRead < readBuffer.Length)
        {
            var bytesRead = diskStream.Read(readBuffer, totalRead, readBuffer.Length - totalRead);
            if (bytesRead == 0) break;
            totalRead += bytesRead;
        }

        // Assert
        await Assert.That(totalRead).IsEqualTo(largeData.Length);
        await Assert.That(readBuffer.SequenceEqual(largeData)).IsTrue();
    }

    [Test]
    public async Task Concurrent_Access_Should_Work()
    {
        // Arrange
        using var diskStream = new DiskStream(_testFilePath);
        var tasks = new List<Task<int>>();

        // Act - Write from multiple threads
        for (int i = 0; i < 10; i++)
        {
            var threadId = i;
            var data = new byte[] { (byte)threadId };
            tasks.Add(Task.Run(() =>
            {
                diskStream.Seek(threadId, SeekOrigin.Begin);
                diskStream.Write(data, 0, data.Length);
                return threadId;
            }));
        }

        await Task.WhenAll(tasks);

        // Assert
        await Assert.That(diskStream.Size).IsGreaterThan(0);

        // Verify data
        for (int i = 0; i < 10; i++)
        {
            diskStream.Seek(i, SeekOrigin.Begin);
            var buffer = new byte[1];
            var bytesRead = diskStream.Read(buffer, 0, 1);
            if (bytesRead > 0)
            {
                await Assert.That(buffer[0] == (byte)i).IsTrue();
            }
        }
    }

    [Test]
    public async Task Size_Property_Should_Reflect_File_Changes()
    {
        // Arrange
        using var diskStream = new DiskStream(_testFilePath);
        var data = new byte[] { 1, 2, 3 };

        // Act & Assert
        await Assert.That(diskStream.Size).IsEqualTo(0);

        diskStream.Write(data, 0, data.Length);
        await Assert.That(diskStream.Size).IsEqualTo(3);

        diskStream.SetLength(11);
        await Assert.That(diskStream.Size).IsEqualTo(11);

        diskStream.SetLength(5);
        await Assert.That(diskStream.Size).IsEqualTo(5);
    }

    [Test]
    public async Task Different_File_Access_Modes_Should_Work()
    {
        // Arrange
        File.WriteAllBytes(_testFilePath, new byte[] { 1, 2, 3 });

        // Act & Assert - Read-only
        using var readOnlyStream = new DiskStream(_testFilePath, FileAccess.Read);
        await Assert.That(readOnlyStream.IsReadable).IsTrue();
        await Assert.That(readOnlyStream.IsWritable).IsFalse();

        // Act & Assert - Write-only
        var writeOnlyPath = Path.Combine(_testDirectory, "writeonly.db");
        using var writeOnlyStream = new DiskStream(writeOnlyPath, FileAccess.Write);
        await Assert.That(writeOnlyStream.IsReadable).IsFalse();
        await Assert.That(writeOnlyStream.IsWritable).IsTrue();
    }
}