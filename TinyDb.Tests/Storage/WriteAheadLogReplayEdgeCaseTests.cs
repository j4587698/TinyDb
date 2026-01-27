using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using TinyDb.Storage;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Storage;

/// <summary>
/// Edge case tests for WriteAheadLog ReplayAsync to improve coverage
/// </summary>
[NotInParallel]
public class WriteAheadLogReplayEdgeCaseTests
{
    private string _dbFile = null!;
    private const int PageSize = 8192;

    [Before(Test)]
    public void Setup()
    {
        _dbFile = Path.Combine(Path.GetTempPath(), $"wal_replay_{Guid.NewGuid():N}.db");
    }

    private string GetWalFile(string dbFile)
    {
        var directory = Path.GetDirectoryName(dbFile) ?? string.Empty;
        var name = Path.GetFileNameWithoutExtension(dbFile);
        var ext = Path.GetExtension(dbFile).TrimStart('.');
        return Path.Combine(directory, $"{name}-wal.{ext}");
    }

    [After(Test)]
    public void Cleanup()
    {
        var walFile = GetWalFile(_dbFile);
        try { if (File.Exists(_dbFile)) File.Delete(_dbFile); } catch { }
        try { if (File.Exists(walFile)) File.Delete(walFile); } catch { }
    }

    [Test]
    public async Task ReplayAsync_EmptyLog_ShouldNotCallApply()
    {
        using var wal = new WriteAheadLog(_dbFile, PageSize, true);
        
        var applyCalled = false;
        await wal.ReplayAsync((pageId, data) =>
        {
            applyCalled = true;
            return Task.CompletedTask;
        });

        await Assert.That(applyCalled).IsFalse();
    }

    [Test]
    public async Task ReplayAsync_WalDisabled_ShouldNotCallApply()
    {
        using var wal = new WriteAheadLog(_dbFile, PageSize, enabled: false);
        
        var applyCalled = false;
        await wal.ReplayAsync((pageId, data) =>
        {
            applyCalled = true;
            return Task.CompletedTask;
        });

        await Assert.That(applyCalled).IsFalse();
    }

    [Test]
    public async Task ReplayAsync_NullApplyFunc_ShouldThrow()
    {
        using var wal = new WriteAheadLog(_dbFile, PageSize, true);
        
        await Assert.That(async () => await wal.ReplayAsync(null!))
            .Throws<ArgumentNullException>();
    }

    [Test]
    public async Task ReplayAsync_IncompleteHeader_ShouldStopReplay()
    {
        var walFile = GetWalFile(_dbFile);
        
        // Create WAL file with incomplete header (less than HeaderSize = 9 bytes)
        File.WriteAllBytes(walFile, new byte[] { 0x01, 0x02, 0x03 });
        
        using var wal = new WriteAheadLog(_dbFile, PageSize, true);
        var appliedPages = new List<uint>();
        
        await wal.ReplayAsync((pageId, data) =>
        {
            appliedPages.Add(pageId);
            return Task.CompletedTask;
        });

        await Assert.That(appliedPages).Count().IsEqualTo(0);
    }

    [Test]
    public async Task ReplayAsync_InvalidEntryType_ShouldStopReplay()
    {
        var walFile = GetWalFile(_dbFile);
        
        // Write entry with valid header structure but invalid type (not 0x01)
        var headerBuffer = new byte[13]; // HeaderSize + 4 for CRC
        headerBuffer[0] = 0xFF; // Invalid type
        BinaryPrimitives.WriteUInt32LittleEndian(headerBuffer.AsSpan(1, 4), 1); // pageId
        BinaryPrimitives.WriteInt32LittleEndian(headerBuffer.AsSpan(5, 4), 10); // length
        BinaryPrimitives.WriteUInt32LittleEndian(headerBuffer.AsSpan(9, 4), 0); // crc
        
        File.WriteAllBytes(walFile, headerBuffer);
        
        using var wal = new WriteAheadLog(_dbFile, PageSize, true);
        var appliedPages = new List<uint>();
        
        await wal.ReplayAsync((pageId, data) =>
        {
            appliedPages.Add(pageId);
            return Task.CompletedTask;
        });

        await Assert.That(appliedPages).Count().IsEqualTo(0);
    }

    [Test]
    public async Task ReplayAsync_InvalidLength_Negative_ShouldStopReplay()
    {
        var walFile = GetWalFile(_dbFile);
        
        // Write entry with negative length
        var headerBuffer = new byte[13];
        headerBuffer[0] = 0x01; // Valid type
        BinaryPrimitives.WriteUInt32LittleEndian(headerBuffer.AsSpan(1, 4), 1); // pageId
        BinaryPrimitives.WriteInt32LittleEndian(headerBuffer.AsSpan(5, 4), -100); // invalid negative length
        BinaryPrimitives.WriteUInt32LittleEndian(headerBuffer.AsSpan(9, 4), 0); // crc
        
        File.WriteAllBytes(walFile, headerBuffer);
        
        using var wal = new WriteAheadLog(_dbFile, PageSize, true);
        var appliedPages = new List<uint>();
        
        await wal.ReplayAsync((pageId, data) =>
        {
            appliedPages.Add(pageId);
            return Task.CompletedTask;
        });

        await Assert.That(appliedPages).Count().IsEqualTo(0);
    }

    [Test]
    public async Task ReplayAsync_InvalidLength_TooLarge_ShouldStopReplay()
    {
        var walFile = GetWalFile(_dbFile);
        
        // Write entry with length larger than max record size
        var headerBuffer = new byte[13];
        headerBuffer[0] = 0x01; // Valid type
        BinaryPrimitives.WriteUInt32LittleEndian(headerBuffer.AsSpan(1, 4), 1); // pageId
        BinaryPrimitives.WriteInt32LittleEndian(headerBuffer.AsSpan(5, 4), PageSize + 1000); // too large
        BinaryPrimitives.WriteUInt32LittleEndian(headerBuffer.AsSpan(9, 4), 0); // crc
        
        File.WriteAllBytes(walFile, headerBuffer);
        
        using var wal = new WriteAheadLog(_dbFile, PageSize, true);
        var appliedPages = new List<uint>();
        
        await wal.ReplayAsync((pageId, data) =>
        {
            appliedPages.Add(pageId);
            return Task.CompletedTask;
        });

        await Assert.That(appliedPages).Count().IsEqualTo(0);
    }

    [Test]
    public async Task ReplayAsync_IncompleteData_ShouldStopReplay()
    {
        var walFile = GetWalFile(_dbFile);
        
        // Write header claiming 100 bytes of data, but only provide 10
        var headerBuffer = new byte[13];
        headerBuffer[0] = 0x01; // Valid type
        BinaryPrimitives.WriteUInt32LittleEndian(headerBuffer.AsSpan(1, 4), 1); // pageId
        BinaryPrimitives.WriteInt32LittleEndian(headerBuffer.AsSpan(5, 4), 100); // length = 100
        BinaryPrimitives.WriteUInt32LittleEndian(headerBuffer.AsSpan(9, 4), 0); // crc
        
        var fileContent = new byte[headerBuffer.Length + 10]; // Only 10 bytes of data
        Array.Copy(headerBuffer, fileContent, headerBuffer.Length);
        File.WriteAllBytes(walFile, fileContent);
        
        using var wal = new WriteAheadLog(_dbFile, PageSize, true);
        var appliedPages = new List<uint>();
        
        await wal.ReplayAsync((pageId, data) =>
        {
            appliedPages.Add(pageId);
            return Task.CompletedTask;
        });

        await Assert.That(appliedPages).Count().IsEqualTo(0);
    }

    [Test]
    public async Task ReplayAsync_CrcMismatch_ShouldStopReplay()
    {
        var walFile = GetWalFile(_dbFile);
        
        // Create valid data
        var testData = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        var correctCrc = System.IO.Hashing.Crc32.HashToUInt32(testData);
        
        // Write header with wrong CRC
        var headerBuffer = new byte[13];
        headerBuffer[0] = 0x01; // Valid type
        BinaryPrimitives.WriteUInt32LittleEndian(headerBuffer.AsSpan(1, 4), 1); // pageId
        BinaryPrimitives.WriteInt32LittleEndian(headerBuffer.AsSpan(5, 4), testData.Length); // length
        BinaryPrimitives.WriteUInt32LittleEndian(headerBuffer.AsSpan(9, 4), correctCrc + 1); // wrong CRC
        
        var fileContent = new byte[headerBuffer.Length + testData.Length];
        Array.Copy(headerBuffer, fileContent, headerBuffer.Length);
        Array.Copy(testData, 0, fileContent, headerBuffer.Length, testData.Length);
        File.WriteAllBytes(walFile, fileContent);
        
        using var wal = new WriteAheadLog(_dbFile, PageSize, true);
        var appliedPages = new List<uint>();
        
        await wal.ReplayAsync((pageId, data) =>
        {
            appliedPages.Add(pageId);
            return Task.CompletedTask;
        });

        await Assert.That(appliedPages).Count().IsEqualTo(0);
    }

    [Test]
    public async Task ReplayAsync_ValidEntry_ShouldApply()
    {
        var walFile = GetWalFile(_dbFile);
        
        // Create valid entry manually
        var testData = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        var crc = System.IO.Hashing.Crc32.HashToUInt32(testData);
        
        var headerBuffer = new byte[13];
        headerBuffer[0] = 0x01; // Valid type
        BinaryPrimitives.WriteUInt32LittleEndian(headerBuffer.AsSpan(1, 4), 42); // pageId = 42
        BinaryPrimitives.WriteInt32LittleEndian(headerBuffer.AsSpan(5, 4), testData.Length);
        BinaryPrimitives.WriteUInt32LittleEndian(headerBuffer.AsSpan(9, 4), crc);
        
        var fileContent = new byte[headerBuffer.Length + testData.Length];
        Array.Copy(headerBuffer, fileContent, headerBuffer.Length);
        Array.Copy(testData, 0, fileContent, headerBuffer.Length, testData.Length);
        File.WriteAllBytes(walFile, fileContent);
        
        using var wal = new WriteAheadLog(_dbFile, PageSize, true);
        var appliedPages = new List<(uint pageId, byte[] data)>();
        
        await wal.ReplayAsync((pageId, data) =>
        {
            appliedPages.Add((pageId, data));
            return Task.CompletedTask;
        });

        await Assert.That(appliedPages).Count().IsEqualTo(1);
        await Assert.That(appliedPages[0].pageId).IsEqualTo(42u);
        await Assert.That(appliedPages[0].data.Length).IsEqualTo(testData.Length);
    }

    [Test]
    public async Task ReplayAsync_MultipleEntries_ShouldApplyAll()
    {
        using (var wal = new WriteAheadLog(_dbFile, PageSize, true))
        {
            for (int i = 1; i <= 5; i++)
            {
                var page = new Page((uint)i, PageSize);
                page.WriteData(0, new byte[] { (byte)i, (byte)(i + 1), (byte)(i + 2) });
                wal.AppendPage(page);
            }
            await wal.FlushLogAsync();
        }

        var appliedPages = new List<uint>();
        using (var wal = new WriteAheadLog(_dbFile, PageSize, true))
        {
            await wal.ReplayAsync((pageId, data) =>
            {
                appliedPages.Add(pageId);
                return Task.CompletedTask;
            });
        }

        await Assert.That(appliedPages).Count().IsEqualTo(5);
        for (uint i = 1; i <= 5; i++)
        {
            await Assert.That(appliedPages).Contains(i);
        }
    }

    [Test]
    public async Task ReplayAsync_ValidThenCorrupt_ShouldApplyOnlyValid()
    {
        var walFile = GetWalFile(_dbFile);
        
        // Write two valid entries, then corrupt the third
        using (var wal = new WriteAheadLog(_dbFile, PageSize, true))
        {
            var page1 = new Page(1, PageSize);
            page1.WriteData(0, new byte[] { 1, 2, 3 });
            wal.AppendPage(page1);

            var page2 = new Page(2, PageSize);
            page2.WriteData(0, new byte[] { 4, 5, 6 });
            wal.AppendPage(page2);
            
            await wal.FlushLogAsync();
        }

        // Add corrupted entry
        var bytes = File.ReadAllBytes(walFile);
        var corruptedBytes = new byte[bytes.Length + 13];
        Array.Copy(bytes, corruptedBytes, bytes.Length);
        corruptedBytes[bytes.Length] = 0xFF; // Invalid type
        File.WriteAllBytes(walFile, corruptedBytes);

        var appliedPages = new List<uint>();
        using (var wal = new WriteAheadLog(_dbFile, PageSize, true))
        {
            await wal.ReplayAsync((pageId, data) =>
            {
                appliedPages.Add(pageId);
                return Task.CompletedTask;
            });
        }

        await Assert.That(appliedPages).Count().IsEqualTo(2);
        await Assert.That(appliedPages).Contains(1u);
        await Assert.That(appliedPages).Contains(2u);
    }

    [Test]
    public async Task ReplayAsync_AfterSuccessfulReplay_LogShouldBeCleared()
    {
        var walFile = GetWalFile(_dbFile);
        
        using (var wal = new WriteAheadLog(_dbFile, PageSize, true))
        {
            var page = new Page(1, PageSize);
            page.WriteData(0, new byte[] { 1, 2, 3 });
            wal.AppendPage(page);
            await wal.FlushLogAsync();
        }

        using (var wal = new WriteAheadLog(_dbFile, PageSize, true))
        {
            await wal.ReplayAsync((pageId, data) => Task.CompletedTask);
            await Assert.That(wal.HasPendingEntries).IsFalse();
        }

        await Assert.That(new FileInfo(walFile).Length).IsEqualTo(0);
    }

    [Test]
    public async Task ReplayAsync_ApplyThrowsException_ShouldPropagate()
    {
        using (var wal = new WriteAheadLog(_dbFile, PageSize, true))
        {
            var page = new Page(1, PageSize);
            page.WriteData(0, new byte[] { 1, 2, 3 });
            wal.AppendPage(page);
            await wal.FlushLogAsync();
        }

        using var wal2 = new WriteAheadLog(_dbFile, PageSize, true);
        await Assert.That(async () => 
        {
            await wal2.ReplayAsync((pageId, data) => 
            {
                throw new InvalidOperationException("Test exception");
            });
        }).Throws<InvalidOperationException>();
    }

    [Test]
    public async Task ReplayAsync_PartialHeaderAfterValidEntry_ShouldStopAndTruncate()
    {
        var walFile = GetWalFile(_dbFile);
        
        // Write a valid entry
        using (var wal = new WriteAheadLog(_dbFile, PageSize, true))
        {
            var page = new Page(1, PageSize);
            page.WriteData(0, new byte[] { 1, 2, 3 });
            wal.AppendPage(page);
            await wal.FlushLogAsync();
        }

        // Append a partial header (less than HeaderSize)
        var bytes = File.ReadAllBytes(walFile);
        var withPartial = new byte[bytes.Length + 5]; // Only 5 bytes of next header
        Array.Copy(bytes, withPartial, bytes.Length);
        withPartial[bytes.Length] = 0x01; // Valid type
        File.WriteAllBytes(walFile, withPartial);

        var appliedPages = new List<uint>();
        using (var wal = new WriteAheadLog(_dbFile, PageSize, true))
        {
            await wal.ReplayAsync((pageId, data) =>
            {
                appliedPages.Add(pageId);
                return Task.CompletedTask;
            });
        }

        await Assert.That(appliedPages).Count().IsEqualTo(1);
        await Assert.That(appliedPages[0]).IsEqualTo(1u);
    }

    [Test]
    public async Task ReplayAsync_ZeroLengthEntry_ShouldStopReplay()
    {
        var walFile = GetWalFile(_dbFile);
        
        // Write entry with zero length
        var headerBuffer = new byte[13];
        headerBuffer[0] = 0x01; // Valid type
        BinaryPrimitives.WriteUInt32LittleEndian(headerBuffer.AsSpan(1, 4), 1); // pageId
        BinaryPrimitives.WriteInt32LittleEndian(headerBuffer.AsSpan(5, 4), 0); // zero length
        BinaryPrimitives.WriteUInt32LittleEndian(headerBuffer.AsSpan(9, 4), 0); // crc
        
        File.WriteAllBytes(walFile, headerBuffer);
        
        using var wal = new WriteAheadLog(_dbFile, PageSize, true);
        var appliedPages = new List<uint>();
        
        await wal.ReplayAsync((pageId, data) =>
        {
            appliedPages.Add(pageId);
            return Task.CompletedTask;
        });

        await Assert.That(appliedPages).Count().IsEqualTo(0);
    }

    [Test]
    public async Task ReplayAsync_CancellationToken_ShouldBeCancellable()
    {
        using (var wal = new WriteAheadLog(_dbFile, PageSize, true))
        {
            // Write several entries
            for (int i = 1; i <= 10; i++)
            {
                var page = new Page((uint)i, PageSize);
                page.WriteData(0, new byte[] { (byte)i });
                wal.AppendPage(page);
            }
            await wal.FlushLogAsync();
        }

        var cts = new CancellationTokenSource();
        var appliedCount = 0;
        
        using var wal2 = new WriteAheadLog(_dbFile, PageSize, true);
        
        // Test that passing already cancelled token throws
        cts.Cancel();
        await Assert.That(async () =>
        {
            await wal2.ReplayAsync((pageId, data) =>
            {
                appliedCount++;
                return Task.CompletedTask;
            }, cts.Token);
        }).Throws<OperationCanceledException>();
    }

    [Test]
    public async Task ReplayAsync_IncompleteHeaderBetween9And13Bytes_ShouldStopReplay()
    {
        var walFile = GetWalFile(_dbFile);
        
        // Write only 10 bytes (between HeaderSize=9 and FullHeaderSize=13)
        var headerBuffer = new byte[10];
        headerBuffer[0] = 0x01; // Valid type
        BinaryPrimitives.WriteUInt32LittleEndian(headerBuffer.AsSpan(1, 4), 1); // pageId
        BinaryPrimitives.WriteInt32LittleEndian(headerBuffer.AsSpan(5, 4), 10); // length
        // Missing last 3 bytes of CRC
        headerBuffer[9] = 0x00;
        
        File.WriteAllBytes(walFile, headerBuffer);
        
        using var wal = new WriteAheadLog(_dbFile, PageSize, true);
        var appliedPages = new List<uint>();
        
        await wal.ReplayAsync((pageId, data) =>
        {
            appliedPages.Add(pageId);
            return Task.CompletedTask;
        });

        // Should stop because header is incomplete (between 9-13 bytes)
        await Assert.That(appliedPages).Count().IsEqualTo(0);
    }
}
