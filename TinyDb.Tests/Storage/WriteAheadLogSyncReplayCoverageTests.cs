using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Threading.Tasks;
using TinyDb.Storage;
using TinyDb.Tests.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Storage;

[NotInParallel]
public sealed class WriteAheadLogSyncReplayCoverageTests : IDisposable
{
    private const int PageSize = 8192;
    private readonly string _dbFile;

    public WriteAheadLogSyncReplayCoverageTests()
    {
        _dbFile = Path.Combine(Path.GetTempPath(), $"wal_sync_cov_{Guid.NewGuid():N}.db");
    }

    public void Dispose()
    {
        try { if (File.Exists(_dbFile)) File.Delete(_dbFile); } catch { }
        try { if (File.Exists(GetWalFile(_dbFile))) File.Delete(GetWalFile(_dbFile)); } catch { }
    }

    [Test]
    public async Task FlushLog_WithPendingEntries_ShouldAdvanceFlushedLsn()
    {
        using var wal = new WriteAheadLog(_dbFile, PageSize, true);
        var page = new Page(1, PageSize);
        page.WriteData(0, new byte[] { 1, 2, 3 });
        wal.AppendPage(page);

        wal.FlushLog();

        var flushedLsn = UnsafeAccessors.WriteAheadLogAccessor.FlushedLsn(wal);
        await Assert.That(flushedLsn > 0).IsTrue();
    }

    [Test]
    public async Task Replay_SyncInvalidRecords_ShouldStopWithoutApply()
    {
        var walFile = GetWalFile(_dbFile);

        async Task ExecuteAndAssertNoApply(byte[] payload)
        {
            File.WriteAllBytes(walFile, payload);

            using var wal = new WriteAheadLog(_dbFile, PageSize, true);
            var applied = new List<uint>();
            wal.Replay((id, _) => applied.Add(id));
            await Assert.That(applied.Count).IsEqualTo(0);
        }

        var invalidTypeHeader = new byte[13];
        invalidTypeHeader[0] = 0x7F;
        BinaryPrimitives.WriteUInt32LittleEndian(invalidTypeHeader.AsSpan(1, 4), 1);
        BinaryPrimitives.WriteInt32LittleEndian(invalidTypeHeader.AsSpan(5, 4), 10);
        BinaryPrimitives.WriteUInt32LittleEndian(invalidTypeHeader.AsSpan(9, 4), 0);
        await ExecuteAndAssertNoApply(invalidTypeHeader);

        var incompleteHeader = new byte[10];
        incompleteHeader[0] = 0x01;
        BinaryPrimitives.WriteUInt32LittleEndian(incompleteHeader.AsSpan(1, 4), 1);
        BinaryPrimitives.WriteInt32LittleEndian(incompleteHeader.AsSpan(5, 4), 10);
        await ExecuteAndAssertNoApply(incompleteHeader);

        var invalidLengthHeader = new byte[13];
        invalidLengthHeader[0] = 0x01;
        BinaryPrimitives.WriteUInt32LittleEndian(invalidLengthHeader.AsSpan(1, 4), 1);
        BinaryPrimitives.WriteInt32LittleEndian(invalidLengthHeader.AsSpan(5, 4), -1);
        BinaryPrimitives.WriteUInt32LittleEndian(invalidLengthHeader.AsSpan(9, 4), 0);
        await ExecuteAndAssertNoApply(invalidLengthHeader);

        var data = new byte[] { 1, 2, 3, 4 };
        var wrongCrcHeader = new byte[13];
        wrongCrcHeader[0] = 0x01;
        BinaryPrimitives.WriteUInt32LittleEndian(wrongCrcHeader.AsSpan(1, 4), 9);
        BinaryPrimitives.WriteInt32LittleEndian(wrongCrcHeader.AsSpan(5, 4), data.Length);
        BinaryPrimitives.WriteUInt32LittleEndian(wrongCrcHeader.AsSpan(9, 4), System.IO.Hashing.Crc32.HashToUInt32(data) + 1);
        var crcPayload = new byte[wrongCrcHeader.Length + data.Length];
        Array.Copy(wrongCrcHeader, crcPayload, wrongCrcHeader.Length);
        Array.Copy(data, 0, crcPayload, wrongCrcHeader.Length, data.Length);
        await ExecuteAndAssertNoApply(crcPayload);
    }

    [Test]
    public async Task Replay_ValidThenCorrupt_ShouldApplyValidAndTruncate()
    {
        var walFile = GetWalFile(_dbFile);

        using (var wal = new WriteAheadLog(_dbFile, PageSize, true))
        {
            for (uint i = 1; i <= 2; i++)
            {
                var page = new Page(i, PageSize);
                page.WriteData(0, new byte[] { (byte)i });
                wal.AppendPage(page);
            }
            wal.FlushLog();
        }

        var validBytes = File.ReadAllBytes(walFile);
        var withCorruptTail = new byte[validBytes.Length + 13];
        Array.Copy(validBytes, withCorruptTail, validBytes.Length);
        withCorruptTail[validBytes.Length] = 0xFF;
        File.WriteAllBytes(walFile, withCorruptTail);

        long beforeReplayLength = new FileInfo(walFile).Length;

        var applied = new List<uint>();
        using (var wal = new WriteAheadLog(_dbFile, PageSize, true))
        {
            wal.Replay((id, _) => applied.Add(id));
        }

        long afterReplayLength = new FileInfo(walFile).Length;
        await Assert.That(applied.Count).IsEqualTo(2);
        await Assert.That(applied.Contains(1u)).IsTrue();
        await Assert.That(applied.Contains(2u)).IsTrue();
        await Assert.That(afterReplayLength < beforeReplayLength).IsTrue();
    }

    [Test]
    public async Task Replay_IncompleteDataRecord_ShouldStopWithoutApply()
    {
        var walFile = GetWalFile(_dbFile);

        var partialData = new byte[] { 1, 2, 3 };
        var header = new byte[13];
        header[0] = 0x01;
        BinaryPrimitives.WriteUInt32LittleEndian(header.AsSpan(1, 4), 1);
        BinaryPrimitives.WriteInt32LittleEndian(header.AsSpan(5, 4), partialData.Length + 8);
        BinaryPrimitives.WriteUInt32LittleEndian(header.AsSpan(9, 4), 0);

        var payload = new byte[header.Length + partialData.Length];
        Array.Copy(header, payload, header.Length);
        Array.Copy(partialData, 0, payload, header.Length, partialData.Length);
        File.WriteAllBytes(walFile, payload);

        using var wal = new WriteAheadLog(_dbFile, PageSize, true);
        var applied = new List<uint>();
        wal.Replay((id, _) => applied.Add(id));

        await Assert.That(applied.Count).IsEqualTo(0);
    }

    [Test]
    public async Task Replay_WhenApplyThrows_ShouldPropagate()
    {
        using (var wal = new WriteAheadLog(_dbFile, PageSize, true))
        {
            var page = new Page(1, PageSize);
            page.WriteData(0, new byte[] { 1 });
            wal.AppendPage(page);
            wal.FlushLog();
        }

        using var replayWal = new WriteAheadLog(_dbFile, PageSize, true);
        await Assert.That(() =>
        {
            replayWal.Replay((_, _) => throw new InvalidOperationException("apply-failed"));
        }).Throws<InvalidOperationException>();
    }

    [Test]
    [SkipInAot]
    public async Task Replay_WhenRecoveryTruncateAlsoFails_ShouldThrowAggregate()
    {
        using (var wal = new WriteAheadLog(_dbFile, PageSize, true))
        {
            var page = new Page(1, PageSize);
            page.WriteData(0, new byte[] { 1 });
            wal.AppendPage(page);
            wal.FlushLog();
        }

        using var replayWal = new WriteAheadLog(_dbFile, PageSize, true);
        var streamField = typeof(WriteAheadLog).GetField("_stream", BindingFlags.NonPublic | BindingFlags.Instance)
            ?? throw new MissingFieldException(typeof(WriteAheadLog).FullName, "_stream");
        ((FileStream?)streamField.GetValue(replayWal))!.Dispose();

        await Assert.That(() => replayWal.Replay((_, _) => { }))
            .Throws<AggregateException>();
    }

    [Test]
    [SkipInAot]
    public async Task ReplayAsync_WhenRecoveryTruncateAlsoFails_ShouldThrowAggregate()
    {
        using (var wal = new WriteAheadLog(_dbFile, PageSize, true))
        {
            var page = new Page(1, PageSize);
            page.WriteData(0, new byte[] { 1 });
            wal.AppendPage(page);
            wal.FlushLog();
        }

        using var replayWal = new WriteAheadLog(_dbFile, PageSize, true);
        var streamField = typeof(WriteAheadLog).GetField("_stream", BindingFlags.NonPublic | BindingFlags.Instance)
            ?? throw new MissingFieldException(typeof(WriteAheadLog).FullName, "_stream");
        ((FileStream?)streamField.GetValue(replayWal))!.Dispose();

        await Assert.That(async () => await replayWal.ReplayAsync((_, _) => Task.CompletedTask))
            .Throws<AggregateException>();
    }

    private static string GetWalFile(string dbFile)
    {
        var directory = Path.GetDirectoryName(dbFile) ?? string.Empty;
        var name = Path.GetFileNameWithoutExtension(dbFile);
        var ext = Path.GetExtension(dbFile).TrimStart('.');
        return Path.Combine(directory, $"{name}-wal.{ext}");
    }
}
