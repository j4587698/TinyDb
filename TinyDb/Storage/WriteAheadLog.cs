using System;
using System.Buffers.Binary;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace TinyDb.Storage;

/// <summary>
/// 简化版写前日志实现，用于保证崩溃恢复能力。
/// </summary>
public sealed class WriteAheadLog : IDisposable
{
    private const byte EntryTypePage = 0x1;
    private const int HeaderSize = 9;

    private readonly string _logFilePath;
    private readonly FileStream? _stream;
    private readonly SemaphoreSlim _mutex = new(1, 1);
    private readonly int _maxRecordSize;
    private bool _disposed;
    private bool _hasPendingEntries;
    private long _flushedLSN;

    /// <summary>
    /// 是否启用 WAL
    /// </summary>
    public bool IsEnabled { get; }
    
    /// <summary>
    /// 当前已刷盘的 LSN
    /// </summary>
    public long FlushedLSN => _flushedLSN;

    /// <summary>
    /// 是否有未提交的日志记录
    /// </summary>
    public bool HasPendingEntries => IsEnabled && _hasPendingEntries;

    public WriteAheadLog(string databaseFilePath, int pageSize, bool enabled, string? walFileNameFormat = null)
    {
        if (string.IsNullOrWhiteSpace(databaseFilePath))
            throw new ArgumentException("Database file path cannot be null or empty", nameof(databaseFilePath));

        _maxRecordSize = Math.Max(pageSize, 0);
        IsEnabled = enabled;
        _logFilePath = GenerateWalFilePath(databaseFilePath, walFileNameFormat ?? "{name}-wal.{ext}");

        if (!IsEnabled)
        {
            TryDeleteExistingLog();
            return;
        }

        var directory = Path.GetDirectoryName(_logFilePath);
        if (!string.IsNullOrEmpty(directory))
        {
            Directory.CreateDirectory(directory);
        }

        _stream = new FileStream(
            _logFilePath,
            FileMode.OpenOrCreate,
            FileAccess.ReadWrite,
            FileShare.Read,
            bufferSize: Math.Max(pageSize, 4096),
            FileOptions.Asynchronous | FileOptions.SequentialScan);

        _stream.Seek(0, SeekOrigin.End);
        _hasPendingEntries = _stream.Length > 0;
    }

    /// <summary>
    /// 生成WAL文件路径，支持格式化占位符
    /// </summary>
    /// <param name="databaseFilePath">数据库文件路径</param>
    /// <param name="format">文件名格式，支持占位符：{name} = 数据库名称，{ext} = 原扩展名</param>
    /// <returns>WAL文件路径</returns>
    private static string GenerateWalFilePath(string databaseFilePath, string format)
    {
        if (string.IsNullOrWhiteSpace(format))
        {
            // 如果格式为空，使用默认行为
            return Path.ChangeExtension(databaseFilePath, ".wal");
        }

        var directory = Path.GetDirectoryName(databaseFilePath) ?? string.Empty;
        var fileNameWithoutExt = Path.GetFileNameWithoutExtension(databaseFilePath);
        var extension = Path.GetExtension(databaseFilePath).TrimStart('.');

        // 替换占位符
        var formattedFileName = format
            .Replace("{name}", fileNameWithoutExt)
            .Replace("{ext}", extension);

        // 确保文件名以.db结尾（如果格式中没有包含扩展名）
        if (!Path.HasExtension(formattedFileName) && !string.IsNullOrEmpty(extension))
        {
            formattedFileName += $".{extension}";
        }

        return Path.Combine(directory, formattedFileName);
    }

    private void TryDeleteExistingLog()
    {
        try
        {
            if (File.Exists(_logFilePath))
            {
                File.Delete(_logFilePath);
            }
        }
        catch
        {
            // 忽略清理失败
        }
    }

    public async Task AppendPageAsync(Page page, CancellationToken cancellationToken = default)
    {
        if (!IsEnabled) return;

        await _mutex.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var stream = _stream!;
            long lsn = stream.Position;
            
            // 更新页面头部的 LSN
            var header = page.Header;
            header.LSN = lsn;
            page.UpdateHeader(header);
            
            var data = page.Snapshot(includeUnusedTail: true);
            await WriteEntryAsync(page.PageID, data, cancellationToken).ConfigureAwait(false);
            _hasPendingEntries = true;
        }
        finally
        {
            _mutex.Release();
        }
    }

    public void AppendPage(Page page)
    {
        if (!IsEnabled) return;

        _mutex.Wait();
        try
        {
            var stream = _stream!;
            long lsn = stream.Position;
            
            // 更新页面头部的 LSN
            var header = page.Header;
            header.LSN = lsn;
            page.UpdateHeader(header);

            var data = page.Snapshot(includeUnusedTail: true);
            WriteEntry(page.PageID, data);
            _hasPendingEntries = true;
        }
        finally
        {
            _mutex.Release();
        }
    }

    private void WriteEntry(uint pageId, byte[] data)
    {
        var stream = _stream!;
        
        // 计算校验和
        var crc32 = System.IO.Hashing.Crc32.HashToUInt32(data);
        
        // 头部格式: [Type(1)] [PageId(4)] [Length(4)] [CRC32(4)]
        Span<byte> header = stackalloc byte[HeaderSize + 4]; // HeaderSize is 9, +4 for CRC = 13
        header[0] = EntryTypePage;
        BinaryPrimitives.WriteUInt32LittleEndian(header[1..5], pageId);
        BinaryPrimitives.WriteInt32LittleEndian(header[5..9], data.Length);
        BinaryPrimitives.WriteUInt32LittleEndian(header[9..13], crc32);
        
        stream.Write(header);
        stream.Write(data, 0, data.Length);
    }

    private async Task WriteEntryAsync(uint pageId, byte[] data, CancellationToken cancellationToken)
    {
        var stream = _stream!;
        
        // 计算校验和
        var crc32 = System.IO.Hashing.Crc32.HashToUInt32(data);
        
        byte[] header = new byte[HeaderSize + 4];
        header[0] = EntryTypePage;
        BinaryPrimitives.WriteUInt32LittleEndian(header.AsSpan(1, 4), pageId);
        BinaryPrimitives.WriteInt32LittleEndian(header.AsSpan(5, 4), data.Length);
        BinaryPrimitives.WriteUInt32LittleEndian(header.AsSpan(9, 4), crc32);
        
        await stream.WriteAsync(header, 0, header.Length, cancellationToken).ConfigureAwait(false);
        await stream.WriteAsync(data, 0, data.Length, cancellationToken).ConfigureAwait(false);
    }

    public async Task FlushToLSNAsync(long targetLSN, CancellationToken cancellationToken = default)
    {
        if (!IsEnabled || targetLSN <= _flushedLSN) return;

        await _mutex.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (targetLSN > _flushedLSN)
            {
                _stream!.Flush(true);
                _flushedLSN = _stream.Position;
            }
        }
        finally
        {
            _mutex.Release();
        }
    }

    public void FlushToLSN(long targetLSN)
    {
        if (!IsEnabled || targetLSN <= _flushedLSN) return;

        _mutex.Wait();
        try
        {
            if (targetLSN > _flushedLSN)
            {
                _stream!.Flush(true);
                _flushedLSN = _stream.Position;
            }
        }
        finally
        {
            _mutex.Release();
        }
    }

    public async Task FlushLogAsync(CancellationToken cancellationToken = default)
    {
        if (!IsEnabled || !_hasPendingEntries) return;

        await _mutex.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_hasPendingEntries)
            {
                _stream!.Flush(true);
                _flushedLSN = _stream.Position;
            }
        }
        finally
        {
            _mutex.Release();
        }
    }

    public void FlushLog()
    {
        if (!IsEnabled || !_hasPendingEntries) return;

        _mutex.Wait();
        try
        {
            if (_hasPendingEntries)
            {
                _stream!.Flush(true);
                _flushedLSN = _stream.Position;
            }
        }
        finally
        {
            _mutex.Release();
        }
    }

    public void Synchronize(Action flushData)
    {
        if (flushData == null) throw new ArgumentNullException(nameof(flushData));

        if (!IsEnabled)
        {
            flushData();
            return;
        }

        _mutex.Wait();
        try
        {
            var stream = _stream!;

            if (_hasPendingEntries)
            {
                stream.Flush(true);
                _flushedLSN = stream.Position;
            }

            flushData();

            if (_hasPendingEntries)
            {
                stream.SetLength(0);
                stream.Seek(0, SeekOrigin.End);
                stream.Flush(true);
                _hasPendingEntries = false;
                _flushedLSN = stream.Position;
            }
        }
        finally
        {
            _mutex.Release();
        }
    }

    public async Task SynchronizeAsync(Func<CancellationToken, Task> flushDataAsync, CancellationToken cancellationToken = default)
    {
        if (flushDataAsync == null) throw new ArgumentNullException(nameof(flushDataAsync));

        if (!IsEnabled)
        {
            await flushDataAsync(cancellationToken).ConfigureAwait(false);
            return;
        }

        await _mutex.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var stream = _stream!;

            if (_hasPendingEntries)
            {
                stream.Flush(true);
                _flushedLSN = stream.Position;
            }

            await flushDataAsync(cancellationToken).ConfigureAwait(false);

            if (_hasPendingEntries)
            {
                stream.SetLength(0);
                stream.Seek(0, SeekOrigin.End);
                stream.Flush(true);
                _hasPendingEntries = false;
                _flushedLSN = stream.Position;
            }
        }
        finally
        {
            _mutex.Release();
        }
    }

    public void Replay(Action<uint, byte[]> apply)
    {
        if (!IsEnabled) return;
        if (apply == null) throw new ArgumentNullException(nameof(apply));

        var stream = _stream!;
        _mutex.Wait();
        long lastSuccessfulPosition = 0;
        try
        {
            stream.Flush(true);
            stream.Seek(0, SeekOrigin.Begin);

            const int FullHeaderSize = HeaderSize + 4; // 13 bytes
            var headerBuffer = new byte[FullHeaderSize];

            while (stream.Position < stream.Length)
            {
                long currentEntryStart = stream.Position;

                var read = stream.Read(headerBuffer, 0, FullHeaderSize);
                if (read < HeaderSize)
                {
                    break;
                }

                if (headerBuffer[0] != EntryTypePage)
                {
                    Console.Error.WriteLine($"WAL: Invalid entry type 0x{headerBuffer[0]:X} at {currentEntryStart}");
                    break;
                }

                var pageId = BinaryPrimitives.ReadUInt32LittleEndian(headerBuffer.AsSpan(1, 4));
                var length = BinaryPrimitives.ReadInt32LittleEndian(headerBuffer.AsSpan(5, 4));

                uint? expectedCrc = null;
                if (read >= FullHeaderSize)
                {
                    expectedCrc = BinaryPrimitives.ReadUInt32LittleEndian(headerBuffer.AsSpan(9, 4));
                }
                else
                {
                    if (read > HeaderSize)
                    {
                        Console.Error.WriteLine($"WAL: Incomplete header at {currentEntryStart}");
                        break;
                    }
                }

                if (length <= 0 || length > _maxRecordSize)
                {
                    Console.Error.WriteLine($"WAL: Invalid record length {length} at {currentEntryStart}");
                    break;
                }

                var buffer = new byte[length];
                var offset = 0;
                while (offset < length)
                {
                    var chunk = stream.Read(buffer, offset, length - offset);
                    if (chunk == 0)
                    {
                        break;
                    }
                    offset += chunk;
                }

                if (offset != length)
                {
                    Console.Error.WriteLine($"WAL: Incomplete data record at {currentEntryStart}");
                    break;
                }

                if (expectedCrc.HasValue)
                {
                    var actualCrc = System.IO.Hashing.Crc32.HashToUInt32(buffer);
                    if (actualCrc != expectedCrc.Value)
                    {
                        Console.Error.WriteLine($"WAL: CRC mismatch at {currentEntryStart}");
                        break;
                    }
                }

                apply(pageId, buffer);
                lastSuccessfulPosition = stream.Position;
            }

            if (lastSuccessfulPosition < stream.Length)
            {
                stream.SetLength(lastSuccessfulPosition);
            }
            else if (lastSuccessfulPosition > 0)
            {
                stream.SetLength(0);
            }

            stream.Seek(0, SeekOrigin.End);
            _hasPendingEntries = stream.Length > 0;
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"WAL: Fatal error during replay: {ex.Message}");
            try { stream.SetLength(lastSuccessfulPosition); } catch { }
            throw;
        }
        finally
        {
            _mutex.Release();
        }
    }

    public async Task ReplayAsync(Func<uint, byte[], Task> applyAsync, CancellationToken cancellationToken = default)
    {
        if (!IsEnabled) return;
        if (applyAsync == null) throw new ArgumentNullException(nameof(applyAsync));

        var stream = _stream!;
        await _mutex.WaitAsync(cancellationToken).ConfigureAwait(false);
        long lastSuccessfulPosition = 0;
        try
        {
            stream.Flush(true);
            stream.Seek(0, SeekOrigin.Begin);

            const int FullHeaderSize = HeaderSize + 4; // 13 bytes
            var headerBuffer = new byte[FullHeaderSize];
            
            while (stream.Position < stream.Length)
            {
                long currentEntryStart = stream.Position;
                
                // 尝试读取完整头部
                var read = await stream.ReadAsync(headerBuffer, 0, FullHeaderSize, cancellationToken).ConfigureAwait(false);
                
                // 如果连最小头部 (HeaderSize = 9) 都读不够，说明文件结束或损坏
                if (read < HeaderSize)
                {
                    break;
                }

                if (headerBuffer[0] != EntryTypePage)
                {
                    Console.Error.WriteLine($"WAL: Invalid entry type 0x{headerBuffer[0]:X} at {currentEntryStart}");
                    break; // 无效类型，停止重放
                }

                var pageId = BinaryPrimitives.ReadUInt32LittleEndian(headerBuffer.AsSpan(1, 4));
                var length = BinaryPrimitives.ReadInt32LittleEndian(headerBuffer.AsSpan(5, 4));
                
                // 读取 CRC32 (如果有)
                uint? expectedCrc = null;
                if (read >= FullHeaderSize)
                {
                    expectedCrc = BinaryPrimitives.ReadUInt32LittleEndian(headerBuffer.AsSpan(9, 4));
                }
                else
                {
                    if (read > HeaderSize) 
                    {
                        Console.Error.WriteLine($"WAL: Incomplete header at {currentEntryStart}");
                        break; 
                    }
                }

                if (length <= 0 || length > _maxRecordSize)
                {
                    Console.Error.WriteLine($"WAL: Invalid record length {length} at {currentEntryStart}");
                    break; // 无效长度
                }

                var buffer = new byte[length];
                var offset = 0;
                while (offset < length)
                {
                    var chunk = await stream.ReadAsync(buffer, offset, length - offset, cancellationToken).ConfigureAwait(false);
                    if (chunk == 0)
                    {
                        break;
                    }
                    offset += chunk;
                }

                if (offset != length)
                {
                    Console.Error.WriteLine($"WAL: Incomplete data record at {currentEntryStart}");
                    break; // 数据不完整
                }

                // 验证校验和 (如果存在)
                if (expectedCrc.HasValue)
                {
                    var actualCrc = System.IO.Hashing.Crc32.HashToUInt32(buffer);
                    if (actualCrc != expectedCrc.Value)
                    {
                        Console.Error.WriteLine($"WAL: CRC mismatch at {currentEntryStart}");
                        break;
                    }
                }

                await applyAsync(pageId, buffer).ConfigureAwait(false);
                lastSuccessfulPosition = stream.Position;
            }

            // 清理或截断日志
            if (lastSuccessfulPosition < stream.Length)
            {
                // 如果没有处理完全部文件（因为损坏或截断），则将文件截断到最后一个有效的记录处
                stream.SetLength(lastSuccessfulPosition);
            }
            else if (lastSuccessfulPosition > 0)
            {
                // 全部重放成功，清空日志
                stream.SetLength(0);
            }

            stream.Seek(0, SeekOrigin.End);
            _hasPendingEntries = stream.Length > 0;
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"WAL: Fatal error during replay: {ex.Message}");
            // 尽力而为：截断到已知正确的位置
            try { stream.SetLength(lastSuccessfulPosition); } catch { }
            throw;
        }
        finally
        {
            _mutex.Release();
        }
    }

    public async Task TruncateAsync(CancellationToken cancellationToken = default)
    {
        if (!IsEnabled) return;

        await _mutex.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var stream = _stream!;
            stream.SetLength(0);
            stream.Seek(0, SeekOrigin.End);
            await stream.FlushAsync(cancellationToken).ConfigureAwait(false);
            _hasPendingEntries = false;
        }
        finally
        {
            _mutex.Release();
        }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        try
        {
            _stream?.Dispose();
        }
        finally
        {
            _mutex.Dispose();
        }
    }
}
