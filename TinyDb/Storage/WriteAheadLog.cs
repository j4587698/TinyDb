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

    /// <summary>
    /// 是否启用 WAL
    /// </summary>
    public bool IsEnabled { get; }

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
        if (!IsEnabled || _stream == null) return;

        var data = page.Snapshot(includeUnusedTail: false);
        if (data.Length == 0) return;

        await _mutex.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
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
        if (!IsEnabled || _stream == null) return;
        var data = page.Snapshot(includeUnusedTail: false);
        if (data.Length == 0) return;

        _mutex.Wait();
        try
        {
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
        if (_stream == null) return;
        Span<byte> header = stackalloc byte[HeaderSize];
        header[0] = EntryTypePage;
        BinaryPrimitives.WriteUInt32LittleEndian(header[1..5], pageId);
        BinaryPrimitives.WriteInt32LittleEndian(header[5..9], data.Length);
        _stream.Write(header);
        _stream.Write(data, 0, data.Length);
    }

    private async Task WriteEntryAsync(uint pageId, byte[] data, CancellationToken cancellationToken)
    {
        if (_stream == null) return;
        byte[] header = new byte[HeaderSize];
        header[0] = EntryTypePage;
        BinaryPrimitives.WriteUInt32LittleEndian(header.AsSpan(1, 4), pageId);
        BinaryPrimitives.WriteInt32LittleEndian(header.AsSpan(5, 4), data.Length);
        await _stream.WriteAsync(header, 0, header.Length, cancellationToken).ConfigureAwait(false);
        await _stream.WriteAsync(data, 0, data.Length, cancellationToken).ConfigureAwait(false);
    }

    public async Task FlushLogAsync(CancellationToken cancellationToken = default)
    {
        if (!IsEnabled || _stream == null || !_hasPendingEntries) return;

        await _mutex.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_hasPendingEntries)
            {
                _stream.Flush(true);
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

        if (!IsEnabled || _stream == null)
        {
            await flushDataAsync(cancellationToken).ConfigureAwait(false);
            return;
        }

        await _mutex.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_hasPendingEntries)
            {
                _stream.Flush(true);
            }

            await flushDataAsync(cancellationToken).ConfigureAwait(false);

            if (_hasPendingEntries)
            {
                _stream.SetLength(0);
                _stream.Seek(0, SeekOrigin.End);
                _stream.Flush(true);
                _hasPendingEntries = false;
            }
        }
        finally
        {
            _mutex.Release();
        }
    }

    public async Task ReplayAsync(Func<uint, byte[], Task> applyAsync, CancellationToken cancellationToken = default)
    {
        if (!IsEnabled || _stream == null) return;
        if (applyAsync == null) throw new ArgumentNullException(nameof(applyAsync));

        await _mutex.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            _stream.Flush(true);
            _stream.Seek(0, SeekOrigin.Begin);

            var headerBuffer = new byte[HeaderSize];
            while (_stream.Position < _stream.Length)
            {
                var read = await _stream.ReadAsync(headerBuffer, 0, HeaderSize, cancellationToken).ConfigureAwait(false);
                if (read < HeaderSize)
                {
                    break;
                }

                if (headerBuffer[0] != EntryTypePage)
                {
                    break;
                }

                var pageId = BinaryPrimitives.ReadUInt32LittleEndian(headerBuffer.AsSpan(1, 4));
                var length = BinaryPrimitives.ReadInt32LittleEndian(headerBuffer.AsSpan(5, 4));

                if (length <= 0 || length > _maxRecordSize)
                {
                    break;
                }

                var buffer = new byte[length];
                var offset = 0;
                while (offset < length)
                {
                    var chunk = await _stream.ReadAsync(buffer, offset, length - offset, cancellationToken).ConfigureAwait(false);
                    if (chunk == 0)
                    {
                        offset = length + 1;
                        break;
                    }
                    offset += chunk;
                }

                if (offset != length)
                {
                    break;
                }

                await applyAsync(pageId, buffer).ConfigureAwait(false);
            }

            _stream.Seek(0, SeekOrigin.End);
            _hasPendingEntries = _stream.Length > 0;
        }
        finally
        {
            _mutex.Release();
        }
    }

    public async Task TruncateAsync(CancellationToken cancellationToken = default)
    {
        if (!IsEnabled || _stream == null) return;

        await _mutex.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            _stream.SetLength(0);
            _stream.Seek(0, SeekOrigin.End);
            await _stream.FlushAsync(cancellationToken).ConfigureAwait(false);
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
