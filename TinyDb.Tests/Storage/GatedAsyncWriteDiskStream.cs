using TinyDb.Storage;

namespace TinyDb.Tests.Storage;

/// <summary>
/// 内存磁盘流：可以把“下一次异步写”挂起，直到测试放行。
/// 挂起前先复制调用方传入的字节，模拟快照已拍下、写盘尚未完成的状态。
/// </summary>
internal sealed class GatedAsyncWriteDiskStream : IDiskStream
{
    private readonly object _gate = new();
    private readonly MemoryStream _stream = new();
    private int _holdNextAsyncWrite;
    private int _asyncWriteCount;
    private TaskCompletionSource _held = NewTcs();
    private TaskCompletionSource _release = NewTcs();

    public string FilePath => "gated-memory";
    public long Size { get { lock (_gate) return _stream.Length; } }
    public bool IsReadable => true;
    public bool IsWritable => true;

    /// <summary>已开始的异步写次数（含被挂起的）。</summary>
    public int AsyncWriteCount => Volatile.Read(ref _asyncWriteCount);

    public void HoldNextAsyncWrite()
    {
        _held = NewTcs();
        _release = NewTcs();
        Volatile.Write(ref _holdNextAsyncWrite, 1);
    }

    public Task WaitForHeldWrite(TimeSpan timeout) => _held.Task.WaitAsync(timeout);

    public void ReleaseHeldWrite() => _release.TrySetResult();

    public byte[] ReadPage(long pageOffset, int pageSize)
    {
        var buffer = new byte[pageSize];
        lock (_gate)
        {
            if (pageOffset >= _stream.Length) return buffer;
            _stream.Position = pageOffset;
            _stream.ReadExactly(buffer, 0, (int)Math.Min(pageSize, _stream.Length - pageOffset));
            return buffer;
        }
    }

    public Task<byte[]> ReadPageAsync(long pageOffset, int pageSize, CancellationToken cancellationToken = default)
        => Task.FromResult(ReadPage(pageOffset, pageSize));

    public void WritePage(long pageOffset, byte[] pageData)
    {
        lock (_gate)
        {
            if (pageOffset + pageData.Length > _stream.Length) _stream.SetLength(pageOffset + pageData.Length);
            _stream.Position = pageOffset;
            _stream.Write(pageData, 0, pageData.Length);
        }
    }

    public async Task WritePageAsync(long pageOffset, byte[] pageData, CancellationToken cancellationToken = default)
    {
        Interlocked.Increment(ref _asyncWriteCount);
        var captured = (byte[])pageData.Clone();
        if (Interlocked.Exchange(ref _holdNextAsyncWrite, 0) == 1)
        {
            _held.TrySetResult();
            await _release.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
        }

        WritePage(pageOffset, captured);
    }

    public void Flush() { }
    public Task FlushAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
    public void SetLength(long length) { lock (_gate) _stream.SetLength(length); }

    public DiskStreamStatistics GetStatistics() => new()
    {
        FilePath = FilePath,
        Size = Size,
        Position = 0,
        IsReadable = true,
        IsWritable = true,
        IsSeekable = true
    };

    public void Dispose()
    {
        _release.TrySetResult();
        _stream.Dispose();
    }

    private static TaskCompletionSource NewTcs() => new(TaskCreationOptions.RunContinuationsAsynchronously);
}
