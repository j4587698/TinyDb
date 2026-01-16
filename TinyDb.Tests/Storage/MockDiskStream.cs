using TinyDb.Storage;

namespace TinyDb.Tests.Storage;

public class MockDiskStream : IDiskStream
{
    private readonly MemoryStream _memoryStream = new();
    
    public bool ShouldThrowOnRead { get; set; }
    public bool ShouldThrowOnWrite { get; set; }
    
    public string FilePath => "memory";
    public long Size => _memoryStream.Length;
    public bool IsReadable => true;
    public bool IsWritable => true;

    public void Dispose() => _memoryStream.Dispose();

    public void Flush() { }
    public Task FlushAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

    public byte[] ReadPage(long pageOffset, int pageSize)
    {
        if (ShouldThrowOnRead) throw new IOException("Simulated Read Error");
        
        var buffer = new byte[pageSize];
        if (pageOffset >= _memoryStream.Length) return buffer;
        
        _memoryStream.Position = pageOffset;
        _memoryStream.Read(buffer, 0, pageSize);
        return buffer;
    }

    public Task<byte[]> ReadPageAsync(long pageOffset, int pageSize, CancellationToken cancellationToken = default)
    {
        return Task.FromResult(ReadPage(pageOffset, pageSize));
    }

    public void SetLength(long length)
    {
        _memoryStream.SetLength(length);
    }

    public void WritePage(long pageOffset, byte[] pageData)
    {
        if (ShouldThrowOnWrite) throw new IOException("Simulated Write Error");
        
        if (pageOffset + pageData.Length > _memoryStream.Length)
        {
            _memoryStream.SetLength(pageOffset + pageData.Length);
        }
        
        _memoryStream.Position = pageOffset;
        _memoryStream.Write(pageData, 0, pageData.Length);
    }

    public Task WritePageAsync(long pageOffset, byte[] pageData, CancellationToken cancellationToken = default)
    {
        WritePage(pageOffset, pageData);
        return Task.CompletedTask;
    }

    public DiskStreamStatistics GetStatistics()
    {
        return new DiskStreamStatistics
        {
            FilePath = FilePath,
            Size = Size,
            Position = _memoryStream.Position,
            IsReadable = IsReadable,
            IsWritable = IsWritable,
            IsSeekable = true
        };
    }
}
