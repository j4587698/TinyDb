using System;
using System.Buffers;
using System.Runtime.InteropServices;

namespace TinyDb.Storage;

/// <summary>
/// 表示数据库中的一个页面，是数据存储的基本单位。
/// </summary>
public sealed class Page : IDisposable
{
    private readonly byte[] _data;
    private readonly object _lock = new();
    private int _pinCount;
    private bool _isDirty;
    private bool _disposed;
    private object? _cachedParsedData;

    /// <summary>
    /// 获取当前的锁定计数（Pin Count）。
    /// </summary>
    public int PinCount => _pinCount;

    /// <summary>
    /// 锁定页面，增加引用计数。
    /// </summary>
    public void Pin() => Interlocked.Increment(ref _pinCount);

    /// <summary>
    /// 解锁页面，减少引用计数。
    /// </summary>
    public void Unpin()
    {
        if (Interlocked.Decrement(ref _pinCount) < 0)
        {
            Interlocked.Exchange(ref _pinCount, 0);
        }
    }

    /// <summary>
    /// 获取页面头部信息。
    /// </summary>
    public PageHeader Header { get; private set; }

    /// <summary>
    /// 获取页面 ID。
    /// </summary>
    public uint PageID => Header.PageID;

    /// <summary>
    /// 获取页面类型。
    /// </summary>
    public PageType PageType => Header.PageType;

    /// <summary>
    /// 获取页面大小（字节）。
    /// </summary>
    public int PageSize { get; }
    
    /// <summary>
    /// 获取或设置已解析数据的缓存对象，用于提高性能。
    /// </summary>
    public object? CachedParsedData
    {
        get { lock (_lock) return _cachedParsedData; }
        set { lock (_lock) _cachedParsedData = value; }
    }
    
    /// <summary>
    /// 数据开始的偏移量（紧跟在页面头部之后）。
    /// </summary>
    public const int DataStartOffset = PageHeader.Size; // 49

    /// <summary>
    /// 获取页面的数据容量。
    /// </summary>
    public int DataCapacity => PageSize - DataStartOffset;

    /// <summary>
    /// 获取当前的写入位置。
    /// </summary>
    public int WritePosition => DataStartOffset + (DataCapacity - Header.FreeBytes);

    /// <summary>
    /// 获取一个值，指示页面是否已被修改（脏页面）。
    /// </summary>
    public bool IsDirty
    {
        get => _isDirty;
        private set => _isDirty = value;
    }

    /// <summary>
    /// 获取有效数据的只读跨度（Span）。
    /// </summary>
    public ReadOnlySpan<byte> ValidDataSpan
    {
        get
        {
            ThrowIfDisposed();
            int length = DataCapacity - Header.FreeBytes;
            return new ReadOnlySpan<byte>(_data, DataStartOffset, length);
        }
    }
    
    /// <summary>
    /// 获取整个页面数据的只读跨度（Span）。
    /// </summary>
    public ReadOnlySpan<byte> FullData
    {
        get
        {
            ThrowIfDisposed();
            return new ReadOnlySpan<byte>(_data);
        }
    }

    /// <summary>
    /// 获取整个页面数据的只读内存块（Memory）。
    /// </summary>
    public ReadOnlyMemory<byte> Memory
    {
        get
        {
            ThrowIfDisposed();
            return new ReadOnlyMemory<byte>(_data);
        }
    }

    internal byte[] Buffer
    {
        get
        {
            ThrowIfDisposed();
            return _data;
        }
    }

    /// <summary>
    /// 初始化一个新的空页面。
    /// </summary>
    /// <param name="pageID">页面 ID。</param>
    /// <param name="pageSize">页面大小。</param>
    /// <param name="pageType">页面类型。</param>
    public Page(uint pageID, int pageSize, PageType pageType = PageType.Empty)
    {
        if (pageSize <= DataStartOffset) throw new ArgumentException("Page size too small", nameof(pageSize));
        PageSize = pageSize;
        _data = new byte[pageSize];
        Header = new PageHeader();
        Header.Initialize(pageType, pageID);
        Header.FreeBytes = (ushort)DataCapacity;
        WriteHeader();
        _isDirty = false;
    }

    /// <summary>
    /// 使用现有数据初始化页面实例。
    /// </summary>
    /// <param name="pageID">预期的页面 ID。</param>
    /// <param name="data">页面二进制数据。</param>
    public Page(uint pageID, byte[] data)
    {
        if (data == null) throw new ArgumentNullException(nameof(data));
        if (data.Length <= DataStartOffset) throw new ArgumentException("Data too small", nameof(data));
        PageSize = data.Length;
        _data = new byte[PageSize];
        Array.Copy(data, _data, PageSize);
        Header = PageHeader.FromByteArray(_data);
        if (Header.PageID != pageID) throw new InvalidOperationException("Page ID mismatch");
    }

    /// <summary>
    /// 重置页面字节并保留指定空间。
    /// </summary>
    /// <param name="reservedBytes">要保留的字节数。</param>
    public void ResetBytes(int reservedBytes)
    {
        ThrowIfDisposed();
        lock (_lock)
        {
            if (reservedBytes < 0 || reservedBytes > DataCapacity) throw new ArgumentOutOfRangeException(nameof(reservedBytes));
            Array.Clear(_data, DataStartOffset, DataCapacity);
            Header.ItemCount = 0;
            Header.FreeBytes = (ushort)(DataCapacity - reservedBytes); 
            Header.UpdateModification();
            WriteHeader();
            _cachedParsedData = null;
            MarkDirty();
        }
    }

    /// <summary>
    /// 向页面追加内容。
    /// </summary>
    /// <param name="content">要追加的二进制内容。</param>
    public void Append(ReadOnlySpan<byte> content)
    {
        ThrowIfDisposed();
        lock (_lock)
        {
            int requiredTotal = 4 + content.Length;
            if (Header.FreeBytes < requiredTotal)
                throw new ArgumentException($"Page full. Required: {requiredTotal}, Free: {Header.FreeBytes}");

            int pos = WritePosition;
            int len = content.Length;
            _data[pos] = (byte)len;
            _data[pos + 1] = (byte)(len >> 8);
            _data[pos + 2] = (byte)(len >> 16);
            _data[pos + 3] = (byte)(len >> 24);
            
            content.CopyTo(new Span<byte>(_data, pos + 4, len));

            Header.FreeBytes -= (ushort)requiredTotal;
            Header.ItemCount++;
            Header.UpdateModification();
            WriteHeader();
            _cachedParsedData = null;
            MarkDirty();
        }
    }

    public byte[] Snapshot(bool includeUnusedTail = true)
    {
        ThrowIfDisposed();
        lock (_lock)
        {
            int length = includeUnusedTail ? _data.Length : WritePosition;
            var result = new byte[length];
            Array.Copy(_data, 0, result, 0, length);
            return result;
        }
    }

    public void SetContent(ReadOnlySpan<byte> content)
    {
        ThrowIfDisposed();
        lock (_lock)
        {
            int requiredTotal = 4 + content.Length;
            if (DataCapacity < requiredTotal) throw new ArgumentException($"Content too large. Capacity: {DataCapacity}, Required: {requiredTotal}");

            Array.Clear(_data, DataStartOffset, DataCapacity);
            Header.ItemCount = 0;
            Header.FreeBytes = (ushort)DataCapacity;

            int pos = WritePosition;
            int len = content.Length;
            _data[pos] = (byte)len;
            _data[pos + 1] = (byte)(len >> 8);
            _data[pos + 2] = (byte)(len >> 16);
            _data[pos + 3] = (byte)(len >> 24);
            
            content.CopyTo(new Span<byte>(_data, pos + 4, len));

            Header.FreeBytes -= (ushort)requiredTotal;
            Header.ItemCount = 1;
            Header.UpdateModification();
            WriteHeader();
            _cachedParsedData = null;
            MarkDirty();
        }
    }

    public void SetContent(byte[] content)
    {
        SetContent(new ReadOnlySpan<byte>(content));
    }

    public void UpdateHeader(PageHeader header) { lock(_lock) { Header = header.Clone(); WriteHeader(); _cachedParsedData = null; MarkDirty(); } }
    public void UpdatePageType(PageType type) { lock(_lock) { Header.PageType = type; WriteHeader(); _cachedParsedData = null; MarkDirty(); } }
    public void SetLinks(uint prev, uint next) { lock(_lock) { Header.PrevPageID = prev; Header.NextPageID = next; WriteHeader(); _cachedParsedData = null; MarkDirty(); } }
    
    public void WriteData(int offset, ReadOnlySpan<byte> data)
    {
        ThrowIfDisposed();
        int physOffset = DataStartOffset + offset;
        if (physOffset + data.Length > PageSize) throw new ArgumentOutOfRangeException(nameof(offset));
        lock(_lock) { data.CopyTo(new Span<byte>(_data, physOffset, data.Length)); _cachedParsedData = null; MarkDirty(); }
    }

    public byte[] ReadBytes(int offset, int length)
    {
        ThrowIfDisposed();
        int physOffset = DataStartOffset + offset;
        if (offset < 0 || length <= 0 || physOffset + length > PageSize) return Array.Empty<byte>();
        lock (_lock)
        {
            var result = new byte[length];
            Array.Copy(_data, physOffset, result, 0, length);
            return result;
        }
    }

    // Legacy Support
    public byte[] ReadData(int offset, int length) 
    {
        return ReadBytes(offset, length);
    }

    public Span<byte> GetDataSpan(int offset, int length)
    {
        ThrowIfDisposed();
        int physOffset = DataStartOffset + offset;
        if (offset < 0 || physOffset + length > PageSize) throw new ArgumentOutOfRangeException(nameof(offset));
        lock (_lock)
        {
            return new Span<byte>(_data, physOffset, length);
        }
    }

    public void UpdateStats(ushort free, ushort count) { lock(_lock) { Header.FreeBytes = free; Header.ItemCount = count; Header.UpdateModification(); WriteHeader(); _cachedParsedData = null; MarkDirty(); } }
    public void UpdateChecksum() { lock(_lock) { Header.Checksum = 0; WriteHeader(); Header.Checksum = Header.CalculateChecksum(_data); WriteHeader(); } }
    public bool VerifyIntegrity() { lock(_lock) { return Header.IsValid() && Header.VerifyChecksum(_data); } }
    public void MarkClean() => IsDirty = false;
    private void MarkDirty() => IsDirty = true;
    private void WriteHeader() { Header.WriteTo(new Span<byte>(_data, 0, PageHeader.Size)); }
    private void ThrowIfDisposed() { if (_disposed) throw new ObjectDisposedException(nameof(Page)); }
    public void Dispose() { _disposed = true; }
    public Page Clone() { return new Page(PageID, _data); }
    public Span<byte> DataSpan => new Span<byte>(_data, DataStartOffset, DataCapacity);
    public int DataSize => DataCapacity; 
    public PageUsageInfo GetUsageInfo() => new PageUsageInfo { PageID = PageID, PageType = PageType, FreeBytes = Header.FreeBytes, ItemCount = Header.ItemCount, IsDirty = IsDirty };
    
    public void ClearData()
    {
        ResetBytes(0);
        lock(_lock)
        {
            Header.PageType = PageType.Empty;
            Header.PrevPageID = 0;
            Header.NextPageID = 0;
            WriteHeader();
            _cachedParsedData = null;
            MarkDirty();
        }
    }
}

public sealed class PageUsageInfo { public uint PageID { get; init; } public PageType PageType { get; init; } public int FreeBytes { get; init; } public int ItemCount { get; init; } public bool IsDirty { get; init; } 
    public override string ToString() => $"Page[{PageID}] Type={PageType} Free={FreeBytes} Items={ItemCount} Dirty={IsDirty}";
}
