using System.Buffers;
using System.Runtime.InteropServices;

namespace SimpleDb.Storage;

/// <summary>
/// 数据库页面，表示数据文件中的一个页面
/// </summary>
public sealed class Page : IDisposable
{
    private readonly byte[] _data;
    private readonly object _lock = new();
    private bool _isDirty;
    private bool _disposed;

    /// <summary>
    /// 页面头部
    /// </summary>
    public PageHeader Header { get; private set; }

    /// <summary>
    /// 页面ID
    /// </summary>
    public uint PageID => Header.PageID;

    /// <summary>
    /// 页面类型
    /// </summary>
    public PageType PageType => Header.PageType;

    /// <summary>
    /// 页面大小
    /// </summary>
    public int PageSize { get; }

    /// <summary>
    /// 数据区域大小
    /// </summary>
    public int DataSize => Header.GetDataSize(PageSize);

    /// <summary>
    /// 是否已修改
    /// </summary>
    public bool IsDirty
    {
        get => _isDirty;
        private set => _isDirty = value;
    }

    /// <summary>
    /// 获取数据区域的 Span
    /// </summary>
    public Span<byte> DataSpan
    {
        get
        {
            ThrowIfDisposed();
            return new Span<byte>(_data, PageHeader.Size, DataSize);
        }
    }

    /// <summary>
    /// 获取数据区域的 Memory
    /// </summary>
    public Memory<byte> DataMemory
    {
        get
        {
            ThrowIfDisposed();
            return new Memory<byte>(_data, PageHeader.Size, DataSize);
        }
    }

    /// <summary>
    /// 获取完整的页面数据（包含头部）
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
    /// 初始化新页面
    /// </summary>
    /// <param name="pageID">页面ID</param>
    /// <param name="pageSize">页面大小</param>
    /// <param name="pageType">页面类型</param>
    public Page(uint pageID, int pageSize, PageType pageType = PageType.Empty)
    {
        if (pageSize <= PageHeader.Size)
            throw new ArgumentException($"Page size must be larger than {PageHeader.Size}", nameof(pageSize));

        PageSize = pageSize;
        _data = new byte[pageSize];
        Header = new PageHeader();
        Header.Initialize(pageType, pageID);

        // 将头部写入数据
        WriteHeader();
        _isDirty = true;
    }

    /// <summary>
    /// 从现有数据创建页面
    /// </summary>
    /// <param name="pageID">页面ID</param>
    /// <param name="data">页面数据</param>
    public Page(uint pageID, byte[] data)
    {
        if (data == null) throw new ArgumentNullException(nameof(data));
        if (data.Length < PageHeader.Size)
            throw new ArgumentException("Data size is too small for a page", nameof(data));

        PageSize = data.Length;
        _data = new byte[PageSize];
        Array.Copy(data, _data, PageSize);

        Header = PageHeader.FromByteArray(_data);

        // 验证页面ID是否匹配
        if (Header.PageID != pageID)
        {
            throw new InvalidOperationException($"Page ID mismatch: expected {pageID}, found {Header.PageID}");
        }

        // 验证页面完整性
        if (!Header.IsValid())
        {
            throw new InvalidOperationException($"Invalid page header for page {pageID}");
        }
    }

    /// <summary>
    /// 克隆页面
    /// </summary>
    public Page Clone()
    {
        ThrowIfDisposed();
        return new Page(PageID, _data);
    }

    /// <summary>
    /// 更新页面头部
    /// </summary>
    /// <param name="header">新的页面头部</param>
    public void UpdateHeader(PageHeader header)
    {
        ThrowIfDisposed();
        lock (_lock)
        {
            Header = header.Clone();
            WriteHeader();
            MarkDirty();
        }
    }

    /// <summary>
    /// 更新页面类型
    /// </summary>
    /// <param name="pageType">新的页面类型</param>
    public void UpdatePageType(PageType pageType)
    {
        ThrowIfDisposed();
        lock (_lock)
        {
            // 直接修改字节数组中的页面类型
            var pageTypeBytes = BitConverter.GetBytes((uint)pageType);
            Buffer.BlockCopy(pageTypeBytes, 0, _data, 4, 4);

            // 更新修改时间
            var now = DateTime.UtcNow.Ticks;
            var timeBytes = BitConverter.GetBytes(now);
            Buffer.BlockCopy(timeBytes, 0, _data, 24, 8);

            WriteHeader();
            MarkDirty();
        }
    }

    /// <summary>
    /// 设置链表链接
    /// </summary>
    /// <param name="prevPageID">前一个页面ID</param>
    /// <param name="nextPageID">下一个页面ID</param>
    public void SetLinks(uint prevPageID, uint nextPageID)
    {
        ThrowIfDisposed();
        lock (_lock)
        {
            // 直接修改字节数组中的链接信息
            var prevBytes = BitConverter.GetBytes(prevPageID);
            var nextBytes = BitConverter.GetBytes(nextPageID);
            Buffer.BlockCopy(prevBytes, 0, _data, 8, 4);
            Buffer.BlockCopy(nextBytes, 0, _data, 12, 4);

            // 更新修改时间
            var now = DateTime.UtcNow.Ticks;
            var timeBytes = BitConverter.GetBytes(now);
            Buffer.BlockCopy(timeBytes, 0, _data, 24, 8);

            WriteHeader();
            MarkDirty();
        }
    }

    /// <summary>
    /// 更新页面统计信息
    /// </summary>
    /// <param name="freeBytes">剩余字节数</param>
    /// <param name="itemCount">项目数量</param>
    public void UpdateStats(ushort freeBytes, ushort itemCount)
    {
        ThrowIfDisposed();
        lock (_lock)
        {
            // 直接修改字节数组中的统计信息
            var freeBytesArray = BitConverter.GetBytes(freeBytes);
            var itemCountArray = BitConverter.GetBytes(itemCount);
            Buffer.BlockCopy(freeBytesArray, 0, _data, 16, 2);
            Buffer.BlockCopy(itemCountArray, 0, _data, 18, 2);

            // 更新修改时间
            var now = DateTime.UtcNow.Ticks;
            var timeBytes = BitConverter.GetBytes(now);
            Buffer.BlockCopy(timeBytes, 0, _data, 24, 8);

            // 直接更新Header属性以保持同步
            Header.FreeBytes = freeBytes;
            Header.ItemCount = itemCount;
            Header.ModifiedAt = now;
            MarkDirty();
        }
    }

    /// <summary>
    /// 在数据区域写入数据
    /// </summary>
    /// <param name="offset">偏移量</param>
    /// <param name="data">要写入的数据</param>
    public void WriteData(int offset, ReadOnlySpan<byte> data)
    {
        ThrowIfDisposed();
        if (offset < 0 || offset + data.Length > DataSize)
            throw new ArgumentOutOfRangeException(nameof(offset));

        lock (_lock)
        {
            data.CopyTo(new Span<byte>(_data, PageHeader.Size + offset, data.Length));
            MarkDirty();
        }
    }

    /// <summary>
    /// 从数据区域读取数据
    /// </summary>
    /// <param name="offset">偏移量</param>
    /// <param name="length">读取长度</param>
    /// <returns>读取的数据</returns>
    public byte[] ReadData(int offset, int length)
    {
        ThrowIfDisposed();
        if (offset < 0 || offset + length > DataSize)
            throw new ArgumentOutOfRangeException(nameof(offset));

        lock (_lock)
        {
            var result = new byte[length];
            Array.Copy(_data, PageHeader.Size + offset, result, 0, length);
            return result;
        }
    }

    /// <summary>
    /// 获取数据区域的 Span
    /// </summary>
    /// <param name="offset">起始偏移量</param>
    /// <param name="length">长度</param>
    /// <returns>数据 Span</returns>
    public Span<byte> GetDataSpan(int offset, int length)
    {
        ThrowIfDisposed();
        if (offset < 0 || offset + length > DataSize)
            throw new ArgumentOutOfRangeException(nameof(offset));

        lock (_lock)
        {
            return new Span<byte>(_data, PageHeader.Size + offset, length);
        }
    }

    /// <summary>
    /// 清空数据区域
    /// </summary>
    public void ClearData()
    {
        ThrowIfDisposed();
        lock (_lock)
        {
            Array.Clear(_data, PageHeader.Size, DataSize);

            // 直接修改字节数组中的统计信息
            var freeBytesArray = BitConverter.GetBytes((ushort)DataSize);
            var itemCountArray = BitConverter.GetBytes((ushort)0);
            Buffer.BlockCopy(freeBytesArray, 0, _data, 16, 2);
            Buffer.BlockCopy(itemCountArray, 0, _data, 18, 2);

            // 更新修改时间
            var now = DateTime.UtcNow.Ticks;
            var timeBytes = BitConverter.GetBytes(now);
            Buffer.BlockCopy(timeBytes, 0, _data, 24, 8);

            WriteHeader();
            MarkDirty();
        }
    }

    /// <summary>
    /// 计算并更新校验和
    /// </summary>
    public void UpdateChecksum()
    {
        ThrowIfDisposed();
        lock (_lock)
        {
            // 计算校验和
            var checksum = CalculatePageChecksum(_data);

            // 直接修改字节数组中的校验和
            var checksumBytes = BitConverter.GetBytes(checksum);
            Buffer.BlockCopy(checksumBytes, 0, _data, 20, 4);

            // 重新写入头部以包含新的校验和
            WriteHeader();
        }
    }

    /// <summary>
    /// 计算页面校验和
    /// </summary>
    /// <param name="data">页面数据</param>
    /// <returns>校验和</returns>
    private static uint CalculatePageChecksum(byte[] data)
    {
        uint checksum = 0;
        for (int i = 0; i < data.Length; i++)
        {
            checksum = ((checksum << 1) | (checksum >> 31)) ^ data[i];
        }
        return checksum;
    }

    /// <summary>
    /// 验证页面完整性
    /// </summary>
    /// <returns>是否有效</returns>
    public bool VerifyIntegrity()
    {
        ThrowIfDisposed();
        lock (_lock)
        {
            return Header.IsValid() && Header.VerifyChecksum(_data);
        }
    }

    /// <summary>
    /// 标记页面为已修改
    /// </summary>
    private void MarkDirty()
    {
        _isDirty = true;
    }

    /// <summary>
    /// 标记页面为干净（已同步到磁盘）
    /// </summary>
    public void MarkClean()
    {
        _isDirty = false;
    }

    /// <summary>
    /// 将页面头部写入数据数组
    /// </summary>
    private void WriteHeader()
    {
        var headerData = Header.ToByteArray();
        Array.Copy(headerData, _data, PageHeader.Size);
    }

    /// <summary>
    /// 获取页面使用统计信息
    /// </summary>
    /// <returns>使用统计信息</returns>
    public PageUsageInfo GetUsageInfo()
    {
        ThrowIfDisposed();
        return new PageUsageInfo
        {
            PageID = PageID,
            PageType = PageType,
            UsedBytes = DataSize - Header.FreeBytes,
            FreeBytes = Header.FreeBytes,
            ItemCount = Header.ItemCount,
            UsageRatio = (double)(DataSize - Header.FreeBytes) / DataSize,
            IsDirty = IsDirty,
            Version = Header.Version,
            ModifiedAt = new DateTime(Header.ModifiedAt, DateTimeKind.Utc)
        };
    }

    /// <summary>
    /// 检查是否已释放
    /// </summary>
    private void ThrowIfDisposed()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(Page));
    }

    /// <summary>
    /// 释放资源
    /// </summary>
    public void Dispose()
    {
        if (!_disposed)
        {
            // 这里可以添加清理逻辑，如释放大型缓冲区等
            _disposed = true;
        }
    }

    /// <summary>
    /// 重写 ToString 方法
    /// </summary>
    public override string ToString()
    {
        return Header.ToString();
    }
}

/// <summary>
/// 页面使用统计信息
/// </summary>
public sealed class PageUsageInfo
{
    public uint PageID { get; init; }
    public PageType PageType { get; init; }
    public int UsedBytes { get; init; }
    public int FreeBytes { get; init; }
    public int ItemCount { get; init; }
    public double UsageRatio { get; init; }
    public bool IsDirty { get; init; }
    public uint Version { get; init; }
    public DateTime ModifiedAt { get; init; }

    public override string ToString()
    {
        return $"Page[{PageID}] {PageType}: {UsedBytes}/{UsedBytes + FreeBytes} bytes ({UsageRatio:P1}), {ItemCount} items, V{Version}";
    }
}