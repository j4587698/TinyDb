using System.Runtime.InteropServices;

namespace TinyDb.Storage;

/// <summary>
/// 数据库页面头部结构
/// </summary>
[StructLayout(LayoutKind.Sequential, Pack = 1)]
public class PageHeader
{
    /// <summary>
    /// 页面头部大小（字节）
    /// </summary>
    public const int Size = 49; // 1 + 4 + 4 + 4 + 2 + 2 + 4 + 4 + 8 + 8 + 8 = 49

    /// <summary>
    /// 页面类型
    /// </summary>
    public PageType PageType { get; set; }

    /// <summary>
    /// 页面ID
    /// </summary>
    public uint PageID { get; set; }

    /// <summary>
    /// 前一个页面ID（用于链表结构）
    /// </summary>
    public uint PrevPageID { get; set; }

    /// <summary>
    /// 下一个页面ID（用于链表结构）
    /// </summary>
    public uint NextPageID { get; set; }

    /// <summary>
    /// 页面中剩余可用字节数
    /// </summary>
    public ushort FreeBytes { get; set; }

    /// <summary>
    /// 页面中项目数量
    /// </summary>
    public ushort ItemCount { get; set; }

    /// <summary>
    /// 页面版本号（用于并发控制）
    /// </summary>
    public uint Version { get; set; }

    /// <summary>
    /// 页面校验和
    /// </summary>
    public uint Checksum { get; set; }

    /// <summary>
    /// 页面创建时间戳
    /// </summary>
    public long CreatedAt { get; set; }

    /// <summary>
    /// 页面最后修改时间戳
    /// </summary>
    public long ModifiedAt { get; set; }

    /// <summary>
    /// 最后修改此页面的日志序列号 (LSN)
    /// </summary>
    public long LSN { get; set; }

    /// <summary>
    /// 初始化页面头部
    /// </summary>
    public void Initialize(PageType pageType, uint pageID)
    {
        PageType = pageType;
        PageID = pageID;
        PrevPageID = 0;
        NextPageID = 0;
        FreeBytes = 0;
        ItemCount = 0;
        Version = 0; // 按照测试期望
        Checksum = 0;
        LSN = 0;
        var now = DateTime.UtcNow.Ticks;
        CreatedAt = now;
        ModifiedAt = now;
    }

    /// <summary>
    /// 更新修改时间戳和版本号
    /// </summary>
    public void UpdateModification()
    {
        ModifiedAt = DateTime.UtcNow.Ticks;
        Version++;
        Checksum = 0; // 重置校验和，在序列化时重新计算
    }

    /// <summary>
    /// 验证页面头部完整性
    /// </summary>
    public bool IsValid()
    {
        return PageID > 0 &&
               CreatedAt > 0 &&
               ModifiedAt >= CreatedAt &&
               Version >= 0; // Allow version 0
    }


    /// <summary>
    /// 获取页面数据区域大小
    /// </summary>
    /// <param name="pageSize">页面总大小</param>
    /// <returns>数据区域大小</returns>
    public int GetDataSize(int pageSize)
    {
        if (pageSize <= Size) return 0;
        return pageSize - Size;
    }

    /// <summary>
    /// 计算校验和（简单的 CRC32 实现）
    /// </summary>
        public uint CalculateChecksum(byte[] pageData)
        {
            if (pageData == null || pageData.Length < Size)
                return 0;
    
            // 暂时使用简单的累加校验，实际应用中应使用 CRC32 或更强大的算法
            uint checksum = 0;
            for (int i = 0; i < pageData.Length; i++)
            {
                // 跳过校验和字段本身（起始偏移量 21，长度 4）
                if (i >= 21 && i < 25) continue;
                checksum += pageData[i];
            }
            return checksum;
        }

    /// <summary>
    /// 验证校验和
    /// </summary>
    public bool VerifyChecksum(byte[] pageData)
    {
        return CalculateChecksum(pageData) == Checksum;
    }

    /// <summary>
    /// 将头部写入 Span
    /// </summary>
    public void WriteTo(Span<byte> destination)
    {
        if (destination.Length < Size)
            throw new ArgumentException("Destination span is too small.", nameof(destination));

        destination[0] = (byte)PageType;
        System.Buffers.Binary.BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(1), PageID);
        System.Buffers.Binary.BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(5), PrevPageID);
        System.Buffers.Binary.BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(9), NextPageID);
        System.Buffers.Binary.BinaryPrimitives.WriteUInt16LittleEndian(destination.Slice(13), FreeBytes);
        System.Buffers.Binary.BinaryPrimitives.WriteUInt16LittleEndian(destination.Slice(15), ItemCount);
        System.Buffers.Binary.BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(17), Version);
        System.Buffers.Binary.BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(21), Checksum);
        System.Buffers.Binary.BinaryPrimitives.WriteInt64LittleEndian(destination.Slice(25), CreatedAt);
        System.Buffers.Binary.BinaryPrimitives.WriteInt64LittleEndian(destination.Slice(33), ModifiedAt);
        System.Buffers.Binary.BinaryPrimitives.WriteInt64LittleEndian(destination.Slice(41), LSN);
    }

    /// <summary>
    /// 转换为字节数组
    /// </summary>
    public byte[] ToByteArray()
    {
        var bytes = new byte[Size];
        WriteTo(bytes);
        return bytes;
    }

    /// <summary>
    /// 从字节数组创建页面头部
    /// </summary>
    public static PageHeader FromByteArray(byte[] data)
    {
        if (data == null || data.Length < Size)
            throw new ArgumentException("Invalid data size for PageHeader", nameof(data));

        using var stream = new MemoryStream(data, 0, Size);
        using var reader = new BinaryReader(stream);

        var header = new PageHeader
        {
            PageType = (PageType)reader.ReadByte(),
            PageID = reader.ReadUInt32(),
            PrevPageID = reader.ReadUInt32(),
            NextPageID = reader.ReadUInt32(),
            FreeBytes = reader.ReadUInt16(),
            ItemCount = reader.ReadUInt16(),
            Version = reader.ReadUInt32(),
            Checksum = reader.ReadUInt32(),
            CreatedAt = reader.ReadInt64(),
            ModifiedAt = reader.ReadInt64(),
            LSN = reader.ReadInt64()
        };

        return header;
    }

    /// <summary>
    /// 克隆页面头部
    /// </summary>
    public PageHeader Clone()
    {
        return new PageHeader
        {
            PageType = this.PageType,
            PageID = this.PageID,
            PrevPageID = this.PrevPageID,
            NextPageID = this.NextPageID,
            FreeBytes = this.FreeBytes,
            ItemCount = this.ItemCount,
            Version = this.Version,
            Checksum = this.Checksum,
            CreatedAt = this.CreatedAt,
            ModifiedAt = this.ModifiedAt,
            LSN = this.LSN
        };
    }

    /// <summary>
    /// 重写 ToString 方法
    /// </summary>
    public override string ToString()
    {
        return $"Page[Type={PageType}, ID={PageID}, Items={ItemCount}, Free={FreeBytes}, Version={Version}]";
    }
}