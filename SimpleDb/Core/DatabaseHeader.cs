using System.Runtime.InteropServices;
using System.Text;

namespace SimpleDb.Core;

/// <summary>
/// 数据库头部信息，存储在文件的第一个页面
/// </summary>
[StructLayout(LayoutKind.Sequential, Pack = 1)]
public struct DatabaseHeader
{
    /// <summary>
    /// 头部大小（字节）
    /// </summary>
    public const int Size = 256;

    /// <summary>
    /// 初始化数据库头部
    /// </summary>
    public DatabaseHeader()
    {
        Magic = MagicNumber;
        DatabaseVersion = Version;
        PageSize = 4096;
        TotalPages = 1;
        UsedPages = 1;
        FirstFreePage = 0;
        CollectionInfoPage = 2;
        IndexInfoPage = 3;
        JournalInfoPage = 4;
        CreatedAt = DateTime.UtcNow.Ticks;
        ModifiedAt = DateTime.UtcNow.Ticks;
        Checksum = 0;
        EnableJournaling = true;
        _reserved = new byte[60];
        _databaseName = new byte[64];
        _userData = new byte[64];
    }

    /// <summary>
    /// 魔数标识 "SDB"
    /// </summary>
    public const uint MagicNumber = 0x44425353; // "SSDB" in little endian

    /// <summary>
    /// 版本号
    /// </summary>
    public const uint Version = 0x00010000; // 1.0.0

    /// <summary>
    /// 魔数
    /// </summary>
    public uint Magic;

    /// <summary>
    /// 数据库版本
    /// </summary>
    public uint DatabaseVersion;

    /// <summary>
    /// 页面大小
    /// </summary>
    public uint PageSize;

    /// <summary>
    /// 总页面数
    /// </summary>
    public uint TotalPages;

    /// <summary>
    /// 已使用页面数
    /// </summary>
    public uint UsedPages;

    /// <summary>
    /// 第一个空闲页面ID
    /// </summary>
    public uint FirstFreePage;

    /// <summary>
    /// 集合信息页面ID
    /// </summary>
    public uint CollectionInfoPage;

    /// <summary>
    /// 索引信息页面ID
    /// </summary>
    public uint IndexInfoPage;

    /// <summary>
    /// 日志信息页面ID
    /// </summary>
    public uint JournalInfoPage;

    /// <summary>
    /// 数据库创建时间戳
    /// </summary>
    public long CreatedAt;

    /// <summary>
    /// 数据库最后修改时间戳
    /// </summary>
    public long ModifiedAt;

    /// <summary>
    /// 数据库校验和
    /// </summary>
    public uint Checksum;

    /// <summary>
    /// 是否启用日志
    /// </summary>
    public bool EnableJournaling;

    /// <summary>
    /// 保留字段
    /// </summary>
    private byte[] _reserved = new byte[60]; // 减少字节以容纳bool字段

    /// <summary>
    /// 数据库名称（UTF-8编码，最多64字节）
    /// </summary>
    private byte[] _databaseName = new byte[64];

    /// <summary>
    /// 用户数据（64字节）
    /// </summary>
    private byte[] _userData = new byte[64];

    /// <summary>
    /// 获取数据库名称
    /// </summary>
    public string DatabaseName
    {
        get
        {
            var bytes = new byte[64];
            for (int i = 0; i < 64; i++)
            {
                if (_databaseName[i] == 0) break;
                bytes[i] = _databaseName[i];
            }
            return Encoding.UTF8.GetString(bytes).TrimEnd('\0');
        }
        set
        {
            if (value == null) throw new ArgumentNullException(nameof(value));
            if (Encoding.UTF8.GetByteCount(value) > 63)
                throw new ArgumentException("Database name is too long (max 63 bytes)", nameof(value));

            var bytes = Encoding.UTF8.GetBytes(value);
            for (int i = 0; i < 64; i++)
            {
                _databaseName[i] = i < bytes.Length ? bytes[i] : (byte)0;
            }
        }
    }

    /// <summary>
    /// 获取用户数据
    /// </summary>
    public byte[] UserData
    {
        get
        {
            unsafe
            {
                var bytes = new byte[64];
                for (int i = 0; i < 64; i++)
                {
                    bytes[i] = _userData[i];
                }
                return bytes;
            }
        }
        set
        {
            if (value == null) throw new ArgumentNullException(nameof(value));
            if (value.Length > 64)
                throw new ArgumentException("User data is too long (max 64 bytes)", nameof(value));

            unsafe
            {
                for (int i = 0; i < 64; i++)
                {
                    _userData[i] = i < value.Length ? value[i] : (byte)0;
                }
            }
        }
    }

    /// <summary>
    /// 初始化数据库头部
    /// </summary>
    /// <param name="pageSize">页面大小</param>
    /// <param name="databaseName">数据库名称</param>
    public void Initialize(uint pageSize, string databaseName = "SimpleDb")
    {
        Magic = MagicNumber;
        DatabaseVersion = Version;
        PageSize = pageSize;
        TotalPages = 1; // 头部页面
        UsedPages = 1;
        FirstFreePage = 0;
        CollectionInfoPage = 0;
        IndexInfoPage = 0;
        JournalInfoPage = 0;
        var now = DateTime.UtcNow.Ticks;
        CreatedAt = now;
        ModifiedAt = now;
        Checksum = 0;
        EnableJournaling = true;
        DatabaseName = databaseName;

        unsafe
        {
            // 清空保留字段
            for (int i = 0; i < 60; i++)
            {
                _reserved[i] = 0;
            }

            // 清空用户数据
            for (int i = 0; i < 64; i++)
            {
                _userData[i] = 0;
            }
        }
    }

    /// <summary>
    /// 验证头部有效性
    /// </summary>
    /// <returns>是否有效</returns>
    public bool IsValid()
    {
        return Magic == MagicNumber &&
               DatabaseVersion >= Version &&
               PageSize >= 4096 &&
               TotalPages >= 1 &&
               UsedPages <= TotalPages &&
               CreatedAt > 0 &&
               ModifiedAt >= CreatedAt;
    }

    /// <summary>
    /// 计算校验和
    /// </summary>
    /// <returns>校验和</returns>
    public uint CalculateChecksum()
    {
        // 简单的 CRC32 实现，这里使用累加作为示例
        var data = ToByteArray();
        uint checksum = 0;

        for (int i = 0; i < Size; i++)
        {
            if (i >= 44 && i < 48) // 跳过校验和字段本身
                continue;

            checksum += data[i];
        }

        return checksum;
    }

    /// <summary>
    /// 验证校验和
    /// </summary>
    /// <returns>是否匹配</returns>
    public bool VerifyChecksum()
    {
        return CalculateChecksum() == Checksum;
    }

    /// <summary>
    /// 更新修改时间和校验和
    /// </summary>
    public void UpdateModification()
    {
        ModifiedAt = DateTime.UtcNow.Ticks;
        Checksum = CalculateChecksum();
    }

    /// <summary>
    /// 转换为字节数组
    /// </summary>
    /// <returns>字节数组</returns>
    public byte[] ToByteArray()
    {
        var buffer = new byte[Size];
        using var stream = new MemoryStream(buffer);
        using var writer = new BinaryWriter(stream);

        writer.Write(Magic);
        writer.Write(DatabaseVersion);
        writer.Write(PageSize);
        writer.Write(TotalPages);
        writer.Write(UsedPages);
        writer.Write(FirstFreePage);
        writer.Write(CollectionInfoPage);
        writer.Write(IndexInfoPage);
        writer.Write(JournalInfoPage);
        writer.Write(CreatedAt);
        writer.Write(ModifiedAt);
        writer.Write(Checksum);
        writer.Write(EnableJournaling);

        unsafe
        {
            // 写入保留字段
            for (int i = 0; i < 60; i++)
            {
                writer.Write(_reserved[i]);
            }

            // 写入数据库名称
            for (int i = 0; i < 64; i++)
            {
                writer.Write(_databaseName[i]);
            }

            // 写入用户数据
            for (int i = 0; i < 64; i++)
            {
                writer.Write(_userData[i]);
            }
        }

        return buffer;
    }

    /// <summary>
    /// 从字节数组创建数据库头部
    /// </summary>
    /// <param name="data">字节数组</param>
    /// <returns>数据库头部</returns>
    public static DatabaseHeader FromByteArray(byte[] data)
    {
        if (data == null || data.Length < Size)
            throw new ArgumentException("Invalid data size for DatabaseHeader", nameof(data));

        using var stream = new MemoryStream(data, 0, Size);
        using var reader = new BinaryReader(stream);

        var header = new DatabaseHeader
        {
            Magic = reader.ReadUInt32(),
            DatabaseVersion = reader.ReadUInt32(),
            PageSize = reader.ReadUInt32(),
            TotalPages = reader.ReadUInt32(),
            UsedPages = reader.ReadUInt32(),
            FirstFreePage = reader.ReadUInt32(),
            CollectionInfoPage = reader.ReadUInt32(),
            IndexInfoPage = reader.ReadUInt32(),
            JournalInfoPage = reader.ReadUInt32(),
            CreatedAt = reader.ReadInt64(),
            ModifiedAt = reader.ReadInt64(),
            Checksum = reader.ReadUInt32(),
            EnableJournaling = reader.ReadBoolean()
        };

        unsafe
        {
            // 读取保留字段
            for (int i = 0; i < 60; i++)
            {
                header._reserved[i] = reader.ReadByte();
            }

            // 读取数据库名称
            for (int i = 0; i < 64; i++)
            {
                header._databaseName[i] = reader.ReadByte();
            }

            // 读取用户数据
            for (int i = 0; i < 64; i++)
            {
                header._userData[i] = reader.ReadByte();
            }
        }

        return header;
    }

    /// <summary>
    /// 克隆数据库头部
    /// </summary>
    /// <returns>新的头部实例</returns>
    public DatabaseHeader Clone()
    {
        return FromByteArray(ToByteArray());
    }

    /// <summary>
    /// 重写 ToString 方法
    /// </summary>
    public override string ToString()
    {
        return $"DatabaseHeader[{DatabaseName}]: v{DatabaseVersion >> 16}.{(DatabaseVersion >> 8) & 0xFF}.{DatabaseVersion & 0xFF}, " +
               $"PageSize={PageSize}, Pages={UsedPages}/{TotalPages}, Created={new DateTime(CreatedAt, DateTimeKind.Utc):yyyy-MM-dd}";
    }
}