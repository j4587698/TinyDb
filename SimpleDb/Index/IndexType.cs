namespace SimpleDb.Index;

/// <summary>
/// 索引类型枚举
/// </summary>
public enum IndexType : byte
{
    /// <summary>
    /// B+ 树索引
    /// </summary>
    BTree = 0x01,

    /// <summary>
    /// 哈希索引
    /// </summary>
    Hash = 0x02,

    /// <summary>
    /// 全文索引
    /// </summary>
    FullText = 0x03,

    /// <summary>
    /// 地理空间索引
    /// </summary>
    GeoSpatial = 0x04
}