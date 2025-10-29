namespace TinyDb.Storage;

/// <summary>
/// 数据库页面类型枚举
/// </summary>
public enum PageType : byte
{
    /// <summary>
    /// 空页面
    /// </summary>
    Empty = 0x00,

    /// <summary>
    /// 数据库头部页面
    /// </summary>
    Header = 0x01,

    /// <summary>
    /// 集合信息页面
    /// </summary>
    Collection = 0x02,

    /// <summary>
    /// 数据页面
    /// </summary>
    Data = 0x03,

    /// <summary>
    /// 索引页面
    /// </summary>
    Index = 0x04,

    /// <summary>
    /// 日志页面
    /// </summary>
    Journal = 0x05,

    /// <summary>
    /// 扩展页面（预留）
    /// </summary>
    Extension = 0x06
}