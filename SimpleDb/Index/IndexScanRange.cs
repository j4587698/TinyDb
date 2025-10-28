using SimpleDb.Bson;

namespace SimpleDb.Index;

/// <summary>
/// 索引扫描范围
/// </summary>
public sealed class IndexScanRange
{
    /// <summary>
    /// 最小键值
    /// </summary>
    public IndexKey MinKey { get; set; } = IndexKey.MinValue;

    /// <summary>
    /// 最大键值
    /// </summary>
    public IndexKey MaxKey { get; set; } = IndexKey.MaxValue;

    /// <summary>
    /// 是否包含最小值
    /// </summary>
    public bool IncludeMin { get; set; } = true;

    /// <summary>
    /// 是否包含最大值
    /// </summary>
    public bool IncludeMax { get; set; } = true;
}