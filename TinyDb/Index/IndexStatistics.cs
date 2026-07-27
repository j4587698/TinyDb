using TinyDb.Bson;

namespace TinyDb.Index;

/// <summary>
/// 索引统计信息
/// </summary>
public sealed class IndexStatistics
{
    public string Name { get; init; } = string.Empty;
    public IndexType Type { get; init; }
    public string[] Fields { get; init; } = Array.Empty<string>();
    public bool IsUnique { get; init; }
    public bool IsSparse { get; init; }
    public int NodeCount { get; init; }
    public int EntryCount { get; init; }
    public int MaxKeysPerNode { get; init; }
    public double AverageKeysPerNode { get; init; }
    public int TreeHeight { get; init; }
    public bool RootIsLeaf { get; init; }

    /// <summary>
    /// 因结构损坏而无法遍历的子树数量，0 表示索引结构完好。
    /// 大于 0 时说明部分索引项已不可达，需重建索引或执行 CompactDatabase 恢复。
    /// </summary>
    public int DamagedSubtreeCount { get; init; }

    /// <summary>
    /// 索引结构是否存在损坏。
    /// </summary>
    public bool IsDamaged => DamagedSubtreeCount > 0;

    public override string ToString()
    {
        var damage = DamagedSubtreeCount > 0 ? $", DAMAGED={DamagedSubtreeCount}" : string.Empty;
        return $"Index[{Name}]: {Type}, {Fields.Length} fields, {EntryCount} entries, " +
               $"{NodeCount} nodes, Height={TreeHeight}{damage}";
    }
}
