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
    public int NodeCount { get; init; }
    public int EntryCount { get; init; }
    public int MaxKeysPerNode { get; init; }
    public double AverageKeysPerNode { get; init; }
    public int TreeHeight { get; init; }
    public bool RootIsLeaf { get; init; }

    public override string ToString()
    {
        return $"Index[{Name}]: {Type}, {Fields.Length} fields, {EntryCount} entries, " +
               $"{NodeCount} nodes, Height={TreeHeight}";
    }
}
