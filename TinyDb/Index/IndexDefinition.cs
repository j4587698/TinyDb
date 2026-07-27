namespace TinyDb.Index;

/// <summary>
/// 索引定义（仅结构性元数据，不含任何需要遍历索引树的统计量）。
/// </summary>
/// <remarks>
/// 复制、重建索引等场景只需要索引的定义。使用 <see cref="IndexStatistics"/> 会连带触发
/// 全树遍历，一旦索引结构存在损坏，这类操作就会失败——而它们恰恰是修复损坏所依赖的手段。
/// </remarks>
public sealed class IndexDefinition
{
    public string Name { get; init; } = string.Empty;

    public IndexType Type { get; init; }

    public string[] Fields { get; init; } = Array.Empty<string>();

    public bool IsUnique { get; init; }

    public bool IsSparse { get; init; }

    public override string ToString()
    {
        return $"Index[{Name}]: {Type}, fields={string.Join(",", Fields)}, unique={IsUnique}, sparse={IsSparse}";
    }
}
