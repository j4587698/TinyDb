using System;

namespace TinyDb.Core;

/// <summary>
/// 数据库统计信息
/// </summary>
public sealed class DatabaseStatistics
{
    public string FilePath { get; init; } = string.Empty;
    public string DatabaseName { get; init; } = string.Empty;
    public uint Version { get; init; }
    public DateTime CreatedAt { get; init; }
    public DateTime ModifiedAt { get; init; }
    public uint PageSize { get; init; }
    public uint TotalPages { get; init; }
    public uint UsedPages { get; init; }
    public uint FreePages { get; init; }
    public int CollectionCount { get; init; }
    public long FileSize { get; init; }
    public int CachedPages { get; init; }
    public double CacheHitRatio { get; init; }
    public bool IsReadOnly { get; init; }
    public bool EnableJournaling { get; init; }

    public override string ToString()
    {
        return $"Database[{DatabaseName}]: {UsedPages}/{TotalPages} pages, {CollectionCount} collections, " +
               $"{FileSize:N0} bytes, HitRatio={CacheHitRatio * 100:F1}%";
    }
}
