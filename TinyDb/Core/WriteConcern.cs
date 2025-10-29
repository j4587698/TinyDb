namespace TinyDb.Core;

/// <summary>
/// 写入关注级别，用于平衡性能与持久化保证
/// </summary>
public enum WriteConcern
{
    /// <summary>
    /// 仅保证内存缓存成功，依赖后台刷新器最终落盘。
    /// </summary>
    None = 0,

    /// <summary>
    /// 写前日志在返回前刷入磁盘，数据页由后台批量落盘。
    /// </summary>
    Journaled = 1,

    /// <summary>
    /// 写前日志与数据页均在返回前强制落盘，提供最强持久化。
    /// </summary>
    Synced = 2
}
