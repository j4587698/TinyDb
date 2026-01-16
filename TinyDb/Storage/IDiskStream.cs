using System;
using System.Threading;
using System.Threading.Tasks;

namespace TinyDb.Storage;

/// <summary>
/// 磁盘流接口，抽象底层文件 I/O 操作
/// </summary>
public interface IDiskStream : IDisposable
{
    /// <summary>
    /// 文件路径（如果是内存流则为标识符）
    /// </summary>
    string FilePath { get; }

    /// <summary>
    /// 文件大小
    /// </summary>
    long Size { get; }

    /// <summary>
    /// 是否可读
    /// </summary>
    bool IsReadable { get; }

    /// <summary>
    /// 是否可写
    /// </summary>
    bool IsWritable { get; }

    /// <summary>
    /// 读取页面数据
    /// </summary>
    /// <param name="pageOffset">页面偏移量</param>
    /// <param name="pageSize">页面大小</param>
    /// <returns>页面数据</returns>
    byte[] ReadPage(long pageOffset, int pageSize);

    /// <summary>
    /// 写入页面数据
    /// </summary>
    /// <param name="pageOffset">页面偏移量</param>
    /// <param name="pageData">页面数据</param>
    void WritePage(long pageOffset, byte[] pageData);

    /// <summary>
    /// 异步读取页面数据
    /// </summary>
    Task<byte[]> ReadPageAsync(long pageOffset, int pageSize, CancellationToken cancellationToken = default);

    /// <summary>
    /// 异步写入页面数据
    /// </summary>
    Task WritePageAsync(long pageOffset, byte[] pageData, CancellationToken cancellationToken = default);

    /// <summary>
    /// 刷新缓冲区到存储介质
    /// </summary>
    void Flush();

    /// <summary>
    /// 异步刷新缓冲区
    /// </summary>
    Task FlushAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// 设置文件长度
    /// </summary>
    void SetLength(long length);

    /// <summary>
    /// 获取文件统计信息
    /// </summary>
    /// <returns>统计信息</returns>
    DiskStreamStatistics GetStatistics();
}
