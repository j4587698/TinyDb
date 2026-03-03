using System;
using System.IO;

namespace Microsoft.IO;

/// <summary>
/// TinyDb 内置的最小兼容实现，仅覆盖当前项目使用到的 API。
/// </summary>
internal sealed class RecyclableMemoryStreamManager
{
    internal sealed class Options
    {
        public int BlockSize { get; set; } = 4096;
        public int LargeBufferMultiple { get; set; } = 1024 * 1024;
        public long MaximumSmallPoolFreeBytes { get; set; } = 1024 * 1024 * 100;
    }

    private readonly Options _options;

    public RecyclableMemoryStreamManager() : this(new Options())
    {
    }

    public RecyclableMemoryStreamManager(Options options)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
    }

    public RecyclableMemoryStream GetStream()
    {
        var initialCapacity = _options.BlockSize > 0 ? _options.BlockSize : 4096;
        return new RecyclableMemoryStream(initialCapacity);
    }
}

/// <summary>
/// TinyDb 内置的最小兼容内存流类型。
/// </summary>
internal sealed class RecyclableMemoryStream : MemoryStream
{
    public RecyclableMemoryStream()
    {
    }

    public RecyclableMemoryStream(int capacity) : base(capacity)
    {
    }
}
