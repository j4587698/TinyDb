using System;

namespace TinyDb.Core;

internal readonly struct RawScanResult
{
    public readonly ReadOnlyMemory<byte> Slice;
    public readonly bool RequiresPostFilter;

    public RawScanResult(ReadOnlyMemory<byte> slice, bool requiresPostFilter)
    {
        Slice = slice;
        RequiresPostFilter = requiresPostFilter;
    }
}

