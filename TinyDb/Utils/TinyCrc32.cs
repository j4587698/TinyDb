using System;
using System.IO.Hashing;

namespace TinyDb.Utils;

internal static class TinyCrc32
{
    private static readonly byte[] ZeroBlock = new byte[256];

    public static uint HashToUInt32(byte[] data)
    {
        if (data == null) throw new ArgumentNullException(nameof(data));
        return HashToUInt32(data.AsSpan());
    }

    public static uint HashToUInt32(ReadOnlySpan<byte> data)
    {
        return Crc32.HashToUInt32(data);
    }

    public static uint HashToUInt32(ReadOnlySpan<byte> first, ReadOnlySpan<byte> second)
    {
        var crc = new Crc32();
        crc.Append(first);
        crc.Append(second);
        return crc.GetCurrentHashAsUInt32();
    }

    public static uint HashToUInt32WithZeroedRange(ReadOnlySpan<byte> data, int zeroStart, int zeroLength)
    {
        if (zeroStart < 0 || zeroLength < 0 || zeroStart > data.Length - zeroLength)
            throw new ArgumentOutOfRangeException(nameof(zeroStart));

        var crc = new Crc32();
        crc.Append(data.Slice(0, zeroStart));

        int zeroEnd = zeroStart + zeroLength;
        while (zeroLength > 0)
        {
            var blockLength = Math.Min(zeroLength, ZeroBlock.Length);
            crc.Append(ZeroBlock.AsSpan(0, blockLength));
            zeroLength -= blockLength;
        }

        crc.Append(data.Slice(zeroEnd));
        return crc.GetCurrentHashAsUInt32();
    }
}
