using System;

namespace TinyDb.Utils;

internal static class TinyCrc32
{
    private const uint Polynomial = 0xEDB88320u;
    private static readonly uint[] LookupTable = CreateLookupTable();

    public static uint HashToUInt32(byte[] data)
    {
        if (data == null) throw new ArgumentNullException(nameof(data));
        return HashToUInt32(data.AsSpan());
    }

    public static uint HashToUInt32(ReadOnlySpan<byte> data)
    {
        uint crc = 0xFFFFFFFFu;
        foreach (var value in data)
        {
            crc = Append(crc, value);
        }

        return ~crc;
    }

    public static uint HashToUInt32WithZeroedRange(ReadOnlySpan<byte> data, int zeroStart, int zeroLength)
    {
        if (zeroStart < 0 || zeroLength < 0 || zeroStart > data.Length - zeroLength)
            throw new ArgumentOutOfRangeException(nameof(zeroStart));

        uint crc = 0xFFFFFFFFu;
        int zeroEnd = zeroStart + zeroLength;

        for (int i = 0; i < data.Length; i++)
        {
            var value = i >= zeroStart && i < zeroEnd ? (byte)0 : data[i];
            crc = Append(crc, value);
        }

        return ~crc;
    }

    private static uint Append(uint crc, byte value)
    {
        return (crc >> 8) ^ LookupTable[(int)((crc ^ value) & 0xFF)];
    }

    private static uint[] CreateLookupTable()
    {
        var table = new uint[256];
        for (uint i = 0; i < table.Length; i++)
        {
            uint value = i;
            for (int bit = 0; bit < 8; bit++)
            {
                value = (value & 1u) != 0 ? (value >> 1) ^ Polynomial : value >> 1;
            }

            table[i] = value;
        }

        return table;
    }
}
