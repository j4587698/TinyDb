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
            crc = (crc >> 8) ^ LookupTable[(int)((crc ^ value) & 0xFF)];
        }

        return ~crc;
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
