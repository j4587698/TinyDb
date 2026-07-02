using System;
using System.Buffers.Binary;

namespace TinyDb.Utils;

internal static class TinyCrc32
{
    private const uint Polynomial = 0xEDB88320u;
    private static readonly uint[][] LookupTables = CreateLookupTables();
    private static uint[] LookupTable => LookupTables[0];

    public static uint HashToUInt32(byte[] data)
    {
        if (data == null) throw new ArgumentNullException(nameof(data));
        return HashToUInt32(data.AsSpan());
    }

    public static uint HashToUInt32(ReadOnlySpan<byte> data)
    {
        return ~Update(0xFFFFFFFFu, data);
    }

    public static uint HashToUInt32(ReadOnlySpan<byte> first, ReadOnlySpan<byte> second)
    {
        uint crc = 0xFFFFFFFFu;
        crc = Update(crc, first);
        crc = Update(crc, second);
        return ~crc;
    }

    public static uint HashToUInt32WithZeroedRange(ReadOnlySpan<byte> data, int zeroStart, int zeroLength)
    {
        if (zeroStart < 0 || zeroLength < 0 || zeroStart > data.Length - zeroLength)
            throw new ArgumentOutOfRangeException(nameof(zeroStart));

        uint crc = Update(0xFFFFFFFFu, data.Slice(0, zeroStart));
        int zeroEnd = zeroStart + zeroLength;
        for (int i = 0; i < zeroLength; i++)
        {
            crc = Append(crc, 0);
        }

        crc = Update(crc, data.Slice(zeroEnd));
        return ~crc;
    }

    private static uint Update(uint crc, ReadOnlySpan<byte> data)
    {
        while (data.Length >= 8)
        {
            crc ^= BinaryPrimitives.ReadUInt32LittleEndian(data);
            crc =
                LookupTables[7][(int)(crc & 0xFF)] ^
                LookupTables[6][(int)((crc >> 8) & 0xFF)] ^
                LookupTables[5][(int)((crc >> 16) & 0xFF)] ^
                LookupTables[4][(int)(crc >> 24)] ^
                LookupTables[3][data[4]] ^
                LookupTables[2][data[5]] ^
                LookupTables[1][data[6]] ^
                LookupTables[0][data[7]];

            data = data.Slice(8);
        }

        foreach (var value in data)
        {
            crc = Append(crc, value);
        }

        return crc;
    }

    private static uint Append(uint crc, byte value)
    {
        return (crc >> 8) ^ LookupTable[(int)((crc ^ value) & 0xFF)];
    }

    private static uint[][] CreateLookupTables()
    {
        var tables = new uint[8][];
        tables[0] = new uint[256];
        for (uint i = 0; i < tables[0].Length; i++)
        {
            uint value = i;
            for (int bit = 0; bit < 8; bit++)
            {
                value = (value & 1u) != 0 ? (value >> 1) ^ Polynomial : value >> 1;
            }

            tables[0][i] = value;
        }

        for (int tableIndex = 1; tableIndex < tables.Length; tableIndex++)
        {
            tables[tableIndex] = new uint[256];
            for (int i = 0; i < tables[tableIndex].Length; i++)
            {
                var value = tables[tableIndex - 1][i];
                tables[tableIndex][i] = (value >> 8) ^ tables[0][(int)(value & 0xFF)];
            }
        }

        return tables;
    }
}
