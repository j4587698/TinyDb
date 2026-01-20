using System;
using System.Runtime.InteropServices;

namespace TinyDb.Bson;

/// <summary>
/// Represents a standard IEEE 754-2008 128-bit decimal floating-point number.
/// </summary>
[StructLayout(LayoutKind.Explicit)]
public struct Decimal128 : IEquatable<Decimal128>, IComparable<Decimal128>, IConvertible
{
    [FieldOffset(0)]
    private readonly ulong _lo;
    [FieldOffset(8)]
    private readonly ulong _hi;

    public static readonly Decimal128 Zero = new Decimal128(0, 0);
    public static readonly Decimal128 MinValue = new Decimal128(0x0000000000000000, 0xf800000000000000);
    public static readonly Decimal128 MaxValue = new Decimal128(0xffffffffffffffff, 0x7800000000000000);

    public Decimal128(ulong lo, ulong hi)
    {
        _lo = lo;
        _hi = hi;
    }

    /// <summary>
    /// Gets the high 64 bits of the Decimal128.
    /// </summary>
    public ulong HighBits => _hi;

    /// <summary>
    /// Gets the low 64 bits of the Decimal128.
    /// </summary>
    public ulong LowBits => _lo;

    /// <summary>
    /// Creates a Decimal128 from a C# decimal.
    /// </summary>
    public Decimal128(decimal value)
    {
        int[] bits = decimal.GetBits(value);
        ulong low = (uint)bits[0] | ((ulong)(uint)bits[1] << 32);
        uint high = (uint)bits[2];
        int flags = bits[3];
        bool sign = (flags & 0x80000000) != 0;
        int scale = (flags >> 16) & 0x7F;

        int exponent = -scale + 6176;

        ulong hi = 0;
        ulong lo = 0;

        lo = low;
        hi = high;

        ulong biasedExponent = (ulong)exponent;
        ulong signBit = sign ? 1UL : 0UL;
        
        _lo = lo;
        _hi = (signBit << 63) | (biasedExponent << 49) | hi;
    }

    /// <summary>
    /// Converts to C# decimal.
    /// </summary>
    public decimal ToDecimal()
    {
        bool sign = (_hi & 0x8000000000000000) != 0;
        ulong combination = (_hi >> 49) & 0x7FFF;
        
        if ((combination & 0x6000) == 0x6000)
        {
            throw new OverflowException("Decimal128 Infinity or NaN cannot be converted to decimal.");
        }
        
        int exponent = (int)(combination & 0x3FFF) - 6176;
        ulong significandHigh = _hi & 0x0001FFFFFFFFFFFF; 
        
        uint low = (uint)_lo;
        uint mid = (uint)(_lo >> 32);
        uint high = (uint)significandHigh;
        
        if (high == 0 && mid == 0 && low == 0) return 0m;
        
        if ((significandHigh >> 32) != 0) 
        {
             throw new OverflowException("Decimal128 value is too large for decimal.");
        }

        int scale = -exponent;
        if (scale < 0 || scale > 28)
        {
             throw new OverflowException($"Decimal128 scale {scale} is out of range for decimal.");
        }

        return new decimal((int)low, (int)mid, (int)high, sign, (byte)scale);
    }

    public override string ToString()
    {
        try
        {
            return ToDecimal().ToString();
        }
        catch
        {
            return "NaN/Infinity/Overflow";
        }
    }

    public bool Equals(Decimal128 other)
    {
        return _lo == other._lo && _hi == other._hi;
    }

    public override bool Equals(object? obj)
    {
        return obj is Decimal128 other && Equals(other);
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(_lo, _hi);
    }

    public int CompareTo(Decimal128 other)
    {
        return ToDecimal().CompareTo(other.ToDecimal());
    }
    
    public byte[] ToBytes()
    {
        var bytes = new byte[16];
        BitConverter.TryWriteBytes(bytes, _lo);
        BitConverter.TryWriteBytes(bytes.AsSpan(8), _hi);
        return bytes;
    }

    public static Decimal128 FromBytes(byte[] bytes)
    {
        if (bytes == null || bytes.Length != 16) throw new ArgumentException("Bytes must be length 16");
        ulong lo = BitConverter.ToUInt64(bytes, 0);
        ulong hi = BitConverter.ToUInt64(bytes, 8);
        return new Decimal128(lo, hi);
    }

    public static implicit operator Decimal128(decimal value) => new Decimal128(value);
    public static implicit operator decimal(Decimal128 value) => value.ToDecimal();

    // IConvertible implementation
    public TypeCode GetTypeCode() => TypeCode.Object;
    public bool ToBoolean(IFormatProvider? provider) => !Equals(Zero);
    public char ToChar(IFormatProvider? provider) => throw new InvalidCastException();
    public sbyte ToSByte(IFormatProvider? provider) => (sbyte)ToDecimal();
    public byte ToByte(IFormatProvider? provider) => (byte)ToDecimal();
    public short ToInt16(IFormatProvider? provider) => (short)ToDecimal();
    public ushort ToUInt16(IFormatProvider? provider) => (ushort)ToDecimal();
    public int ToInt32(IFormatProvider? provider) => (int)ToDecimal();
    public uint ToUInt32(IFormatProvider? provider) => (uint)ToDecimal();
    public long ToInt64(IFormatProvider? provider) => (long)ToDecimal();
    public ulong ToUInt64(IFormatProvider? provider) => (ulong)ToDecimal();
    public float ToSingle(IFormatProvider? provider) => (float)ToDecimal();
    public double ToDouble(IFormatProvider? provider) => (double)ToDecimal();
    public decimal ToDecimal(IFormatProvider? provider) => ToDecimal();
    public DateTime ToDateTime(IFormatProvider? provider) => throw new InvalidCastException();
    public string ToString(IFormatProvider? provider) => ToString();
    public object ToType(Type conversionType, IFormatProvider? provider)
    {
        if (conversionType == typeof(decimal)) return ToDecimal();
        if (conversionType == typeof(Decimal128)) return this;
        return Convert.ChangeType(ToDecimal(), conversionType, provider);
    }
}