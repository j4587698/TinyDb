using System;
using System.Buffers.Binary;
using System.Numerics;
using System.Runtime.InteropServices;

namespace TinyDb.Bson;

/// <summary>
/// Represents a standard IEEE 754-2008 128-bit decimal floating-point number.
/// </summary>
[StructLayout(LayoutKind.Explicit)]
public struct Decimal128 : IEquatable<Decimal128>, IComparable<Decimal128>, IConvertible
{
    private const int ExponentBias = 6176;
    private const ulong SignMask = 0x8000000000000000UL;
    private const ulong SpecialMask = 0x7800000000000000UL;
    private const ulong InfinityMask = 0x7800000000000000UL;
    private const ulong NaNMask = 0x7C00000000000000UL;
    private static readonly BigInteger UInt96Max = (BigInteger.One << 96) - BigInteger.One;

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

        int exponent = -scale + ExponentBias;

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
        if (TryToDecimal(out var result))
        {
            return result;
        }

        throw new OverflowException("Decimal128 value cannot be converted to decimal.");
    }

    public bool TryToDecimal(out decimal result)
    {
        if (!TryDecodeFinite(out var sign, out var exponent, out var significand))
        {
            result = default;
            return false;
        }

        if (significand.IsZero)
        {
            result = 0m;
            return true;
        }

        var scale = -exponent;
        if (scale < 0)
        {
            significand *= BigInteger.Pow(10, -scale);
            scale = 0;
        }

        while (scale > 28 && significand % 10 == 0)
        {
            significand /= 10;
            scale--;
        }

        if (scale > 28)
        {
            result = default;
            return false;
        }

        if (significand > UInt96Max)
        {
            result = default;
            return false;
        }

        uint low = (uint)(significand & 0xFFFFFFFF);
        uint mid = (uint)((significand >> 32) & 0xFFFFFFFF);
        uint high = (uint)((significand >> 64) & 0xFFFFFFFF);
        result = new decimal((int)low, (int)mid, (int)high, sign, (byte)scale);
        return true;
    }

    public override string ToString()
    {
        if (!TryDecodeFinite(out var sign, out var exponent, out var significand))
        {
            if ((_hi & NaNMask) == NaNMask) return "NaN";
            return (_hi & SignMask) != 0 ? "-Infinity" : "Infinity";
        }

        if (significand.IsZero) return "0";

        var digits = significand.ToString();
        if (exponent >= 0)
        {
            return (sign ? "-" : string.Empty) + digits + new string('0', exponent);
        }

        var scale = -exponent;
        string result;
        if (scale < digits.Length)
        {
            var point = digits.Length - scale;
            result = digits.Insert(point, ".");
        }
        else
        {
            result = "0." + new string('0', scale - digits.Length) + digits;
        }

        result = result.TrimEnd('0').TrimEnd('.');
        return sign ? "-" + result : result;
    }

    public bool Equals(Decimal128 other)
    {
        return CompareTo(other) == 0;
    }

    public override bool Equals(object? obj)
    {
        return obj is Decimal128 other && Equals(other);
    }

    public override int GetHashCode()
    {
        if (!TryDecodeFinite(out var sign, out var exponent, out var significand))
        {
            return HashCode.Combine(_hi, _lo);
        }

        if (significand.IsZero)
        {
            return 0;
        }

        while (significand % 10 == 0)
        {
            significand /= 10;
            exponent++;
        }

        return HashCode.Combine(sign, exponent, significand);
    }

    public int CompareTo(Decimal128 other)
    {
        if (TryDecodeFinite(out var leftSign, out var leftExponent, out var leftSignificand) &&
            other.TryDecodeFinite(out var rightSign, out var rightExponent, out var rightSignificand))
        {
            return CompareFinite(leftSign, leftExponent, leftSignificand, rightSign, rightExponent, rightSignificand);
        }

        var highComparison = _hi.CompareTo(other._hi);
        return highComparison != 0 ? highComparison : _lo.CompareTo(other._lo);
    }

    private bool TryDecodeFinite(out bool sign, out int exponent, out BigInteger significand)
    {
        sign = (_hi & SignMask) != 0;

        if ((_hi & SpecialMask) == InfinityMask)
        {
            exponent = 0;
            significand = BigInteger.Zero;
            return false;
        }

        ulong significandHigh;
        int biasedExponent;
        if ((_hi & 0x6000000000000000UL) == 0x6000000000000000UL)
        {
            biasedExponent = (int)((_hi & 0x1FFFE00000000000UL) >> 47);
            significandHigh = 0x0000800000000000UL | (_hi & 0x00001FFFFFFFFFFFUL);
        }
        else
        {
            biasedExponent = (int)((_hi & 0x7FFF800000000000UL) >> 49);
            significandHigh = _hi & 0x00007FFFFFFFFFFFUL;
        }

        exponent = biasedExponent - ExponentBias;
        significand = (new BigInteger(significandHigh) << 64) | new BigInteger(_lo);
        return true;
    }

    private static int CompareFinite(
        bool leftSign,
        int leftExponent,
        BigInteger leftSignificand,
        bool rightSign,
        int rightExponent,
        BigInteger rightSignificand)
    {
        if (leftSignificand.IsZero && rightSignificand.IsZero) return 0;
        if (leftSign != rightSign) return leftSign ? -1 : 1;

        var commonExponent = Math.Min(leftExponent, rightExponent);
        if (leftExponent != commonExponent)
        {
            leftSignificand *= BigInteger.Pow(10, leftExponent - commonExponent);
        }

        if (rightExponent != commonExponent)
        {
            rightSignificand *= BigInteger.Pow(10, rightExponent - commonExponent);
        }

        var comparison = leftSignificand.CompareTo(rightSignificand);
        return leftSign ? -comparison : comparison;
    }
    
    public byte[] ToBytes()
    {
        var bytes = new byte[16];
        BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(0, 8), _lo);
        BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(8, 8), _hi);
        return bytes;
    }

    public static Decimal128 FromBytes(byte[] bytes)
    {
        if (bytes == null || bytes.Length != 16) throw new ArgumentException("Bytes must be length 16");
        ulong lo = BinaryPrimitives.ReadUInt64LittleEndian(bytes.AsSpan(0, 8));
        ulong hi = BinaryPrimitives.ReadUInt64LittleEndian(bytes.AsSpan(8, 8));
        return new Decimal128(lo, hi);
    }

    public static implicit operator Decimal128(decimal value) => new Decimal128(value);
    public static implicit operator decimal(Decimal128 value) => value.ToDecimal();

    // IConvertible implementation
    public TypeCode GetTypeCode() => TypeCode.Object;
    public bool ToBoolean(IFormatProvider? provider) => CompareTo(Zero) != 0;
    public char ToChar(IFormatProvider? provider) => throw new InvalidCastException();
    public sbyte ToSByte(IFormatProvider? provider) => (sbyte)ToDecimal();
    public byte ToByte(IFormatProvider? provider) => (byte)ToDecimal();
    public short ToInt16(IFormatProvider? provider) => (short)ToDecimal();
    public ushort ToUInt16(IFormatProvider? provider) => (ushort)ToDecimal();
    public int ToInt32(IFormatProvider? provider) => (int)ToDecimal();
    public uint ToUInt32(IFormatProvider? provider) => (uint)ToDecimal();
    public long ToInt64(IFormatProvider? provider) => (long)ToDecimal();
    public ulong ToUInt64(IFormatProvider? provider) => (ulong)ToDecimal();
    public float ToSingle(IFormatProvider? provider) => (float)ToDouble(provider);
    public double ToDouble(IFormatProvider? provider)
    {
        if (!TryDecodeFinite(out var sign, out var exponent, out var significand))
        {
            if ((_hi & NaNMask) == NaNMask) return double.NaN;
            return sign ? double.NegativeInfinity : double.PositiveInfinity;
        }

        var result = (double)significand * Math.Pow(10, exponent);
        return sign ? -result : result;
    }
    public decimal ToDecimal(IFormatProvider? provider) => ToDecimal();
    public DateTime ToDateTime(IFormatProvider? provider) => throw new InvalidCastException();
    public string ToString(IFormatProvider? provider) => ToString();
    public object ToType(Type conversionType, IFormatProvider? provider)
    {
        if (conversionType == typeof(decimal)) return ToDecimal();
        if (conversionType == typeof(Decimal128)) return this;
        if (conversionType == typeof(double)) return ToDouble(provider);
        if (conversionType == typeof(float)) return ToSingle(provider);
        return Convert.ChangeType(ToDecimal(), conversionType, provider);
    }
}
