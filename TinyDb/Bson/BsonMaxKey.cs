using System;

namespace TinyDb.Bson;

/// <summary>
/// BSON 最大键类型
/// </summary>
public sealed class BsonMaxKey : BsonValue
{
    private static readonly BsonMaxKey _value = new();

    public static BsonMaxKey Value => _value;

    public override BsonType BsonType => BsonType.MaxKey;

    public override object? RawValue => null;

    private BsonMaxKey() { }

    public override int CompareTo(BsonValue? other)
    {
        if (other is null) return 1;
        if (other is BsonMaxKey) return 0;
        return 1; // MaxKey is always greater than everything else
    }

    public override bool Equals(BsonValue? other) => other is BsonMaxKey;

    public override int GetHashCode() => BsonType.GetHashCode();

    public override TypeCode GetTypeCode() => TypeCode.Object;
    public override bool ToBoolean(IFormatProvider? provider) => true;
    public override byte ToByte(IFormatProvider? provider) => 255;
    public override char ToChar(IFormatProvider? provider) => char.MaxValue;
    public override DateTime ToDateTime(IFormatProvider? provider) => DateTime.MaxValue;
    public override decimal ToDecimal(IFormatProvider? provider) => decimal.MaxValue;
    public override double ToDouble(IFormatProvider? provider) => double.MaxValue;
    public override short ToInt16(IFormatProvider? provider) => short.MaxValue;
    public override int ToInt32(IFormatProvider? provider) => int.MaxValue;
    public override long ToInt64(IFormatProvider? provider) => long.MaxValue;
    public override sbyte ToSByte(IFormatProvider? provider) => sbyte.MaxValue;
    public override float ToSingle(IFormatProvider? provider) => float.MaxValue;
    public override string ToString(IFormatProvider? provider) => "$maxKey";
    public override string ToString() => "$maxKey";
    public override object ToType(Type conversionType, IFormatProvider? provider) => throw new InvalidCastException();
    public override ushort ToUInt16(IFormatProvider? provider) => ushort.MaxValue;
    public override uint ToUInt32(IFormatProvider? provider) => uint.MaxValue;
    public override ulong ToUInt64(IFormatProvider? provider) => ulong.MaxValue;
}
