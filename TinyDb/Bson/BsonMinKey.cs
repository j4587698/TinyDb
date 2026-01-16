using System;

namespace TinyDb.Bson;

/// <summary>
/// BSON 最小键类型
/// </summary>
public sealed class BsonMinKey : BsonValue
{
    private static readonly BsonMinKey _value = new();

    public static BsonMinKey Value => _value;

    public override BsonType BsonType => BsonType.MinKey;

    public override object? RawValue => null;

    private BsonMinKey() { }

    public override int CompareTo(BsonValue? other)
    {
        if (other is null) return 1;
        if (other is BsonMinKey) return 0;
        return -1; // MinKey is always smaller than everything else
    }

    public override bool Equals(BsonValue? other) => other is BsonMinKey;

    public override int GetHashCode() => BsonType.GetHashCode();

    public override TypeCode GetTypeCode() => TypeCode.Object;
    public override bool ToBoolean(IFormatProvider? provider) => false;
    public override byte ToByte(IFormatProvider? provider) => 0;
    public override char ToChar(IFormatProvider? provider) => '\0';
    public override DateTime ToDateTime(IFormatProvider? provider) => DateTime.MinValue;
    public override decimal ToDecimal(IFormatProvider? provider) => 0m;
    public override double ToDouble(IFormatProvider? provider) => 0.0;
    public override short ToInt16(IFormatProvider? provider) => 0;
    public override int ToInt32(IFormatProvider? provider) => 0;
    public override long ToInt64(IFormatProvider? provider) => 0;
    public override sbyte ToSByte(IFormatProvider? provider) => 0;
    public override float ToSingle(IFormatProvider? provider) => 0.0f;
    public override string ToString(IFormatProvider? provider) => "$minKey";
    public override string ToString() => "$minKey";
    public override object ToType(Type conversionType, IFormatProvider? provider) => throw new InvalidCastException();
    public override ushort ToUInt16(IFormatProvider? provider) => 0;
    public override uint ToUInt32(IFormatProvider? provider) => 0;
    public override ulong ToUInt64(IFormatProvider? provider) => 0;
}
