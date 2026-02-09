using System;
using TinyDb.Bson;
using TinyDb.Serialization;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Serialization;

public sealed class BsonConversionMissingLineCoverageTests4
{
    private enum SampleEnum
    {
        A = 0,
        B = 1
    }

    private sealed class FakeGuidStringBsonValue : BsonValue
    {
        private readonly string _value;

        public FakeGuidStringBsonValue(string value)
        {
            _value = value;
        }

        public override BsonType BsonType => BsonType.String;
        public override object? RawValue => _value;

        public override int CompareTo(BsonValue? other) => string.Compare(ToString(), other?.ToString(), StringComparison.Ordinal);
        public override bool Equals(BsonValue? other) => other is not null && string.Equals(ToString(), other.ToString(), StringComparison.Ordinal);
        public override int GetHashCode() => StringComparer.Ordinal.GetHashCode(ToString());

        public override TypeCode GetTypeCode() => TypeCode.String;
        public override bool ToBoolean(IFormatProvider? provider) => Convert.ToBoolean(_value, provider);
        public override byte ToByte(IFormatProvider? provider) => Convert.ToByte(_value, provider);
        public override char ToChar(IFormatProvider? provider) => Convert.ToChar(_value, provider);
        public override DateTime ToDateTime(IFormatProvider? provider) => Convert.ToDateTime(_value, provider);
        public override decimal ToDecimal(IFormatProvider? provider) => Convert.ToDecimal(_value, provider);
        public override double ToDouble(IFormatProvider? provider) => Convert.ToDouble(_value, provider);
        public override short ToInt16(IFormatProvider? provider) => Convert.ToInt16(_value, provider);
        public override int ToInt32(IFormatProvider? provider) => Convert.ToInt32(_value, provider);
        public override long ToInt64(IFormatProvider? provider) => Convert.ToInt64(_value, provider);
        public override sbyte ToSByte(IFormatProvider? provider) => Convert.ToSByte(_value, provider);
        public override float ToSingle(IFormatProvider? provider) => Convert.ToSingle(_value, provider);
        public override string ToString(IFormatProvider? provider) => _value;
        public override object ToType(Type conversionType, IFormatProvider? provider) => Convert.ChangeType(_value, conversionType, provider)!;
        public override ushort ToUInt16(IFormatProvider? provider) => Convert.ToUInt16(_value, provider);
        public override uint ToUInt32(IFormatProvider? provider) => Convert.ToUInt32(_value, provider);
        public override ulong ToUInt64(IFormatProvider? provider) => Convert.ToUInt64(_value, provider);
    }

    private sealed class NullToStringKey
    {
        public override string? ToString() => null;
    }

    [Test]
    public async Task FromBsonValue_Object_WhenNullBson_ShouldReturnNull()
    {
        var value = BsonConversion.FromBsonValue(BsonNull.Value, typeof(object));
        await Assert.That(value).IsNull();
    }

    [Test]
    public async Task FromBsonValue_WhenBsonValueNull_ShouldThrow()
    {
        await Assert.That(() => BsonConversion.FromBsonValue(null!, typeof(int)))
            .ThrowsExactly<ArgumentNullException>();
    }

    [Test]
    public async Task FromBsonValue_WhenTargetTypeNull_ShouldThrow()
    {
        await Assert.That(() => BsonConversion.FromBsonValue(new BsonInt32(1), null!))
            .ThrowsExactly<ArgumentNullException>();
    }

    [Test]
    public async Task FromBsonValueEnum_WhenBsonValueNull_ShouldThrow()
    {
        await Assert.That(() => BsonConversion.FromBsonValueEnum<SampleEnum>(null!))
            .ThrowsExactly<ArgumentNullException>();
    }

    [Test]
    public async Task FromBsonValue_String_FromNonStringBson_ShouldUseToString()
    {
        var result = (string)BsonConversion.FromBsonValue(new BsonInt32(123), typeof(string))!;
        await Assert.That(result).IsEqualTo("123");
    }

    [Test]
    public async Task FromBsonValue_DateTime_FromBsonString_ShouldParse()
    {
        var dt = new DateTime(2025, 1, 2, 3, 4, 5, DateTimeKind.Utc);
        var value = (DateTime)BsonConversion.FromBsonValue(new BsonString(dt.ToString("O")), typeof(DateTime))!;
        await Assert.That(value).IsEqualTo(dt);
    }

    [Test]
    public async Task FromBsonValue_Guid_FromBsonBinary_DefaultSubType_ShouldParse()
    {
        var guid = Guid.NewGuid();
        var bin = new BsonBinary(guid.ToByteArray(), BsonBinary.BinarySubType.Generic);
        var result = (Guid)BsonConversion.FromBsonValue(bin, typeof(Guid))!;
        await Assert.That(result).IsEqualTo(guid);
    }

    [Test]
    public async Task FromBsonValue_Guid_FromNonStringNonBinary_ShouldParseFromToString()
    {
        var guid = Guid.NewGuid();
        var fake = new FakeGuidStringBsonValue(guid.ToString());
        var result = (Guid)BsonConversion.FromBsonValue(fake, typeof(Guid))!;
        await Assert.That(result).IsEqualTo(guid);
    }

    [Test]
    public async Task ToBsonValue_Dictionary_KeyToStringNull_ShouldFallbackToEmptyKey()
    {
        var dict = new System.Collections.Hashtable
        {
            [new NullToStringKey()] = 123
        };

        await Assert.That(() => BsonConversion.ToBsonValue(dict))
            .ThrowsExactly<NotSupportedException>();
    }
}
