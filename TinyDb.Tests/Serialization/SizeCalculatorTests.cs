using TinyDb.Bson;
using TinyDb.Serialization;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Serialization;

public class SizeCalculatorTests
{
    [Test]
    public async Task CalculateSize_Should_Match_Actual_Serialized_Size()
    {
        var calc = new SizeCalculator();
        
        var doc = new BsonDocument();
        doc = doc.Set("bin", new BsonBinary(new byte[] { 1, 2, 3 }));
        doc = doc.Set("reg", new BsonRegularExpression("p", "o"));
        doc = doc.Set("ts", new BsonTimestamp(123));
        doc = doc.Set("dec", new BsonDecimal128(123.45m));
        
        var expected = calc.CalculateDocumentSize(doc);
        var actual = BsonSerializer.SerializeDocument(doc).Length;
        
        await Assert.That(actual).IsEqualTo(expected);
    }

    [Test]
    public async Task CalculateSize_Unsupported_Should_Throw()
    {
        var calc = new SizeCalculator();
        var custom = new CustomBsonValue();
        await Assert.That(() => calc.CalculateSize(custom)).Throws<NotSupportedException>();
    }

    private class CustomBsonValue : BsonValue
    {
        public override BsonType BsonType => (BsonType)0x7F;
        public override object? RawValue => null;
        public override int CompareTo(BsonValue? other) => 0;
        public override bool Equals(BsonValue? other) => false;
        public override int GetHashCode() => 0;
        public override TypeCode GetTypeCode() => TypeCode.Object;
        public override bool ToBoolean(IFormatProvider? provider) => false;
        public override byte ToByte(IFormatProvider? provider) => 0;
        public override char ToChar(IFormatProvider? provider) => '\0';
        public override DateTime ToDateTime(IFormatProvider? provider) => DateTime.MinValue;
        public override decimal ToDecimal(IFormatProvider? provider) => 0;
        public override double ToDouble(IFormatProvider? provider) => 0;
        public override short ToInt16(IFormatProvider? provider) => 0;
        public override int ToInt32(IFormatProvider? provider) => 0;
        public override long ToInt64(IFormatProvider? provider) => 0;
        public override sbyte ToSByte(IFormatProvider? provider) => 0;
        public override float ToSingle(IFormatProvider? provider) => 0;
        public override string ToString(IFormatProvider? provider) => "";
        public override object ToType(Type conversionType, IFormatProvider? provider) => null!;
        public override ushort ToUInt16(IFormatProvider? provider) => 0;
        public override uint ToUInt32(IFormatProvider? provider) => 0;
        public override ulong ToUInt64(IFormatProvider? provider) => 0;
    }
}
