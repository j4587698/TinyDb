using System;
using System.Reflection;
using TinyDb.Bson;
using TinyDb.IdGeneration;
using TUnit.Assertions;
using TUnit.Core;

namespace TinyDb.Tests.IdGeneration;

public class IdGenerationHelperConvertGeneratedIdBranchCoverageTests
{
    private sealed class NullToStringObject
    {
        public override string? ToString() => null;
    }

    private sealed class CustomRawBsonValue : BsonValue
    {
        public CustomRawBsonValue(object? rawValue) => RawValue = rawValue;

        public override BsonType BsonType => BsonType.Document;
        public override object? RawValue { get; }

        public override int CompareTo(BsonValue? other) => 0;
        public override bool Equals(BsonValue? other) => ReferenceEquals(this, other);
        public override int GetHashCode() => 0;

        public override TypeCode GetTypeCode() => TypeCode.Object;
        public override bool ToBoolean(IFormatProvider? provider) => throw new InvalidCastException();
        public override byte ToByte(IFormatProvider? provider) => throw new InvalidCastException();
        public override char ToChar(IFormatProvider? provider) => throw new InvalidCastException();
        public override DateTime ToDateTime(IFormatProvider? provider) => throw new InvalidCastException();
        public override decimal ToDecimal(IFormatProvider? provider) => throw new InvalidCastException();
        public override double ToDouble(IFormatProvider? provider) => throw new InvalidCastException();
        public override short ToInt16(IFormatProvider? provider) => throw new InvalidCastException();
        public override int ToInt32(IFormatProvider? provider) => throw new InvalidCastException();
        public override long ToInt64(IFormatProvider? provider) => throw new InvalidCastException();
        public override sbyte ToSByte(IFormatProvider? provider) => throw new InvalidCastException();
        public override float ToSingle(IFormatProvider? provider) => throw new InvalidCastException();
        public override string ToString(IFormatProvider? provider) => RawValue?.ToString() ?? string.Empty;
        public override object ToType(Type conversionType, IFormatProvider? provider) => throw new InvalidCastException();
        public override ushort ToUInt16(IFormatProvider? provider) => throw new InvalidCastException();
        public override uint ToUInt32(IFormatProvider? provider) => throw new InvalidCastException();
        public override ulong ToUInt64(IFormatProvider? provider) => throw new InvalidCastException();
    }

    private enum SampleEnum
    {
        A = 1
    }

    private static object? ConvertGeneratedId(BsonValue bsonValue, Type targetType)
    {
        var method = typeof(IdGenerationHelper<IdGenerationHelperTests.EntityWithGuidV4>)
            .GetMethod("ConvertGeneratedId", BindingFlags.NonPublic | BindingFlags.Static);
        if (method == null) throw new InvalidOperationException("ConvertGeneratedId not found");
        return method.Invoke(null, new object[] { bsonValue, targetType });
    }

    [Test]
    public async Task ConvertGeneratedId_WhenTargetTypeNull_ShouldThrow()
    {
        await Assert.That(() =>
        {
            try
            {
                ConvertGeneratedId(new BsonInt32(1), null!);
                return;
            }
            catch (TargetInvocationException tie)
            {
                throw tie.InnerException!;
            }
        }).Throws<ArgumentNullException>();
    }

    [Test]
    public async Task ConvertGeneratedId_Guid_WhenRawValueToStringReturnsNull_ShouldThrowFormatException()
    {
        await Assert.That(() =>
        {
            try
            {
                ConvertGeneratedId(new CustomRawBsonValue(new NullToStringObject()), typeof(Guid));
                return;
            }
            catch (TargetInvocationException tie)
            {
                throw tie.InnerException!;
            }
        }).Throws<FormatException>();
    }

    [Test]
    public async Task ConvertGeneratedId_ObjectId_WhenRawValueToStringReturnsNull_ShouldThrowArgumentNullException()
    {
        await Assert.That(() =>
        {
            try
            {
                ConvertGeneratedId(new CustomRawBsonValue(new NullToStringObject()), typeof(ObjectId));
                return;
            }
            catch (TargetInvocationException tie)
            {
                throw tie.InnerException!;
            }
        }).Throws<ArgumentNullException>();
    }

    [Test]
    public async Task ConvertGeneratedId_Enum_WhenRawValueToStringReturnsNull_ShouldThrow()
    {
        await Assert.That(() =>
        {
            try
            {
                ConvertGeneratedId(new CustomRawBsonValue(new NullToStringObject()), typeof(SampleEnum));
                return;
            }
            catch (TargetInvocationException tie)
            {
                throw tie.InnerException!;
            }
        }).Throws<ArgumentException>();
    }
}
