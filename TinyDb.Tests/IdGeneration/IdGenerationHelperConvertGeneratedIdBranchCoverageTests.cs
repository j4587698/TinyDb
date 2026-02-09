using System;
using System.Globalization;
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

    private enum ByteEnum : byte { A = 1, B = 2 }
    private enum SByteEnum : sbyte { A = 1, B = 2 }
    private enum Int16Enum : short { A = 1, B = 2 }
    private enum UInt16Enum : ushort { A = 1, B = 2 }
    private enum Int32Enum : int { A = 1, B = 2 }
    private enum UInt32Enum : uint { A = 1, B = 2 }
    private enum Int64Enum : long { A = 1, B = 2 }
    private enum UInt64Enum : ulong { A = 1, B = 2 }

    private static bool TryConvertEnumFromString(Type enumType, string enumText, out object enumValue)
    {
        var method = typeof(IdGenerationHelper<IdGenerationHelperTests.EntityWithGuidV4>)
            .GetMethod("TryConvertEnumFromString", BindingFlags.NonPublic | BindingFlags.Static);
        if (method == null) throw new InvalidOperationException("TryConvertEnumFromString not found");

        var args = new object?[] { enumType, enumText, null };
        var success = (bool)method.Invoke(null, args)!;
        enumValue = args[2]!;
        return success;
    }

    private static ulong ToUInt64(object underlyingValue, Type underlyingType)
    {
        var method = typeof(IdGenerationHelper<IdGenerationHelperTests.EntityWithGuidV4>)
            .GetMethod("ToUInt64", BindingFlags.NonPublic | BindingFlags.Static);
        if (method == null) throw new InvalidOperationException("ToUInt64 not found");

        try
        {
            return (ulong)method.Invoke(null, new object[] { underlyingValue, underlyingType })!;
        }
        catch (TargetInvocationException tie) when (tie.InnerException != null)
        {
            throw tie.InnerException;
        }
    }

    private static object FromUInt64(ulong value, Type underlyingType)
    {
        var method = typeof(IdGenerationHelper<IdGenerationHelperTests.EntityWithGuidV4>)
            .GetMethod("FromUInt64", BindingFlags.NonPublic | BindingFlags.Static);
        if (method == null) throw new InvalidOperationException("FromUInt64 not found");

        try
        {
            return method.Invoke(null, new object[] { value, underlyingType })!;
        }
        catch (TargetInvocationException tie) when (tie.InnerException != null)
        {
            throw tie.InnerException;
        }
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

    [Test]
    public async Task ConvertGeneratedId_Enum_WhenInvalidString_ShouldThrowArgumentException()
    {
        await Assert.That(() =>
        {
            try
            {
                ConvertGeneratedId(new BsonString("DoesNotExist"), typeof(Int32Enum));
                return;
            }
            catch (TargetInvocationException tie)
            {
                throw tie.InnerException!;
            }
        }).Throws<ArgumentException>();
    }

    [Test]
    public async Task ConvertGeneratedId_Enum_WhenNumericRawValue_ShouldConvert()
    {
        var value = ConvertGeneratedId(new BsonInt32(1), typeof(Int32Enum));
        await Assert.That(value).IsEqualTo(Int32Enum.A);
    }

    [Test]
    public async Task ConvertGeneratedId_Enum_WhenNumericConversionFails_ShouldThrowArgumentException()
    {
        await Assert.That(() =>
        {
            try
            {
                ConvertGeneratedId(new BsonDateTime(DateTime.UtcNow), typeof(Int32Enum));
                return;
            }
            catch (TargetInvocationException tie)
            {
                throw tie.InnerException!;
            }
        }).Throws<ArgumentException>();
    }

    [Test]
    public async Task ConvertGeneratedId_NonEnum_WhenFallbackConvertChangeType_ShouldConvert()
    {
        var value = ConvertGeneratedId(new BsonInt32(1), typeof(long));
        await Assert.That(value).IsEqualTo(1L);
    }

    [Test]
    public async Task TryConvertEnumFromString_ShouldHandleEdgeCases()
    {
        await Assert.That(TryConvertEnumFromString(typeof(Int32Enum), " ", out _)).IsFalse();
        await Assert.That(TryConvertEnumFromString(typeof(Int32Enum), ",", out _)).IsFalse();
        await Assert.That(TryConvertEnumFromString(typeof(Int32Enum), "DoesNotExist", out _)).IsFalse();

        await Assert.That(TryConvertEnumFromString(typeof(Int32Enum), "a", out var byName)).IsTrue();
        await Assert.That((Int32Enum)byName).IsEqualTo(Int32Enum.A);

        await Assert.That(TryConvertEnumFromString(typeof(Int32Enum), "1", out var byNumber)).IsTrue();
        await Assert.That((Int32Enum)byNumber).IsEqualTo(Int32Enum.A);

        await Assert.That(TryConvertEnumFromString(typeof(Int32Enum), "A,DoesNotExist", out _)).IsFalse();
    }

    [Test]
    public async Task TryConvertEnumFromString_WhenMultipleTokens_ShouldCombineUnderlyingValues_ForAllUnderlyingTypes()
    {
        await Assert.That(TryConvertEnumFromString(typeof(ByteEnum), "A,B", out var e1)).IsTrue();
        await Assert.That((ByteEnum)e1).IsEqualTo((ByteEnum)3);

        await Assert.That(TryConvertEnumFromString(typeof(SByteEnum), "A,B", out var e2)).IsTrue();
        await Assert.That((SByteEnum)e2).IsEqualTo((SByteEnum)3);

        await Assert.That(TryConvertEnumFromString(typeof(Int16Enum), "A,B", out var e3)).IsTrue();
        await Assert.That((Int16Enum)e3).IsEqualTo((Int16Enum)3);

        await Assert.That(TryConvertEnumFromString(typeof(UInt16Enum), "A,B", out var e4)).IsTrue();
        await Assert.That((UInt16Enum)e4).IsEqualTo((UInt16Enum)3);

        await Assert.That(TryConvertEnumFromString(typeof(Int32Enum), "A,B", out var e5)).IsTrue();
        await Assert.That((Int32Enum)e5).IsEqualTo((Int32Enum)3);

        await Assert.That(TryConvertEnumFromString(typeof(UInt32Enum), "A,B", out var e6)).IsTrue();
        await Assert.That((UInt32Enum)e6).IsEqualTo((UInt32Enum)3);

        await Assert.That(TryConvertEnumFromString(typeof(Int64Enum), "A,B", out var e7)).IsTrue();
        await Assert.That((Int64Enum)e7).IsEqualTo((Int64Enum)3);

        await Assert.That(TryConvertEnumFromString(typeof(UInt64Enum), "A,B", out var e8)).IsTrue();
        await Assert.That((UInt64Enum)e8).IsEqualTo((UInt64Enum)3);
    }

    [Test]
    public async Task ToUInt64_And_FromUInt64_WithUnsupportedType_ShouldThrow()
    {
        await Assert.That(() => ToUInt64('a', typeof(char))).Throws<NotSupportedException>();
        await Assert.That(() => FromUInt64(1, typeof(char))).Throws<NotSupportedException>();
    }
}
