using TinyDb.IdGeneration;
using TinyDb.Bson;
using TinyDb.Attributes;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.IdGeneration;

public class IdGenerationCoverageTests
{
    private sealed class RawBsonValue : BsonValue
    {
        public RawBsonValue(object? rawValue) => RawValue = rawValue;

        public override BsonType BsonType => BsonType.Undefined;
        public override object? RawValue { get; }

        public override int CompareTo(BsonValue? other) => 0;
        public override bool Equals(BsonValue? other) => ReferenceEquals(this, other);
        public override int GetHashCode() => RawValue?.GetHashCode() ?? 0;

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

    private sealed class TextValue
    {
        private readonly string _text;
        public TextValue(string text) => _text = text;
        public override string ToString() => _text;
    }

    public enum IdEnum { None, Some }
    public class EnumIdClass { [IdGeneration(IdGenerationStrategy.IdentityInt)] public IdEnum Id { get; set; } }
    public class GuidIdClass { [IdGeneration(IdGenerationStrategy.GuidV4)] public Guid Id { get; set; } }
    public class StringIdClass { [IdGeneration(IdGenerationStrategy.ObjectId)] public string Id { get; set; } = ""; }
    public class UnsupportedDecimalIdClass { [IdGeneration(IdGenerationStrategy.GuidV4)] public decimal Id { get; set; } }
    public class InvalidStrategyIdClass { [IdGeneration((IdGenerationStrategy)123)] public int Id { get; set; } }

    [Test]
    public async Task ConvertGeneratedId_Coverage()
    {
        // Internal method, test through public API or reflection
        var method = typeof(IdGenerationHelper<GuidIdClass>).GetMethod("ConvertGeneratedId", 
            System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.NonPublic);

        // Guid from bytes
        var guid = Guid.NewGuid();
        var bytes = guid.ToByteArray();
        var res1 = method!.Invoke(null, new object[] { new BsonBinary(bytes), typeof(Guid) });
        await Assert.That(res1).IsEqualTo(guid);

        // ObjectId from string
        var oid = ObjectId.NewObjectId();
        var res2 = method!.Invoke(null, new object[] { new BsonString(oid.ToString()), typeof(ObjectId) });
        await Assert.That(res2).IsEqualTo(oid);

        // Enum conversion
        var res3 = method!.Invoke(null, new object[] { new BsonString("Some"), typeof(IdEnum) });
        await Assert.That(res3).IsEqualTo(IdEnum.Some);

        // String conversion from non-string raw value
        var resStr = method.Invoke(null, new object[] { new BsonInt32(123), typeof(string) });
        await Assert.That(resStr).IsEqualTo("123");
         
        // Null raw value
        var res4 = method!.Invoke(null, new object[] { BsonNull.Value, typeof(string) });
        await Assert.That(res4).IsNull();

        // Guid conversion fallback (ToString-based)
        var guid2 = Guid.NewGuid();
        var resGuidFallback = method.Invoke(null, new object[] { new RawBsonValue(new TextValue(guid2.ToString())), typeof(Guid) });
        await Assert.That(resGuidFallback).IsEqualTo(guid2);

        // ObjectId conversion fallback (ToString-based)
        var oid2 = ObjectId.NewObjectId();
        var resObjFallback = method.Invoke(null, new object[] { new RawBsonValue(new TextValue(oid2.ToString())), typeof(ObjectId) });
        await Assert.That(resObjFallback).IsEqualTo(oid2);

        // Convert.ChangeType path
        var resInt = method.Invoke(null, new object[] { new BsonString("42"), typeof(int) });
        await Assert.That(resInt).IsEqualTo(42);
    }

    [Test]
    public async Task IdentityGenerator_UnsupportedType_ShouldThrow()
    {
        var gen = new IdentityGenerator();
        var prop = typeof(EnumIdClass).GetProperty("Id")!;
        await Assert.That(() => gen.GenerateId(typeof(EnumIdClass), prop)).Throws<InvalidOperationException>();
    }

    [Test]
    public async Task ShouldGenerateId_Coverage()
    {
        var entity = new GuidIdClass { Id = Guid.NewGuid() };
        // Already has valid ID
        await Assert.That(IdGenerationHelper<GuidIdClass>.ShouldGenerateId(entity)).IsFalse();
        
        entity.Id = Guid.Empty;
        await Assert.That(IdGenerationHelper<GuidIdClass>.ShouldGenerateId(entity)).IsTrue();
    }

    [Test]
    public async Task GenerateIdForEntity_UnsupportedPropertyType_ShouldReturnFalse()
    {
        var e = new UnsupportedDecimalIdClass();
        await Assert.That(IdGenerationHelper<UnsupportedDecimalIdClass>.GenerateIdForEntity(e)).IsFalse();
    }

    [Test]
    public async Task GenerateIdForEntity_InvalidStrategy_ShouldReturnFalse()
    {
        var e = new InvalidStrategyIdClass();
        await Assert.That(IdGenerationHelper<InvalidStrategyIdClass>.GenerateIdForEntity(e)).IsFalse();
    }
}
