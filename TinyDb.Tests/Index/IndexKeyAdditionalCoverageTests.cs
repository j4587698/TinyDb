using System;
using System.Linq;
using TinyDb.Bson;
using TinyDb.Index;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Index;

public class IndexKeyAdditionalCoverageTests
{
    [Test]
    public async Task CompareTo_DifferentBsonTypes_ShouldCoverTypeOrderMatrix()
    {
        var int32Key = new IndexKey(new BsonInt32(0));
        var stringKey = new IndexKey(new BsonString("0"));

        var samples = new[]
        {
            new IndexKey(BsonMinKey.Value),
            new IndexKey(BsonValue.Null),
            new IndexKey(new BsonBoolean(false)),
            new IndexKey(new BsonInt64(0)),
            new IndexKey(new BsonDouble(0.0)),
            new IndexKey(new BsonDecimal128(0m)),
            new IndexKey(new BsonObjectId(ObjectId.NewObjectId())),
            new IndexKey(new BsonDateTime(DateTime.UnixEpoch)),
            new IndexKey(new BsonBinary(new byte[] { 1 })),
            new IndexKey(new BsonArray(new BsonValue[] { 1 })),
            new IndexKey(new BsonDocument().Set("a", 1)),
            new IndexKey(new BsonRegularExpression("a", "i")),
            new IndexKey(new BsonJavaScript("return 1;")),
            new IndexKey(new BsonJavaScriptWithScope("return x;", new BsonDocument().Set("x", 1))),
            new IndexKey(new BsonTimestamp(1, 1)),
            new IndexKey(new BsonSymbol("s")),
            new IndexKey(BsonMaxKey.Value)
        };

        foreach (var key in samples)
        {
            await Assert.That(key.CompareTo(int32Key)).IsNotEqualTo(0);
        }

        await Assert.That(int32Key.CompareTo(stringKey)).IsNegative();
        await Assert.That(stringKey.CompareTo(int32Key)).IsPositive();

        await Assert.That(IndexKey.MinValue.CompareTo(IndexKey.MaxValue)).IsNegative();
        await Assert.That(IndexKey.MaxValue.CompareTo(IndexKey.MinValue)).IsPositive();
    }

    [Test]
    public async Task CompareTo_SameBsonTypes_ShouldCoverValueComparisons_And_DefaultFallback()
    {
        await Assert.That(new IndexKey(new BsonInt64(1)).CompareTo(new IndexKey(new BsonInt64(2)))).IsNegative();
        await Assert.That(new IndexKey(new BsonDouble(1.0)).CompareTo(new IndexKey(new BsonDouble(2.0)))).IsNegative();
        await Assert.That(new IndexKey(new BsonBoolean(false)).CompareTo(new IndexKey(new BsonBoolean(true)))).IsNegative();
        await Assert.That(new IndexKey(new BsonDecimal128(1m)).CompareTo(new IndexKey(new BsonDecimal128(2m)))).IsNegative();

        var bin1 = new IndexKey(new BsonBinary(new byte[] { 1 }));
        var bin2 = new IndexKey(new BsonBinary(new byte[] { 2 }));
        await Assert.That(bin1.CompareTo(bin2)).IsNegative();

        var doc1 = new IndexKey(new BsonDocument().Set("a", 1));
        var doc2 = new IndexKey(new BsonDocument().Set("a", 2));
        await Assert.That(doc1.CompareTo(doc2)).IsNotEqualTo(0);
    }

    [Test]
    public async Task CompareTo_SameBsonType_Timestamp_ShouldUseDefaultComparison()
    {
        var t1 = new IndexKey(new BsonTimestamp(1, 1));
        var t2 = new IndexKey(new BsonTimestamp(1, 2));

        await Assert.That(t1.CompareTo(t2)).IsNotEqualTo(0);
    }

    [Test]
    public async Task CompareTo_SameMinKey_And_SameMaxKey_ShouldUseDefaultComparison()
    {
        var min1 = new IndexKey(BsonMinKey.Value);
        var min2 = new IndexKey(BsonMinKey.Value);

        await Assert.That(min1.CompareTo(min2)).IsEqualTo(0);

        var max1 = new IndexKey(BsonMaxKey.Value);
        var max2 = new IndexKey(BsonMaxKey.Value);

        await Assert.That(max1.CompareTo(max2)).IsEqualTo(0);
    }

    [Test]
    public async Task Equals_ObjectOverload_And_HashCode_ShouldHandleNonKeyAndNullValues()
    {
        var key = new IndexKey(new BsonString("x"));
        await Assert.That(key.Equals("not-a-key")).IsFalse();

        var withNullEntry = new IndexKey(new BsonValue[] { null! });
        await Assert.That(() => withNullEntry.GetHashCode()).ThrowsNothing();

        var str = withNullEntry.ToString();
        await Assert.That(str).Contains("null");
    }

    [Test]
    public async Task CompareTo_ShouldCover_Undefined_NullTypeOrder_And_DefaultOrder()
    {
        var intKey = new IndexKey(new BsonInt32(1));

        var nullType = new IndexKey(new FakeBsonValue(BsonType.Null, isNull: false));
        var undefinedType = new IndexKey(new FakeBsonValue(BsonType.Undefined));
        var endType = new IndexKey(new FakeBsonValue(BsonType.End));

        await Assert.That(nullType.CompareTo(intKey)).IsNotEqualTo(0);
        await Assert.That(undefinedType.CompareTo(intKey)).IsNotEqualTo(0);
        await Assert.That(endType.CompareTo(intKey)).IsNotEqualTo(0);
    }

    private sealed class FakeBsonValue : BsonValue
    {
        private readonly BsonType _type;
        private readonly bool _isNull;

        public FakeBsonValue(BsonType type, bool isNull = false)
        {
            _type = type;
            _isNull = isNull;
        }

        public override BsonType BsonType => _type;
        public override bool IsNull => _isNull;
        public override object? RawValue => null;

        public override int CompareTo(BsonValue? other) => 0;
        public override bool Equals(BsonValue? other) => ReferenceEquals(this, other);
        public override int GetHashCode() => (int)_type;

        public override TypeCode GetTypeCode() => TypeCode.Object;
        public override bool ToBoolean(IFormatProvider? provider) => false;
        public override byte ToByte(IFormatProvider? provider) => 0;
        public override char ToChar(IFormatProvider? provider) => '\0';
        public override DateTime ToDateTime(IFormatProvider? provider) => default;
        public override decimal ToDecimal(IFormatProvider? provider) => 0m;
        public override double ToDouble(IFormatProvider? provider) => 0d;
        public override short ToInt16(IFormatProvider? provider) => 0;
        public override int ToInt32(IFormatProvider? provider) => 0;
        public override long ToInt64(IFormatProvider? provider) => 0L;
        public override sbyte ToSByte(IFormatProvider? provider) => 0;
        public override float ToSingle(IFormatProvider? provider) => 0f;
        public override string ToString(IFormatProvider? provider) => string.Empty;
        public override object ToType(Type conversionType, IFormatProvider? provider) => throw new InvalidCastException();
        public override ushort ToUInt16(IFormatProvider? provider) => 0;
        public override uint ToUInt32(IFormatProvider? provider) => 0u;
        public override ulong ToUInt64(IFormatProvider? provider) => 0UL;
    }
}
