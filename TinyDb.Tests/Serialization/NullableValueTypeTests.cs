using TinyDb.Attributes;
using TinyDb.Bson;
using TinyDb.Serialization;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Serialization;

public class NullableValueTypeTests
{
    [Test]
    public async Task DependentScalar_NullableValueType_ShouldRoundTrip()
    {
        var entity = new NullableScalarContainer
        {
            Id = 1,
            Detail = new NullableScalarDetail
            {
                A = 12345,
                B = null
            }
        };

        var doc = AotBsonMapper.ToDocument(entity);
        var restored = AotBsonMapper.FromDocument<NullableScalarContainer>(doc);

        await Assert.That(restored.Id).IsEqualTo(1);
        await Assert.That(restored.Detail!.A).IsEqualTo((ushort?)12345);
        await Assert.That(restored.Detail.B).IsNull();
    }

    [Test]
    public async Task DependentScalar_NullDetail_ShouldRoundTrip()
    {
        var entity = new NullableScalarContainer
        {
            Id = 7,
            Detail = null
        };

        var document = AotBsonMapper.ToDocument(entity);
        var restored = AotBsonMapper.FromDocument<NullableScalarContainer>(document);

        await Assert.That(restored.Id).IsEqualTo(7);
        await Assert.That(restored.Detail).IsNull();
    }

    [Test]
    public async Task MainPath_ListOfNullableValueType_ShouldRoundTrip()
    {
        var entity = new NullableScalarContainer
        {
            Id = 1,
            Detail = new NullableScalarDetail { A = 10, B = 20 },
            Scores = [100, null, 65535]
        };

        var document = AotBsonMapper.ToDocument(entity);
        var restored = AotBsonMapper.FromDocument<NullableScalarContainer>(document);

        await Assert.That(restored.Scores).Count().IsEqualTo(3);
        await Assert.That(restored.Scores![0]).IsEqualTo((ushort?)100);
        await Assert.That(restored.Scores[1]).IsNull();
        await Assert.That(restored.Scores[2]).IsEqualTo((ushort?)65535);
    }

    [Test]
    public async Task EntityPath_DictionaryOfNullableValueType_ShouldRoundTrip()
    {
        var entity = new NullableScalarContainer
        {
            Id = 2,
            Detail = new NullableScalarDetail { A = 1, B = 2 },
            Metrics = new Dictionary<string, int?> { ["a"] = 10, ["b"] = null, ["c"] = -5 }
        };

        var document = AotBsonMapper.ToDocument(entity);
        var restored = AotBsonMapper.FromDocument<NullableScalarContainer>(document);

        await Assert.That(restored.Metrics!["a"]).IsEqualTo((int?)10);
        await Assert.That(restored.Metrics["b"]).IsNull();
        await Assert.That(restored.Metrics["c"]).IsEqualTo((int?)(-5));
    }

    [Test]
    public async Task DependentPath_CollectionOfNullableValueType_ShouldRoundTrip()
    {
        var entity = new NullableScalarContainer
        {
            Id = 3,
            Detail = new NullableScalarDetail
            {
                A = 5,
                B = 6,
                Tags = [77, null, 88]
            }
        };

        var document = AotBsonMapper.ToDocument(entity);
        var restored = AotBsonMapper.FromDocument<NullableScalarContainer>(document);

        await Assert.That(restored.Detail!.Tags).Count().IsEqualTo(3);
        await Assert.That(restored.Detail.Tags![0]).IsEqualTo((int?)77);
        await Assert.That(restored.Detail.Tags[1]).IsNull();
        await Assert.That(restored.Detail.Tags[2]).IsEqualTo((int?)88);
    }

    [Test]
    public async Task FromBsonValueGeneric_ShouldSupportNullableValueType()
    {
        BsonValue value = new BsonInt32(12345);

        var result = BsonConversion.FromBsonValue<ushort?>(value);

        await Assert.That(result).IsEqualTo((ushort?)12345);
    }

    [Test]
    public async Task MainPath_NonNullableSmallIntegers_ShouldRoundTrip()
    {
        var entity = new SmallIntegerContainer
        {
            ByteValue = 255,
            SByteValue = -100,
            ShortValue = -32000,
            UShortValue = 65000,
            UIntValue = 4_000_000_000,
            ULongValue = 18_446_744_073_709_551_615,
            IntList = [1, 2, 3],
            ByteList = [10, 20],
            UShortlist = new ushort[] { 100, 200 },
            UInt = new Dictionary<string, uint> { ["a"] = 1, ["b"] = 2 }
        };

        var document = AotBsonMapper.ToDocument(entity);
        var restored = AotBsonMapper.FromDocument<SmallIntegerContainer>(document);

        await Assert.That(restored.ByteValue).IsEqualTo((byte)255);
        await Assert.That(restored.SByteValue).IsEqualTo((sbyte)(-100));
        await Assert.That(restored.ShortValue).IsEqualTo((short)(-32000));
        await Assert.That(restored.UShortValue).IsEqualTo((ushort)65000);
        await Assert.That(restored.UIntValue).IsEqualTo(4_000_000_000u);
        await Assert.That(restored.ULongValue).IsEqualTo(18_446_744_073_709_551_615UL);

        await Assert.That(restored.IntList).Count().IsEqualTo(3);
        await Assert.That(restored.IntList[0]).IsEqualTo(1);
        await Assert.That(restored.ByteList).Count().IsEqualTo(2);
        await Assert.That(restored.ByteList[0]).IsEqualTo((byte)10);
        await Assert.That(restored.UShortlist).Count().IsEqualTo(2);
        await Assert.That(restored.UShortlist[1]).IsEqualTo((ushort)200);
        await Assert.That(restored.UInt["a"]).IsEqualTo(1u);
        await Assert.That(restored.UInt["b"]).IsEqualTo(2u);
    }

    [Test]
    public async Task DependentPath_NonNullableSmallIntegers_ShouldRoundTrip()
    {
        var entity = new NullableScalarContainer
        {
            Id = 9,
            Detail = new NullableScalarDetail
            {
                A = 42,
                B = 7,
                Codes = new SmallIntegerDetail { ByteValue = 99, UShortValue = 300, ULongValue = 9 }
            }
        };

        var document = AotBsonMapper.ToDocument(entity);
        var restored = AotBsonMapper.FromDocument<NullableScalarContainer>(document);

        await Assert.That(restored.Detail!.Codes!.ByteValue).IsEqualTo((byte)99);
        await Assert.That(restored.Detail.Codes.UShortValue).IsEqualTo((ushort)300);
        await Assert.That(restored.Detail.Codes.ULongValue).IsEqualTo(9UL);
    }

    [Test]
    public async Task AotFallback_NullableDateTimeContainers_ShouldConvert()
    {
        var when = new DateTime(2026, 1, 2, 3, 4, 5, DateTimeKind.Utc);
        var array = new BsonArray(new BsonValue[] { new BsonDateTime(when), BsonNull.Value });
        var doc = new BsonDocument().Set("a", new BsonDateTime(when)).Set("b", BsonNull.Value);

        var list = (List<DateTime?>)AotBsonMapper.ConvertValue(array, typeof(List<DateTime?>))!;
        await Assert.That(list).Count().IsEqualTo(2);
        await Assert.That(list[0]).IsEqualTo(when);
        await Assert.That(list[1]).IsNull();

        var values = (DateTime?[])AotBsonMapper.ConvertValue(array, typeof(DateTime?[]))!;
        await Assert.That(values.Length).IsEqualTo(2);
        await Assert.That(values[0]).IsEqualTo(when);
        await Assert.That(values[1]).IsNull();

        var map = (Dictionary<string, DateTime?>)AotBsonMapper.ConvertValue(doc, typeof(Dictionary<string, DateTime?>))!;
        await Assert.That(map["a"]).IsEqualTo(when);
        await Assert.That(map["b"]).IsNull();
    }

    [Test]
    public async Task AotFallback_NonNullableScalarContainers_ShouldConvert()
    {
        var when = new DateTime(2026, 1, 2, 3, 4, 5, DateTimeKind.Utc);
        var array = new BsonArray(new BsonValue[] { new BsonDateTime(when) });
        var doc = new BsonDocument().Set("a", new BsonDateTime(when));

        var list = (List<DateTime>)AotBsonMapper.ConvertValue(array, typeof(List<DateTime>))!;
        await Assert.That(list[0]).IsEqualTo(when);

        var map = (Dictionary<string, DateTime>)AotBsonMapper.ConvertValue(doc, typeof(Dictionary<string, DateTime>))!;
        await Assert.That(map["a"]).IsEqualTo(when);

        var guidArray = (Guid?[])AotBsonMapper.ConvertValue(
            new BsonArray(new BsonValue[] { new BsonString(Guid.Empty.ToString()), BsonNull.Value }),
            typeof(Guid?[]))!;
        await Assert.That(guidArray[0]).IsEqualTo(Guid.Empty);
        await Assert.That(guidArray[1]).IsNull();
    }

    [Test]
    public async Task BsonConversionUntyped_NullableDateTimeContainers_ShouldConvert()
    {
        var when = new DateTime(2026, 1, 2, 3, 4, 5, DateTimeKind.Utc);
        var array = new BsonArray(new BsonValue[] { new BsonDateTime(when), BsonNull.Value });
        var doc = new BsonDocument().Set("a", new BsonDateTime(when)).Set("b", BsonNull.Value);

        var list = (List<DateTime?>)BsonConversion.FromBsonValue(array, typeof(List<DateTime?>))!;
        await Assert.That(list[0]).IsEqualTo(when);
        await Assert.That(list[1]).IsNull();

        var values = (DateTime?[])BsonConversion.FromBsonValue(array, typeof(DateTime?[]))!;
        await Assert.That(values[0]).IsEqualTo(when);
        await Assert.That(values[1]).IsNull();

        var map = (Dictionary<string, DateTime?>)BsonConversion.FromBsonValue(doc, typeof(Dictionary<string, DateTime?>))!;
        await Assert.That(map["a"]).IsEqualTo(when);
        await Assert.That(map["b"]).IsNull();
    }

    [Test]
    public async Task BsonConversionGeneric_NullableDateTimeContainers_ShouldConvert()
    {
        var when = new DateTime(2026, 1, 2, 3, 4, 5, DateTimeKind.Utc);
        var array = new BsonArray(new BsonValue[] { new BsonDateTime(when), BsonNull.Value });
        var doc = new BsonDocument().Set("a", new BsonDateTime(when)).Set("b", BsonNull.Value);

        var list = BsonConversion.FromBsonValue<List<DateTime?>>(array)!;
        await Assert.That(list[0]).IsEqualTo(when);
        await Assert.That(list[1]).IsNull();

        var values = BsonConversion.FromBsonValue<DateTime?[]>(array)!;
        await Assert.That(values[0]).IsEqualTo(when);
        await Assert.That(values[1]).IsNull();

        var map = BsonConversion.FromBsonValue<Dictionary<string, DateTime?>>(doc)!;
        await Assert.That(map["a"]).IsEqualTo(when);
        await Assert.That(map["b"]).IsNull();
    }
}

[Entity]
public class NullableScalarContainer
{
    public int Id { get; set; }

    public NullableScalarDetail? Detail { get; set; }

    public List<ushort?>? Scores { get; set; }

    public Dictionary<string, int?>? Metrics { get; set; }
}

public class NullableScalarDetail
{
    public ushort? A { get; set; }

    public int? B { get; set; }

    public List<int?>? Tags { get; set; }

    public SmallIntegerDetail? Codes { get; set; }
}

public class SmallIntegerDetail
{
    public byte ByteValue { get; set; }

    public ushort UShortValue { get; set; }

    public ulong ULongValue { get; set; }
}

[Entity]
public class SmallIntegerContainer
{
    public int Id { get; set; }

    public byte ByteValue { get; set; }

    public sbyte SByteValue { get; set; }

    public short ShortValue { get; set; }

    public ushort UShortValue { get; set; }

    public uint UIntValue { get; set; }

    public ulong ULongValue { get; set; }

    public List<int>? IntList { get; set; }

    public List<byte>? ByteList { get; set; }

    public ushort[]? UShortlist { get; set; }

    public Dictionary<string, uint>? UInt { get; set; }
}