using TinyDb.Core;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Core;

public class EngineBatchInsertTests : IDisposable
{
    private sealed class UnsupportedBsonValue : BsonValue
    {
        public override BsonType BsonType => BsonType.Undefined;
        public override object? RawValue => null;

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
        public override string ToString(IFormatProvider? provider) => string.Empty;
        public override object ToType(Type conversionType, IFormatProvider? provider) => throw new InvalidCastException();
        public override ushort ToUInt16(IFormatProvider? provider) => throw new InvalidCastException();
        public override uint ToUInt32(IFormatProvider? provider) => throw new InvalidCastException();
        public override ulong ToUInt64(IFormatProvider? provider) => throw new InvalidCastException();
    }

    private readonly string _testDbPath;
    private readonly TinyDbEngine _engine;

    public EngineBatchInsertTests()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"eng_batch_{Guid.NewGuid():N}.db");
        _engine = new TinyDbEngine(_testDbPath);
    }

    public void Dispose()
    {
        _engine.Dispose();
        try { if (File.Exists(_testDbPath)) File.Delete(_testDbPath); } catch { }
    }

    [Test]
    public async Task InsertDocuments_EmptyArray_Should_Return_Zero()
    {
        var result = _engine.InsertDocuments("col", Array.Empty<BsonDocument>());
        await Assert.That(result).IsEqualTo(0);
    }

    [Test]
    public async Task InsertDocuments_With_Null_Docs_Should_Skip_Them()
    {
        var docs = new BsonDocument[] { null!, new BsonDocument().Set("a", 1) };
        var result = _engine.InsertDocuments("col", docs);
        await Assert.That(result).IsEqualTo(1);
    }

    [Test]
    public async Task InsertDocuments_LargeDoc_And_UniqueIndexViolation_ShouldThrowAggregateException()
    {
        var idx = _engine.GetIndexManager("col");
        idx.CreateIndex("idx_code_unique", new[] { "Code" }, unique: true);

        var largePayload = new string('x', 20000);
        var docs = new[]
        {
            new BsonDocument().Set("Code", "dup").Set("payload", largePayload),
            new BsonDocument().Set("Code", "dup").Set("payload", "small")
        };

        await Assert.That(() => _engine.InsertDocuments("col", docs)).Throws<AggregateException>();
    }

    [Test]
    public async Task InsertDocuments_WhenSerializationFails_ShouldAggregateException()
    {
        var docs = new[]
        {
            new BsonDocument().Set("bad", new UnsupportedBsonValue()),
            new BsonDocument().Set("ok", 1)
        };

        await Assert.That(() => _engine.InsertDocuments("col_serialization", docs)).Throws<AggregateException>();
    }
}
