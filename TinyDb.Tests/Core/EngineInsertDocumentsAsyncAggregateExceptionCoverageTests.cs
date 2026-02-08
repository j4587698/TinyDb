using System;
using System.IO;
using TinyDb.Bson;
using TinyDb.Core;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Core;

[NotInParallel]
public sealed class EngineInsertDocumentsAsyncAggregateExceptionCoverageTests : IDisposable
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

    private readonly string _dbPath;
    private readonly TinyDbEngine _engine;

    public EngineInsertDocumentsAsyncAggregateExceptionCoverageTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"eng_batch_async_{Guid.NewGuid():N}.db");
        _engine = new TinyDbEngine(_dbPath);
    }

    public void Dispose()
    {
        _engine.Dispose();
        try { if (File.Exists(_dbPath)) File.Delete(_dbPath); } catch { }
    }

    [Test]
    public async Task InsertAsync_WhenSerializationFails_ShouldThrowAggregateException()
    {
        var collection = _engine.GetCollection<BsonDocument>("col_async_serialization");
        var docs = new[]
        {
            new BsonDocument().Set("bad", new UnsupportedBsonValue()),
            new BsonDocument().Set("ok", 1)
        };

        await Assert.That(async () => await collection.InsertAsync(docs))
            .Throws<AggregateException>();
    }

    [Test]
    public async Task InsertAsync_WhenUniqueIndexViolationOccurs_ShouldThrowAggregateException()
    {
        _engine.EnsureIndex("col_async_unique", "Code", "idx_code_unique", unique: true);
        var collection = _engine.GetCollection<BsonDocument>("col_async_unique");
        var docs = new[]
        {
            new BsonDocument().Set("Code", "dup"),
            new BsonDocument().Set("Code", "dup")
        };

        await Assert.That(async () => await collection.InsertAsync(docs))
            .Throws<AggregateException>();
    }
}

