using TinyDb.Bson;
using TinyDb.Serialization;
using System.Globalization;
using System.Text;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Serialization;

public class BsonExtendedTests
{
    [Test]
    public async Task BsonSerializer_AllTypes_RoundTrip()
    {
        var doc = new BsonDocument();
        doc = doc.Set("string", "Hello World")
                 .Set("int", 123)
                 .Set("long", 1234567890L)
                 .Set("double", 3.14159)
                 .Set("bool", true)
                 .Set("null", BsonNull.Value)
                 .Set("datetime", new DateTime(2025, 1, 1, 12, 0, 0, DateTimeKind.Utc))
                 .Set("objectId", ObjectId.NewObjectId())
                 .Set("decimal", 123.456m)
                 .Set("binary", new BsonBinary(new byte[] { 1, 2, 3 }))
                 .Set("regex", new BsonRegularExpression("^abc", "i"))
                 .Set("timestamp", new BsonTimestamp(987654321L));

        var nestedArr = new BsonArray();
        nestedArr = nestedArr.AddValue("one").AddValue(2).AddValue(false);
        doc = doc.Set("array", nestedArr);

        var nestedDoc = new BsonDocument();
        nestedDoc = nestedDoc.Set("key", "value");
        doc = doc.Set("nested", nestedDoc);

        // Act
        var data = BsonSerializer.SerializeDocument(doc);
        var result = BsonSerializer.DeserializeDocument(data);

        // Assert
        await Assert.That(result["string"].ToString()).IsEqualTo("Hello World");
        await Assert.That(result["int"].As<int>()).IsEqualTo(123);
        await Assert.That(result["long"].As<long>()).IsEqualTo(1234567890L);
        await Assert.That(result["double"].As<double>()).IsEqualTo(3.14159);
        await Assert.That(result["bool"].As<bool>()).IsTrue();
        await Assert.That(result["null"].IsNull).IsTrue();
        await Assert.That(result["datetime"].As<DateTime>()).IsEqualTo(new DateTime(2025, 1, 1, 12, 0, 0, DateTimeKind.Utc));
        await Assert.That(result["decimal"].As<decimal>()).IsEqualTo(123.456m);
        await Assert.That(result["binary"].As<byte[]>().Length).IsEqualTo(3);
        await Assert.That(((BsonRegularExpression)result["regex"]).Pattern).IsEqualTo("^abc");
        await Assert.That(((BsonTimestamp)result["timestamp"]).Value).IsEqualTo(987654321L);
        
        var arr = (BsonArray)result["array"];
        await Assert.That(arr.Count).IsEqualTo(3);
        await Assert.That(arr[0].ToString()).IsEqualTo("one");
    }

    [Test]
    public async Task BsonSerializer_SizeCalculation_Matches_SerializedLength()
    {
        var doc = new BsonDocument();
        doc = doc.Set("name", "Test")
                 .Set("value", 42)
                 .Set("list", new BsonArray().AddValue(1).AddValue(2));

        var expectedSize = BsonSerializer.CalculateDocumentSize(doc);
        var actualData = BsonSerializer.SerializeDocument(doc);

        await Assert.That(actualData.Length).IsEqualTo(expectedSize);
    }

    [Test]
    public async Task BsonReader_Should_Throw_On_Size_Mismatch()
    {
        var doc = new BsonDocument().Set("a", 1);
        var data = BsonSerializer.SerializeDocument(doc);
        
        // Corrupt the size field (first 4 bytes)
        data[0] = 0xFF; 
        data[1] = 0x7F; // Make it very large

        await Assert.That(() => BsonSerializer.DeserializeDocument(data))
            .Throws<Exception>();
    }

    [Test]
    public async Task BsonReader_ReadString_With_Missing_Null_Terminator_Should_Throw()
    {
        using var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms);
        writer.Write(5); // Length
        writer.Write(Encoding.UTF8.GetBytes("test"));
        writer.Write((byte)1); // Incorrect null terminator (not 0)
        ms.Position = 0;

        using var reader = new BsonReader(ms);
        await Assert.That(() => reader.ReadString()).Throws<InvalidOperationException>();
    }

    [Test]
    public async Task BsonReader_ReadValue_With_Unsupported_Type_Should_Throw()
    {
        using var ms = new MemoryStream(new byte[] { 0xEE }); // Unsupported BsonType
        using var reader = new BsonReader(ms);
        await Assert.That(() => reader.ReadValue()).Throws<NotSupportedException>();
    }

    [Test]
    public async Task BsonWriter_Unsupported_Types_Should_Throw()
    {
        using var ms = new MemoryStream();
        using var writer = new BsonWriter(ms);
        
        await Assert.That(() => writer.WriteValue(new BsonValueWrapper(BsonType.JavaScript))).Throws<NotSupportedException>();
        await Assert.That(() => writer.WriteValue(new BsonValueWrapper(BsonType.Symbol))).Throws<NotSupportedException>();
        await Assert.That(() => writer.WriteValue(new BsonValueWrapper(BsonType.Undefined))).Throws<NotSupportedException>();
    }

    private class BsonValueWrapper : BsonValue
    {
        private readonly BsonType _type;
        public BsonValueWrapper(BsonType type) => _type = type;
        public override BsonType BsonType => _type;
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
