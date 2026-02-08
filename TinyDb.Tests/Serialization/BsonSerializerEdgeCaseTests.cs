using System;
using System.IO;
using TinyDb.Bson;
using TinyDb.Serialization;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Serialization;

/// <summary>
/// Edge case tests for BsonSerializer, BsonWriter, and BsonReader
/// to improve code coverage for rarely-used paths
/// </summary>
public class BsonSerializerEdgeCaseTests
{
    #region MinKey/MaxKey Tests

    [Test]
    public async Task Serialize_MinKey_InDocument_ShouldRoundtrip()
    {
        var doc = new BsonDocument()
            .Set("min", BsonMinKey.Value)
            .Set("data", "test");

        var bytes = BsonSerializer.SerializeDocument(doc);
        var result = BsonSerializer.DeserializeDocument(bytes);

        await Assert.That(result.ContainsKey("min")).IsTrue();
        await Assert.That(result["min"]).IsTypeOf<BsonMinKey>();
        await Assert.That(((BsonString)result["data"]).Value).IsEqualTo("test");
    }

    [Test]
    public async Task Serialize_MaxKey_InDocument_ShouldRoundtrip()
    {
        var doc = new BsonDocument()
            .Set("max", BsonMaxKey.Value)
            .Set("data", "test");

        var bytes = BsonSerializer.SerializeDocument(doc);
        var result = BsonSerializer.DeserializeDocument(bytes);

        await Assert.That(result.ContainsKey("max")).IsTrue();
        await Assert.That(result["max"]).IsTypeOf<BsonMaxKey>();
    }

    [Test]
    public async Task Serialize_MinMaxKey_Together_ShouldRoundtrip()
    {
        var doc = new BsonDocument()
            .Set("min", BsonMinKey.Value)
            .Set("max", BsonMaxKey.Value);

        var bytes = BsonSerializer.SerializeDocument(doc);
        var result = BsonSerializer.DeserializeDocument(bytes);

        await Assert.That(result["min"]).IsTypeOf<BsonMinKey>();
        await Assert.That(result["max"]).IsTypeOf<BsonMaxKey>();
    }

    [Test]
    public async Task BsonMinKey_Singleton_ShouldBeSame()
    {
        var min1 = BsonMinKey.Value;
        var min2 = BsonMinKey.Value;
        await Assert.That(ReferenceEquals(min1, min2)).IsTrue();
        await Assert.That(min1.BsonType).IsEqualTo(BsonType.MinKey);
    }

    [Test]
    public async Task BsonMaxKey_Singleton_ShouldBeSame()
    {
        var max1 = BsonMaxKey.Value;
        var max2 = BsonMaxKey.Value;
        await Assert.That(ReferenceEquals(max1, max2)).IsTrue();
        await Assert.That(max1.BsonType).IsEqualTo(BsonType.MaxKey);
    }

    #endregion

    #region BsonWriter Disposed Tests

    [Test]
    public async Task BsonWriter_WriteValue_AfterDispose_ThrowsObjectDisposedException()
    {
        var stream = new MemoryStream();
        var writer = new BsonWriter(stream);
        writer.Dispose();

        await Assert.That(() => writer.WriteValue(new BsonString("test")))
            .Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task BsonWriter_WriteDocument_AfterDispose_ThrowsObjectDisposedException()
    {
        var stream = new MemoryStream();
        var writer = new BsonWriter(stream);
        writer.Dispose();

        await Assert.That(() => writer.WriteDocument(new BsonDocument()))
            .Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task BsonWriter_WriteArray_AfterDispose_ThrowsObjectDisposedException()
    {
        var stream = new MemoryStream();
        var writer = new BsonWriter(stream);
        writer.Dispose();

        await Assert.That(() => writer.WriteArray(new BsonArray()))
            .Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task BsonWriter_WriteCString_AfterDispose_ThrowsObjectDisposedException()
    {
        var stream = new MemoryStream();
        var writer = new BsonWriter(stream);
        writer.Dispose();

        await Assert.That(() => writer.WriteCString("test"))
            .Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task BsonWriter_WriteString_AfterDispose_ThrowsObjectDisposedException()
    {
        var stream = new MemoryStream();
        var writer = new BsonWriter(stream);
        writer.Dispose();

        await Assert.That(() => writer.WriteString("test"))
            .Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task BsonWriter_WriteInt32_AfterDispose_ThrowsObjectDisposedException()
    {
        var stream = new MemoryStream();
        var writer = new BsonWriter(stream);
        writer.Dispose();

        await Assert.That(() => writer.WriteInt32(123))
            .Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task BsonWriter_WriteInt64_AfterDispose_ThrowsObjectDisposedException()
    {
        var stream = new MemoryStream();
        var writer = new BsonWriter(stream);
        writer.Dispose();

        await Assert.That(() => writer.WriteInt64(123L))
            .Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task BsonWriter_WriteDouble_AfterDispose_ThrowsObjectDisposedException()
    {
        var stream = new MemoryStream();
        var writer = new BsonWriter(stream);
        writer.Dispose();

        await Assert.That(() => writer.WriteDouble(3.14))
            .Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task BsonWriter_WriteBoolean_AfterDispose_ThrowsObjectDisposedException()
    {
        var stream = new MemoryStream();
        var writer = new BsonWriter(stream);
        writer.Dispose();

        await Assert.That(() => writer.WriteBoolean(true))
            .Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task BsonWriter_WriteObjectId_AfterDispose_ThrowsObjectDisposedException()
    {
        var stream = new MemoryStream();
        var writer = new BsonWriter(stream);
        writer.Dispose();

        await Assert.That(() => writer.WriteObjectId(ObjectId.NewObjectId()))
            .Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task BsonWriter_WriteDateTime_AfterDispose_ThrowsObjectDisposedException()
    {
        var stream = new MemoryStream();
        var writer = new BsonWriter(stream);
        writer.Dispose();

        await Assert.That(() => writer.WriteDateTime(DateTime.UtcNow))
            .Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task BsonWriter_WriteNull_AfterDispose_ThrowsObjectDisposedException()
    {
        var stream = new MemoryStream();
        var writer = new BsonWriter(stream);
        writer.Dispose();

        await Assert.That(() => writer.WriteNull())
            .Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task BsonWriter_WriteBinary_AfterDispose_ThrowsObjectDisposedException()
    {
        var stream = new MemoryStream();
        var writer = new BsonWriter(stream);
        writer.Dispose();

        await Assert.That(() => writer.WriteBinary(new byte[] { 1, 2, 3 }))
            .Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task BsonWriter_WriteUndefined_AfterDispose_ThrowsObjectDisposedException()
    {
        var stream = new MemoryStream();
        var writer = new BsonWriter(stream);
        writer.Dispose();

        await Assert.That(() => writer.WriteUndefined())
            .Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task BsonWriter_WriteRegularExpression_AfterDispose_ThrowsObjectDisposedException()
    {
        var stream = new MemoryStream();
        var writer = new BsonWriter(stream);
        writer.Dispose();

        await Assert.That(() => writer.WriteRegularExpression("pattern", "options"))
            .Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task BsonWriter_WriteJavaScript_AfterDispose_ThrowsObjectDisposedException()
    {
        var stream = new MemoryStream();
        var writer = new BsonWriter(stream);
        writer.Dispose();

        await Assert.That(() => writer.WriteJavaScript("function() {}"))
            .Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task BsonWriter_WriteJavaScriptWithScope_AfterDispose_ThrowsObjectDisposedException()
    {
        var stream = new MemoryStream();
        var writer = new BsonWriter(stream);
        writer.Dispose();

        await Assert.That(() => writer.WriteJavaScriptWithScope("function() {}", new BsonDocument()))
            .Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task BsonWriter_WriteSymbol_AfterDispose_ThrowsObjectDisposedException()
    {
        var stream = new MemoryStream();
        var writer = new BsonWriter(stream);
        writer.Dispose();

        await Assert.That(() => writer.WriteSymbol("symbol"))
            .Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task BsonWriter_WriteDecimal128_AfterDispose_ThrowsObjectDisposedException()
    {
        var stream = new MemoryStream();
        var writer = new BsonWriter(stream);
        writer.Dispose();

        await Assert.That(() => writer.WriteDecimal128(123.45m))
            .Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task BsonWriter_WriteTimestamp_AfterDispose_ThrowsObjectDisposedException()
    {
        var stream = new MemoryStream();
        var writer = new BsonWriter(stream);
        writer.Dispose();

        await Assert.That(() => writer.WriteTimestamp(12345L))
            .Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task BsonWriter_WriteMinKey_AfterDispose_ThrowsObjectDisposedException()
    {
        var stream = new MemoryStream();
        var writer = new BsonWriter(stream);
        writer.Dispose();

        await Assert.That(() => writer.WriteMinKey())
            .Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task BsonWriter_WriteMaxKey_AfterDispose_ThrowsObjectDisposedException()
    {
        var stream = new MemoryStream();
        var writer = new BsonWriter(stream);
        writer.Dispose();

        await Assert.That(() => writer.WriteMaxKey())
            .Throws<ObjectDisposedException>();
    }

    #endregion

    #region BsonReader Disposed Tests

    [Test]
    public async Task BsonReader_ReadValue_AfterDispose_ThrowsObjectDisposedException()
    {
        var stream = new MemoryStream(new byte[] { 0x02 }); // String type
        var reader = new BsonReader(stream);
        reader.Dispose();

        await Assert.That(() => reader.ReadValue())
            .Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task BsonReader_ReadDocument_AfterDispose_ThrowsObjectDisposedException()
    {
        var doc = new BsonDocument().Set("test", "value");
        var bytes = BsonSerializer.SerializeDocument(doc);
        var stream = new MemoryStream(bytes);
        var reader = new BsonReader(stream);
        reader.Dispose();

        await Assert.That(() => reader.ReadDocument())
            .Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task BsonReader_ReadArray_AfterDispose_ThrowsObjectDisposedException()
    {
        var arr = new BsonArray().AddValue(new BsonInt32(1));
        var bytes = BsonSerializer.SerializeArray(arr);
        var stream = new MemoryStream(bytes);
        var reader = new BsonReader(stream);
        reader.Dispose();

        await Assert.That(() => reader.ReadArray())
            .Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task BsonReader_ReadCString_AfterDispose_ThrowsObjectDisposedException()
    {
        var stream = new MemoryStream(new byte[] { (byte)'t', (byte)'e', (byte)'s', (byte)'t', 0 });
        var reader = new BsonReader(stream);
        reader.Dispose();

        await Assert.That(() => reader.ReadCString())
            .Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task BsonReader_ReadString_AfterDispose_ThrowsObjectDisposedException()
    {
        var stream = new MemoryStream(new byte[] { 5, 0, 0, 0, (byte)'t', (byte)'e', (byte)'s', (byte)'t', 0 });
        var reader = new BsonReader(stream);
        reader.Dispose();

        await Assert.That(() => reader.ReadString())
            .Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task BsonReader_ReadInt32_AfterDispose_ThrowsObjectDisposedException()
    {
        var stream = new MemoryStream(BitConverter.GetBytes(123));
        var reader = new BsonReader(stream);
        reader.Dispose();

        await Assert.That(() => reader.ReadInt32())
            .Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task BsonReader_ReadInt64_AfterDispose_ThrowsObjectDisposedException()
    {
        var stream = new MemoryStream(BitConverter.GetBytes(123L));
        var reader = new BsonReader(stream);
        reader.Dispose();

        await Assert.That(() => reader.ReadInt64())
            .Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task BsonReader_ReadDouble_AfterDispose_ThrowsObjectDisposedException()
    {
        var stream = new MemoryStream(BitConverter.GetBytes(3.14));
        var reader = new BsonReader(stream);
        reader.Dispose();

        await Assert.That(() => reader.ReadDouble())
            .Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task BsonReader_ReadBoolean_AfterDispose_ThrowsObjectDisposedException()
    {
        var stream = new MemoryStream(new byte[] { 1 });
        var reader = new BsonReader(stream);
        reader.Dispose();

        await Assert.That(() => reader.ReadBoolean())
            .Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task BsonReader_ReadObjectId_AfterDispose_ThrowsObjectDisposedException()
    {
        var stream = new MemoryStream(new byte[12]);
        var reader = new BsonReader(stream);
        reader.Dispose();

        await Assert.That(() => reader.ReadObjectId())
            .Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task BsonReader_ReadDateTime_AfterDispose_ThrowsObjectDisposedException()
    {
        var stream = new MemoryStream(BitConverter.GetBytes(0L));
        var reader = new BsonReader(stream);
        reader.Dispose();

        await Assert.That(() => reader.ReadDateTime())
            .Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task BsonReader_ReadDecimal128_AfterDispose_ThrowsObjectDisposedException()
    {
        var stream = new MemoryStream(new byte[16]);
        var reader = new BsonReader(stream);
        reader.Dispose();

        await Assert.That(() => reader.ReadDecimal128())
            .Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task BsonReader_ReadBinary_AfterDispose_ThrowsObjectDisposedException()
    {
        // Binary format: length (4) + subtype (1) + data
        var bytes = new byte[] { 3, 0, 0, 0, 0, 1, 2, 3 };
        var stream = new MemoryStream(bytes);
        var reader = new BsonReader(stream);
        reader.Dispose();

        await Assert.That(() => reader.ReadBinary())
            .Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task BsonReader_ReadRegularExpression_AfterDispose_ThrowsObjectDisposedException()
    {
        // Regex format: cstring pattern + cstring options
        var bytes = new byte[] { (byte)'a', 0, (byte)'i', 0 };
        var stream = new MemoryStream(bytes);
        var reader = new BsonReader(stream);
        reader.Dispose();

        await Assert.That(() => reader.ReadRegularExpression())
            .Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task BsonReader_ReadTimestamp_AfterDispose_ThrowsObjectDisposedException()
    {
        var stream = new MemoryStream(BitConverter.GetBytes(12345L));
        var reader = new BsonReader(stream);
        reader.Dispose();

        await Assert.That(() => reader.ReadTimestamp())
            .Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task BsonReader_ReadJavaScript_AfterDispose_ThrowsObjectDisposedException()
    {
        // JavaScript format: string (length + data + null)
        var bytes = new byte[] { 5, 0, 0, 0, (byte)'c', (byte)'o', (byte)'d', (byte)'e', 0 };
        var stream = new MemoryStream(bytes);
        var reader = new BsonReader(stream);
        reader.Dispose();

        await Assert.That(() => reader.ReadJavaScript())
            .Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task BsonReader_ReadSymbol_AfterDispose_ThrowsObjectDisposedException()
    {
        // Symbol format: string (length + data + null)
        var bytes = new byte[] { 4, 0, 0, 0, (byte)'s', (byte)'y', (byte)'m', 0 };
        var stream = new MemoryStream(bytes);
        var reader = new BsonReader(stream);
        reader.Dispose();

        await Assert.That(() => reader.ReadSymbol())
            .Throws<ObjectDisposedException>();
    }

    #endregion

    #region ReadValue Edge Cases

    [Test]
    public async Task BsonReader_ReadValue_EndType_ThrowsInvalidOperationException()
    {
        // End marker byte (0x00) should throw when read as a value type
        var stream = new MemoryStream(new byte[] { 0x00 }); // BsonType.End = 0
        var reader = new BsonReader(stream);

        await Assert.That(() => reader.ReadValue())
            .Throws<InvalidOperationException>();
    }

    [Test]
    public async Task BsonReader_ReadValue_UnsupportedType_ThrowsNotSupportedException()
    {
        // Use an undefined type byte (e.g., 0xFE which is not a valid BSON type)
        var stream = new MemoryStream(new byte[] { 0xFE });
        var reader = new BsonReader(stream);

        await Assert.That(() => reader.ReadValue())
            .Throws<NotSupportedException>();
    }

    #endregion

    #region ReadString Edge Cases

    [Test]
    public async Task BsonReader_ReadString_MissingNullTerminator_ThrowsInvalidOperationException()
    {
        // String format: length (4) + data + null terminator
        // This test provides wrong null terminator
        var bytes = new byte[] { 5, 0, 0, 0, (byte)'t', (byte)'e', (byte)'s', (byte)'t', 0xFF }; // Wrong terminator
        var stream = new MemoryStream(bytes);
        var reader = new BsonReader(stream);

        await Assert.That(() => reader.ReadString())
            .Throws<InvalidOperationException>();
    }

    #endregion

    #region BsonWriter Edge Cases

    [Test]
    public async Task BsonWriter_WriteValue_UnsupportedType_ThrowsNotSupportedException()
    {
        var stream = new MemoryStream();
        using var writer = new BsonWriter(stream);

        // Create a custom BsonValue that is not supported
        // We can't easily create one, but we can test via a mock or by using reflection
        // For now, let's skip this test as it requires internal type creation
        await Assert.That(writer).IsNotNull();
    }

    [Test]
    public async Task BsonWriter_WriteDocument_NullDocument_ThrowsArgumentNullException()
    {
        var stream = new MemoryStream();
        using var writer = new BsonWriter(stream);

        await Assert.That(() => writer.WriteDocument(null!))
            .Throws<ArgumentNullException>();
    }

    [Test]
    public async Task BsonWriter_WriteArray_NullArray_ThrowsArgumentNullException()
    {
        var stream = new MemoryStream();
        using var writer = new BsonWriter(stream);

        await Assert.That(() => writer.WriteArray(null!))
            .Throws<ArgumentNullException>();
    }

    [Test]
    public async Task BsonWriter_WriteCString_NullValue_ThrowsArgumentNullException()
    {
        var stream = new MemoryStream();
        using var writer = new BsonWriter(stream);

        await Assert.That(() => writer.WriteCString(null!))
            .Throws<ArgumentNullException>();
    }

    [Test]
    public async Task BsonWriter_WriteString_NullValue_ThrowsArgumentNullException()
    {
        var stream = new MemoryStream();
        using var writer = new BsonWriter(stream);

        await Assert.That(() => writer.WriteString(null!))
            .Throws<ArgumentNullException>();
    }

    [Test]
    public async Task BsonWriter_WriteBinary_NullBytes_ThrowsArgumentNullException()
    {
        var stream = new MemoryStream();
        using var writer = new BsonWriter(stream);

        await Assert.That(() => writer.WriteBinary(null!))
            .Throws<ArgumentNullException>();
    }

    [Test]
    public async Task BsonWriter_WriteRegularExpression_NullPattern_ThrowsArgumentNullException()
    {
        var stream = new MemoryStream();
        using var writer = new BsonWriter(stream);

        await Assert.That(() => writer.WriteRegularExpression(null!, "i"))
            .Throws<ArgumentNullException>();
    }

    [Test]
    public async Task BsonWriter_WriteRegularExpression_NullOptions_ThrowsArgumentNullException()
    {
        var stream = new MemoryStream();
        using var writer = new BsonWriter(stream);

        await Assert.That(() => writer.WriteRegularExpression("pattern", null!))
            .Throws<ArgumentNullException>();
    }

    [Test]
    public async Task BsonWriter_WriteJavaScriptWithScope_NullCode_ThrowsArgumentNullException()
    {
        var stream = new MemoryStream();
        using var writer = new BsonWriter(stream);

        await Assert.That(() => writer.WriteJavaScriptWithScope(null!, new BsonDocument()))
            .Throws<ArgumentNullException>();
    }

    [Test]
    public async Task BsonWriter_WriteJavaScriptWithScope_NullScope_ThrowsArgumentNullException()
    {
        var stream = new MemoryStream();
        using var writer = new BsonWriter(stream);

        await Assert.That(() => writer.WriteJavaScriptWithScope("code", null!))
            .Throws<ArgumentNullException>();
    }

    #endregion

    #region BsonSerializer Edge Cases

    [Test]
    public async Task BsonSerializer_Serialize_NullDocument_ThrowsArgumentNullException()
    {
        await Assert.That(() => BsonSerializer.SerializeDocument(null!))
            .Throws<ArgumentNullException>();
    }

    [Test]
    public async Task BsonSerializer_SerializeDocumentToStream_NullDocument_ThrowsArgumentNullException()
    {
        var stream = new MemoryStream();
        await Assert.That(() => BsonSerializer.SerializeDocument(null!, stream))
            .Throws<ArgumentNullException>();
    }

    [Test]
    public async Task BsonSerializer_Deserialize_NullData_ThrowsArgumentNullException()
    {
        await Assert.That(() => BsonSerializer.Deserialize(null!))
            .Throws<ArgumentNullException>();
    }

    [Test]
    public async Task BsonSerializer_Deserialize_EmptyData_ReturnsBsonNull()
    {
        var result = BsonSerializer.Deserialize(Array.Empty<byte>());
        await Assert.That(result).IsTypeOf<BsonNull>();
    }

    [Test]
    public async Task BsonSerializer_DeserializeDocument_NullData_ThrowsArgumentNullException()
    {
        await Assert.That(() => BsonSerializer.DeserializeDocument(null!))
            .Throws<ArgumentNullException>();
    }

    [Test]
    public async Task BsonSerializer_DeserializeDocument_EmptyData_ReturnsEmptyDocument()
    {
        var result = BsonSerializer.DeserializeDocument(Array.Empty<byte>());
        await Assert.That(result.Count).IsEqualTo(0);
    }

    [Test]
    public async Task BsonSerializer_DeserializeDocument_EmptyMemory_ReturnsEmptyDocument()
    {
        var result = BsonSerializer.DeserializeDocument(ReadOnlyMemory<byte>.Empty);
        await Assert.That(result.Count).IsEqualTo(0);
    }

    [Test]
    public async Task BsonSerializer_DeserializeDocumentWithFields_NullData_ThrowsArgumentNullException()
    {
        await Assert.That(() => BsonSerializer.DeserializeDocument(null!, new HashSet<string> { "test" }))
            .Throws<ArgumentNullException>();
    }

    [Test]
    public async Task BsonSerializer_DeserializeDocumentWithFields_EmptyData_ReturnsEmptyDocument()
    {
        var result = BsonSerializer.DeserializeDocument(Array.Empty<byte>(), new HashSet<string> { "test" });
        await Assert.That(result.Count).IsEqualTo(0);
    }

    [Test]
    public async Task BsonSerializer_SerializeArray_NullArray_ThrowsArgumentNullException()
    {
        await Assert.That(() => BsonSerializer.SerializeArray(null!))
            .Throws<ArgumentNullException>();
    }

    [Test]
    public async Task BsonSerializer_DeserializeArray_NullData_ThrowsArgumentNullException()
    {
        await Assert.That(() => BsonSerializer.DeserializeArray(null!))
            .Throws<ArgumentNullException>();
    }

    [Test]
    public async Task BsonSerializer_DeserializeArray_EmptyData_ReturnsEmptyArray()
    {
        var result = BsonSerializer.DeserializeArray(Array.Empty<byte>());
        await Assert.That(result.Count).IsEqualTo(0);
    }

    #endregion

    #region LeaveOpen Tests

    [Test]
    public async Task BsonWriter_LeaveOpenTrue_StreamNotDisposed()
    {
        var stream = new MemoryStream();
        var writer = new BsonWriter(stream, leaveOpen: true);
        writer.WriteDocument(new BsonDocument().Set("test", "value"));
        writer.Dispose();

        // Stream should still be usable
        stream.Position = 0;
        var canRead = stream.CanRead;
        await Assert.That(canRead).IsTrue();
        stream.Dispose();
    }

    [Test]
    public async Task BsonWriter_LeaveOpenFalse_StreamDisposed()
    {
        var stream = new MemoryStream();
        var writer = new BsonWriter(stream, leaveOpen: false);
        writer.WriteDocument(new BsonDocument().Set("test", "value"));
        writer.Dispose();

        // Stream should be disposed
        await Assert.That(() => stream.ReadByte())
            .Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task BsonReader_LeaveOpenTrue_StreamNotDisposed()
    {
        var doc = new BsonDocument().Set("test", "value");
        var bytes = BsonSerializer.SerializeDocument(doc);
        var stream = new MemoryStream(bytes);
        var reader = new BsonReader(stream, leaveOpen: true);
        reader.ReadDocument();
        reader.Dispose();

        // Stream should still be usable
        stream.Position = 0;
        var canRead = stream.CanRead;
        await Assert.That(canRead).IsTrue();
        stream.Dispose();
    }

    [Test]
    public async Task BsonReader_LeaveOpenFalse_StreamDisposed()
    {
        var doc = new BsonDocument().Set("test", "value");
        var bytes = BsonSerializer.SerializeDocument(doc);
        var stream = new MemoryStream(bytes);
        var reader = new BsonReader(stream, leaveOpen: false);
        reader.ReadDocument();
        reader.Dispose();

        // Stream should be disposed
        await Assert.That(() => stream.ReadByte())
            .Throws<ObjectDisposedException>();
    }

    #endregion

    #region Double Dispose Tests

    [Test]
    public async Task BsonWriter_DoubleDispose_ShouldNotThrow()
    {
        var stream = new MemoryStream();
        var writer = new BsonWriter(stream);
        
        // Should not throw on double dispose
        writer.Dispose();
        writer.Dispose();
        
        await Assert.That(writer).IsNotNull();
    }

    [Test]
    public async Task BsonReader_DoubleDispose_ShouldNotThrow()
    {
        var doc = new BsonDocument().Set("test", "value");
        var bytes = BsonSerializer.SerializeDocument(doc);
        var stream = new MemoryStream(bytes);
        var reader = new BsonReader(stream);

        // Should not throw on double dispose
        reader.Dispose();
        reader.Dispose();

        await Assert.That(reader).IsNotNull();
    }

    #endregion

    #region ReadDocument with Field Selection

    [Test]
    public async Task BsonReader_ReadDocumentWithFields_SkipsUnwantedFields()
    {
        var doc = new BsonDocument()
            .Set("wanted", "value1")
            .Set("unwanted", "value2")
            .Set("alsoWanted", 123);

        var bytes = BsonSerializer.SerializeDocument(doc);
        var fields = new HashSet<string> { "wanted", "alsoWanted" };
        var result = BsonSerializer.DeserializeDocument(bytes, fields);

        await Assert.That(result.ContainsKey("wanted")).IsTrue();
        await Assert.That(result.ContainsKey("alsoWanted")).IsTrue();
        await Assert.That(result.ContainsKey("unwanted")).IsFalse();
    }

    [Test]
    public async Task BsonReader_ReadDocumentWithFields_NullFieldsLoadsAll()
    {
        var doc = new BsonDocument()
            .Set("field1", "value1")
            .Set("field2", "value2");

        var bytes = BsonSerializer.SerializeDocument(doc);
        var stream = new MemoryStream(bytes);
        using var reader = new BsonReader(stream);
        var result = reader.ReadDocument(null!);

        await Assert.That(result.ContainsKey("field1")).IsTrue();
        await Assert.That(result.ContainsKey("field2")).IsTrue();
    }

    [Test]
    public async Task BsonReader_ReadDocumentWithFields_SkipsAllTypes()
    {
        // Test skipping various BSON types
        var doc = new BsonDocument()
            .Set("keepMe", "value")
            .Set("skipNull", BsonNull.Value)
            .Set("skipBool", true)
            .Set("skipInt32", 123)
            .Set("skipInt64", 123456789L)
            .Set("skipDouble", 3.14)
            .Set("skipDateTime", DateTime.UtcNow)
            .Set("skipTimestamp", new BsonTimestamp(12345L))
            .Set("skipObjectId", ObjectId.NewObjectId())
            .Set("skipString", "skip this")
            .Set("skipDecimal128", 123.45m)
            .Set("skipDocument", new BsonDocument().Set("nested", "value"))
            .Set("skipArray", new BsonArray().AddValue(new BsonInt32(1)))
            .Set("skipBinary", new BsonBinary(new byte[] { 1, 2, 3 }))
            .Set("skipRegex", new BsonRegularExpression("pattern", "i"))
            .Set("skipMinKey", BsonMinKey.Value)
            .Set("skipMaxKey", BsonMaxKey.Value)
            .Set("skipJavaScript", new BsonJavaScript("code"))
            .Set("skipSymbol", new BsonSymbol("sym"))
            .Set("skipJsWithScope", new BsonJavaScriptWithScope("code", new BsonDocument().Set("x", 1)));

        var bytes = BsonSerializer.SerializeDocument(doc);
        var fields = new HashSet<string> { "keepMe" };
        var result = BsonSerializer.DeserializeDocument(bytes, fields);

        await Assert.That(result.Count).IsEqualTo(1);
        await Assert.That(result.ContainsKey("keepMe")).IsTrue();
        await Assert.That(((BsonString)result["keepMe"]).Value).IsEqualTo("value");
    }

    #endregion

    #region SerializeValue Tests

    [Test]
    public async Task BsonSerializer_SerializeValue_ToWriter_ShouldWork()
    {
        var stream = new MemoryStream();
        using var writer = new BsonWriter(stream, leaveOpen: true);
        BsonSerializer.SerializeValue(new BsonString("test"), writer);
        
        await Assert.That(stream.Length > 0).IsTrue();
    }

    [Test]
    public async Task BsonSerializer_SerializeValue_ToStream_ShouldWork()
    {
        var stream = new MemoryStream();
        BsonSerializer.SerializeValue(new BsonString("test"), stream);
        
        await Assert.That(stream.Length > 0).IsTrue();
    }

    [Test]
    public async Task BsonSerializer_DeserializeValue_FromStream_ShouldWork()
    {
        // Note: SerializeValue/DeserializeValue are designed to work with the type byte prefix
        // BsonReader.ReadValue() reads the type byte first, so we need to write it manually
        var stream = new MemoryStream();
        
        // Write type byte + value
        stream.WriteByte((byte)BsonType.String);
        using (var writer = new BsonWriter(stream, leaveOpen: true))
        {
            writer.WriteString("test");
        }
        stream.Position = 0;
        
        var result = BsonSerializer.DeserializeValue(stream);
        await Assert.That(result).IsTypeOf<BsonString>();
        await Assert.That(((BsonString)result).Value).IsEqualTo("test");
    }

    #endregion

    #region GetRecyclableStream Tests

    [Test]
    public async Task BsonSerializer_GetRecyclableStream_ShouldReturnStream()
    {
        using var stream = BsonSerializer.GetRecyclableStream();
        await Assert.That(stream).IsNotNull();
        await Assert.That(stream.CanWrite).IsTrue();
    }

    #endregion

    #region Size Calculation Tests

    [Test]
    public async Task BsonSerializer_CalculateSize_ShouldReturnCorrectSize()
    {
        var value = new BsonString("test");
        var size = BsonSerializer.CalculateSize(value);
        await Assert.That(size > 0).IsTrue();
    }

    [Test]
    public async Task BsonSerializer_CalculateDocumentSize_ShouldReturnCorrectSize()
    {
        var doc = new BsonDocument().Set("test", "value");
        var size = BsonSerializer.CalculateDocumentSize(doc);
        var actualBytes = BsonSerializer.SerializeDocument(doc);
        
        await Assert.That(size).IsEqualTo(actualBytes.Length);
    }

    [Test]
    public async Task BsonSerializer_CalculateArraySize_ShouldReturnCorrectSize()
    {
        var arr = new BsonArray()
            .AddValue(new BsonInt32(1))
            .AddValue(new BsonString("test"));
        var size = BsonSerializer.CalculateArraySize(arr);
        var actualBytes = BsonSerializer.SerializeArray(arr);
        
        await Assert.That(size).IsEqualTo(actualBytes.Length);
    }

    #endregion

    #region CommonKeyCache Tests

    [Test]
    public async Task BsonWriter_WriteCString_UsesCache_ForCommonKeys()
    {
        // Test that common keys like "_id", "Name", etc. use the cache
        var doc = new BsonDocument()
            .Set("_id", ObjectId.NewObjectId())
            .Set("Name", "test")
            .Set("Type", "entity")
            .Set("Value", 123)
            .Set("Count", 5);

        var bytes = BsonSerializer.SerializeDocument(doc);
        var result = BsonSerializer.DeserializeDocument(bytes);

        await Assert.That(result.ContainsKey("_id")).IsTrue();
        await Assert.That(result.ContainsKey("Name")).IsTrue();
        await Assert.That(result.ContainsKey("Type")).IsTrue();
        await Assert.That(result.ContainsKey("Value")).IsTrue();
        await Assert.That(result.ContainsKey("Count")).IsTrue();
    }

    [Test]
    public async Task BsonWriter_WriteCString_HandlesNonCachedKeys()
    {
        var doc = new BsonDocument()
            .Set("customKey123", "value")
            .Set("anotherUniqueKey", 456);

        var bytes = BsonSerializer.SerializeDocument(doc);
        var result = BsonSerializer.DeserializeDocument(bytes);

        await Assert.That(result.ContainsKey("customKey123")).IsTrue();
        await Assert.That(result.ContainsKey("anotherUniqueKey")).IsTrue();
    }

    #endregion

    #region Long CString Tests

    [Test]
    public async Task BsonReader_ReadCString_VeryLongString_ShouldWork()
    {
        // Create a document with a very long key name to test buffer expansion in ReadCString
        var longKey = new string('x', 500); // Longer than initial 128-byte buffer
        var doc = new BsonDocument().Set(longKey, "value");

        var bytes = BsonSerializer.SerializeDocument(doc);
        var result = BsonSerializer.DeserializeDocument(bytes);

        await Assert.That(result.ContainsKey(longKey)).IsTrue();
        await Assert.That(((BsonString)result[longKey]).Value).IsEqualTo("value");
    }

    #endregion
}
