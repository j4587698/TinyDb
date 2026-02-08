using System;
using System.IO;
using System.Linq;
using System.Collections.Generic;
using TinyDb.Bson;
using TinyDb.Serialization;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Serialization;

public class BsonReaderCoverageTests
{
    [Test]
    public async Task ReadValue_AllTypes_ShouldWork()
    {
        using var ms = new MemoryStream();
        using var writer = new BsonWriter(ms, true);
        
        var oid = ObjectId.NewObjectId();
        var now = DateTime.UtcNow;
        var guid = Guid.NewGuid();
        
        writer.WriteValue(BsonNull.Value);
        writer.WriteValue(new BsonString("test"));
        writer.WriteValue(new BsonInt32(123));
        writer.WriteValue(new BsonInt64(456));
        writer.WriteValue(new BsonDouble(1.23));
        writer.WriteValue(new BsonBoolean(true));
        writer.WriteValue(new BsonObjectId(oid));
        writer.WriteValue(new BsonDateTime(now));
        writer.WriteValue(new BsonDecimal128(new Decimal128(789)));
        writer.WriteValue(new BsonTimestamp(999));
        writer.WriteValue(new BsonRegularExpression("abc", "i"));
        writer.WriteValue(new BsonJavaScript("code"));
        writer.WriteValue(new BsonSymbol("sym"));
        
        // Complex types
        writer.WriteValue(new BsonDocument().Set("a", 1));
        writer.WriteValue(new BsonArray(new BsonValue[] { 1, 2 }));
        writer.WriteValue(new BsonBinary(new byte[] { 1, 2, 3 }));
        writer.WriteValue(new BsonJavaScriptWithScope("code", new BsonDocument().Set("v", 1)));

        ms.Position = 0;
        using var reader = new BsonReader(ms);
        
        // Null
        await Assert.That(reader.ReadValue(BsonType.Null)).IsEqualTo(BsonNull.Value);
        // String
        await Assert.That(((BsonString)reader.ReadValue(BsonType.String)).Value).IsEqualTo("test");
        // Int32
        await Assert.That(((BsonInt32)reader.ReadValue(BsonType.Int32)).Value).IsEqualTo(123);
        // Int64
        await Assert.That(((BsonInt64)reader.ReadValue(BsonType.Int64)).Value).IsEqualTo(456);
        // Double
        await Assert.That(((BsonDouble)reader.ReadValue(BsonType.Double)).Value).IsEqualTo(1.23);
        // Boolean
        await Assert.That(((BsonBoolean)reader.ReadValue(BsonType.Boolean)).Value).IsTrue();
        // ObjectId
        await Assert.That(((BsonObjectId)reader.ReadValue(BsonType.ObjectId)).Value).IsEqualTo(oid);
        // DateTime (precision check might be needed)
        // Bson stores milliseconds.
        var readDt = ((BsonDateTime)reader.ReadValue(BsonType.DateTime)).Value;
        // Check tolerance manually
        await Assert.That(Math.Abs((readDt - now).TotalMilliseconds)).IsLessThan(1.0);
        
        // Decimal128
        await Assert.That(((BsonDecimal128)reader.ReadValue(BsonType.Decimal128)).Value.ToDecimal()).IsEqualTo(789m);
        
        // Timestamp
        await Assert.That(((BsonTimestamp)reader.ReadValue(BsonType.Timestamp)).Value).IsEqualTo(999);
        
        // Regex
        var regex = (BsonRegularExpression)reader.ReadValue(BsonType.RegularExpression);
        await Assert.That(regex.Pattern).IsEqualTo("abc");
        await Assert.That(regex.Options).IsEqualTo("i");
        
        // JS
        await Assert.That(((BsonJavaScript)reader.ReadValue(BsonType.JavaScript)).Code).IsEqualTo("code");
        
        // Symbol
        await Assert.That(((BsonSymbol)reader.ReadValue(BsonType.Symbol)).Name).IsEqualTo("sym");
        
        // Document
        var doc = (BsonDocument)reader.ReadValue(BsonType.Document);
        await Assert.That(((BsonInt32)doc["a"]).Value).IsEqualTo(1);
        
        // Array
        var arr = (BsonArray)reader.ReadValue(BsonType.Array);
        await Assert.That(arr.Count).IsEqualTo(2);
        
        // Binary
        var bin = (BsonBinary)reader.ReadValue(BsonType.Binary);
        await Assert.That(bin.Bytes.SequenceEqual(new byte[] { 1, 2, 3 })).IsTrue();
        
        // JSWithScope
        var jsScope = (BsonJavaScriptWithScope)reader.ReadValue(BsonType.JavaScriptWithScope);
        await Assert.That(jsScope.Code).IsEqualTo("code");
        await Assert.That(((BsonInt32)jsScope.Scope["v"]).Value).IsEqualTo(1);
    }

    [Test]
    public async Task ReadDocument_WithFields_ShouldSkipUnwanted()
    {
        using var ms = new MemoryStream();
        using var writer = new BsonWriter(ms, true);
        
        var doc = new BsonDocument()
            .Set("keep1", "v1")
            .Set("skip1", new BsonDocument().Set("sub", "val")) // Complex skip
            .Set("skip2", new BsonArray(new BsonValue[] { 1, 2, 3 })) // Array skip
            .Set("keep2", 123)
            .Set("skip3", "v3");
            
        writer.WriteDocument(doc);
        
        ms.Position = 0;
        using var reader = new BsonReader(ms);
        
        var fields = new HashSet<string> { "keep1", "keep2" };
        var result = reader.ReadDocument(fields);
        
        await Assert.That(result.Count).IsEqualTo(2);
        await Assert.That(result.ContainsKey("keep1")).IsTrue();
        await Assert.That(result.ContainsKey("keep2")).IsTrue();
        await Assert.That(result.ContainsKey("skip1")).IsFalse();
    }
    
    [Test]
    public async Task ReadString_MissingNullTerminator_ShouldThrow()
    {
        using var ms = new MemoryStream();
        using var binaryWriter = new BinaryWriter(ms);
        
        // Length 5 (including null)
        binaryWriter.Write(5);
        // "test" (4 bytes)
        binaryWriter.Write(System.Text.Encoding.UTF8.GetBytes("test"));
        // NO null terminator (write something else or nothing)
        binaryWriter.Write((byte)1); 
        
        ms.Position = 0;
        using var reader = new BsonReader(ms);
        await Assert.That(() => reader.ReadString()).Throws<InvalidOperationException>();
    }

    [Test]
    public async Task ReadValue_UnsupportedType_ShouldThrow()
    {
        using var ms = new MemoryStream();
        using var reader = new BsonReader(ms);
        await Assert.That(() => reader.ReadValue((BsonType)250)).Throws<NotSupportedException>();
    }
    
    [Test]
    public async Task SkipValue_AllTypes_ShouldWork()
    {
        // This effectively tests SkipValue via ReadDocument with filtering
        using var ms = new MemoryStream();
        using var writer = new BsonWriter(ms, true);
        
        var doc = new BsonDocument()
            .Set("a", 1)
            .Set("b", 2.0)
            .Set("c", "s")
            .Set("d", new BsonDocument())
            .Set("e", new BsonArray())
            .Set("f", new BsonBinary(new byte[1]))
            .Set("g", ObjectId.NewObjectId())
            .Set("h", true)
            .Set("i", DateTime.UtcNow)
            .Set("j", BsonNull.Value)
            .Set("k", new BsonRegularExpression("p", "o"))
            .Set("l", new BsonJavaScript("c"))
            .Set("m", new BsonSymbol("s"))
            .Set("n", new BsonJavaScriptWithScope("c", new BsonDocument()))
            .Set("o", new BsonInt64(1))
            .Set("p", new BsonDecimal128(new Decimal128(1)));
            
        writer.WriteDocument(doc);
        
        ms.Position = 0;
        using var reader = new BsonReader(ms);
        
        // Read only "a" and "p", skipping everything in between
        var result = reader.ReadDocument(new HashSet<string> { "a", "p" });
        await Assert.That(result.Count).IsEqualTo(2);
        await Assert.That(result.ContainsKey("a")).IsTrue();
        await Assert.That(result.ContainsKey("p")).IsTrue();
    }
}
