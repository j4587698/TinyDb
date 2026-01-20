using System;
using System.IO;
using System.Text;
using System.Collections.Generic;
using TinyDb.Bson;
using TinyDb.Serialization;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Serialization;

public class BsonSpanReaderCoverageTests
{
    [Test]
    public async Task ReadValue_AllTypes_ShouldWork()
    {
        using var ms = new MemoryStream();
        using var writer = new BsonWriter(ms, true);
        
        var oid = ObjectId.NewObjectId();
        var now = DateTime.UtcNow;
        
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
        writer.WriteValue(new BsonDocument().Set("a", 1));
        writer.WriteValue(new BsonArray(new BsonValue[] { 1, 2 }));
        writer.WriteValue(new BsonBinary(new byte[] { 1, 2, 3 }));
        writer.WriteValue(new BsonJavaScriptWithScope("code", new BsonDocument().Set("v", 1)));

        var bytes = ms.ToArray();
        
        var results = ReadAllTypesHelper(bytes);
        
        await Assert.That(results["Null"]).IsEqualTo(BsonNull.Value);
        await Assert.That(((BsonString)results["String"]).Value).IsEqualTo("test");
        await Assert.That(((BsonInt32)results["Int32"]).Value).IsEqualTo(123);
        await Assert.That(((BsonInt64)results["Int64"]).Value).IsEqualTo(456);
        await Assert.That(((BsonDouble)results["Double"]).Value).IsEqualTo(1.23);
        await Assert.That(((BsonBoolean)results["Boolean"]).Value).IsTrue();
        await Assert.That(((BsonObjectId)results["ObjectId"]).Value).IsEqualTo(oid);
        
        var readDt = ((BsonDateTime)results["DateTime"]).Value;
        await Assert.That(Math.Abs((readDt - now).TotalMilliseconds)).IsLessThan(1.0);
        
        await Assert.That(((BsonDecimal128)results["Decimal128"]).Value.ToDecimal()).IsEqualTo(789m);
        await Assert.That(((BsonTimestamp)results["Timestamp"]).Value).IsEqualTo(999);
        
        var regex = (BsonRegularExpression)results["Regex"];
        await Assert.That(regex.Pattern).IsEqualTo("abc");
        await Assert.That(regex.Options).IsEqualTo("i");
        
        await Assert.That(((BsonJavaScript)results["JavaScript"]).Code).IsEqualTo("code");
        await Assert.That(((BsonSymbol)results["Symbol"]).Name).IsEqualTo("sym");
        
        var doc = (BsonDocument)results["Document"];
        await Assert.That(((BsonInt32)doc["a"]).Value).IsEqualTo(1);
        
        var arr = (BsonArray)results["Array"];
        await Assert.That(arr.Count).IsEqualTo(2);
        
        var bin = (BsonBinary)results["Binary"];
        await Assert.That(bin.Bytes).IsEquivalentTo(new byte[] { 1, 2, 3 });
        
        var jsScope = (BsonJavaScriptWithScope)results["JavaScriptWithScope"];
        await Assert.That(jsScope.Code).IsEqualTo("code");
        await Assert.That(((BsonInt32)jsScope.Scope["v"]).Value).IsEqualTo(1);
    }

    private Dictionary<string, BsonValue> ReadAllTypesHelper(byte[] bytes)
    {
        var reader = new BsonSpanReader(bytes);
        var dict = new Dictionary<string, BsonValue>();
        
        dict["Null"] = reader.ReadValue(BsonType.Null);
        dict["String"] = reader.ReadValue(BsonType.String);
        dict["Int32"] = reader.ReadValue(BsonType.Int32);
        dict["Int64"] = reader.ReadValue(BsonType.Int64);
        dict["Double"] = reader.ReadValue(BsonType.Double);
        dict["Boolean"] = reader.ReadValue(BsonType.Boolean);
        dict["ObjectId"] = reader.ReadValue(BsonType.ObjectId);
        dict["DateTime"] = reader.ReadValue(BsonType.DateTime);
        dict["Decimal128"] = reader.ReadValue(BsonType.Decimal128);
        dict["Timestamp"] = reader.ReadValue(BsonType.Timestamp);
        dict["Regex"] = reader.ReadValue(BsonType.RegularExpression);
        dict["JavaScript"] = reader.ReadValue(BsonType.JavaScript);
        dict["Symbol"] = reader.ReadValue(BsonType.Symbol);
        dict["Document"] = reader.ReadValue(BsonType.Document);
        dict["Array"] = reader.ReadValue(BsonType.Array);
        dict["Binary"] = reader.ReadValue(BsonType.Binary);
        dict["JavaScriptWithScope"] = reader.ReadValue(BsonType.JavaScriptWithScope);
        
        return dict;
    }

    [Test]
    public async Task ReadDocument_WithFields_ShouldSkipUnwanted()
    {
        var doc = new BsonDocument()
            .Set("keep1", "v1")
            .Set("skip1", new BsonDocument().Set("sub", "val"))
            .Set("skip2", new BsonArray(new BsonValue[] { 1, 2, 3 }))
            .Set("keep2", 123)
            .Set("skip3", "v3");
            
        var bytes = BsonSerializer.SerializeDocument(doc);
        
        var fields = new HashSet<string> { "keep1", "keep2" };
        var result = ReadDocWithFieldsHelper(bytes, fields);
        
        await Assert.That(result.Count).IsEqualTo(2);
        await Assert.That(result.ContainsKey("keep1")).IsTrue();
        await Assert.That(result.ContainsKey("keep2")).IsTrue();
        await Assert.That(result.ContainsKey("skip1")).IsFalse();
    }
    
    private BsonDocument ReadDocWithFieldsHelper(byte[] bytes, HashSet<string> fields)
    {
        var reader = new BsonSpanReader(bytes);
        return reader.ReadDocument(fields);
    }
    
    [Test]
    public async Task Error_Handling_ShouldThrow()
    {
        // 1. Truncated Data
        var bytes = new byte[4]; 
        await Assert.That(() => ReadDocumentHelper(bytes)).Throws<EndOfStreamException>();
        
        // 2. Invalid Document Size
        using var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms);
        writer.Write(10); 
        writer.Write((byte)BsonType.End);
        bytes = ms.ToArray(); 
        await Assert.That(() => ReadDocumentHelper(bytes)).Throws<InvalidOperationException>();

        // 3. String missing null
        ms.SetLength(0);
        writer.Write(5); // len
        writer.Write(Encoding.UTF8.GetBytes("test")); // 4 bytes
        bytes = ms.ToArray();
        await Assert.That(() => ReadStringHelper(bytes)).Throws<EndOfStreamException>();
        
        // 4. Unsupported Type
        bytes = new byte[10];
        await Assert.That(() => ReadValueHelper(bytes, (BsonType)250)).Throws<NotSupportedException>();
    }

    private void ReadDocumentHelper(byte[] bytes)
    {
        var reader = new BsonSpanReader(bytes);
        reader.ReadDocument();
    }

    private void ReadStringHelper(byte[] bytes)
    {
        var reader = new BsonSpanReader(bytes);
        reader.ReadString();
    }

    private void ReadValueHelper(byte[] bytes, BsonType type)
    {
        var reader = new BsonSpanReader(bytes);
        reader.ReadValue(type);
    }
}
