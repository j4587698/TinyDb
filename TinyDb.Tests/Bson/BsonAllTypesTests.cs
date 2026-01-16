using TinyDb.Bson;
using TinyDb.Serialization;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Bson;

public class BsonAllTypesTests
{
    [Test]
    public async Task BsonWriter_Reader_Should_Handle_All_Types()
    {
        var doc = new BsonDocument();
        doc = doc.Set("null", BsonNull.Value);
        doc = doc.Set("string", new BsonString("test"));
        doc = doc.Set("int32", new BsonInt32(int.MaxValue));
        doc = doc.Set("int64", new BsonInt64(long.MaxValue));
        doc = doc.Set("double", new BsonDouble(1.23));
        doc = doc.Set("bool_t", new BsonBoolean(true));
        doc = doc.Set("bool_f", new BsonBoolean(false));
        doc = doc.Set("oid", new BsonObjectId(ObjectId.NewObjectId()));
        doc = doc.Set("date", new BsonDateTime(DateTime.UtcNow));
        doc = doc.Set("dec128", new BsonDecimal128(123.456m));
        doc = doc.Set("binary", new BsonBinary(new byte[] { 1, 2 }));
        doc = doc.Set("regex", new BsonRegularExpression("abc", "i"));
        doc = doc.Set("ts", new BsonTimestamp(12345));
        
        var bytes = BsonSerializer.SerializeDocument(doc);
        var readDoc = BsonSerializer.DeserializeDocument(bytes);
        
        await Assert.That(readDoc["null"].IsNull).IsTrue();
        await Assert.That(readDoc["string"].ToString()).IsEqualTo("test");
        await Assert.That(readDoc["int32"].As<int>()).IsEqualTo(int.MaxValue);
        await Assert.That(readDoc["int64"].As<long>()).IsEqualTo(long.MaxValue);
        await Assert.That(readDoc["double"].As<double>()).IsEqualTo(1.23);
        await Assert.That(readDoc["bool_t"].As<bool>()).IsTrue();
        await Assert.That(readDoc["bool_f"].As<bool>()).IsFalse();
        await Assert.That(readDoc["dec128"].As<decimal>()).IsEqualTo(123.456m);
        await Assert.That(readDoc["binary"].As<byte[]>().Length).IsEqualTo(2);
        await Assert.That(((BsonRegularExpression)readDoc["regex"]).Pattern).IsEqualTo("abc");
        await Assert.That(((BsonTimestamp)readDoc["ts"]).Value).IsEqualTo(12345);
    }
    
    [Test]
    public async Task BsonArray_Should_Handle_Nested_Types()
    {
        var arr = new BsonArray();
        arr = arr.AddValue(1).AddValue("s");
        var bytes = BsonSerializer.SerializeArray(arr);
        var readArr = BsonSerializer.DeserializeArray(bytes);
        await Assert.That(readArr.Count).IsEqualTo(2);
        await Assert.That(readArr[0].As<int>()).IsEqualTo(1);
        await Assert.That(readArr[1].ToString()).IsEqualTo("s");
    }
}
