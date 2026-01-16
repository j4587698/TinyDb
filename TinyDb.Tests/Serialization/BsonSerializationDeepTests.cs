using TinyDb.Bson;
using TinyDb.Serialization;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Serialization;

public class BsonSerializationDeepTests
{
    [Test]
    public async Task BsonWriter_Reader_Raw_Methods_Should_Work()
    {
        var oid = ObjectId.NewObjectId();
        var now = DateTime.UtcNow;
        // BSON DateTime precision is ms
        now = new DateTime(now.Year, now.Month, now.Day, now.Hour, now.Minute, now.Second, now.Millisecond, DateTimeKind.Utc);

        byte[] bytes;
        using (var ms = new MemoryStream())
        {
            using (var writer = new BsonWriter(ms))
            {
                writer.WriteInt32(123);
                writer.WriteInt64(456L);
                writer.WriteDouble(1.23);
                writer.WriteBoolean(true);
                writer.WriteString("hello");
                writer.WriteCString("cstring");
                writer.WriteNull();
                writer.WriteObjectId(oid);
                writer.WriteDateTime(now);
                writer.WriteTimestamp(789L);
                writer.WriteBinary(new byte[] { 1 });
                writer.WriteRegularExpression("p", "o");
            }
            bytes = ms.ToArray();
        }
        
        using (var reader = new BsonReader(new MemoryStream(bytes)))
        {
            await Assert.That(reader.ReadInt32().Value).IsEqualTo(123);
            await Assert.That(reader.ReadInt64().Value).IsEqualTo(456L);
            await Assert.That(reader.ReadDouble().Value).IsEqualTo(1.23);
            await Assert.That(reader.ReadBoolean().Value).IsTrue();
            await Assert.That(reader.ReadString().Value).IsEqualTo("hello");
            await Assert.That(reader.ReadCString()).IsEqualTo("cstring");
            await Assert.That(reader.ReadValue(BsonType.Null)).IsEqualTo(BsonNull.Value);
            await Assert.That(reader.ReadObjectId().Value).IsEqualTo(oid);
            await Assert.That(reader.ReadDateTime().Value).IsEqualTo(now);
            await Assert.That(reader.ReadTimestamp().Value).IsEqualTo(789L);
            await Assert.That(reader.ReadBinary().Bytes.Length).IsEqualTo(1);
            await Assert.That(reader.ReadRegularExpression().Pattern).IsEqualTo("p");
        }
    }
}
