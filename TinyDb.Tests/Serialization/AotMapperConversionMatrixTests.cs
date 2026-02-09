using TinyDb.Serialization;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Serialization;

public class AotMapperConversionMatrixTests
{
    [Test]
    public async Task Convert_From_BsonString_To_All_Types()
    {
        var val = new BsonString("123");
        await Assert.That(AotBsonMapper.ConvertValue(val, typeof(string))).IsEqualTo("123");
        await Assert.That(AotBsonMapper.ConvertValue(val, typeof(int))).IsEqualTo(123);
        await Assert.That(AotBsonMapper.ConvertValue(val, typeof(long))).IsEqualTo(123L);
        await Assert.That(AotBsonMapper.ConvertValue(val, typeof(double))).IsEqualTo(123.0);
        await Assert.That(AotBsonMapper.ConvertValue(val, typeof(decimal))).IsEqualTo(123m);
        
        var guidStr = Guid.NewGuid().ToString();
        await Assert.That(AotBsonMapper.ConvertValue(new BsonString(guidStr), typeof(Guid))).IsEqualTo(Guid.Parse(guidStr));
        
        var oidStr = ObjectId.NewObjectId().ToString();
        await Assert.That(AotBsonMapper.ConvertValue(new BsonString(oidStr), typeof(ObjectId))).IsEqualTo(new ObjectId(oidStr));
        
        var dateStr = DateTime.UtcNow.ToString("o");
        var date = DateTime.Parse(dateStr, null, System.Globalization.DateTimeStyles.RoundtripKind);
        await Assert.That(AotBsonMapper.ConvertValue(new BsonString(dateStr), typeof(DateTime))).IsEqualTo(date);
        
        var enumStr = new BsonString("A");
        await Assert.That(AotBsonMapper.ConvertEnumValue<TestEnum>(enumStr)).IsEqualTo(TestEnum.A);
        
        var boolStr = new BsonString("true");
        await Assert.That(AotBsonMapper.ConvertValue(boolStr, typeof(bool))).IsEqualTo(true);
        
        var byteArrStr = Convert.ToBase64String(new byte[] { 1, 2, 3 });
        var bytes = (byte[])AotBsonMapper.ConvertValue(new BsonString(byteArrStr), typeof(byte[]))!;
        await Assert.That(bytes.Length).IsEqualTo(3);
        
        // SByte/Byte/Short/UShort/UInt/ULong/Float
        await Assert.That(AotBsonMapper.ConvertValue(val, typeof(byte))).IsEqualTo((byte)123);
        await Assert.That(AotBsonMapper.ConvertValue(val, typeof(sbyte))).IsEqualTo((sbyte)123);
        await Assert.That(AotBsonMapper.ConvertValue(val, typeof(short))).IsEqualTo((short)123);
        await Assert.That(AotBsonMapper.ConvertValue(val, typeof(ushort))).IsEqualTo((ushort)123);
        await Assert.That(AotBsonMapper.ConvertValue(val, typeof(uint))).IsEqualTo(123u);
        await Assert.That(AotBsonMapper.ConvertValue(val, typeof(ulong))).IsEqualTo(123ul);
        await Assert.That(AotBsonMapper.ConvertValue(val, typeof(float))).IsEqualTo(123.0f);
    }

    [Test]
    public async Task Convert_From_BsonInt32_To_All_Types()
    {
        var val = new BsonInt32(1);
        await Assert.That(AotBsonMapper.ConvertValue(val, typeof(int))).IsEqualTo(1);
        await Assert.That(AotBsonMapper.ConvertValue(val, typeof(long))).IsEqualTo(1L);
        await Assert.That(AotBsonMapper.ConvertValue(val, typeof(double))).IsEqualTo(1.0);
        await Assert.That(AotBsonMapper.ConvertValue(val, typeof(decimal))).IsEqualTo(1m);
        await Assert.That(AotBsonMapper.ConvertValue(val, typeof(bool))).IsEqualTo(true);
        await Assert.That(AotBsonMapper.ConvertValue(val, typeof(string))).IsEqualTo("1");
        await Assert.That(AotBsonMapper.ConvertValue(val, typeof(TestEnum))).IsEqualTo(TestEnum.B); // B=1
        
        await Assert.That(AotBsonMapper.ConvertValue(val, typeof(byte))).IsEqualTo((byte)1);
        await Assert.That(AotBsonMapper.ConvertValue(val, typeof(sbyte))).IsEqualTo((sbyte)1);
        await Assert.That(AotBsonMapper.ConvertValue(val, typeof(short))).IsEqualTo((short)1);
        await Assert.That(AotBsonMapper.ConvertValue(val, typeof(ushort))).IsEqualTo((ushort)1);
        await Assert.That(AotBsonMapper.ConvertValue(val, typeof(uint))).IsEqualTo(1u);
        await Assert.That(AotBsonMapper.ConvertValue(val, typeof(ulong))).IsEqualTo(1ul);
        await Assert.That(AotBsonMapper.ConvertValue(val, typeof(float))).IsEqualTo(1.0f);
    }

    [Test]
    public async Task Convert_From_BsonInt64_To_All_Types()
    {
        var val = new BsonInt64(1);
        await Assert.That(AotBsonMapper.ConvertValue(val, typeof(int))).IsEqualTo(1);
        await Assert.That(AotBsonMapper.ConvertValue(val, typeof(long))).IsEqualTo(1L);
        await Assert.That(AotBsonMapper.ConvertValue(val, typeof(double))).IsEqualTo(1.0);
        await Assert.That(AotBsonMapper.ConvertValue(val, typeof(string))).IsEqualTo("1");
        await Assert.That(AotBsonMapper.ConvertValue(val, typeof(TestEnum))).IsEqualTo(TestEnum.B);
        
        // Smaller types
        await Assert.That(AotBsonMapper.ConvertValue(val, typeof(byte))).IsEqualTo((byte)1);
    }

    [Test]
    public async Task Convert_From_BsonDouble_To_All_Types()
    {
        var val = new BsonDouble(1.0);
        await Assert.That(AotBsonMapper.ConvertValue(val, typeof(int))).IsEqualTo(1);
        await Assert.That(AotBsonMapper.ConvertValue(val, typeof(long))).IsEqualTo(1L);
        await Assert.That(AotBsonMapper.ConvertValue(val, typeof(double))).IsEqualTo(1.0);
        await Assert.That(AotBsonMapper.ConvertValue(val, typeof(decimal))).IsEqualTo(1m);
        
        await Assert.That(AotBsonMapper.ConvertValue(val, typeof(byte))).IsEqualTo((byte)1);
        await Assert.That(AotBsonMapper.ConvertValue(val, typeof(float))).IsEqualTo(1.0f);
    }

    [Test]
    public async Task Convert_From_BsonDecimal128_To_All_Types()
    {
        var val = new BsonDecimal128(1.0m);
        await Assert.That(AotBsonMapper.ConvertValue(val, typeof(decimal))).IsEqualTo(1.0m);
        await Assert.That(AotBsonMapper.ConvertValue(val, typeof(double))).IsEqualTo(1.0);
    }

    [Test]
    public async Task Convert_From_BsonBinary_To_All_Types()
    {
        var guid = Guid.NewGuid();
        var val = new BsonBinary(guid, BsonBinary.BinarySubType.Uuid);
        await Assert.That(AotBsonMapper.ConvertValue(val, typeof(Guid))).IsEqualTo(guid);
        
        var bytes = new byte[] { 1, 2 };
        var val2 = new BsonBinary(bytes);
        await Assert.That(AotBsonMapper.ConvertValue(val2, typeof(byte[]))).IsEqualTo(bytes);
    }

    [Test]
    public async Task Convert_From_BsonDateTime_To_All_Types()
    {
        var now = DateTime.UtcNow;
        var val = new BsonDateTime(now);
        // Compare with tolerance or exact depending on BSON precision (ms)
        var result = (DateTime)AotBsonMapper.ConvertValue(val, typeof(DateTime))!;
        await Assert.That((result - now).TotalMilliseconds).IsLessThan(1);
    }

    public enum TestEnum
    {
        A = 0,
        B = 1
    }
}
