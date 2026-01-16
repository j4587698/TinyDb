using TinyDb.Serialization;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Serialization;

public class AotMapperTests
{
    [Test]
    public async Task ConvertPrimitiveValue_Should_Handle_Conversions()
    {
        // String -> *
        var strVal = new BsonString("123");
        await Assert.That(BsonMapper.ConvertFromBsonValue(strVal, typeof(int))).IsEqualTo(123);
        await Assert.That(BsonMapper.ConvertFromBsonValue(strVal, typeof(long))).IsEqualTo(123L);
        await Assert.That(BsonMapper.ConvertFromBsonValue(strVal, typeof(double))).IsEqualTo(123.0);
        await Assert.That(BsonMapper.ConvertFromBsonValue(strVal, typeof(decimal))).IsEqualTo(123m);
        
        var boolStr = new BsonString("true");
        await Assert.That(BsonMapper.ConvertFromBsonValue(boolStr, typeof(bool))).IsEqualTo(true);
        
        var guidStr = new BsonString(Guid.Empty.ToString());
        await Assert.That(BsonMapper.ConvertFromBsonValue(guidStr, typeof(Guid))).IsEqualTo(Guid.Empty);

        // Int32 -> *
        var intVal = new BsonInt32(123);
        await Assert.That(BsonMapper.ConvertFromBsonValue(intVal, typeof(string))).IsEqualTo("123");
        await Assert.That(BsonMapper.ConvertFromBsonValue(intVal, typeof(long))).IsEqualTo(123L);
        await Assert.That(BsonMapper.ConvertFromBsonValue(intVal, typeof(double))).IsEqualTo(123.0);
        await Assert.That(BsonMapper.ConvertFromBsonValue(intVal, typeof(decimal))).IsEqualTo(123m); // Falls to ToString
        await Assert.That(BsonMapper.ConvertFromBsonValue(intVal, typeof(MyEnum))).IsEqualTo(MyEnum.B);

        // Double -> *
        var dblVal = new BsonDouble(123.45);
        await Assert.That(BsonMapper.ConvertFromBsonValue(dblVal, typeof(int))).IsEqualTo(123);
        await Assert.That(BsonMapper.ConvertFromBsonValue(dblVal, typeof(long))).IsEqualTo(123L);
        await Assert.That(BsonMapper.ConvertFromBsonValue(dblVal, typeof(decimal))).IsEqualTo(123.45m);
        await Assert.That(BsonMapper.ConvertFromBsonValue(dblVal, typeof(float))).IsEqualTo(123.45f);

        // Binary -> Guid/Byte[]
        var bytes = new byte[] { 1, 2, 3 };
        var binVal = new BsonBinary(bytes);
        await Assert.That(BsonMapper.ConvertFromBsonValue(binVal, typeof(byte[]))).IsEqualTo(bytes);
        
        var guid = Guid.NewGuid();
        var guidBin = new BsonBinary(guid);
        await Assert.That(BsonMapper.ConvertFromBsonValue(guidBin, typeof(Guid))).IsEqualTo(guid);

        // Byte, Short, SByte, Float
        var smallInt = new BsonInt32(10);
        await Assert.That(BsonMapper.ConvertFromBsonValue(smallInt, typeof(byte))).IsEqualTo((byte)10);
        await Assert.That(BsonMapper.ConvertFromBsonValue(smallInt, typeof(short))).IsEqualTo((short)10);
        await Assert.That(BsonMapper.ConvertFromBsonValue(smallInt, typeof(sbyte))).IsEqualTo((sbyte)10);
        await Assert.That(BsonMapper.ConvertFromBsonValue(smallInt, typeof(float))).IsEqualTo(10.0f);

        // UInt32, UInt64, UInt16
        await Assert.That(BsonMapper.ConvertFromBsonValue(smallInt, typeof(uint))).IsEqualTo(10u);
        await Assert.That(BsonMapper.ConvertFromBsonValue(smallInt, typeof(ulong))).IsEqualTo(10ul);
        await Assert.That(BsonMapper.ConvertFromBsonValue(smallInt, typeof(ushort))).IsEqualTo((ushort)10);
    }

    [Test]
    public async Task GetPropertyValue_Should_Return_Value_Via_Reflection_Fallback()
    {
        var entity = new AotMapperTests.MyUnregisteredEntity { Name = "Test", Value = 123 };
        
        var name = AotBsonMapper.GetPropertyValue(entity, "Name");
        var val = AotBsonMapper.GetPropertyValue(entity, "Value");

        await Assert.That(name).IsEqualTo("Test");
        await Assert.That(val).IsEqualTo(123);
    }

    public class MyUnregisteredEntity
    {
        public string Name { get; set; } = "";
        public int Value { get; set; }
    }

    public enum MyEnum { A = 0, B = 123 }
}
