using System;
using System.Globalization;
using TinyDb.Bson;
using TinyDb.Serialization;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Serialization;

public class AotBsonMapperExtendedTests
{
    private enum TestEnum { A, B }

    [Test]
    public async Task ConvertPrimitiveValue_NumericConversions()
    {
        BsonValue i32 = 100;
        BsonValue i64 = 200L;
        BsonValue dbl = 300.5;
        BsonValue str = "400";

        // short
        await Assert.That(AotBsonMapper.ConvertValue(i32, typeof(short))).IsEqualTo((short)100);
        await Assert.That(AotBsonMapper.ConvertValue(i64, typeof(short))).IsEqualTo((short)200);
        await Assert.That(AotBsonMapper.ConvertValue(dbl, typeof(short))).IsEqualTo((short)300);
        await Assert.That(AotBsonMapper.ConvertValue(str, typeof(short))).IsEqualTo((short)400);

        // byte
        await Assert.That(AotBsonMapper.ConvertValue(i32, typeof(byte))).IsEqualTo((byte)100);
        
        // sbyte
        await Assert.That(AotBsonMapper.ConvertValue(i32, typeof(sbyte))).IsEqualTo((sbyte)100);
        
        // uint
        await Assert.That(AotBsonMapper.ConvertValue(i32, typeof(uint))).IsEqualTo(100u);
        
        // ulong
        await Assert.That(AotBsonMapper.ConvertValue(i64, typeof(ulong))).IsEqualTo(200ul);
        
        // ushort
        await Assert.That(AotBsonMapper.ConvertValue(i32, typeof(ushort))).IsEqualTo((ushort)100);
        
        // float
        await Assert.That(AotBsonMapper.ConvertValue(dbl, typeof(float))).IsEqualTo(300.5f);
    }

    [Test]
    public async Task ConvertPrimitiveValue_Enum_ShouldWork()
    {
        await Assert.That(AotBsonMapper.ConvertValue((BsonValue)"A", typeof(TestEnum))).IsEqualTo(TestEnum.A);
        await Assert.That(AotBsonMapper.ConvertValue((BsonValue)1, typeof(TestEnum))).IsEqualTo(TestEnum.B);
        await Assert.That(AotBsonMapper.ConvertValue((BsonValue)0L, typeof(TestEnum))).IsEqualTo(TestEnum.A);
    }

    [Test]
    public async Task ConvertPrimitiveValue_Guid_ShouldWork()
    {
        var guid = Guid.NewGuid();
        var bin = new BsonBinary(guid.ToByteArray(), BsonBinary.BinarySubType.Uuid);
        
        await Assert.That(AotBsonMapper.ConvertValue(bin, typeof(Guid))).IsEqualTo(guid);
        await Assert.That(AotBsonMapper.ConvertValue((BsonValue)guid.ToString(), typeof(Guid))).IsEqualTo(guid);
    }

    [Test]
    public async Task ConvertPrimitiveValue_ByteArray_ShouldWork()
    {
        byte[] data = { 1, 2, 3 };
        var bin = new BsonBinary(data);
        var base64 = Convert.ToBase64String(data);
        
        await Assert.That(AotBsonMapper.ConvertValue(bin, typeof(byte[]))).IsEquivalentTo(data);
        await Assert.That(AotBsonMapper.ConvertValue((BsonValue)base64, typeof(byte[]))).IsEquivalentTo(data);
    }

    [Test]
    public async Task ConvertPrimitiveValue_DateTime_ShouldWork()
    {
        var now = DateTime.UtcNow;
        // BsonDateTime stores ms precision
        var nowMs = new DateTime(now.Year, now.Month, now.Day, now.Hour, now.Minute, now.Second, now.Millisecond, DateTimeKind.Utc);
        
        await Assert.That(AotBsonMapper.ConvertValue(new BsonDateTime(nowMs), typeof(DateTime))).IsEqualTo(nowMs);
        await Assert.That(AotBsonMapper.ConvertValue((BsonValue)nowMs.ToString("o"), typeof(DateTime))).IsEqualTo(nowMs);
    }

    [Test]
    public async Task ConvertPrimitiveValue_Checked_Overflow_ShouldThrow()
    {
        BsonValue big = long.MaxValue;
        await Assert.That(() => AotBsonMapper.ConvertValue(big, typeof(int))).Throws<OverflowException>();
    }
}
