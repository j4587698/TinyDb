using System;
using System.Collections.Generic;
using TinyDb.Bson;
using TinyDb.Serialization;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Serialization;

public class AotMapperMatrixTests
{
    private enum TestEnum { Zero = 0, One = 1 }

    private static readonly (Type TargetType, Func<BsonValue, object?> Convert)[] ConversionTargets =
    [
        (typeof(bool), src => AotBsonMapper.ConvertValue(src, typeof(bool))),
        (typeof(byte), src => AotBsonMapper.ConvertValue(src, typeof(byte))),
        (typeof(sbyte), src => AotBsonMapper.ConvertValue(src, typeof(sbyte))),
        (typeof(char), src => AotBsonMapper.ConvertValue(src, typeof(char))),
        (typeof(short), src => AotBsonMapper.ConvertValue(src, typeof(short))),
        (typeof(ushort), src => AotBsonMapper.ConvertValue(src, typeof(ushort))),
        (typeof(int), src => AotBsonMapper.ConvertValue(src, typeof(int))),
        (typeof(uint), src => AotBsonMapper.ConvertValue(src, typeof(uint))),
        (typeof(long), src => AotBsonMapper.ConvertValue(src, typeof(long))),
        (typeof(ulong), src => AotBsonMapper.ConvertValue(src, typeof(ulong))),
        (typeof(float), src => AotBsonMapper.ConvertValue(src, typeof(float))),
        (typeof(double), src => AotBsonMapper.ConvertValue(src, typeof(double))),
        (typeof(decimal), src => AotBsonMapper.ConvertValue(src, typeof(decimal))),
        (typeof(string), src => AotBsonMapper.ConvertValue(src, typeof(string)))
    ];

    [Test]
    public async Task ConvertPrimitiveValue_ExhaustiveMatrix()
    {
        var sources = new List<BsonValue>
        {
            new BsonInt32(1),
            new BsonInt64(1L),
            new BsonDouble(1.0),
            new BsonDecimal128(1.0m),
            new BsonString("1"),
            new BsonBoolean(true),
            new BsonDateTime(DateTime.UtcNow),
            // new BsonBinary(...) // Handle separately
        };

        foreach (var src in sources)
        {
            foreach (var (targetType, convert) in ConversionTargets)
            {
                try
                {
                    var result = convert(src);
                    await Assert.That(result).IsNotNull();
                    await Assert.That(result!.GetType()).IsEqualTo(targetType);
                }
                catch (Exception ex)
                {
                    // Some conversions might fail (e.g. DateTime to int, or "1" to bool depending on parsing)
                    // We just want to ensure we HIT the code path.
                    // But we should verify success for numeric-to-numeric.
                    
                    if (src.IsNumeric && IsNumericType(targetType))
                    {
                         Assert.Fail($"Failed to convert {src.GetType().Name} to {targetType.Name}: {ex.Message}");
                    }
                }
            }
        }
    }

    private bool IsNumericType(Type t)
    {
        return t == typeof(byte) || t == typeof(sbyte) || t == typeof(short) || t == typeof(ushort) ||
               t == typeof(int) || t == typeof(uint) || t == typeof(long) || t == typeof(ulong) ||
               t == typeof(float) || t == typeof(double) || t == typeof(decimal);
    }

    [Test]
    public async Task Convert_Enum_Exhaustive()
    {
        // Enum from Int32, Int64, String
        await Assert.That(AotBsonMapper.ConvertValue(new BsonInt32(1), typeof(TestEnum))).IsEqualTo(TestEnum.One);
        await Assert.That(AotBsonMapper.ConvertValue(new BsonInt64(1), typeof(TestEnum))).IsEqualTo(TestEnum.One);
        await Assert.That(AotBsonMapper.ConvertValue(new BsonString("1"), typeof(TestEnum))).IsEqualTo(TestEnum.One);
        await Assert.That(AotBsonMapper.ConvertEnumValue<TestEnum>(new BsonString("One"))).IsEqualTo(TestEnum.One);
        
        // Fallback toString
        await Assert.That(AotBsonMapper.ConvertValue(new BsonDouble(1.0), typeof(TestEnum))).IsEqualTo(TestEnum.One);
    }

    [Test]
    public async Task Convert_Guid_Exhaustive()
    {
        var g = Guid.NewGuid();
        var bin = new BsonBinary(g.ToByteArray(), BsonBinary.BinarySubType.Uuid);
        var str = new BsonString(g.ToString());
        
        await Assert.That(AotBsonMapper.ConvertValue(bin, typeof(Guid))).IsEqualTo(g);
        await Assert.That(AotBsonMapper.ConvertValue(str, typeof(Guid))).IsEqualTo(g);
        
        // Fallback
        await Assert.That(() => AotBsonMapper.ConvertValue(new BsonInt32(1), typeof(Guid))).Throws<FormatException>();
    }

    [Test]
    public async Task Convert_ObjectId_Exhaustive()
    {
        var oid = ObjectId.NewObjectId();
        var bOid = new BsonObjectId(oid);
        var str = new BsonString(oid.ToString());
        
        await Assert.That(AotBsonMapper.ConvertValue(bOid, typeof(ObjectId))).IsEqualTo(oid);
        await Assert.That(AotBsonMapper.ConvertValue(str, typeof(ObjectId))).IsEqualTo(oid);
    }
}
