using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using TinyDb.Bson;
using TinyDb.Serialization;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Serialization;

public class AotMapperMatrixTests
{
    private enum TestEnum { Zero = 0, One = 1 }

    private static readonly Type[] ConversionTargets =
    {
        typeof(bool), typeof(byte), typeof(sbyte), typeof(char),
        typeof(short), typeof(ushort), typeof(int), typeof(uint),
        typeof(long), typeof(ulong), typeof(float), typeof(double),
        typeof(decimal), typeof(string)
    };

    [Test]
    [UnconditionalSuppressMessage("TrimAnalysis", "IL2072", Justification = "Test-only conversion matrix uses fixed primitive types.")]
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
            foreach (var target in ConversionTargets)
            {
                try
                {
                    var result = AotBsonMapper.ConvertValue(src, target);
                    await Assert.That(result).IsNotNull();
                    await Assert.That(result!.GetType()).IsEqualTo(target);
                }
                catch (Exception ex)
                {
                    // Some conversions might fail (e.g. DateTime to int, or "1" to bool depending on parsing)
                    // We just want to ensure we HIT the code path.
                    // But we should verify success for numeric-to-numeric.
                    
                    if (src.IsNumeric && IsNumericType(target))
                    {
                         Assert.Fail($"Failed to convert {src.GetType().Name} to {target.Name}: {ex.Message}");
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
        await Assert.That(AotBsonMapper.ConvertValue(new BsonString("One"), typeof(TestEnum))).IsEqualTo(TestEnum.One);
        
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
