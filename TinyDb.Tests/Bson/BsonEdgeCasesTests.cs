using System;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Bson;

public class BsonEdgeCasesTests
{
    [Test]
    public async Task BsonMinKey_MaxKey_Comparisons()
    {
        var min = BsonMinKey.Value;
        var max = BsonMaxKey.Value;
        var nullVal = BsonNull.Value;
        var intVal = new BsonInt32(1);

        await Assert.That(min.CompareTo(min)).IsEqualTo(0);
        await Assert.That(max.CompareTo(max)).IsEqualTo(0);
        
        await Assert.That(min.CompareTo(max)).IsLessThan(0);
        await Assert.That(max.CompareTo(min)).IsGreaterThan(0);

        await Assert.That(min.CompareTo(intVal)).IsLessThan(0);
        await Assert.That(max.CompareTo(intVal)).IsGreaterThan(0);
        
        await Assert.That(min.CompareTo(nullVal)).IsLessThan(0);
    }

    [Test]
    public async Task BsonMinKey_IConvertible_Coverage()
    {
        var min = BsonMinKey.Value;
        
        await Assert.That(min.ToBoolean(null)).IsFalse();
        await Assert.That(min.ToInt32(null)).IsEqualTo(0);
        await Assert.That(min.ToDouble(null)).IsEqualTo(0.0);
        await Assert.That(min.ToString(null)).IsEqualTo("$minKey");
        await Assert.That(min.GetTypeCode()).IsEqualTo(TypeCode.Object);
        await Assert.That(() => min.ToType(typeof(int), null)).Throws<InvalidCastException>();
        
        // Cover remaining methods
        await Assert.That(min.ToByte(null)).IsEqualTo((byte)0);
        await Assert.That(min.ToChar(null)).IsEqualTo('\0');
        await Assert.That(min.ToDateTime(null)).IsEqualTo(DateTime.MinValue);
        await Assert.That(min.ToDecimal(null)).IsEqualTo(0m);
        await Assert.That(min.ToInt16(null)).IsEqualTo((short)0);
        await Assert.That(min.ToInt64(null)).IsEqualTo(0L);
        await Assert.That(min.ToSByte(null)).IsEqualTo((sbyte)0);
        await Assert.That(min.ToSingle(null)).IsEqualTo(0.0f);
        await Assert.That(min.ToUInt16(null)).IsEqualTo((ushort)0);
        await Assert.That(min.ToUInt32(null)).IsEqualTo(0u);
        await Assert.That(min.ToUInt64(null)).IsEqualTo(0ul);
    }

    [Test]
    public async Task BsonMaxKey_IConvertible_Coverage()
    {
        var max = BsonMaxKey.Value;
        
        await Assert.That(max.ToBoolean(null)).IsTrue(); // MaxKey is truthy
        await Assert.That(max.ToInt32(null)).IsEqualTo(int.MaxValue);
        await Assert.That(max.ToString(null)).IsEqualTo("$maxKey");
        
        // Similar to MinKey, cover rest
        await Assert.That(max.ToByte(null)).IsEqualTo((byte)255);
        await Assert.That(max.ToChar(null)).IsEqualTo(char.MaxValue);
        await Assert.That(max.ToDateTime(null)).IsEqualTo(DateTime.MaxValue); 
    }

    [Test]
    public async Task BsonBoolean_Constants_And_Conversion()
    {
        var t = new BsonBoolean(true);
        var f = new BsonBoolean(false);

        await Assert.That(t.Value).IsTrue();
        await Assert.That(f.Value).IsFalse();
        
        await Assert.That(t).IsEqualTo(new BsonBoolean(true));
        await Assert.That(f).IsEqualTo(new BsonBoolean(false));
        
        await Assert.That(t.CompareTo(f)).IsGreaterThan(0);
        
        // IConvertible
        await Assert.That(t.ToBoolean(null)).IsTrue();
        await Assert.That(t.ToString()).IsEqualTo("true"); // lowercase per implementation
    }

    [Test]
    public async Task BsonDouble_SpecialValues()
    {
        var inf = new BsonDouble(double.PositiveInfinity);
        var nan = new BsonDouble(double.NaN);
        var zero = new BsonDouble(0.0);

        await Assert.That(inf.Value).IsEqualTo(double.PositiveInfinity);
        await Assert.That(nan.Value).IsNaN();
        
        // NaN comparisons
        await Assert.That(nan.CompareTo(inf)).IsLessThan(0); // NaN is usually considered smaller in some sorts, or check impl
        // C# double.NaN.CompareTo(Inf) returns -1.
    }

    [Test]
    public async Task BsonObjectId_EdgeCases()
    {
        var oid = ObjectId.NewObjectId();
        var same = new BsonObjectId(oid);
        var other = new BsonObjectId(ObjectId.NewObjectId());

        await Assert.That(same).IsEqualTo(new BsonObjectId(oid));
        await Assert.That(same.GetHashCode()).IsEqualTo(new BsonObjectId(oid).GetHashCode());
        
        await Assert.That(same.ToString()).IsEqualTo(oid.ToString());
    }
}
