using TinyDb.Bson;
using TinyDb.Tests.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using System.Globalization;
using System.Runtime.CompilerServices;

namespace TinyDb.Tests.Bson;

public class BsonExhaustiveIConvertibleTests
{
    private static readonly IFormatProvider Provider = CultureInfo.InvariantCulture;

    [Test]
    public async Task BsonObjectId_Exhaustive()
    {
        var oid = ObjectId.NewObjectId();
        var bson = new BsonObjectId(oid);
        var s = oid.ToString();

        await Assert.That(() => bson.ToBoolean(Provider)).Throws<FormatException>();
        await Assert.That(() => bson.ToByte(Provider)).Throws<FormatException>();
        await Assert.That(() => bson.ToChar(Provider)).Throws<FormatException>();
        await Assert.That(() => bson.ToDateTime(Provider)).Throws<InvalidCastException>();
        await Assert.That(() => bson.ToDecimal(Provider)).Throws<FormatException>();
        await Assert.That(() => bson.ToDouble(Provider)).Throws<FormatException>();
        await Assert.That(() => bson.ToInt16(Provider)).Throws<FormatException>();
        await Assert.That(() => bson.ToInt32(Provider)).Throws<FormatException>();
        await Assert.That(() => bson.ToInt64(Provider)).Throws<FormatException>();
        await Assert.That(() => bson.ToSByte(Provider)).Throws<FormatException>();
        await Assert.That(() => bson.ToSingle(Provider)).Throws<FormatException>();
        await Assert.That(() => bson.ToUInt16(Provider)).Throws<FormatException>();
        await Assert.That(() => bson.ToUInt32(Provider)).Throws<FormatException>();
        await Assert.That(() => bson.ToUInt64(Provider)).Throws<FormatException>();
        
        await Assert.That(bson.ToType(typeof(ObjectId), Provider)).IsEqualTo(oid);
        await Assert.That(bson.ToType(typeof(BsonObjectId), Provider)).IsEqualTo(bson);
        await Assert.That(bson.ToType(typeof(object), Provider)).IsEqualTo(bson);
        await Assert.That(bson.ToType(typeof(string), Provider)).IsEqualTo(s);

        // Implicits
        BsonObjectId imp1 = oid;
        await Assert.That(imp1.Value).IsEqualTo(oid);
        ObjectId imp2 = imp1;
        await Assert.That(imp2).IsEqualTo(oid);
    }

    [Test]
    public async Task BsonInt64_Exhaustive()
    {
        var val = 123L; // Use smaller value to avoid byte overflow
        var bson = new BsonInt64(val);

        await Assert.That(bson.ToBoolean(Provider)).IsTrue();
        await Assert.That(bson.ToByte(Provider)).IsEqualTo((byte)val);
        await Assert.That(bson.ToChar(Provider)).IsEqualTo((char)val);
        await Assert.That(() => bson.ToDateTime(Provider)).Throws<InvalidCastException>();
        await Assert.That(bson.ToDecimal(Provider)).IsEqualTo((decimal)val);
        await Assert.That(bson.ToDouble(Provider)).IsEqualTo((double)val);
        await Assert.That(bson.ToInt16(Provider)).IsEqualTo((short)val);
        await Assert.That(bson.ToInt32(Provider)).IsEqualTo((int)val);
        await Assert.That(bson.ToInt64(Provider)).IsEqualTo(val);
        await Assert.That(bson.ToSByte(Provider)).IsEqualTo((sbyte)val);
        await Assert.That(bson.ToSingle(Provider)).IsEqualTo((float)val);
        await Assert.That(bson.ToUInt16(Provider)).IsEqualTo((ushort)val);
        await Assert.That(bson.ToUInt32(Provider)).IsEqualTo((uint)val);
        await Assert.That(bson.ToUInt64(Provider)).IsEqualTo((ulong)val);
        
        await Assert.That(bson.ToType(typeof(long), Provider)).IsEqualTo(val);
        await Assert.That(bson.ToType(typeof(BsonInt64), Provider)).IsEqualTo(bson);
        await Assert.That(bson.ToType(typeof(object), Provider)).IsEqualTo(bson);
        await Assert.That(bson.ToType(typeof(string), Provider)).IsEqualTo(val.ToString(Provider));

        // Implicits
        BsonInt64 imp1 = val;
        await Assert.That(imp1.Value).IsEqualTo(val);
        long imp2 = imp1;
        await Assert.That(imp2).IsEqualTo(val);
    }

    [Test]
    public async Task BsonDouble_Exhaustive()
    {
        var val = 123.0; // Use whole number to avoid char issues or just expect exception
        var bson = new BsonDouble(val);

        await Assert.That(bson.ToBoolean(Provider)).IsTrue();
        await Assert.That(bson.ToByte(Provider)).IsEqualTo((byte)val);
        await Assert.That(() => bson.ToChar(Provider)).Throws<InvalidCastException>();
        await Assert.That(() => bson.ToDateTime(Provider)).Throws<InvalidCastException>();
        await Assert.That(bson.ToDecimal(Provider)).IsEqualTo((decimal)val);
        await Assert.That(bson.ToDouble(Provider)).IsEqualTo(val);
        await Assert.That(bson.ToInt16(Provider)).IsEqualTo((short)val);
        await Assert.That(bson.ToInt32(Provider)).IsEqualTo((int)val);
        await Assert.That(bson.ToInt64(Provider)).IsEqualTo((long)val);
        await Assert.That(bson.ToSByte(Provider)).IsEqualTo((sbyte)val);
        await Assert.That(bson.ToSingle(Provider)).IsEqualTo((float)val);
        await Assert.That(bson.ToUInt16(Provider)).IsEqualTo((ushort)val);
        await Assert.That(bson.ToUInt32(Provider)).IsEqualTo((uint)val);
        await Assert.That(bson.ToUInt64(Provider)).IsEqualTo((ulong)val);
        
        await Assert.That(bson.ToType(typeof(double), Provider)).IsEqualTo(val);
        await Assert.That(bson.ToType(typeof(BsonDouble), Provider)).IsEqualTo(bson);
        await Assert.That(bson.ToType(typeof(object), Provider)).IsEqualTo(bson);
        await Assert.That(bson.ToType(typeof(string), Provider)).IsEqualTo(val.ToString(Provider));

        // Implicits
        BsonDouble imp1 = val;
        await Assert.That(imp1.Value).IsEqualTo(val);
        double imp2 = imp1;
        await Assert.That(imp2).IsEqualTo(val);
    }

    [Test]
    public async Task BsonBoolean_Exhaustive()
    {
        var bson = new BsonBoolean(true);

        await Assert.That(bson.ToBoolean(Provider)).IsTrue();
        await Assert.That(bson.ToByte(Provider)).IsEqualTo((byte)1);
        await Assert.That(() => bson.ToChar(Provider)).Throws<InvalidCastException>();
        await Assert.That(() => bson.ToDateTime(Provider)).Throws<InvalidCastException>();
        await Assert.That(bson.ToDecimal(Provider)).IsEqualTo(1m);
        await Assert.That(bson.ToDouble(Provider)).IsEqualTo(1.0);
        await Assert.That(bson.ToInt16(Provider)).IsEqualTo((short)1);
        await Assert.That(bson.ToInt32(Provider)).IsEqualTo(1);
        await Assert.That(bson.ToInt64(Provider)).IsEqualTo(1L);
        await Assert.That(bson.ToSByte(Provider)).IsEqualTo((sbyte)1);
        await Assert.That(bson.ToSingle(Provider)).IsEqualTo(1.0f);
        await Assert.That(bson.ToUInt16(Provider)).IsEqualTo((ushort)1);
        await Assert.That(bson.ToUInt32(Provider)).IsEqualTo((uint)1);
        await Assert.That(bson.ToUInt64(Provider)).IsEqualTo((ulong)1);
        
        await Assert.That(bson.ToType(typeof(bool), Provider)).IsEqualTo(true);
        await Assert.That(bson.ToType(typeof(BsonBoolean), Provider)).IsEqualTo(bson);
        await Assert.That(bson.ToType(typeof(object), Provider)).IsEqualTo(bson);
        await Assert.That(bson.ToType(typeof(string), Provider)).IsEqualTo("true");

        // Implicits
        BsonBoolean imp1 = true;
        await Assert.That(imp1.Value).IsTrue();
        bool imp2 = imp1;
        await Assert.That(imp2).IsTrue();
    }

    [Test]
    public async Task BsonTimestamp_Exhaustive()
    {
        var val = 1735689600L << 32 | 123U;
        var bson = new BsonTimestamp(val);

        await Assert.That(() => bson.ToBoolean(Provider)).Throws<InvalidCastException>();
        await Assert.That(() => bson.ToByte(Provider)).Throws<InvalidCastException>();
        await Assert.That(() => bson.ToChar(Provider)).Throws<InvalidCastException>();
        await Assert.That(bson.ToDateTime(Provider)).IsEqualTo(bson.ToDateTime());
        await Assert.That(bson.ToDecimal(Provider)).IsEqualTo((decimal)val);
        await Assert.That(bson.ToDouble(Provider)).IsEqualTo((double)val);
        await Assert.That(bson.ToInt16(Provider)).IsEqualTo((short)val);
        await Assert.That(bson.ToInt32(Provider)).IsEqualTo((int)val);
        await Assert.That(bson.ToInt64(Provider)).IsEqualTo(val);
        await Assert.That(bson.ToSByte(Provider)).IsEqualTo((sbyte)val);
        await Assert.That(bson.ToSingle(Provider)).IsEqualTo((float)val);
        await Assert.That(bson.ToUInt16(Provider)).IsEqualTo((ushort)val);
        await Assert.That(bson.ToUInt32(Provider)).IsEqualTo((uint)val);
        await Assert.That(bson.ToUInt64(Provider)).IsEqualTo((ulong)val);
        
        await Assert.That(bson.ToType(typeof(long), Provider)).IsEqualTo(val);
        await Assert.That(bson.ToType(typeof(BsonTimestamp), Provider)).IsEqualTo(bson);
        await Assert.That(bson.ToType(typeof(object), Provider)).IsEqualTo(bson);
        await Assert.That(bson.ToType(typeof(string), Provider)).IsEqualTo(bson.ToString());

        // Implicits
        BsonTimestamp imp1 = val;
        await Assert.That(imp1.Value).IsEqualTo(val);
        long imp2 = imp1;
        await Assert.That(imp2).IsEqualTo(val);
    }

    [Test]
    public async Task BsonArray_Exhaustive()
    {
        var array = new BsonArray().AddValue(1).AddValue(2);
        
        await Assert.That(array.ToBoolean(Provider)).IsTrue();
        await Assert.That(new BsonArray().ToBoolean(Provider)).IsFalse();
        
        await Assert.That(array.ToInt32(Provider)).IsEqualTo(2);
        await Assert.That(array.ToInt64(Provider)).IsEqualTo(2L);
        await Assert.That(array.ToByte(Provider)).IsEqualTo((byte)2);
        await Assert.That(array.ToDecimal(Provider)).IsEqualTo(2m);
        await Assert.That(array.ToDouble(Provider)).IsEqualTo(2.0);
        await Assert.That(array.ToInt16(Provider)).IsEqualTo((short)2);
        await Assert.That(array.ToSByte(Provider)).IsEqualTo((sbyte)2);
        await Assert.That(array.ToSingle(Provider)).IsEqualTo(2.0f);
        await Assert.That(array.ToUInt16(Provider)).IsEqualTo((ushort)2);
        await Assert.That(array.ToUInt32(Provider)).IsEqualTo((uint)2);
        await Assert.That(array.ToUInt64(Provider)).IsEqualTo((ulong)2);
        
        await Assert.That(array.ToString(Provider)).IsEqualTo(array.ToString());
        
        // ToType
        await Assert.That(array.ToType(typeof(int), Provider)).IsEqualTo(2);
        await Assert.That(array.ToType(typeof(string), Provider)).IsEqualTo(array.ToString());

        // Modifiers
        var array2 = array.AddValue(3);
        await Assert.That(array2.Count).IsEqualTo(3);
        await Assert.That(array.Count).IsEqualTo(2); // original unchanged

        var array3 = array2.Set(0, 10);
        await Assert.That(array3[0].ToInt32()).IsEqualTo(10);
        
        var array4 = array3.InsertValue(1, 20);
        await Assert.That(array4[1].ToInt32()).IsEqualTo(20);
        
        var array5 = array4.RemoveValue(20);
        await Assert.That(array5.Contains(20)).IsFalse();
        
        var array6 = array5.RemoveAtValue(0);
        await Assert.That(array6.Count).IsEqualTo(2);

        // Search
        await Assert.That(array.Contains(1)).IsTrue();
        await Assert.That(array.IndexOf(2)).IsEqualTo(1);
        await Assert.That(array.IndexOf(99)).IsEqualTo(-1);

        // FromList
        var list = new List<object?> { 1, "a", true, null };
        var bsonArray = BsonArray.FromList(list);
        await Assert.That(bsonArray.Count).IsEqualTo(4);
        await Assert.That(bsonArray[0].ToInt32()).IsEqualTo(1);
        await Assert.That(bsonArray[1].ToString()).IsEqualTo("a");
        await Assert.That(bsonArray[2].ToBoolean(null)).IsTrue();
        await Assert.That(bsonArray[3].IsNull).IsTrue();
    }

    [Test]
    public async Task BsonArrayValue_Internal_Coverage()
    {
        // Direct access via InternalsVisibleTo
        var array = new BsonArray().AddValue(1);
        BsonValue wrapper = new BsonArrayValue(array);
        
        await Assert.That(wrapper.BsonType).IsEqualTo(BsonType.Array);
        await Assert.That(wrapper.IsArray).IsTrue();
        await Assert.That(wrapper.RawValue).IsEqualTo(array);
        
        await Assert.That(wrapper.Equals(wrapper)).IsTrue();
        await Assert.That(wrapper.Equals(array)).IsFalse(); // Different wrapper/type
        
        await Assert.That(wrapper.CompareTo(wrapper)).IsEqualTo(0);
        await Assert.That(wrapper.CompareTo(array)).IsEqualTo(0); // BsonArrayValue.CompareTo handles BsonArray
        await Assert.That(wrapper.CompareTo(new BsonInt32(1))).IsNotEqualTo(0);
        
        await Assert.That(wrapper.ToString()).IsEqualTo(array.ToString());
        await Assert.That(wrapper.ToInt32(Provider)).IsEqualTo(1);
    }

    [Test]
    public async Task BsonMaxKey_Exhaustive()
    {
        var bson = BsonMaxKey.Value;
        await Assert.That(bson.ToBoolean(Provider)).IsTrue();
        await Assert.That(bson.ToByte(Provider)).IsEqualTo((byte)255);
        await Assert.That(bson.ToChar(Provider)).IsEqualTo(char.MaxValue);
        await Assert.That(bson.ToDateTime(Provider)).IsEqualTo(DateTime.MaxValue);
        await Assert.That(bson.ToDecimal(Provider)).IsEqualTo(decimal.MaxValue);
        await Assert.That(bson.ToDouble(Provider)).IsEqualTo(double.MaxValue);
        await Assert.That(bson.ToInt16(Provider)).IsEqualTo(short.MaxValue);
        await Assert.That(bson.ToInt32(Provider)).IsEqualTo(int.MaxValue);
        await Assert.That(bson.ToInt64(Provider)).IsEqualTo(long.MaxValue);
        await Assert.That(bson.ToSByte(Provider)).IsEqualTo(sbyte.MaxValue);
        await Assert.That(bson.ToSingle(Provider)).IsEqualTo(float.MaxValue);
        await Assert.That(bson.ToUInt16(Provider)).IsEqualTo(ushort.MaxValue);
        await Assert.That(bson.ToUInt32(Provider)).IsEqualTo(uint.MaxValue);
        await Assert.That(bson.ToUInt64(Provider)).IsEqualTo(ulong.MaxValue);
        await Assert.That(bson.ToString(Provider)).IsEqualTo("$maxKey");
    }

    [Test]
    public async Task BsonMinKey_Exhaustive()
    {
        var bson = BsonMinKey.Value;
        await Assert.That(bson.ToBoolean(Provider)).IsFalse();
        await Assert.That(bson.ToByte(Provider)).IsEqualTo((byte)0);
        await Assert.That(bson.ToChar(Provider)).IsEqualTo((char)0);
        await Assert.That(bson.ToDateTime(Provider)).IsEqualTo(DateTime.MinValue);
        await Assert.That(bson.ToDecimal(Provider)).IsEqualTo(0m);
        await Assert.That(bson.ToDouble(Provider)).IsEqualTo(0.0);
        await Assert.That(bson.ToInt16(Provider)).IsEqualTo((short)0);
        await Assert.That(bson.ToInt32(Provider)).IsEqualTo(0);
        await Assert.That(bson.ToInt64(Provider)).IsEqualTo(0L);
        await Assert.That(bson.ToSByte(Provider)).IsEqualTo((sbyte)0);
        await Assert.That(bson.ToSingle(Provider)).IsEqualTo(0.0f);
        await Assert.That(bson.ToUInt16(Provider)).IsEqualTo((ushort)0);
        await Assert.That(bson.ToUInt32(Provider)).IsEqualTo((uint)0);
        await Assert.That(bson.ToUInt64(Provider)).IsEqualTo((ulong)0L);
        await Assert.That(bson.ToString(Provider)).IsEqualTo("$minKey");
    }
}
