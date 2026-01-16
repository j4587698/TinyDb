using System;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TinyDb.Bson;

namespace TinyDb.Tests.Bson;

public class ObjectIdCoverageTests
{
    [Test]
    public async Task Default_ObjectId_Should_Be_Uninitialized()
    {
        ObjectId def = default;
        // Accessing properties on default struct should throw because _bytes is null
        await Assert.ThrowsAsync<InvalidOperationException>(async () => _ = def.TimestampSeconds);
        await Assert.ThrowsAsync<InvalidOperationException>(async () => _ = def.Machine);
        await Assert.ThrowsAsync<InvalidOperationException>(async () => _ = def.Pid);
        await Assert.ThrowsAsync<InvalidOperationException>(async () => _ = def.Counter);
        await Assert.ThrowsAsync<InvalidOperationException>(async () => _ = def.ToByteArray());
    }

    [Test]
    public async Task Empty_ObjectId_Should_Be_Initialized_And_Zero()
    {
        var empty = ObjectId.Empty;
        await Assert.That(empty.TimestampSeconds).IsEqualTo(0);
        await Assert.That(empty.Machine).IsEqualTo(0);
        await Assert.That((int)empty.Pid).IsEqualTo(0);
        await Assert.That(empty.Counter).IsEqualTo(0);
        await Assert.That(empty.ToByteArray().Length).IsEqualTo(12);
        await Assert.That(empty.ToString()).IsEqualTo("000000000000000000000000");
    }

    [Test]
    public async Task Constructor_Validations()
    {
        await Assert.ThrowsAsync<ArgumentNullException>(async () => new ObjectId((byte[])null!));
        await Assert.ThrowsAsync<ArgumentException>(async () => new ObjectId(new byte[11]));
        await Assert.ThrowsAsync<ArgumentNullException>(async () => new ObjectId((string)null!));
        await Assert.ThrowsAsync<ArgumentException>(async () => new ObjectId("too_short"));
        await Assert.ThrowsAsync<FormatException>(async () => new ObjectId("zzzzzzzzzzzzzzzzzzzzzzzz")); // Invalid hex
    }

    [Test]
    public async Task Component_Constructor_Should_Work()
    {
        var timestamp = new DateTime(2023, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        int machine = 0x123456;
        short pid = 0x7890;
        int counter = 0x112233;

        var oid = new ObjectId(timestamp, machine, pid, counter);
        
        // Tolerance for timestamp precision (seconds)
        var restoredTimestamp = oid.Timestamp;
        await Assert.That((restoredTimestamp - timestamp).TotalSeconds).IsLessThan(1);
        
        await Assert.That(oid.Machine).IsEqualTo(machine);
        await Assert.That((int)oid.Pid).IsEqualTo((int)pid);
        await Assert.That(oid.Counter).IsEqualTo(counter);
    }

    [Test]
    public async Task Comparison_Operators()
    {
        var oid1 = ObjectId.NewObjectId();
        var oid2 = ObjectId.NewObjectId();
        
        // Ensure they are different
        while (oid1.CompareTo(oid2) == 0)
        {
             oid2 = ObjectId.NewObjectId();
        }
        
        if (oid1.CompareTo(oid2) > 0)
        {
            var temp = oid1;
            oid1 = oid2;
            oid2 = temp;
        }
        
        await Assert.That(oid1 < oid2).IsTrue();
        await Assert.That(oid1 <= oid2).IsTrue();
        await Assert.That(oid2 > oid1).IsTrue();
        await Assert.That(oid2 >= oid1).IsTrue();
        await Assert.That(oid1 == oid1).IsTrue();
        await Assert.That(oid1 != oid2).IsTrue();
        
        await Assert.That(oid1.CompareTo(oid2)).IsLessThan(0);
        await Assert.That(oid1.CompareTo(oid1)).IsEqualTo(0);
    }

    [Test]
    public async Task Equals_Coverage()
    {
        var oid = ObjectId.NewObjectId();
        // Use object.Equals to force usage of overridden Equals(object) or verify behavior
        await Assert.That(object.Equals(oid, oid)).IsTrue();
        await Assert.That(object.Equals(oid, "string")).IsFalse();
        await Assert.That(object.Equals(oid, null)).IsFalse();
        
        // Direct call
        await Assert.That(oid.Equals(oid)).IsTrue();
    }

    [Test]
    public async Task IConvertible_Coverage()
    {
        var oid = ObjectId.NewObjectId();
        IConvertible convertible = oid;
        
        await Assert.That(convertible.GetTypeCode()).IsEqualTo(TypeCode.Object);
        await Assert.That(convertible.ToString(null)).IsEqualTo(oid.ToString());
        await Assert.That(convertible.ToType(typeof(string), null)).IsEqualTo(oid.ToString());
        await Assert.That(convertible.ToType(typeof(ObjectId), null)).IsEqualTo(oid);
        await Assert.That((byte[])convertible.ToType(typeof(byte[]), null)).IsEquivalentTo(oid.ToByteArray().ToArray());
        
        // Unsupported types
        await Assert.ThrowsAsync<InvalidCastException>(async () => convertible.ToBoolean(null));
        await Assert.ThrowsAsync<InvalidCastException>(async () => convertible.ToByte(null));
        await Assert.ThrowsAsync<InvalidCastException>(async () => convertible.ToChar(null));
        // ToDateTime is supported
        await Assert.That(convertible.ToDateTime(null)).IsEqualTo(oid.Timestamp);
        
        await Assert.ThrowsAsync<InvalidCastException>(async () => convertible.ToDecimal(null));
        await Assert.ThrowsAsync<InvalidCastException>(async () => convertible.ToDouble(null));
        await Assert.ThrowsAsync<InvalidCastException>(async () => convertible.ToInt16(null));
        await Assert.ThrowsAsync<InvalidCastException>(async () => convertible.ToInt32(null));
        await Assert.ThrowsAsync<InvalidCastException>(async () => convertible.ToInt64(null));
        await Assert.ThrowsAsync<InvalidCastException>(async () => convertible.ToSByte(null));
        await Assert.ThrowsAsync<InvalidCastException>(async () => convertible.ToSingle(null));
        await Assert.ThrowsAsync<InvalidCastException>(async () => convertible.ToUInt16(null));
        await Assert.ThrowsAsync<InvalidCastException>(async () => convertible.ToUInt32(null));
        await Assert.ThrowsAsync<InvalidCastException>(async () => convertible.ToUInt64(null));
    }
}