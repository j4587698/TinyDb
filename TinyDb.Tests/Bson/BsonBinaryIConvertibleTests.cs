using System;
using System.Threading.Tasks;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Bson;

/// <summary>
/// BsonBinary IConvertible 接口测试
/// </summary>
public class BsonBinaryIConvertibleTests
{
    [Test]
    public async Task BsonBinary_Constructor_WithBytes_ShouldWork()
    {
        var bytes = new byte[] { 1, 2, 3, 4, 5 };
        var binary = new BsonBinary(bytes);
        
        await Assert.That(binary.Bytes).IsEquivalentTo(bytes);
        await Assert.That(binary.SubType).IsEqualTo(BsonBinary.BinarySubType.Generic);
        await Assert.That(binary.Value).IsEquivalentTo(bytes);
    }

    [Test]
    public async Task BsonBinary_Constructor_WithGuid_ShouldWork()
    {
        var guid = Guid.NewGuid();
        var binary = new BsonBinary(guid);
        
        await Assert.That(binary.SubType).IsEqualTo(BsonBinary.BinarySubType.Uuid);
        await Assert.That(binary.Bytes.Length).IsEqualTo(16);
    }

    [Test]
    public async Task BsonBinary_Constructor_WithNullBytes_ShouldThrow()
    {
        await Assert.That(() => new BsonBinary(null!))
            .Throws<ArgumentNullException>();
    }

    [Test]
    public async Task BsonBinary_BsonType_ShouldBeBinary()
    {
        var binary = new BsonBinary(new byte[] { 1, 2, 3 });
        await Assert.That(binary.BsonType).IsEqualTo(BsonType.Binary);
    }

    [Test]
    public async Task BsonBinary_RawValue_ShouldReturnBytes()
    {
        var bytes = new byte[] { 1, 2, 3 };
        var binary = new BsonBinary(bytes);
        
        await Assert.That(binary.RawValue).IsEqualTo(bytes);
    }

    [Test]
    public async Task BsonBinary_ImplicitConversion_FromBytes()
    {
        byte[] bytes = new byte[] { 1, 2, 3 };
        BsonBinary binary = bytes;
        
        await Assert.That(binary.Bytes).IsEquivalentTo(bytes);
    }

    [Test]
    public async Task BsonBinary_ImplicitConversion_ToBytes()
    {
        var binary = new BsonBinary(new byte[] { 1, 2, 3 });
        byte[] bytes = binary;
        
        await Assert.That(bytes).IsEquivalentTo(new byte[] { 1, 2, 3 });
    }

    [Test]
    public async Task BsonBinary_Equals_SameBytes_ShouldReturnTrue()
    {
        var binary1 = new BsonBinary(new byte[] { 1, 2, 3 });
        var binary2 = new BsonBinary(new byte[] { 1, 2, 3 });
        
        await Assert.That(binary1.Equals(binary2)).IsTrue();
    }

    [Test]
    public async Task BsonBinary_Equals_DifferentBytes_ShouldReturnFalse()
    {
        var binary1 = new BsonBinary(new byte[] { 1, 2, 3 });
        var binary2 = new BsonBinary(new byte[] { 1, 2, 4 });
        
        await Assert.That(binary1.Equals(binary2)).IsFalse();
    }

    [Test]
    public async Task BsonBinary_Equals_DifferentSubType_ShouldReturnFalse()
    {
        var binary1 = new BsonBinary(new byte[] { 1, 2, 3 }, BsonBinary.BinarySubType.Generic);
        var binary2 = new BsonBinary(new byte[] { 1, 2, 3 }, BsonBinary.BinarySubType.Md5);
        
        await Assert.That(binary1.Equals(binary2)).IsFalse();
    }

    [Test]
    public async Task BsonBinary_Equals_NonBsonBinary_ShouldReturnFalse()
    {
        var binary = new BsonBinary(new byte[] { 1, 2, 3 });
        var notBinary = new BsonInt32(123);
        
        await Assert.That(binary.Equals(notBinary)).IsFalse();
    }

    [Test]
    public async Task BsonBinary_GetHashCode_SameBytes_ShouldBeEqual()
    {
        var binary1 = new BsonBinary(new byte[] { 1, 2, 3 });
        var binary2 = new BsonBinary(new byte[] { 1, 2, 3 });
        
        await Assert.That(binary1.GetHashCode()).IsEqualTo(binary2.GetHashCode());
    }

    [Test]
    public async Task BsonBinary_ToString_ShouldReturnDescription()
    {
        var binary = new BsonBinary(new byte[] { 1, 2, 3 });
        var str = binary.ToString();
        
        await Assert.That(str).Contains("Binary");
        await Assert.That(str).Contains("3 bytes");
    }

    [Test]
    public async Task BsonBinary_CompareTo_Null_ShouldReturn1()
    {
        var binary = new BsonBinary(new byte[] { 1, 2, 3 });
        
        await Assert.That(binary.CompareTo(null)).IsEqualTo(1);
    }

    [Test]
    public async Task BsonBinary_CompareTo_DifferentLength_ShouldCompareByLength()
    {
        var binary1 = new BsonBinary(new byte[] { 1, 2, 3 });
        var binary2 = new BsonBinary(new byte[] { 1, 2, 3, 4, 5 });
        
        await Assert.That(binary1.CompareTo(binary2)).IsLessThan(0);
    }

    [Test]
    public async Task BsonBinary_CompareTo_SameLengthDifferentSubType_ShouldCompareBySubType()
    {
        var binary1 = new BsonBinary(new byte[] { 1, 2, 3 }, BsonBinary.BinarySubType.Generic);
        var binary2 = new BsonBinary(new byte[] { 1, 2, 3 }, BsonBinary.BinarySubType.Md5);
        
        await Assert.That(binary1.CompareTo(binary2)).IsNotEqualTo(0);
    }

    [Test]
    public async Task BsonBinary_CompareTo_SameLengthSameSubType_ShouldCompareByContent()
    {
        var binary1 = new BsonBinary(new byte[] { 1, 2, 3 });
        var binary2 = new BsonBinary(new byte[] { 1, 2, 4 });
        
        await Assert.That(binary1.CompareTo(binary2)).IsLessThan(0);
    }

    [Test]
    public async Task BsonBinary_CompareTo_OtherBsonType_ShouldCompareByBsonType()
    {
        var binary = new BsonBinary(new byte[] { 1, 2, 3 });
        var intVal = new BsonInt32(123);
        
        var result = binary.CompareTo(intVal);
        await Assert.That(result).IsNotEqualTo(0);
    }

    // IConvertible 测试
    [Test]
    public async Task BsonBinary_GetTypeCode_ShouldReturnObject()
    {
        var binary = new BsonBinary(new byte[] { 1, 2, 3 });
        await Assert.That(binary.GetTypeCode()).IsEqualTo(TypeCode.Object);
    }

    [Test]
    public async Task BsonBinary_ToBoolean_ShouldThrow()
    {
        var binary = new BsonBinary(new byte[] { 1, 2, 3 });
        await Assert.That(() => binary.ToBoolean(null)).Throws<InvalidCastException>();
    }

    [Test]
    public async Task BsonBinary_ToByte_ShouldThrow()
    {
        var binary = new BsonBinary(new byte[] { 1, 2, 3 });
        await Assert.That(() => binary.ToByte(null)).Throws<InvalidCastException>();
    }

    [Test]
    public async Task BsonBinary_ToChar_ShouldThrow()
    {
        var binary = new BsonBinary(new byte[] { 1, 2, 3 });
        await Assert.That(() => binary.ToChar(null)).Throws<InvalidCastException>();
    }

    [Test]
    public async Task BsonBinary_ToDateTime_ShouldThrow()
    {
        var binary = new BsonBinary(new byte[] { 1, 2, 3 });
        await Assert.That(() => binary.ToDateTime(null)).Throws<InvalidCastException>();
    }

    [Test]
    public async Task BsonBinary_ToDecimal_ShouldThrow()
    {
        var binary = new BsonBinary(new byte[] { 1, 2, 3 });
        await Assert.That(() => binary.ToDecimal(null)).Throws<InvalidCastException>();
    }

    [Test]
    public async Task BsonBinary_ToDouble_ShouldThrow()
    {
        var binary = new BsonBinary(new byte[] { 1, 2, 3 });
        await Assert.That(() => binary.ToDouble(null)).Throws<InvalidCastException>();
    }

    [Test]
    public async Task BsonBinary_ToInt16_ShouldThrow()
    {
        var binary = new BsonBinary(new byte[] { 1, 2, 3 });
        await Assert.That(() => binary.ToInt16(null)).Throws<InvalidCastException>();
    }

    [Test]
    public async Task BsonBinary_ToInt32_ShouldThrow()
    {
        var binary = new BsonBinary(new byte[] { 1, 2, 3 });
        await Assert.That(() => binary.ToInt32(null)).Throws<InvalidCastException>();
    }

    [Test]
    public async Task BsonBinary_ToInt64_ShouldThrow()
    {
        var binary = new BsonBinary(new byte[] { 1, 2, 3 });
        await Assert.That(() => binary.ToInt64(null)).Throws<InvalidCastException>();
    }

    [Test]
    public async Task BsonBinary_ToSByte_ShouldThrow()
    {
        var binary = new BsonBinary(new byte[] { 1, 2, 3 });
        await Assert.That(() => binary.ToSByte(null)).Throws<InvalidCastException>();
    }

    [Test]
    public async Task BsonBinary_ToSingle_ShouldThrow()
    {
        var binary = new BsonBinary(new byte[] { 1, 2, 3 });
        await Assert.That(() => binary.ToSingle(null)).Throws<InvalidCastException>();
    }

    [Test]
    public async Task BsonBinary_ToStringWithProvider_ShouldReturnDescription()
    {
        var binary = new BsonBinary(new byte[] { 1, 2, 3 });
        var str = binary.ToString(null);
        
        await Assert.That(str).Contains("Binary");
    }

    [Test]
    public async Task BsonBinary_ToType_ShouldThrow()
    {
        var binary = new BsonBinary(new byte[] { 1, 2, 3 });
        await Assert.That(() => binary.ToType(typeof(int), null)).Throws<InvalidCastException>();
    }

    [Test]
    public async Task BsonBinary_ToUInt16_ShouldThrow()
    {
        var binary = new BsonBinary(new byte[] { 1, 2, 3 });
        await Assert.That(() => binary.ToUInt16(null)).Throws<InvalidCastException>();
    }

    [Test]
    public async Task BsonBinary_ToUInt32_ShouldThrow()
    {
        var binary = new BsonBinary(new byte[] { 1, 2, 3 });
        await Assert.That(() => binary.ToUInt32(null)).Throws<InvalidCastException>();
    }

    [Test]
    public async Task BsonBinary_ToUInt64_ShouldThrow()
    {
        var binary = new BsonBinary(new byte[] { 1, 2, 3 });
        await Assert.That(() => binary.ToUInt64(null)).Throws<InvalidCastException>();
    }

    [Test]
    public async Task BsonBinary_SubTypes_ShouldWork()
    {
        var generic = new BsonBinary(new byte[] { 1 }, BsonBinary.BinarySubType.Generic);
        var function = new BsonBinary(new byte[] { 1 }, BsonBinary.BinarySubType.Function);
        var binaryOld = new BsonBinary(new byte[] { 1 }, BsonBinary.BinarySubType.BinaryOld);
        var uuidLegacy = new BsonBinary(new byte[] { 1 }, BsonBinary.BinarySubType.UuidLegacy);
        var uuid = new BsonBinary(new byte[] { 1 }, BsonBinary.BinarySubType.Uuid);
        var md5 = new BsonBinary(new byte[] { 1 }, BsonBinary.BinarySubType.Md5);
        var userDefined = new BsonBinary(new byte[] { 1 }, BsonBinary.BinarySubType.UserDefined);

        await Assert.That(generic.SubType).IsEqualTo(BsonBinary.BinarySubType.Generic);
        await Assert.That(function.SubType).IsEqualTo(BsonBinary.BinarySubType.Function);
        await Assert.That(binaryOld.SubType).IsEqualTo(BsonBinary.BinarySubType.BinaryOld);
        await Assert.That(uuidLegacy.SubType).IsEqualTo(BsonBinary.BinarySubType.UuidLegacy);
        await Assert.That(uuid.SubType).IsEqualTo(BsonBinary.BinarySubType.Uuid);
        await Assert.That(md5.SubType).IsEqualTo(BsonBinary.BinarySubType.Md5);
        await Assert.That(userDefined.SubType).IsEqualTo(BsonBinary.BinarySubType.UserDefined);
    }
}
