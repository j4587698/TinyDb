using System.Text;
using TinyDb.Utils;
using TUnit.Assertions;

namespace TinyDb.Tests.Utils;

public class TinyCrc32Tests
{
    [Test]
    public async Task HashToUInt32_EmptyData_ShouldReturnZero()
    {
        var crc = TinyCrc32.HashToUInt32(Array.Empty<byte>());
        await Assert.That(crc).IsEqualTo(0u);
    }

    [Test]
    public async Task HashToUInt32_StandardVector_ShouldMatchKnownValue()
    {
        var data = Encoding.ASCII.GetBytes("123456789");
        var crc = TinyCrc32.HashToUInt32(data);
        await Assert.That(crc).IsEqualTo(0xCBF43926u);
    }

    [Test]
    public async Task HashToUInt32_NullByteArray_ShouldThrow()
    {
        await Assert.That(() => TinyCrc32.HashToUInt32((byte[])null!))
            .Throws<ArgumentNullException>();
    }

    [Test]
    public async Task HashToUInt32WithZeroedRange_ShouldIgnoreSelectedBytes()
    {
        var data = Encoding.ASCII.GetBytes("abcd1234efgh");
        var expected = Encoding.ASCII.GetBytes("abcd1234efgh");
        Array.Clear(expected, 4, 4);

        var crc = TinyCrc32.HashToUInt32WithZeroedRange(data, 4, 4);

        await Assert.That(crc).IsEqualTo(TinyCrc32.HashToUInt32(expected));
    }
}
