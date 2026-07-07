using Microsoft.IO;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Compatibility;

public sealed class RecyclableMemoryStreamCompatTests
{
    [Test]
    public async Task Write_AfterSeekBeyondLength_ShouldZeroGap()
    {
        using var stream = new RecyclableMemoryStream(16);
        var dirty = Enumerable.Repeat((byte)0x5A, 16).ToArray();
        stream.Write(dirty);
        stream.SetLength(1);

        stream.Seek(8, SeekOrigin.Begin);
        stream.Write(new byte[] { 0x7F, 0x80 });

        var data = stream.ToArray();

        await Assert.That(data.Length).IsEqualTo(10);
        await Assert.That(data[0]).IsEqualTo((byte)0x5A);
        for (var i = 1; i < 8; i++)
        {
            await Assert.That(data[i]).IsEqualTo((byte)0);
        }

        await Assert.That(data[8]).IsEqualTo((byte)0x7F);
        await Assert.That(data[9]).IsEqualTo((byte)0x80);
    }

    [Test]
    public async Task WriteByte_AfterSeekBeyondLength_ShouldZeroGap()
    {
        using var stream = new RecyclableMemoryStream(16);
        var dirty = Enumerable.Repeat((byte)0x5A, 16).ToArray();
        stream.Write(dirty);
        stream.SetLength(1);

        stream.Seek(8, SeekOrigin.Begin);
        stream.WriteByte(0x7F);

        var data = stream.ToArray();

        await Assert.That(data.Length).IsEqualTo(9);
        await Assert.That(data[0]).IsEqualTo((byte)0x5A);
        for (var i = 1; i < 8; i++)
        {
            await Assert.That(data[i]).IsEqualTo((byte)0);
        }

        await Assert.That(data[8]).IsEqualTo((byte)0x7F);
    }
}
