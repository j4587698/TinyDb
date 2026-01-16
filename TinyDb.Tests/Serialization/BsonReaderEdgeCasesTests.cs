using TinyDb.Serialization;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Serialization;

public class BsonReaderEdgeCasesTests
{
    [Test]
    public async Task ReadDocument_SizeMismatch_Should_Throw()
    {
        using var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms);
        writer.Write(10); // expectedContentSize = 6
        writer.Write((byte)BsonType.End); // actualContentSize = 1
        
        ms.Position = 0;
        using var reader = new BsonReader(ms);
        await Assert.That(() => reader.ReadDocument()).Throws<InvalidOperationException>();
    }

    [Test]
    public async Task ReadArray_SizeMismatch_Should_Throw()
    {
        using var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms);
        writer.Write(10); // expectedContentSize = 6
        writer.Write((byte)BsonType.End); // actualContentSize = 1
        
        ms.Position = 0;
        using var reader = new BsonReader(ms);
        await Assert.That(() => reader.ReadArray()).Throws<InvalidOperationException>();
    }

    [Test]
    public async Task ReadValue_UnexpectedEnd_Should_Throw()
    {
        using var ms = new MemoryStream();
        using var reader = new BsonReader(ms);
        await Assert.That(() => reader.ReadValue(BsonType.End)).Throws<InvalidOperationException>();
    }
}
