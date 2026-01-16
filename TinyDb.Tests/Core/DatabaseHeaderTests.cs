using TinyDb.Core;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Core;

public class DatabaseHeaderTests
{
    [Test]
    public async Task Header_Serialization_Should_Work()
    {
        var header = new DatabaseHeader();
        header.DatabaseName = "TestDB";
        header.PageSize = 8192;
        
        var bytes = header.ToByteArray();
        var replayed = DatabaseHeader.FromByteArray(bytes);
        
        await Assert.That(replayed.Magic).IsEqualTo(DatabaseHeader.MagicNumber);
        await Assert.That(replayed.DatabaseName).IsEqualTo("TestDB");
        await Assert.That(replayed.PageSize).IsEqualTo(8192u);
    }

    [Test]
    public async Task Header_Checksum_Should_Work()
    {
        var header = new DatabaseHeader();
        header.DatabaseName = "ChecksumTest";
        
        var checksum1 = header.CalculateChecksum();
        header.Checksum = checksum1;
        await Assert.That(header.VerifyChecksum()).IsTrue();
        
        header.PageSize = 16384;
        await Assert.That(header.VerifyChecksum()).IsFalse();
    }
}
