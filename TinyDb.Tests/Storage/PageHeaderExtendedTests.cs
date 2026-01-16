using TinyDb.Storage;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Storage;

public class PageHeaderExtendedTests
{
    [Test]
    public async Task PageHeader_RoundTrip_Should_Work()
    {
        var header = new PageHeader();
        header.Initialize(PageType.Data, 123);
        header.PrevPageID = 10;
        header.NextPageID = 20;
        header.FreeBytes = 500;
        header.ItemCount = 5;
        header.Version = 1;
        header.Checksum = 999;
        
        var bytes = header.ToByteArray();
        var replayed = PageHeader.FromByteArray(bytes);
        
        await Assert.That(replayed.PageID).IsEqualTo(123u);
        await Assert.That(replayed.PrevPageID).IsEqualTo(10u);
        await Assert.That(replayed.NextPageID).IsEqualTo(20u);
        await Assert.That(replayed.FreeBytes).IsEqualTo((ushort)500);
        await Assert.That(replayed.ItemCount).IsEqualTo((ushort)5);
        await Assert.That(replayed.Version).IsEqualTo(1u);
        await Assert.That(replayed.Checksum).IsEqualTo(999u);
    }

    [Test]
    public async Task PageHeader_IsValid_Should_Be_Correct()
    {
        var header = new PageHeader();
        await Assert.That(header.IsValid()).IsFalse(); // PageID 0
        
        header.Initialize(PageType.Data, 1);
        await Assert.That(header.IsValid()).IsTrue();
    }

    [Test]
    public async Task PageHeader_Checksum_Methods_Should_Work()
    {
        var header = new PageHeader();
        var data = new byte[100];
        new Random().NextBytes(data);
        
        var cs = header.CalculateChecksum(data);
        header.Checksum = cs;
        await Assert.That(header.VerifyChecksum(data)).IsTrue();
        
        data[0]++;
        await Assert.That(header.VerifyChecksum(data)).IsFalse();
    }
}
