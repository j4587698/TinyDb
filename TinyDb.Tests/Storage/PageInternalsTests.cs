using TinyDb.Storage;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Storage;

public class PageInternalsTests
{
    [Test]
    public async Task Page_Metadata_Update_Should_Work()
    {
        var page = new Page(1, 4096, PageType.Data);
        var originalTime = page.Header.ModifiedAt;
        
        await Task.Delay(10);
        page.UpdateStats(100, 2);
        
        await Assert.That(page.Header.FreeBytes).IsEqualTo((ushort)100);
        await Assert.That(page.Header.ItemCount).IsEqualTo((ushort)2);
        await Assert.That(page.Header.ModifiedAt).IsGreaterThan(originalTime);
        await Assert.That(page.IsDirty).IsTrue();
    }

    [Test]
    public async Task Page_ClearData_Should_Reset_State()
    {
        var page = new Page(1, 4096, PageType.Data);
        page.WriteData(0, new byte[] { 1, 2, 3 });
        
        page.ClearData();
        
        await Assert.That(page.PageType).IsEqualTo(PageType.Empty);
        await Assert.That(page.Header.ItemCount).IsEqualTo((ushort)0);
        await Assert.That(page.ReadData(0, 3).All(b => b == 0)).IsTrue();
    }

    [Test]
    public async Task Page_Checksum_Verification_Should_Work()
    {
        var page = new Page(1, 4096, PageType.Data);
        page.WriteData(0, new byte[] { 1, 2, 3 });
        page.UpdateChecksum();
        
        await Assert.That(page.VerifyIntegrity()).IsTrue();
    }
}
