using TinyDb.Storage;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Storage;

public class PagePinningTests
{
    private const int PageSize = 4096;

    [Test]
    public async Task Unpin_WhenNotPinned_ShouldClampToZero()
    {
        using var page = new Page(1u, PageSize, PageType.Data);

        page.Unpin();

        await Assert.That(page.PinCount).IsEqualTo(0);
    }

    [Test]
    public async Task PinAndUnpin_ShouldReturnToZero()
    {
        using var page = new Page(1u, PageSize, PageType.Data);

        page.Pin();
        await Assert.That(page.PinCount).IsEqualTo(1);

        page.Unpin();
        await Assert.That(page.PinCount).IsEqualTo(0);
    }
}

