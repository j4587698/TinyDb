using TinyDb.Storage;
using TinyDb.Core;
using TinyDb.Attributes;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Storage;

public class PageManagerScanTests : IDisposable
{
    private readonly string _testDbPath;

    public PageManagerScanTests()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"pm_scan_{Guid.NewGuid()}.db");
    }

    public void Dispose()
    {
        try { if (File.Exists(_testDbPath)) File.Delete(_testDbPath); } catch {}
    }

    [Test]
    public async Task PageManager_Should_Recover_Free_Pages_On_Restart()
    {
        // 1. Create DB and create some holes
        {
            using var engine = new TinyDbEngine(_testDbPath);
            var collection = engine.GetCollection<PageManagerScanTestDoc>();

            // Insert enough data to use multiple pages. PageSize default 4096.
            // 500 byte doc -> ~8 docs per page.
            // 100 docs -> ~13 pages.
            var docs = Enumerable.Range(1, 100).Select(i => new PageManagerScanTestDoc { Id = i, Data = new string('x', 500) }).ToList();
            foreach(var d in docs) collection.Insert(d);

            // Delete all docs to ensure pages are freed.
            foreach(var d in docs) collection.Delete(d.Id);

            // Pages should be added to _freePages queue in memory.
            // PageManager.FreePage writes PageType.Empty to disk.
        }

        // 2. Restart Engine - PageManager should scan and find free pages (Empty pages)
        {
            using var engine = new TinyDbEngine(_testDbPath);
            var stats = engine.GetStatistics();

            // Should have found free pages
            await Assert.That(stats.FreePages).IsGreaterThan(0u);

            // Verify reuse logic by inserting new data
            var collection = engine.GetCollection<PageManagerScanTestDoc>();
            for(int i=0; i<10; i++)
            {
                collection.Insert(new PageManagerScanTestDoc { Id = 1000 + i, Data = "New" });
            }

            var newStats = engine.GetStatistics();
            // Free pages should decrease as they are reused
            await Assert.That(newStats.FreePages).IsLessThan(stats.FreePages);
        }
    }
}

[Entity("test_docs")]
public class PageManagerScanTestDoc
{
    public int Id { get; set; }
    public string Data { get; set; } = "";
}
