using TinyDb.Core;
using TinyDb.Bson;
using TinyDb.Storage;
using System.Reflection;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Core;

public class EngineCorruptPageTests : IDisposable
{
    private readonly string _testDbPath;
    private TinyDbEngine _engine;

    public EngineCorruptPageTests()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"eng_corrupt_{Guid.NewGuid():N}.db");
        _engine = new TinyDbEngine(_testDbPath);
    }

    public void Dispose()
    {
        _engine.Dispose();
        try { if (File.Exists(_testDbPath)) File.Delete(_testDbPath); } catch { }
    }

    [Test]
    public async Task FindAll_Should_Handle_Page_Read_Error_By_Skipping()
    {
        var colName = "col";
        var id = _engine.InsertDocument(colName, new BsonDocument().Set("_id", 1));
        _engine.Flush();
        
        // Ensure cache is initialized
        _engine.FindAll(colName).ToList();
        
        // Dispose engine to release file lock
        _engine.Dispose();
        
        // Corrupt data page. Header is page 1 (8192 bytes). 
        // Data page is likely page 4 or higher.
        using (var fs = new FileStream(_testDbPath, FileMode.Open, FileAccess.Write, FileShare.ReadWrite))
        {
            fs.Seek(8192 * 3, SeekOrigin.Begin); // Corrupt from page 3 onwards
            var garbage = new byte[8192 * 5];
            new Random().NextBytes(garbage);
            fs.Write(garbage, 0, garbage.Length);
        }
        
        // Re-open engine
        _engine = new TinyDbEngine(_testDbPath);
        
        // FindAll should handle read error in FetchDocumentsByLocations
        var all = _engine.FindAll(colName).ToList();
        
        // document should be gone from results and cache
        await Assert.That(all.Count).IsEqualTo(0);
        // await Assert.That(_engine.GetCachedDocumentCount(colName)).IsEqualTo(0);
    }
}
