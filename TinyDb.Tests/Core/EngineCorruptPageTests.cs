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

    public EngineCorruptPageTests()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"eng_corrupt_{Guid.NewGuid():N}.db");
    }

    public void Dispose()
    {
        try { if (File.Exists(_testDbPath)) File.Delete(_testDbPath); } catch { }
    }

    [Test]
    public async Task FindAll_ShouldThrow_WhenPageReadFails()
    {
        TinyDbEngine? engine = null;
        try
        {
            engine = new TinyDbEngine(_testDbPath);
            var colName = "col";
            var id = engine.InsertDocument(colName, new BsonDocument().Set("_id", 1));
            engine.Flush();
            
            // Ensure cache is initialized
            engine.FindAll(colName).ToList();
            
            // Dispose engine to release file lock
            engine.Dispose();
            
            // Corrupt data page. Header is page 1 (8192 bytes). 
            // Data page is likely page 4 or higher.
            using (var fs = new FileStream(_testDbPath, FileMode.Open, FileAccess.Write, FileShare.ReadWrite))
            {
                fs.Seek(8192 * 3, SeekOrigin.Begin); // Corrupt from page 3 onwards
                var garbage = new byte[8192 * 5];
                new Random().NextBytes(garbage);
                fs.Write(garbage, 0, garbage.Length);
            }
            
            await Assert.That(() => new TinyDbEngine(_testDbPath))
                .Throws<InvalidOperationException>();
        }
        finally
        {
            engine?.Dispose();
        }
    }
}
