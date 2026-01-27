using TinyDb.Core;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Core;

public class EnginePropertiesTests : IDisposable
{
    private readonly string _testDbPath;
    private readonly TinyDbEngine _engine;

    public EnginePropertiesTests()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"eng_prop_{Guid.NewGuid():N}.db");
        _engine = new TinyDbEngine(_testDbPath);
    }

    public void Dispose()
    {
        _engine.Dispose();
        try { if (File.Exists(_testDbPath)) File.Delete(_testDbPath); } catch { }
    }

    [Test]
    public async Task Engine_Properties_Should_Be_Correct()
    {
        await Assert.That(_engine.FilePath).IsEqualTo(Path.GetFullPath(_testDbPath));
        await Assert.That(_engine.IsInitialized).IsTrue();
        await Assert.That(_engine.GetWalEnabled()).IsFalse();
        await Assert.That(_engine.CollectionCount).IsEqualTo(0);
        await Assert.That(_engine.Options).IsNotNull();
        
        // Header is a struct, so it cannot be null. Check a property instead.
        await Assert.That(_engine.Header.Magic).IsEqualTo(DatabaseHeader.MagicNumber);
        await Assert.That(_engine.Header.DatabaseName).IsEqualTo("TinyDb");
    }

    [Test]
    public async Task GetCollectionNames_Should_Return_Names()
    {
        _engine.GetCollection<object>("Col1");
        _engine.GetCollection<object>("Col2");
        
        var names = _engine.GetCollectionNames().ToList();
        await Assert.That(names.Count).IsEqualTo(2);
        await Assert.That(names).Contains("Col1");
        await Assert.That(names).Contains("Col2");
    }
}
