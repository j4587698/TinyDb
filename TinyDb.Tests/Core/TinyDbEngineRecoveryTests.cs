using TinyDb.Core;
using TinyDb.Storage;
using TinyDb.Attributes;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Core;

public class TinyDbEngineRecoveryTests : IDisposable
{
    private readonly string _testDbPath;

    public TinyDbEngineRecoveryTests()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"recovery_{Guid.NewGuid():N}.db");
    }

    public void Dispose()
    {
        try 
        { 
            if (File.Exists(_testDbPath)) File.Delete(_testDbPath);
            
            var directory = Path.GetDirectoryName(_testDbPath)!;
            var fileNameNoExt = Path.GetFileNameWithoutExtension(_testDbPath);
            foreach (var file in Directory.GetFiles(directory, $"{fileNameNoExt}*"))
            {
                try { File.Delete(file); } catch { }
            }
        } catch { }
    }

    [Entity("data")]
    public class DataItem
    {
        public int Id { get; set; }
        public string Value { get; set; } = "";
    }

    [Test]
    public async Task Engine_Should_Recover_From_WAL_On_Startup()
    {
        // 1. Create DB and enable WAL
        using (var engine = new TinyDbEngine(_testDbPath, new TinyDbOptions { EnableJournaling = true }))
        {
            var collection = engine.GetCollection<DataItem>();
            // Insert data
            for (int i = 0; i < 10; i++)
            {
                collection.Insert(new DataItem { Id = i, Value = $"V{i}" });
            }
        }

        // Calculate correct WAL path (default format: {name}-wal.{ext})
        var directory = Path.GetDirectoryName(_testDbPath)!;
        var fileNameNoExt = Path.GetFileNameWithoutExtension(_testDbPath);
        var ext = Path.GetExtension(_testDbPath).TrimStart('.');
        var walPath = Path.Combine(directory, $"{fileNameNoExt}-wal.{ext}");

        var dbBackup = File.ReadAllBytes(_testDbPath);

        // Step 2: Add more data
        using (var engine = new TinyDbEngine(_testDbPath, new TinyDbOptions { EnableJournaling = true }))
        {
            var col = engine.GetCollection<DataItem>();
            for(int i=0; i<5; i++) col.Insert(new DataItem { Id = i });
            
            // Backup WAL while active (simulate crash state before full flush)
            File.Copy(walPath, walPath + ".bak", true);
        }
        
        // Step 3: Rollback DB file and restore WAL
        File.WriteAllBytes(_testDbPath, dbBackup);
        File.Copy(walPath + ".bak", walPath, true);
        
        // Step 4: Open Engine
        using (var engine = new TinyDbEngine(_testDbPath, new TinyDbOptions { EnableJournaling = true }))
        {
            var col = engine.GetCollection<DataItem>();
            var all = col.FindAll().ToList();
            
            // Should include recovered items
            await Assert.That(all.Count).IsGreaterThan(1);
            await Assert.That(all.Any(x => x.Id == 4)).IsTrue();
        }
    }
}
