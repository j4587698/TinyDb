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

    [Test]
    public async Task Engine_Should_Recover_From_WAL_On_Startup()
    {
        // 1. Create DB and enable WAL
        using (var engine = new TinyDbEngine(_testDbPath, new TinyDbOptions { EnableJournaling = true }))
        {
            var collection = engine.GetCollection<RecoveryDataItem>();
            // Insert data
            for (int i = 1; i <= 10; i++)
            {
                collection.Insert(new RecoveryDataItem { Id = i, Value = $"V{i}" });
            }
        }

        // Calculate correct WAL path (default format: {name}-wal.{ext})
        var directory = Path.GetDirectoryName(_testDbPath)!;
        var fileNameNoExt = Path.GetFileNameWithoutExtension(_testDbPath);
        var ext = Path.GetExtension(_testDbPath).TrimStart('.');
        var walPath = Path.Combine(directory, $"{fileNameNoExt}-wal.{ext}");

        var dbBackup = File.ReadAllBytes(_testDbPath);

        // Step 2: Add more data (use new IDs to avoid unique constraint violation)
        using (var engine = new TinyDbEngine(_testDbPath, new TinyDbOptions { EnableJournaling = true }))
        {
            var col = engine.GetCollection<RecoveryDataItem>();
            for(int i=11; i<=15; i++) col.Insert(new RecoveryDataItem { Id = i, Value = $"V{i}" });

            // Backup WAL while active (simulate crash state before full flush)
            File.Copy(walPath, walPath + ".bak", true);
        }

        // Step 3: Rollback DB file and restore WAL
        File.WriteAllBytes(_testDbPath, dbBackup);
        File.Copy(walPath + ".bak", walPath, true);

        // Step 4: Open Engine
        using (var engine = new TinyDbEngine(_testDbPath, new TinyDbOptions { EnableJournaling = true }))
        {
            var col = engine.GetCollection<RecoveryDataItem>();
            var all = col.FindAll().ToList();

            // Should include recovered items
            await Assert.That(all.Count).IsGreaterThan(1);
            await Assert.That(all.Any(x => x.Id == 4)).IsTrue();
        }
    }

    [Test]
    public async Task Engine_Should_Reopen_ExistingDatabase_WithPersistedPageSize()
    {
        using (var engine = new TinyDbEngine(_testDbPath, new TinyDbOptions { PageSize = 4096 }))
        {
            var collection = engine.GetCollection<RecoveryDataItem>();
            collection.Insert(new RecoveryDataItem { Id = 1, Value = "legacy-page-size" });
        }

        using (var engine = new TinyDbEngine(_testDbPath))
        {
            await Assert.That(engine.Options.PageSize).IsEqualTo(4096u);

            var collection = engine.GetCollection<RecoveryDataItem>();
            var item = collection.FindById(1);

            await Assert.That(item).IsNotNull();
            await Assert.That(item!.Value).IsEqualTo("legacy-page-size");
        }

        using (var engine = new TinyDbEngine(_testDbPath))
        {
            await Assert.That(engine.Options.PageSize).IsEqualTo(4096u);

            var collection = engine.GetCollection<RecoveryDataItem>();
            var item = collection.FindById(1);

            await Assert.That(item).IsNotNull();
            await Assert.That(item!.Value).IsEqualTo("legacy-page-size");
        }
    }

    [Test]
    public async Task Engine_Should_WriteValidHeader_DuringInitialCreation()
    {
        using var engine = new TinyDbEngine(_testDbPath);

        byte[] bytes;
        using (var stream = new FileStream(_testDbPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
        {
            bytes = new byte[stream.Length];
            _ = stream.Read(bytes, 0, bytes.Length);
        }

        var headerBytes = bytes.Skip(Page.DataStartOffset).Take(DatabaseHeader.Size).ToArray();
        var header = DatabaseHeader.FromByteArray(headerBytes);

        await Assert.That(header.IsValid()).IsTrue();
        await Assert.That(header.CollectionInfoPage).IsNotEqualTo(0u);
        await Assert.That(header.IndexInfoPage).IsNotEqualTo(0u);
        await Assert.That(header.TotalPages).IsGreaterThanOrEqualTo(3u);
    }

    [Test]
    public async Task Engine_Should_IgnoreStaleWal_WhenMainDatabaseWasDeleted()
    {
        var options = new TinyDbOptions { EnableJournaling = true };

        using (var engine = new TinyDbEngine(_testDbPath, options))
        {
            var collection = engine.GetCollection<RecoveryDataItem>();
            collection.Insert(new RecoveryDataItem { Id = 1, Value = "stale-wal" });
        }

        var directory = Path.GetDirectoryName(_testDbPath)!;
        var fileNameNoExt = Path.GetFileNameWithoutExtension(_testDbPath);
        var ext = Path.GetExtension(_testDbPath).TrimStart('.');
        var walPath = Path.Combine(directory, $"{fileNameNoExt}-wal.{ext}");

        await Assert.That(File.Exists(walPath)).IsTrue();
        File.Delete(_testDbPath);

        using (var engine = new TinyDbEngine(_testDbPath, options))
        {
            var collection = engine.GetCollection<RecoveryDataItem>();
            await Assert.That(collection.FindAll().Any(item => item.Value == "stale-wal")).IsFalse();
        }

        using (var engine = new TinyDbEngine(_testDbPath, options))
        {
            var collection = engine.GetCollection<RecoveryDataItem>();
            await Assert.That(collection.FindAll().Any(item => item.Value == "stale-wal")).IsFalse();
        }
    }
}

[Entity("data")]
public class RecoveryDataItem
{
    public int Id { get; set; }
    public string Value { get; set; } = "";
}
