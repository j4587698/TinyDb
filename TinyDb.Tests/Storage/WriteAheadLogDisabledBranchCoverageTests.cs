using System;
using System.IO;
using System.Reflection;
using System.Threading.Tasks;
using TinyDb.Storage;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Storage;

public class WriteAheadLogDisabledBranchCoverageTests
{
    [Test]
    public async Task GenerateWalFilePath_WhenDatabasePathHasNoDirectory_ShouldFallbackToEmptyDirectory()
    {
        var method = typeof(WriteAheadLog).GetMethod("GenerateWalFilePath", BindingFlags.NonPublic | BindingFlags.Static);
        await Assert.That(method).IsNotNull();

        var walPath = (string)method!.Invoke(null, new object[] { "db.db", "{name}-wal.{ext}" })!;
        await Assert.That(walPath).IsEqualTo("db-wal.db");
    }

    [Test]
    public async Task GenerateWalFilePath_WhenDatabasePathIsRoot_ShouldHandleNullDirectoryName()
    {
        var method = typeof(WriteAheadLog).GetMethod("GenerateWalFilePath", BindingFlags.NonPublic | BindingFlags.Static);
        await Assert.That(method).IsNotNull();

        var root = Path.GetPathRoot(Environment.CurrentDirectory)!;
        var walPath = (string)method!.Invoke(null, new object[] { root, "{name}-wal.{ext}" })!;

        await Assert.That(walPath).IsEqualTo("-wal.");
    }

    [Test]
    public async Task DisabledWal_AppendAndTruncate_ShouldReturnEarly()
    {
        var dir = Path.Combine(Path.GetTempPath(), $"tinydb_wal_disabled_{Guid.NewGuid():N}");
        var dbPath = Path.Combine(dir, "db.db");

        try
        {
            Directory.CreateDirectory(dir);
            using var wal = new WriteAheadLog(dbPath, pageSize: 4096, enabled: false);

            wal.AppendPage(null!);
            await wal.AppendPageAsync(null!);
            await wal.TruncateAsync();

            await Assert.That(wal.IsEnabled).IsFalse();
        }
        finally
        {
            try { if (Directory.Exists(dir)) Directory.Delete(dir, true); } catch { }
        }
    }
}
