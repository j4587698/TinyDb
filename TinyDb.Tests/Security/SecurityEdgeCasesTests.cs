using System;
using System.IO;
using TinyDb.Core;
using TinyDb.Security;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Security;

public class SecurityEdgeCasesTests : IDisposable
{
    private readonly string _dbPath;
    private readonly TinyDbEngine _engine;

    public SecurityEdgeCasesTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"sec_edge_{Guid.NewGuid()}.db");
        _engine = new TinyDbEngine(_dbPath);
    }

    public void Dispose()
    {
        _engine.Dispose();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
    }

    [Test]
    public async Task ChangePassword_Unprotected_ShouldFail()
    {
        // Unprotected DB
        var result = DatabaseSecurity.ChangePassword(_engine, "old", "newpass");
        await Assert.That(result).IsFalse();
    }

    [Test]
    public async Task RemovePassword_Unprotected_ShouldFail()
    {
        var result = DatabaseSecurity.RemovePassword(_engine, "pass");
        await Assert.That(result).IsFalse();
    }

    [Test]
    public async Task HasSecurityMetadata_ShortFile_ShouldReturnFalse()
    {
        var shortPath = Path.Combine(Path.GetTempPath(), $"short_{Guid.NewGuid()}.db");
        await File.WriteAllBytesAsync(shortPath, new byte[10]); // Less than Header Size
        
        var has = DatabaseSecurity.HasSecurityMetadata(shortPath);
        await Assert.That(has).IsFalse();
        
        File.Delete(shortPath);
    }
    
    [Test]
    public async Task HasSecurityMetadata_MissingFile_ShouldReturnFalse()
    {
        var missingPath = Path.Combine(Path.GetTempPath(), $"missing_{Guid.NewGuid()}.db");
        var has = DatabaseSecurity.HasSecurityMetadata(missingPath);
        await Assert.That(has).IsFalse();
    }

    [Test]
    public async Task CreateSecureDatabase_WeakPassword_ShouldThrow()
    {
        await Assert.That(() => DatabaseSecurity.CreateSecureDatabase(_engine, "123")).Throws<ArgumentException>();
        await Assert.That(() => DatabaseSecurity.CreateSecureDatabase(_engine, "")).Throws<ArgumentException>();
    }
}
