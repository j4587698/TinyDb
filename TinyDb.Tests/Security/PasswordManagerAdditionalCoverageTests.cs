using TinyDb.Security;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Security;

public sealed class PasswordManagerAdditionalCoverageTests : IDisposable
{
    private readonly string _dbPath;

    public PasswordManagerAdditionalCoverageTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"pwd_mgr_{Guid.NewGuid():N}.db");
    }

    public void Dispose()
    {
        try { if (File.Exists(_dbPath)) File.Delete(_dbPath); } catch { }
    }

    [Test]
    public async Task IsPasswordProtected_WhenHeaderHasSecurityMetadata_ShouldReturnTrue()
    {
        var header = new TinyDb.Core.DatabaseHeader();
        header.SetSecurityMetadata(new TinyDb.Core.DatabaseSecurityMetadata(
            new byte[TinyDb.Core.DatabaseSecurityMetadata.SaltLength],
            new byte[TinyDb.Core.DatabaseSecurityMetadata.KeyHashLength]));

        File.WriteAllBytes(_dbPath, header.ToByteArray());

        await Assert.That(PasswordManager.IsPasswordProtected(_dbPath)).IsTrue();
    }
}
