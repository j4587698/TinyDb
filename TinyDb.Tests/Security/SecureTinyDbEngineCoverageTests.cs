using TinyDb.Bson;
using TinyDb.Security;
using TinyDb.Core;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Security;

public class SecureTinyDbEngineCoverageTests : IDisposable
{
    private readonly List<string> _dbFiles = new();

    private string GetDbPath()
    {
        var path = $"test_secure_engine_{Guid.NewGuid():N}.db";
        _dbFiles.Add(path);
        return path;
    }

    public void Dispose()
    {
        foreach (var file in _dbFiles)
        {
            if (File.Exists(file)) try { File.Delete(file); } catch { }
        }
    }

    [Test]
    public async Task Constructor_InvalidPasswords_ShouldThrow()
    {
        var path = GetDbPath();
        await Assert.That(() => new SecureTinyDbEngine(path, "123")).Throws<ArgumentException>();
        await Assert.That(() => new SecureTinyDbEngine(path, "   ")).Throws<ArgumentException>();
    }

    [Test]
    public async Task Constructor_NullFilePath_ShouldThrow()
    {
        await Assert.That(() => new SecureTinyDbEngine(null!, "password123"))
            .Throws<ArgumentNullException>();

        await Assert.That(() => new SecureTinyDbEngine(null!))
            .Throws<ArgumentNullException>();
    }

    [Test]
    public async Task Constructor_PasswordProvided_ButDbNotSecure_ShouldAutoSecure()
    {
        var path = GetDbPath();
        // Create normal db first
        using (var engine = new TinyDbEngine(path)) { }
        
        // SecureTinyDbEngine should auto-secure it because TinyDbEngine does so
        using var secureEngine = new SecureTinyDbEngine(path, "password123");
        await Assert.That(secureEngine.IsAuthenticated).IsTrue();
        await Assert.That(secureEngine.IsPasswordProtected()).IsTrue();
    }

    [Test]
    public async Task Constructor_NoPassword_ButDbIsSecure_ShouldThrow()
    {
        var path = GetDbPath();
        // Create secure db
        using (var engine = new TinyDbEngine(path))
        {
            DatabaseSecurity.CreateSecureDatabase(engine, "password123");
        }

        await Assert.That(() => new SecureTinyDbEngine(path))
            .Throws<UnauthorizedAccessException>();
    }

    [Test]
    public async Task SetPassword_AlreadySecure_ShouldThrow()
    {
        var path = GetDbPath();
        using var secureEngine = new SecureTinyDbEngine(path, "password123", createIfNotExists: true);
        
        await Assert.That(() => secureEngine.SetPassword("newpassword"))
            .Throws<InvalidOperationException>();
    }

    [Test]
    public async Task Operations_AfterDispose_ShouldThrow()
    {
        var path = GetDbPath();
        var secureEngine = new SecureTinyDbEngine(path, "password123", createIfNotExists: true);
        secureEngine.Dispose();
        
        await Assert.That(() => secureEngine.GetCollectionNames()).Throws<ObjectDisposedException>();
        await Assert.That(() => secureEngine.IsPasswordProtected()).Throws<ObjectDisposedException>();
        await Assert.That(() => secureEngine.CollectionExists("test")).Throws<ObjectDisposedException>();
        await Assert.That(() => secureEngine.DropCollection("test")).Throws<ObjectDisposedException>();
        await Assert.That(() => secureEngine.GetDatabaseStats()).Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task BasicOperations_Coverage()
    {
        var path = GetDbPath();
        using var secureEngine = new SecureTinyDbEngine(path, "password123", createIfNotExists: true);
        
        await Assert.That(secureEngine.IsAuthenticated).IsTrue();
        await Assert.That(secureEngine.IsPasswordProtected()).IsTrue();
        await Assert.That(secureEngine.FilePath).IsEqualTo(path);
        await Assert.That(secureEngine.Engine).IsNotNull();
        
        await Assert.That(secureEngine.GetCollectionNames()).IsEmpty();
        await Assert.That(secureEngine.CollectionExists("test")).IsFalse();
        
        await Assert.That(secureEngine.GetDatabaseStats()).IsNotNull();
    }

    [Test]
    public async Task DropCollection_ShouldWork_WhenAuthenticated()
    {
        var path = GetDbPath();
        using var secureEngine = new SecureTinyDbEngine(path, "password123", createIfNotExists: true);

        var col = secureEngine.Engine.GetBsonCollection("test");
        col.Insert(new BsonDocument().Set("_id", 1).Set("x", 1));

        await Assert.That(secureEngine.CollectionExists("test")).IsTrue();

        secureEngine.DropCollection("test");

        await Assert.That(secureEngine.CollectionExists("test")).IsFalse();
    }

    [Test]
    public async Task Operations_WhenNotAuthenticated_ShouldThrow()
    {
        var path = GetDbPath();
        using var secureEngine = new SecureTinyDbEngine(path, "password123", createIfNotExists: true);

        var field = typeof(SecureTinyDbEngine)
            .GetField("_isAuthenticated", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);

        field!.SetValue(secureEngine, false);

        await Assert.That(() => secureEngine.CollectionExists("test"))
            .Throws<UnauthorizedAccessException>();
    }
}
