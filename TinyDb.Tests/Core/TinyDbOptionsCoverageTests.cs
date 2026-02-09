using TinyDb.Core;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Core;

public class TinyDbOptionsCoverageTests
{
    [Test]
    public async Task Validate_InvalidPageSize_ShouldThrow()
    {
        var options = new TinyDbOptions { PageSize = 4095 };
        await Assert.That(() => options.Validate()).Throws<ArgumentException>();
        
        options.PageSize = 4097; // Not power of 2
        await Assert.That(() => options.Validate()).Throws<ArgumentException>();
    }

    [Test]
    public async Task Validate_InvalidCacheSize_ShouldThrow()
    {
        var options = new TinyDbOptions { CacheSize = 0 };
        await Assert.That(() => options.Validate()).Throws<ArgumentException>();
    }

    [Test]
    public async Task Validate_InvalidTimeout_ShouldThrow()
    {
        var options = new TinyDbOptions { Timeout = TimeSpan.Zero };
        await Assert.That(() => options.Validate()).Throws<ArgumentException>();
    }

    [Test]
    public async Task Validate_InvalidDatabaseName_ShouldThrow()
    {
        var options = new TinyDbOptions { DatabaseName = "" };
        await Assert.That(() => options.Validate()).Throws<ArgumentException>();
        
        options.DatabaseName = new string('a', 100);
        await Assert.That(() => options.Validate()).Throws<ArgumentException>();
    }

    [Test]
    public async Task Validate_InvalidUserData_ShouldThrow()
    {
        var options = new TinyDbOptions { UserData = new byte[100] };
        await Assert.That(() => options.Validate()).Throws<ArgumentException>();
    }

    [Test]
    public async Task Validate_Encryption_ShouldWork()
    {
        var options = new TinyDbOptions { EnableEncryption = true, EncryptionKey = null };
        await Assert.That(() => options.Validate()).Throws<ArgumentException>();
        
        options.EncryptionKey = new byte[10]; // Wrong length
        await Assert.That(() => options.Validate()).Throws<ArgumentException>();
        
        options.EncryptionKey = new byte[32]; // Correct AES-256 length
        options.Validate(); // Should pass
    }

    [Test]
    public async Task Clone_And_ToString_Coverage()
    {
        var options = new TinyDbOptions 
        { 
            DatabaseName = "MyDb", 
            UserData = new byte[] { 1, 2, 3 },
            EnableJournaling = true
        };
        
        var clone = options.Clone();
        await Assert.That(clone.DatabaseName).IsEqualTo(options.DatabaseName);
        await Assert.That(clone.UserData).IsNotNull();
        await Assert.That(clone.UserData!.Length).IsEqualTo(3);
        await Assert.That(clone.EnableJournaling).IsTrue();
        
        var str = options.ToString();
        await Assert.That(str).Contains("Journaling=True");
    }

    [Test]
    public async Task SynchronousWrites_Property_Coverage()
    {
        var options = new TinyDbOptions();
        options.SynchronousWrites = true;
        await Assert.That(options.WriteConcern).IsEqualTo(WriteConcern.Synced);
        await Assert.That(options.SynchronousWrites).IsTrue();
        
        options.SynchronousWrites = false;
        await Assert.That(options.WriteConcern).IsEqualTo(WriteConcern.Journaled);
        await Assert.That(options.SynchronousWrites).IsFalse();
    }
}
