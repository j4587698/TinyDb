using TinyDb.Core;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Core;

public class TinyDbOptionsTests
{
    [Test]
    public async Task Default_Options_Should_Be_Valid()
    {
        var options = new TinyDbOptions();
        options.Validate();
        
        await Assert.That(options.PageSize).IsEqualTo(TinyDbOptions.DefaultPageSize);
        await Assert.That(options.EnableJournaling).IsFalse();
    }

    [Test]
    public async Task Validate_Small_PageSize_Should_Throw()
    {
        var options = new TinyDbOptions { PageSize = 1024 };
        var ex = await Assert.That(() => options.Validate()).Throws<ArgumentException>();
        await Assert.That(ex!.Message).Contains("Page size must be at least 4096 bytes");
    }

    [Test]
    public async Task SynchronousWrites_Property_Should_Sync_With_WriteConcern()
    {
        var options = new TinyDbOptions();
        
        options.SynchronousWrites = true;
        await Assert.That(options.WriteConcern).IsEqualTo(WriteConcern.Synced);
        
        options.SynchronousWrites = false;
        await Assert.That(options.WriteConcern).IsEqualTo(WriteConcern.Journaled);
    }

    [Test]
    public async Task Properties_Should_Store_Values()
    {
        var options = new TinyDbOptions
        {
            DatabaseName = "CustomDB",
            EnableCompression = true,
            EnableEncryption = true,
            EncryptionKey = new byte[32],
            Password = "pass",
            CacheSize = 500,
            ReadOnly = true,
            StrictMode = false
        };
        
        await Assert.That(options.DatabaseName).IsEqualTo("CustomDB");
        await Assert.That(options.EnableCompression).IsTrue();
        await Assert.That(options.EnableEncryption).IsTrue();
        await Assert.That(options.Password).IsEqualTo("pass");
        await Assert.That(options.CacheSize).IsEqualTo(500);
        await Assert.That(options.ReadOnly).IsTrue();
        await Assert.That(options.StrictMode).IsFalse();
    }
}