using System;
using TinyDb.Core;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Core;

public class TinyDbOptionsFullTests
{
    [Test]
    public async Task TinyDbOptions_Properties_ShouldWork()
    {
        var options = new TinyDbOptions
        {
            PageSize = 4096,
            CacheSize = 500,
            EnableJournaling = true,
            WalFileNameFormat = "test.wal",
            EnableAutoCheckpoint = false,
            Timeout = TimeSpan.FromSeconds(10),
            ReadOnly = true,
            StrictMode = false,
            DatabaseName = "MyDb",
            UserData = new byte[10],
            EnableCompression = true,
            EnableEncryption = true,
            EncryptionKey = new byte[16],
            Password = "pass",
            MaxTransactionSize = 5000,
            MaxTransactions = 50,
            TransactionTimeout = TimeSpan.FromMinutes(1),
            WriteConcern = WriteConcern.Journaled,
            BackgroundFlushInterval = TimeSpan.FromMilliseconds(200)
        };

        await Assert.That(options.PageSize).IsEqualTo(4096u);
        await Assert.That(options.CacheSize).IsEqualTo(500);
        await Assert.That(options.EnableJournaling).IsTrue();
        await Assert.That(options.ReadOnly).IsTrue();
        await Assert.That(options.DatabaseName).IsEqualTo("MyDb");
        await Assert.That(options.WriteConcern).IsEqualTo(WriteConcern.Journaled);
        await Assert.That(options.SynchronousWrites).IsFalse();
        
        options.SynchronousWrites = true;
        await Assert.That(options.WriteConcern).IsEqualTo(WriteConcern.Synced);
        
        await Assert.That(options.ToString()).Contains("PageSize=4096");
    }

    [Test]
    public async Task TinyDbOptions_Validate_Should_Throw_On_Invalid_Values()
    {
        var options = new TinyDbOptions();
        
        options.PageSize = 1000; // Not power of 2 and too small
        await Assert.That(() => options.Validate()).Throws<ArgumentException>();
        
        options.PageSize = 4096;
        options.CacheSize = 0;
        await Assert.That(() => options.Validate()).Throws<ArgumentException>();
        
        options.CacheSize = 1000;
        options.DatabaseName = "";
        await Assert.That(() => options.Validate()).Throws<ArgumentException>();
        
        options.DatabaseName = "valid";
        options.EnableEncryption = true;
        options.EncryptionKey = null;
        await Assert.That(() => options.Validate()).Throws<ArgumentException>();
        
        options.EncryptionKey = new byte[8]; // Too short
        await Assert.That(() => options.Validate()).Throws<ArgumentException>();
    }

    [Test]
    public async Task TinyDbOptions_Clone_ShouldWork()
    {
        var options = new TinyDbOptions { DatabaseName = "Original" };
        var clone = options.Clone();
        
        await Assert.That(clone.DatabaseName).IsEqualTo("Original");
        clone.DatabaseName = "Changed";
        await Assert.That(options.DatabaseName).IsEqualTo("Original");
    }
}
