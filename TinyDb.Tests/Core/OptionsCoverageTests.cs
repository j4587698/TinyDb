using System;
using System.Linq;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TinyDb.Core;

namespace TinyDb.Tests.Core;

public class OptionsCoverageTests
{
    [Test]
    public async Task Validate_PageSize_ShouldCheckConstraints()
    {
        var options = new TinyDbOptions();
        
        // Too small
        options.PageSize = 1024;
        await Assert.ThrowsAsync<ArgumentException>(() => { options.Validate(); return Task.CompletedTask; });

        // Not power of two (e.g. 4097)
        options.PageSize = 4097;
        await Assert.ThrowsAsync<ArgumentException>(() => { options.Validate(); return Task.CompletedTask; });

        // Valid
        options.PageSize = 4096;
        options.Validate();
    }

    [Test]
    public async Task Validate_CacheSize_ShouldCheckPositive()
    {
        var options = new TinyDbOptions { CacheSize = 0 };
        await Assert.ThrowsAsync<ArgumentException>(() => { options.Validate(); return Task.CompletedTask; });

        options.CacheSize = -1;
        await Assert.ThrowsAsync<ArgumentException>(() => { options.Validate(); return Task.CompletedTask; });
    }

    [Test]
    public async Task Validate_Timeout_ShouldCheckPositive()
    {
        var options = new TinyDbOptions { Timeout = TimeSpan.Zero };
        await Assert.ThrowsAsync<ArgumentException>(() => { options.Validate(); return Task.CompletedTask; });

        options.Timeout = TimeSpan.FromSeconds(-1);
        await Assert.ThrowsAsync<ArgumentException>(() => { options.Validate(); return Task.CompletedTask; });
    }

    [Test]
    public async Task Validate_DatabaseName_ShouldCheckConstraints()
    {
        var options = new TinyDbOptions { DatabaseName = "" };
        await Assert.ThrowsAsync<ArgumentException>(() => { options.Validate(); return Task.CompletedTask; });

        options.DatabaseName = new string('a', 64); // > 63 bytes
        await Assert.ThrowsAsync<ArgumentException>(() => { options.Validate(); return Task.CompletedTask; });
    }

    [Test]
    public async Task Validate_UserData_ShouldCheckLength()
    {
        var options = new TinyDbOptions { UserData = new byte[65] };
        await Assert.ThrowsAsync<ArgumentException>(() => { options.Validate(); return Task.CompletedTask; });
    }

    [Test]
    public async Task Validate_Encryption_ShouldCheckKey()
    {
        var options = new TinyDbOptions { EnableEncryption = true, EncryptionKey = null };
        await Assert.ThrowsAsync<ArgumentException>(() => { options.Validate(); return Task.CompletedTask; });

        options.EncryptionKey = Array.Empty<byte>();
        await Assert.ThrowsAsync<ArgumentException>(() => { options.Validate(); return Task.CompletedTask; });

        options.EncryptionKey = new byte[15]; // < 16
        await Assert.ThrowsAsync<ArgumentException>(() => { options.Validate(); return Task.CompletedTask; });

        options.EncryptionKey = new byte[16];
        options.Validate();
    }

    [Test]
    public async Task Validate_TransactionSettings_ShouldCheckPositive()
    {
        var options = new TinyDbOptions { MaxTransactionSize = 0 };
        await Assert.ThrowsAsync<ArgumentException>(() => { options.Validate(); return Task.CompletedTask; });

        options.MaxTransactionSize = 1;
        options.MaxTransactions = 0;
        await Assert.ThrowsAsync<ArgumentException>(() => { options.Validate(); return Task.CompletedTask; });

        options.MaxTransactions = 1;
        options.TransactionTimeout = TimeSpan.Zero;
        await Assert.ThrowsAsync<ArgumentException>(() => { options.Validate(); return Task.CompletedTask; });
    }

    [Test]
    public async Task Validate_FlushSettings_ShouldCheckConstraints()
    {
        var options = new TinyDbOptions { BackgroundFlushInterval = TimeSpan.FromSeconds(-1) };
        await Assert.ThrowsAsync<ArgumentException>(() => { options.Validate(); return Task.CompletedTask; });

        options.BackgroundFlushInterval = System.Threading.Timeout.InfiniteTimeSpan;
        options.Validate();

        options.BackgroundFlushInterval = TimeSpan.FromSeconds(1);
        options.JournalFlushDelay = TimeSpan.FromSeconds(-1);
        await Assert.ThrowsAsync<ArgumentException>(() => { options.Validate(); return Task.CompletedTask; });
    }

    [Test]
    public async Task Clone_ShouldCopyAllProperties()
    {
        var original = new TinyDbOptions
        {
            PageSize = 4096,
            CacheSize = 500,
            EnableJournaling = true,
            WalFileNameFormat = "custom-{name}",
            EnableAutoCheckpoint = false,
            Timeout = TimeSpan.FromSeconds(10),
            ReadOnly = true,
            StrictMode = false,
            DatabaseName = "TestDb",
            UserData = new byte[] { 1, 2, 3 },
            EnableCompression = true,
            EnableEncryption = true,
            EncryptionKey = new byte[16],
            Password = "pass",
            MaxTransactionSize = 5000,
            MaxTransactions = 50,
            TransactionTimeout = TimeSpan.FromMinutes(1),
            WriteConcern = WriteConcern.Journaled,
            BackgroundFlushInterval = TimeSpan.FromSeconds(5),
            JournalFlushDelay = TimeSpan.Zero
        };

        var cloned = original.Clone();

        await Assert.That(cloned.PageSize).IsEqualTo(original.PageSize);
        await Assert.That(cloned.CacheSize).IsEqualTo(original.CacheSize);
        await Assert.That(cloned.EnableJournaling).IsEqualTo(original.EnableJournaling);
        await Assert.That(cloned.WalFileNameFormat).IsEqualTo(original.WalFileNameFormat);
        await Assert.That(cloned.EnableAutoCheckpoint).IsEqualTo(original.EnableAutoCheckpoint);
        await Assert.That(cloned.Timeout).IsEqualTo(original.Timeout);
        await Assert.That(cloned.ReadOnly).IsEqualTo(original.ReadOnly);
        await Assert.That(cloned.StrictMode).IsEqualTo(original.StrictMode);
        await Assert.That(cloned.DatabaseName).IsEqualTo(original.DatabaseName);
        await Assert.That(cloned.UserData.SequenceEqual(original.UserData)).IsTrue();
        await Assert.That(cloned.UserData).IsNotSameReferenceAs(original.UserData); // Deep copy check
        await Assert.That(cloned.EnableCompression).IsEqualTo(original.EnableCompression);
        await Assert.That(cloned.EnableEncryption).IsEqualTo(original.EnableEncryption);
        await Assert.That(cloned.EncryptionKey.SequenceEqual(original.EncryptionKey)).IsTrue();
        await Assert.That(cloned.Password).IsEqualTo(original.Password);
        await Assert.That(cloned.MaxTransactionSize).IsEqualTo(original.MaxTransactionSize);
        await Assert.That(cloned.MaxTransactions).IsEqualTo(original.MaxTransactions);
        await Assert.That(cloned.TransactionTimeout).IsEqualTo(original.TransactionTimeout);
        await Assert.That(cloned.WriteConcern).IsEqualTo(original.WriteConcern);
        await Assert.That(cloned.BackgroundFlushInterval).IsEqualTo(original.BackgroundFlushInterval);
        await Assert.That(cloned.JournalFlushDelay).IsEqualTo(original.JournalFlushDelay);
    }

    [Test]
    public async Task ToString_ShouldReturnCorrectFormat()
    {
        var options = new TinyDbOptions();
        var str = options.ToString();
        await Assert.That(str).Contains("TinyDbOptions");
        await Assert.That(str).Contains("PageSize=");
    }

    [Test]
    public async Task SynchronousWrites_ShouldMapToWriteConcern()
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
