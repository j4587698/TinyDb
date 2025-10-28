using System.Linq;
using SimpleDb.Core;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace SimpleDb.Tests.Core;

public class SimpleDbOptionsTests
{
    private static SimpleDbOptions CreateValidOptions() => new()
    {
        DatabaseName = "OptionsTestDb",
        PageSize = 4096,
        CacheSize = 128,
        Timeout = TimeSpan.FromSeconds(5),
        EnableJournaling = true,
        UserData = Array.Empty<byte>(),
        MaxTransactionSize = 256
    };

    [Test]
    public Task Validate_Should_Pass_For_Default_Options()
    {
        var options = CreateValidOptions();

        options.Validate();

        return Task.CompletedTask;
    }

    [Test]
    public async Task Validate_Should_Throw_For_PageSize_Less_Than_Minimum()
    {
        var options = CreateValidOptions();
        options.PageSize = 1024;

        await Assert.That(() => options.Validate()).Throws<ArgumentException>();
    }

    [Test]
    public async Task Validate_Should_Throw_For_PageSize_Not_Power_Of_Two()
    {
        var options = CreateValidOptions();
        options.PageSize = 5000;

        await Assert.That(() => options.Validate()).Throws<ArgumentException>();
    }

    [Test]
    public async Task Validate_Should_Throw_When_Encryption_Enabled_Without_Key()
    {
        var options = CreateValidOptions();
        options.EnableEncryption = true;
        options.EncryptionKey = Array.Empty<byte>();

        await Assert.That(() => options.Validate()).Throws<ArgumentException>();
    }

    [Test]
    public async Task Validate_Should_Throw_When_UserData_Exceeds_Limit()
    {
        var options = CreateValidOptions();
        options.UserData = new byte[65];

        await Assert.That(() => options.Validate()).Throws<ArgumentException>();
    }

    [Test]
    public async Task Clone_Should_Create_Independent_Copy()
    {
        var options = new SimpleDbOptions
        {
            DatabaseName = "CloneSource",
            PageSize = 8192,
            CacheSize = 256,
            EnableJournaling = false,
            Timeout = TimeSpan.FromSeconds(12),
            UserData = new byte[] { 1, 2, 3, 4 },
            EnableEncryption = true,
            EncryptionKey = Enumerable.Range(1, 16).Select(i => (byte)i).ToArray(),
            MaxTransactionSize = 512,
            SynchronousWrites = false
        };

        var clone = options.Clone();

        await Assert.That(clone.DatabaseName).IsEqualTo(options.DatabaseName);
        await Assert.That(clone.PageSize).IsEqualTo(options.PageSize);
        await Assert.That(clone.CacheSize).IsEqualTo(options.CacheSize);
        await Assert.That(clone.EnableJournaling).IsEqualTo(options.EnableJournaling);
        await Assert.That(clone.Timeout).IsEqualTo(options.Timeout);
        await Assert.That(clone.MaxTransactionSize).IsEqualTo(options.MaxTransactionSize);
        await Assert.That(clone.SynchronousWrites).IsEqualTo(options.SynchronousWrites);

        await Assert.That(clone.UserData.SequenceEqual(options.UserData)).IsTrue();
        await Assert.That(clone.EncryptionKey!.SequenceEqual(options.EncryptionKey!)).IsTrue();

        clone.UserData[0] = 9;
        clone.EncryptionKey![0] = 9;

        await Assert.That(ReferenceEquals(clone.UserData, options.UserData)).IsFalse();
        await Assert.That(ReferenceEquals(clone.EncryptionKey, options.EncryptionKey)).IsFalse();
        await Assert.That(options.UserData[0]).IsEqualTo((byte)1);
        await Assert.That(options.EncryptionKey![0]).IsEqualTo((byte)1);
    }
}
