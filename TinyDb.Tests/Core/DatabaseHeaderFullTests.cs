using System;
using System.Runtime.InteropServices;
using TinyDb.Core;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Core;

public class DatabaseHeaderFullTests
{
    [Test]
    public async Task DatabaseHeader_ToAndFromByteArray_ShouldWork()
    {
        var header = new DatabaseHeader();
        header.PageSize = 8192;
        header.TotalPages = 100;
        
        var bytes = header.ToByteArray();
        await Assert.That(bytes.Length).IsEqualTo(DatabaseHeader.Size);
        
        var header2 = DatabaseHeader.FromByteArray(bytes);
        await Assert.That(header2.PageSize).IsEqualTo(8192u);
        await Assert.That(header2.TotalPages).IsEqualTo(100u);
        await Assert.That(header2.Magic).IsEqualTo(DatabaseHeader.MagicNumber);
    }

    [Test]
    public async Task DatabaseHeader_CalculateChecksum_ShouldWork()
    {
        var header = new DatabaseHeader();
        var checksum1 = header.CalculateChecksum();
        
        header.TotalPages = 2;
        var checksum2 = header.CalculateChecksum();
        
        await Assert.That(checksum1).IsNotEqualTo(checksum2);
    }

    [Test]
    public async Task DatabaseHeader_SecurityMetadata_ShouldWork()
    {
        var header = new DatabaseHeader();
        var salt = new byte[16];
        new Random().NextBytes(salt);
        var hash = new byte[32];
        new Random().NextBytes(hash);
        
        var metadata = new TinyDb.Core.DatabaseSecurityMetadata(salt, hash);
        header.SetSecurityMetadata(metadata);
        
        await Assert.That(header.TryGetSecurityMetadata(out var metadata2)).IsTrue();
        await Assert.That(metadata2.Salt.SequenceEqual(salt)).IsTrue();
        await Assert.That(metadata2.KeyHash.SequenceEqual(hash)).IsTrue();
    }

    [Test]
    public async Task DatabaseHeader_FreePageCount_ShouldRoundTripAndSurviveSecurityMetadata()
    {
        var header = new DatabaseHeader();
        header.FreePageCount = 42;

        var salt = new byte[16];
        var hash = new byte[32];
        new Random().NextBytes(salt);
        new Random().NextBytes(hash);
        header.SetSecurityMetadata(new DatabaseSecurityMetadata(salt, hash));

        var replayed = DatabaseHeader.FromByteArray(header.ToByteArray());

        await Assert.That(replayed.HasFreePageCount).IsTrue();
        await Assert.That(replayed.FreePageCount).IsEqualTo(42u);
        await Assert.That(replayed.TryGetSecurityMetadata(out var metadata)).IsTrue();
        await Assert.That(metadata.Salt.SequenceEqual(salt)).IsTrue();
        await Assert.That(metadata.KeyHash.SequenceEqual(hash)).IsTrue();
        await Assert.That(DatabaseHeader.ReservedHeaderExtensionBytes).IsGreaterThanOrEqualTo(8);
        await Assert.That(DatabaseHeader.TrailingHeaderExtensionBytes).IsEqualTo(11);
    }
}
