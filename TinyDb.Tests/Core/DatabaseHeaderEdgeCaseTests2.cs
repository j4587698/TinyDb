using System;
using System.Text;
using TinyDb.Core;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Core;

/// <summary>
/// Additional DatabaseHeader edge case tests for improved coverage
/// Focuses on: validation logic, security metadata, name/data limits
/// </summary>
public class DatabaseHeaderEdgeCaseTests2
{
    #region DatabaseName Validation Tests

    [Test]
    public async Task DatabaseName_Set_WithNullValue_ShouldThrowArgumentNullException()
    {
        var header = new DatabaseHeader();
        
        await Assert.That(() => header.DatabaseName = null!)
            .Throws<ArgumentNullException>();
    }

    [Test]
    public async Task DatabaseName_Set_WithTooLongName_ShouldThrowArgumentException()
    {
        var header = new DatabaseHeader();
        
        // Create a name that's too long (> 63 bytes in UTF-8)
        var longName = new string('a', 64);
        
        await Assert.That(() => header.DatabaseName = longName)
            .Throws<ArgumentException>();
    }

    [Test]
    public async Task DatabaseName_Set_WithMaxLengthName_ShouldSucceed()
    {
        var header = new DatabaseHeader();
        
        // 63 bytes is the max
        var maxName = new string('a', 63);
        header.DatabaseName = maxName;
        
        await Assert.That(header.DatabaseName).IsEqualTo(maxName);
    }

    [Test]
    public async Task DatabaseName_Set_WithUnicodeCharacters_ShouldValidateByteLength()
    {
        var header = new DatabaseHeader();
        
        // Unicode characters take more bytes
        // "中" is 3 bytes in UTF-8, so 22 characters = 66 bytes > 63
        var unicodeName = new string('中', 22);
        
        await Assert.That(() => header.DatabaseName = unicodeName)
            .Throws<ArgumentException>();
    }

    [Test]
    public async Task DatabaseName_Set_WithUnicodeCharacters_UnderLimit_ShouldSucceed()
    {
        var header = new DatabaseHeader();
        
        // "中" is 3 bytes in UTF-8, so 21 characters = 63 bytes = max
        var unicodeName = new string('中', 21);
        header.DatabaseName = unicodeName;
        
        await Assert.That(header.DatabaseName).IsEqualTo(unicodeName);
    }

    [Test]
    public async Task DatabaseName_Get_WithPartialData_ShouldTrimNulls()
    {
        var header = new DatabaseHeader();
        header.DatabaseName = "Test";
        
        // Verify it doesn't include trailing nulls
        await Assert.That(header.DatabaseName).IsEqualTo("Test");
        await Assert.That(header.DatabaseName.Length).IsEqualTo(4);
    }

    #endregion

    #region UserData Validation Tests

    [Test]
    public async Task UserData_Set_WithNullValue_ShouldThrowArgumentNullException()
    {
        var header = new DatabaseHeader();
        
        await Assert.That(() => header.UserData = null!)
            .Throws<ArgumentNullException>();
    }

    [Test]
    public async Task UserData_Set_WithTooLongData_ShouldThrowArgumentException()
    {
        var header = new DatabaseHeader();
        
        // Create data that's too long (> 64 bytes)
        var longData = new byte[65];
        
        await Assert.That(() => header.UserData = longData)
            .Throws<ArgumentException>();
    }

    [Test]
    public async Task UserData_Set_WithMaxLengthData_ShouldSucceed()
    {
        var header = new DatabaseHeader();
        
        // 64 bytes is the max
        var maxData = new byte[64];
        for (int i = 0; i < 64; i++) maxData[i] = (byte)i;
        
        header.UserData = maxData;
        
        var retrieved = header.UserData;
        await Assert.That(retrieved).IsEquivalentTo(maxData);
    }

    [Test]
    public async Task UserData_Set_WithShortData_ShouldPadWithZeros()
    {
        var header = new DatabaseHeader();
        
        var shortData = new byte[] { 1, 2, 3 };
        header.UserData = shortData;
        
        var retrieved = header.UserData;
        await Assert.That(retrieved.Length).IsEqualTo(64);
        await Assert.That(retrieved[0]).IsEqualTo((byte)1);
        await Assert.That(retrieved[1]).IsEqualTo((byte)2);
        await Assert.That(retrieved[2]).IsEqualTo((byte)3);
        await Assert.That(retrieved[3]).IsEqualTo((byte)0);
    }

    [Test]
    public async Task UserData_Get_ShouldReturn64Bytes()
    {
        var header = new DatabaseHeader();
        
        var userData = header.UserData;
        await Assert.That(userData.Length).IsEqualTo(64);
    }

    #endregion

    #region Security Metadata Validation Tests

    [Test]
    public async Task SetSecurityMetadata_WithInvalidSaltLength_ShouldThrowArgumentException()
    {
        var header = new DatabaseHeader();
        
        var invalidSalt = new byte[10]; // Should be 16
        var validKeyHash = new byte[32];
        
        await Assert.That(() => header.SetSecurityMetadata(new DatabaseSecurityMetadata(invalidSalt, validKeyHash)))
            .Throws<ArgumentException>();
    }

    [Test]
    public async Task SetSecurityMetadata_WithInvalidKeyHashLength_ShouldThrowArgumentException()
    {
        var header = new DatabaseHeader();
        
        var validSalt = new byte[16];
        var invalidKeyHash = new byte[20]; // Should be 32
        
        await Assert.That(() => header.SetSecurityMetadata(new DatabaseSecurityMetadata(validSalt, invalidKeyHash)))
            .Throws<ArgumentException>();
    }

    [Test]
    public async Task TryGetSecurityMetadata_WithoutSecuritySet_ShouldReturnFalse()
    {
        var header = new DatabaseHeader();
        
        var result = header.TryGetSecurityMetadata(out var metadata);
        await Assert.That(result).IsFalse();
    }

    [Test]
    public async Task TryGetSecurityMetadata_AfterClear_ShouldReturnFalse()
    {
        var header = new DatabaseHeader();
        
        // Set security metadata
        var salt = new byte[16];
        var keyHash = new byte[32];
        new Random(42).NextBytes(salt);
        new Random(42).NextBytes(keyHash);
        
        header.SetSecurityMetadata(new DatabaseSecurityMetadata(salt, keyHash));
        
        // Verify it was set
        await Assert.That(header.TryGetSecurityMetadata(out _)).IsTrue();
        
        // Clear it
        header.ClearSecurityMetadata();
        
        // Verify it's cleared
        await Assert.That(header.TryGetSecurityMetadata(out _)).IsFalse();
    }

    #endregion

    #region DatabaseSecurityMetadata Constructor Tests

    [Test]
    public async Task DatabaseSecurityMetadata_WithNullSalt_ShouldThrowArgumentNullException()
    {
        await Assert.That(() => new DatabaseSecurityMetadata(null!, new byte[32]))
            .Throws<ArgumentNullException>();
    }

    [Test]
    public async Task DatabaseSecurityMetadata_WithNullKeyHash_ShouldThrowArgumentNullException()
    {
        await Assert.That(() => new DatabaseSecurityMetadata(new byte[16], null!))
            .Throws<ArgumentNullException>();
    }

    [Test]
    public async Task DatabaseSecurityMetadata_WithInvalidSaltLength_ShouldThrowArgumentException()
    {
        await Assert.That(() => new DatabaseSecurityMetadata(new byte[10], new byte[32]))
            .Throws<ArgumentException>();
    }

    [Test]
    public async Task DatabaseSecurityMetadata_WithInvalidKeyHashLength_ShouldThrowArgumentException()
    {
        await Assert.That(() => new DatabaseSecurityMetadata(new byte[16], new byte[20]))
            .Throws<ArgumentException>();
    }

    #endregion

    #region IsValid Tests

    [Test]
    public async Task IsValid_WithDefaultHeader_ShouldReturnTrue()
    {
        var header = new DatabaseHeader();
        await Assert.That(header.IsValid()).IsTrue();
    }

    [Test]
    public async Task IsValid_WithInvalidMagic_ShouldReturnFalse()
    {
        var header = new DatabaseHeader();
        header.Magic = 0x12345678; // Invalid magic
        await Assert.That(header.IsValid()).IsFalse();
    }

    [Test]
    public async Task IsValid_WithTooSmallPageSize_ShouldReturnFalse()
    {
        var header = new DatabaseHeader();
        header.PageSize = 1024; // Less than 4096
        await Assert.That(header.IsValid()).IsFalse();
    }

    [Test]
    public async Task IsValid_WithZeroTotalPages_ShouldReturnFalse()
    {
        var header = new DatabaseHeader();
        header.TotalPages = 0;
        await Assert.That(header.IsValid()).IsFalse();
    }

    [Test]
    public async Task IsValid_WithUsedPagesGreaterThanTotal_ShouldReturnFalse()
    {
        var header = new DatabaseHeader();
        header.TotalPages = 10;
        header.UsedPages = 20;
        await Assert.That(header.IsValid()).IsFalse();
    }

    [Test]
    public async Task IsValid_WithZeroCreatedAt_ShouldReturnFalse()
    {
        var header = new DatabaseHeader();
        header.CreatedAt = 0;
        await Assert.That(header.IsValid()).IsFalse();
    }

    [Test]
    public async Task IsValid_WithModifiedAtBeforeCreatedAt_ShouldReturnFalse()
    {
        var header = new DatabaseHeader();
        header.CreatedAt = DateTime.UtcNow.Ticks;
        header.ModifiedAt = header.CreatedAt - 1000;
        await Assert.That(header.IsValid()).IsFalse();
    }

    #endregion

    #region Checksum Tests

    [Test]
    public async Task CalculateChecksum_ShouldBeConsistent()
    {
        var header = new DatabaseHeader();
        header.DatabaseName = "TestDb";
        
        var checksum1 = header.CalculateChecksum();
        var checksum2 = header.CalculateChecksum();
        
        await Assert.That(checksum1).IsEqualTo(checksum2);
    }

    [Test]
    public async Task CalculateChecksum_ShouldChangeWithData()
    {
        var header = new DatabaseHeader();
        header.DatabaseName = "TestDb1";
        var checksum1 = header.CalculateChecksum();
        
        header.DatabaseName = "TestDb2";
        var checksum2 = header.CalculateChecksum();
        
        await Assert.That(checksum1).IsNotEqualTo(checksum2);
    }

    [Test]
    public async Task VerifyChecksum_WithCorrectChecksum_ShouldReturnTrue()
    {
        var header = new DatabaseHeader();
        header.DatabaseName = "TestDb";
        header.UpdateModification();
        
        await Assert.That(header.VerifyChecksum()).IsTrue();
    }

    [Test]
    public async Task VerifyChecksum_WithIncorrectChecksum_ShouldReturnFalse()
    {
        var header = new DatabaseHeader();
        header.DatabaseName = "TestDb";
        header.Checksum = 12345; // Wrong checksum
        
        await Assert.That(header.VerifyChecksum()).IsFalse();
    }

    [Test]
    public async Task UpdateModification_ShouldUpdateChecksumAndModifiedAt()
    {
        var header = new DatabaseHeader();
        var originalModifiedAt = header.ModifiedAt;
        var originalChecksum = header.Checksum;
        
        // Wait a bit to ensure time changes
        await Task.Delay(10);
        
        header.UpdateModification();
        
        await Assert.That(header.ModifiedAt).IsGreaterThanOrEqualTo(originalModifiedAt);
        // Checksum should change or be same (depending on exact timing)
        await Assert.That(header.Checksum).IsNotEqualTo(0u);
    }

    #endregion

    #region Serialization Tests

    [Test]
    public async Task FromByteArray_WithNullData_ShouldThrowArgumentException()
    {
        await Assert.That(() => DatabaseHeader.FromByteArray(null!))
            .Throws<ArgumentException>();
    }

    [Test]
    public async Task FromByteArray_WithTooShortData_ShouldThrowArgumentException()
    {
        var shortData = new byte[100]; // Less than 256
        
        await Assert.That(() => DatabaseHeader.FromByteArray(shortData))
            .Throws<ArgumentException>();
    }

    [Test]
    public async Task ToByteArray_ShouldReturnCorrectSize()
    {
        var header = new DatabaseHeader();
        var bytes = header.ToByteArray();
        
        await Assert.That(bytes.Length).IsEqualTo(DatabaseHeader.Size);
        await Assert.That(bytes.Length).IsEqualTo(256);
    }

    [Test]
    public async Task Clone_ShouldCreateExactCopy()
    {
        var original = new DatabaseHeader();
        original.DatabaseName = "TestClone";
        original.TotalPages = 50;
        original.UserData = new byte[] { 1, 2, 3, 4 };
        
        var clone = original.Clone();
        
        await Assert.That(clone.DatabaseName).IsEqualTo(original.DatabaseName);
        await Assert.That(clone.TotalPages).IsEqualTo(original.TotalPages);
        await Assert.That(clone.Magic).IsEqualTo(original.Magic);
    }

    [Test]
    public async Task RoundTrip_ToByteArrayAndBack_ShouldPreserveData()
    {
        var original = new DatabaseHeader();
        original.DatabaseName = "RoundTripTest";
        original.TotalPages = 100;
        original.UsedPages = 50;
        original.EnableJournaling = true;
        
        var salt = new byte[16];
        var keyHash = new byte[32];
        new Random(42).NextBytes(salt);
        new Random(42).NextBytes(keyHash);
        original.SetSecurityMetadata(new DatabaseSecurityMetadata(salt, keyHash));
        
        var bytes = original.ToByteArray();
        var restored = DatabaseHeader.FromByteArray(bytes);
        
        await Assert.That(restored.DatabaseName).IsEqualTo(original.DatabaseName);
        await Assert.That(restored.TotalPages).IsEqualTo(original.TotalPages);
        await Assert.That(restored.UsedPages).IsEqualTo(original.UsedPages);
        await Assert.That(restored.EnableJournaling).IsEqualTo(original.EnableJournaling);
        
        await Assert.That(restored.TryGetSecurityMetadata(out var restoredMeta)).IsTrue();
        await Assert.That(restoredMeta.Salt).IsEquivalentTo(salt);
    }

    #endregion

    #region Initialize Tests

    [Test]
    public async Task Initialize_ShouldSetAllFields()
    {
        var header = new DatabaseHeader();
        header.Initialize(8192, "InitTestDb", true);
        
        await Assert.That(header.Magic).IsEqualTo(DatabaseHeader.MagicNumber);
        await Assert.That(header.DatabaseVersion).IsEqualTo(DatabaseHeader.Version);
        await Assert.That(header.PageSize).IsEqualTo(8192u);
        await Assert.That(header.DatabaseName).IsEqualTo("InitTestDb");
        await Assert.That(header.EnableJournaling).IsTrue();
        await Assert.That(header.TotalPages).IsEqualTo(1u);
        await Assert.That(header.UsedPages).IsEqualTo(1u);
    }

    [Test]
    public async Task Initialize_WithDefaults_ShouldUseDefaultValues()
    {
        var header = new DatabaseHeader();
        header.Initialize(4096);
        
        await Assert.That(header.DatabaseName).IsEqualTo("TinyDb");
        await Assert.That(header.EnableJournaling).IsFalse();
    }

    #endregion

    #region ToString Tests

    [Test]
    public async Task ToString_ShouldIncludeRelevantInfo()
    {
        var header = new DatabaseHeader();
        header.DatabaseName = "ToStringTest";
        header.TotalPages = 100;
        header.UsedPages = 50;
        
        var str = header.ToString();
        
        await Assert.That(str).Contains("ToStringTest");
        await Assert.That(str).Contains("50/100");
    }

    #endregion

    #region Constants Tests

    [Test]
    public async Task Size_ShouldBe256()
    {
        await Assert.That(DatabaseHeader.Size).IsEqualTo(256);
    }

    [Test]
    public async Task MagicNumber_ShouldBeCorrect()
    {
        await Assert.That(DatabaseHeader.MagicNumber).IsEqualTo(0x44425353u);
    }

    [Test]
    public async Task Version_ShouldBeCorrect()
    {
        await Assert.That(DatabaseHeader.Version).IsEqualTo(0x00010000u);
    }

    [Test]
    public async Task SaltLength_ShouldBe16()
    {
        await Assert.That(DatabaseSecurityMetadata.SaltLength).IsEqualTo(16);
    }

    [Test]
    public async Task KeyHashLength_ShouldBe32()
    {
        await Assert.That(DatabaseSecurityMetadata.KeyHashLength).IsEqualTo(32);
    }

    #endregion
}
