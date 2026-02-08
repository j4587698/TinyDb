using System;
using System.Linq;
using System.Reflection;
using TinyDb.Core;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Core;

public class TinyDbOptionsBranchCoverageTests3
{
    [Test]
    public async Task IsPowerOfTwo_ShouldCoverZeroPowerAndNonPower()
    {
        var method = typeof(TinyDbOptions).GetMethod("IsPowerOfTwo", BindingFlags.NonPublic | BindingFlags.Static);
        await Assert.That(method).IsNotNull();

        await Assert.That((bool)method!.Invoke(null, new object[] { 0u })!).IsFalse();
        await Assert.That((bool)method.Invoke(null, new object[] { 8u })!).IsTrue();
        await Assert.That((bool)method.Invoke(null, new object[] { 6u })!).IsFalse();
    }

    [Test]
    public async Task Clone_ShouldHandleNullAndNonNullArrays()
    {
        var options1 = new TinyDbOptions { UserData = null, EncryptionKey = null };
        var clone1 = options1.Clone();
        await Assert.That(clone1.UserData).IsNotNull();
        await Assert.That(clone1.UserData!.Length).IsEqualTo(0);
        await Assert.That(clone1.EncryptionKey).IsNull();

        var options2 = new TinyDbOptions
        {
            UserData = new byte[] { 1, 2, 3 },
            EncryptionKey = Enumerable.Range(0, 16).Select(i => (byte)i).ToArray()
        };

        var clone2 = options2.Clone();
        await Assert.That(clone2.UserData).IsNotNull();
        await Assert.That(ReferenceEquals(options2.UserData, clone2.UserData)).IsFalse();
        await Assert.That(clone2.UserData!.SequenceEqual(options2.UserData!)).IsTrue();

        await Assert.That(clone2.EncryptionKey).IsNotNull();
        await Assert.That(ReferenceEquals(options2.EncryptionKey, clone2.EncryptionKey)).IsFalse();
        await Assert.That(clone2.EncryptionKey!.SequenceEqual(options2.EncryptionKey!)).IsTrue();
    }
}

