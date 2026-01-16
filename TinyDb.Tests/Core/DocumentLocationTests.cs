using TinyDb.Core;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Core;

public class DocumentLocationTests
{
    [Test]
    public async Task DocumentLocation_Should_Store_Values_Correctly()
    {
        var loc = new DocumentLocation(123, 456);
        await Assert.That(loc.PageId).IsEqualTo((uint)123);
        await Assert.That(loc.EntryIndex).IsEqualTo((ushort)456);
    }

    [Test]
    public async Task DocumentLocation_Empty_Should_Be_Zero()
    {
        var loc = DocumentLocation.Empty;
        await Assert.That(loc.PageId).IsEqualTo(0u);
        await Assert.That(loc.EntryIndex).IsEqualTo((ushort)0);
        await Assert.That(loc.IsEmpty).IsTrue();
    }

    [Test]
    public async Task DocumentLocation_Equals_Should_Work()
    {
        var loc1 = new DocumentLocation(1, 1);
        var loc2 = new DocumentLocation(1, 1);
        var loc3 = new DocumentLocation(1, 2);
        var loc4 = new DocumentLocation(2, 1);

        await Assert.That(loc1.Equals(loc2)).IsTrue();
        await Assert.That(loc1.Equals((object)loc2)).IsTrue();
        await Assert.That(loc1.Equals(loc3)).IsFalse();
        await Assert.That(loc1.Equals(loc4)).IsFalse();
        await Assert.That(loc1.Equals(null)).IsFalse();
    }

    [Test]
    public async Task DocumentLocation_GetHashCode_Should_Work()
    {
        var loc1 = new DocumentLocation(1, 1);
        var loc2 = new DocumentLocation(1, 1);
        await Assert.That(loc1.GetHashCode()).IsEqualTo(loc2.GetHashCode());
    }

    [Test]
    public async Task DocumentLocation_Serialization_Should_Work()
    {
        var loc = new DocumentLocation(12345, 6789);
        var serialized = loc.ToInt64();
        var deserialized = DocumentLocation.FromInt64(serialized);

        await Assert.That(deserialized.PageId).IsEqualTo(loc.PageId);
        await Assert.That(deserialized.EntryIndex).IsEqualTo(loc.EntryIndex);
    }
}
