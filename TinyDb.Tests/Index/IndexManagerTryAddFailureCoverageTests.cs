using TinyDb.Index;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Index;

public sealed class IndexManagerTryAddFailureCoverageTests
{
    [Test]
    public async Task CreateIndex_WhenEquivalentIndexAlreadyExists_ShouldReturnFalseAndKeepExistingIndex()
    {
        using var manager = new IndexManager("col");

        await Assert.That(manager.CreateIndex("idx", new[] { "A" }, unique: false)).IsTrue();
        var original = manager.GetIndex("idx");

        await Assert.That(manager.CreateIndex("idx", new[] { "a" }, unique: false)).IsFalse();

        await Assert.That(ReferenceEquals(manager.GetIndex("idx"), original)).IsTrue();
    }
}
