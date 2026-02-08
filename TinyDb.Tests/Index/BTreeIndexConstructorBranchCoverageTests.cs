using System;
using TinyDb.Index;
using TinyDb.Storage;
using TinyDb.Tests.Storage;
using TUnit.Assertions;
using TUnit.Core;

namespace TinyDb.Tests.Index;

public class BTreeIndexConstructorBranchCoverageTests
{
    [Test]
    public async Task Ctor_WithNullName_ShouldThrow()
    {
        using var pm = new PageManager(new MockDiskStream());
        await Assert.That(() => new BTreeIndex(pm, null!, new[] { "a" })).Throws<ArgumentNullException>();
    }

    [Test]
    public async Task Ctor_WithNullFields_ShouldThrow()
    {
        using var pm = new PageManager(new MockDiskStream());
        await Assert.That(() => new BTreeIndex(pm, "idx", null!)).Throws<ArgumentNullException>();
    }

    [Test]
    public async Task Ctor_WithEmptyFields_ShouldThrow()
    {
        using var pm = new PageManager(new MockDiskStream());
        await Assert.That(() => new BTreeIndex(pm, "idx", Array.Empty<string>())).Throws<ArgumentException>();
    }
}

