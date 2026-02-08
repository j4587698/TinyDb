using System;
using TinyDb.Storage;
using TUnit.Assertions;
using TUnit.Core;

namespace TinyDb.Tests.Storage;

public class DiskStreamCtorNullBranchCoverageTests
{
    [Test]
    public async Task Ctor_NullFilePath_ShouldThrow()
    {
        await Assert.That(() => new DiskStream(null!)).Throws<ArgumentNullException>();
    }
}

