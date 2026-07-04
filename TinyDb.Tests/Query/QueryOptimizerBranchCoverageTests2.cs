using System;
using TinyDb.Query;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Query;

public class QueryOptimizerBranchCoverageTests2
{
    [Test]
    public async Task Ctor_NullEngine_ShouldThrow()
    {
        await Assert.That(() => new QueryOptimizer(null!)).Throws<ArgumentNullException>();
    }
}
