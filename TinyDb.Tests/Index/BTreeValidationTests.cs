using System;
using TinyDb.Bson;
using TinyDb.Index;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Index;

public class BTreeValidationTests
{
    [Test]
    public async Task IndexValidationResult_ToString_ShouldWork()
    {
        var result = new IndexValidationResult
        {
            TotalIndexes = 5,
            ValidIndexes = 4,
            InvalidIndexes = 1,
            Errors = new List<string> { "Error 1" }
        };
        
        await Assert.That(result.IsValid).IsFalse();
        await Assert.That(result.ToString()).Contains("4/5 valid");
    }

    [Test]
    public async Task BTreeNodeStatistics_ShouldWork()
    {
        var stats = new BTreeNodeStatistics
        {
            IsLeaf = true,
            KeyCount = 10,
            ChildCount = 0,
            DocumentCount = 10,
            MaxKeys = 20,
            MinKeys = 5,
            IsFull = false,
            NeedsMerge = false
        };
        
        await Assert.That(stats.KeyCount).IsEqualTo(10);
        await Assert.That(stats.MaxKeys).IsEqualTo(20);
    }
}
