using System;
using System.Collections.Generic;
using TinyDb.Core;
using TinyDb.Storage;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Core;

public class StatisticsCoverageTests
{
    [Test]
    public async Task LargeDocumentStatistics_ToString_ShouldWork()
    {
        var stats = new LargeDocumentStatistics
        {
            IndexPageId = 1,
            TotalLength = 1024,
            PageCount = 2,
            FirstDataPageId = 2
        };
        
        await Assert.That(stats.ToString()).Contains("LargeDoc[Index=1, Size=1,024 bytes, Pages=2]");
    }

    [Test]
    public async Task TransactionManagerStatistics_ToString_ShouldWork()
    {
        var stats = new TransactionManagerStatistics
        {
            ActiveTransactionCount = 5,
            MaxTransactions = 10,
            TotalOperations = 100,
            AverageTransactionAge = 1.5,
            States = new Dictionary<TransactionState, int>()
        };
        
        await Assert.That(stats.ToString()).Contains("TransactionManager: 5/10 active");
        await Assert.That(stats.ToString()).Contains("100 total operations");
        await Assert.That(stats.ToString()).Contains("AvgAge=1.5s");
    }

    [Test]
    public async Task TransactionStatistics_ToString_ShouldWork()
    {
        var stats = new TransactionStatistics
        {
            TransactionId = Guid.NewGuid(),
            State = TransactionState.Committed,
            Duration = TimeSpan.FromSeconds(2.5),
            OperationCount = 10,
            IsReadOnly = false
        };
        
        await Assert.That(stats.ToString()).Contains("Transaction[");
        await Assert.That(stats.ToString()).Contains("Committed");
        await Assert.That(stats.ToString()).Contains("10 ops");
        await Assert.That(stats.ToString()).Contains("2.5s");
        await Assert.That(stats.ToString()).Contains("read-write");
    }
}
