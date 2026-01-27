using System;
using TinyDb.Core;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Core;

/// <summary>
/// Tests for DatabaseStatistics to improve coverage
/// </summary>
public class DatabaseStatisticsTests
{
    [Test]
    public async Task DatabaseStatistics_DefaultValues_ShouldBeSet()
    {
        var stats = new DatabaseStatistics();

        await Assert.That(stats.FilePath).IsEqualTo(string.Empty);
        await Assert.That(stats.DatabaseName).IsEqualTo(string.Empty);
        await Assert.That(stats.Version).IsEqualTo(0u);
        await Assert.That(stats.PageSize).IsEqualTo(0u);
        await Assert.That(stats.TotalPages).IsEqualTo(0u);
        await Assert.That(stats.UsedPages).IsEqualTo(0u);
        await Assert.That(stats.FreePages).IsEqualTo(0u);
        await Assert.That(stats.CollectionCount).IsEqualTo(0);
        await Assert.That(stats.FileSize).IsEqualTo(0L);
        await Assert.That(stats.CachedPages).IsEqualTo(0);
        await Assert.That(stats.CacheHitRatio).IsEqualTo(0.0);
        await Assert.That(stats.IsReadOnly).IsFalse();
        await Assert.That(stats.EnableJournaling).IsFalse();
    }

    [Test]
    public async Task DatabaseStatistics_InitValues_ShouldBeSet()
    {
        var now = DateTime.UtcNow;
        var stats = new DatabaseStatistics
        {
            FilePath = "/path/to/db.db",
            DatabaseName = "TestDb",
            Version = 1u,
            CreatedAt = now,
            ModifiedAt = now,
            PageSize = 4096u,
            TotalPages = 100u,
            UsedPages = 50u,
            FreePages = 50u,
            CollectionCount = 5,
            FileSize = 409600L,
            CachedPages = 20,
            CacheHitRatio = 0.85,
            IsReadOnly = false,
            EnableJournaling = true
        };

        await Assert.That(stats.FilePath).IsEqualTo("/path/to/db.db");
        await Assert.That(stats.DatabaseName).IsEqualTo("TestDb");
        await Assert.That(stats.Version).IsEqualTo(1u);
        await Assert.That(stats.CreatedAt).IsEqualTo(now);
        await Assert.That(stats.ModifiedAt).IsEqualTo(now);
        await Assert.That(stats.PageSize).IsEqualTo(4096u);
        await Assert.That(stats.TotalPages).IsEqualTo(100u);
        await Assert.That(stats.UsedPages).IsEqualTo(50u);
        await Assert.That(stats.FreePages).IsEqualTo(50u);
        await Assert.That(stats.CollectionCount).IsEqualTo(5);
        await Assert.That(stats.FileSize).IsEqualTo(409600L);
        await Assert.That(stats.CachedPages).IsEqualTo(20);
        await Assert.That(stats.CacheHitRatio).IsEqualTo(0.85);
        await Assert.That(stats.IsReadOnly).IsFalse();
        await Assert.That(stats.EnableJournaling).IsTrue();
    }

    [Test]
    public async Task DatabaseStatistics_ToString_ShouldFormatCorrectly()
    {
        var stats = new DatabaseStatistics
        {
            DatabaseName = "TestDb",
            UsedPages = 50,
            TotalPages = 100,
            CollectionCount = 5,
            FileSize = 409600,
            CacheHitRatio = 0.85
        };

        var str = stats.ToString();

        await Assert.That(str).Contains("TestDb");
        await Assert.That(str).Contains("50/100 pages");
        await Assert.That(str).Contains("5 collections");
        await Assert.That(str).Contains("409,600 bytes");
        await Assert.That(str).Contains("85");
    }

    [Test]
    public async Task DatabaseStatistics_ToString_WithZeroValues_ShouldWork()
    {
        var stats = new DatabaseStatistics
        {
            DatabaseName = "",
            UsedPages = 0,
            TotalPages = 0,
            CollectionCount = 0,
            FileSize = 0,
            CacheHitRatio = 0
        };

        var str = stats.ToString();

        await Assert.That(str).Contains("0/0 pages");
        await Assert.That(str).Contains("0 collections");
    }

    [Test]
    public async Task DatabaseStatistics_ToString_WithLargeFileSize_ShouldFormat()
    {
        var stats = new DatabaseStatistics
        {
            DatabaseName = "LargeDb",
            UsedPages = 1000000,
            TotalPages = 2000000,
            CollectionCount = 100,
            FileSize = 8589934592L, // 8GB
            CacheHitRatio = 0.999
        };

        var str = stats.ToString();

        await Assert.That(str).Contains("LargeDb");
        await Assert.That(str).Contains("1000000/2000000 pages");
    }
}
