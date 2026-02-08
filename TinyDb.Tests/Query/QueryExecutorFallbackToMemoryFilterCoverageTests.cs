using System;
using System.IO;
using System.Linq;
using TinyDb.Attributes;
using TinyDb.Collections;
using TinyDb.Core;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Query;

public sealed class QueryExecutorFallbackToMemoryFilterCoverageTests : IDisposable
{
    private static class Thrower
    {
        public static int ThrowingProperty => throw new InvalidOperationException("boom");
    }

    private readonly string _dbPath;
    private readonly TinyDbEngine _engine;
    private readonly ITinyCollection<Item> _items;

    public QueryExecutorFallbackToMemoryFilterCoverageTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"query_exec_fb_{Guid.NewGuid():N}.db");
        _engine = new TinyDbEngine(_dbPath);
        _items = _engine.GetCollection<Item>("items");

        _items.Insert(new Item { Id = 1, Value = 10 });
        _items.Insert(new Item { Id = 2, Value = 20 });
    }

    public void Dispose()
    {
        _engine.Dispose();
        try { if (File.Exists(_dbPath)) File.Delete(_dbPath); } catch { }
    }

    [Test]
    public async Task QueryExecutor_WhenParserThrows_ShouldThrow_InAotOnlyMode()
    {
        await Assert.That(() => _items.Query()
                .Where(x => x.Id < 0 && Thrower.ThrowingProperty == 0)
                .ToList())
            .Throws<NotSupportedException>();
    }

    [Entity]
    public sealed class Item
    {
        public int Id { get; set; }
        public int Value { get; set; }
    }
}
