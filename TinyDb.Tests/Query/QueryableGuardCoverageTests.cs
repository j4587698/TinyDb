using System;
using System.IO;
using System.Linq;
using TinyDb.Core;
using TinyDb.Query;
using TinyDb.Tests.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Query;

public sealed class QueryableGuardCoverageTests
{
    private sealed class DummyEntity
    {
        public int Id { get; set; }
    }

    [Test]
    public async Task InternalCtor_WhenExecutorNull_ShouldThrow()
    {
        var provider = TestQueryables.InMemory(Enumerable.Empty<DummyEntity>()).Provider;

        await Assert.That(() => new Queryable<DummyEntity, DummyEntity>(null!, "c", provider))
            .Throws<ArgumentNullException>();
    }

    [Test]
    public async Task InternalCtor_WhenCollectionNameWhitespace_ShouldThrow()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"qry_guard_ws_{Guid.NewGuid():N}.db");
        try
        {
            using var engine = new TinyDbEngine(dbPath);
            var executor = new QueryExecutor(engine);
            var provider = TestQueryables.InMemory(Enumerable.Empty<DummyEntity>()).Provider;

            await Assert.That(() => new Queryable<DummyEntity, DummyEntity>(executor, " ", provider))
                .Throws<ArgumentNullException>();
        }
        finally
        {
            TryDelete(dbPath);
        }
    }

    [Test]
    public async Task InternalCtor_WhenProviderNull_ShouldThrow()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"qry_guard_provider_{Guid.NewGuid():N}.db");
        try
        {
            using var engine = new TinyDbEngine(dbPath);
            var executor = new QueryExecutor(engine);

            await Assert.That(() => new Queryable<DummyEntity, DummyEntity>(executor, "c", (IQueryProvider)null!))
                .Throws<ArgumentNullException>();
        }
        finally
        {
            TryDelete(dbPath);
        }
    }

    private static void TryDelete(string path)
    {
        try { if (File.Exists(path)) File.Delete(path); } catch { }
    }
}
