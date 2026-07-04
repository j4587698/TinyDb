using TinyDb.Bson;
using TinyDb.Core;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Concurrency;

[NotInParallel]
public sealed class CollectionWriteGranularityTests : IDisposable
{
    private readonly string _path;
    private readonly TinyDbEngine _engine;

    public CollectionWriteGranularityTests()
    {
        _path = Path.Combine(Path.GetTempPath(), $"tinydb_write_granularity_{Guid.NewGuid():N}.db");
        _engine = new TinyDbEngine(_path, CreateConcurrencyOptions());
    }

    public void Dispose()
    {
        _engine.Dispose();
        DeleteDatabaseFiles(_path);
    }

    [Test]
    public async Task ConcurrentInserts_DifferentIds_ShouldAllPersist()
    {
        const string collection = "concurrent_insert_distinct";
        const int workers = 8;
        const int perWorker = 40;

        var tasks = Enumerable.Range(0, workers)
            .Select(worker => Task.Run(() =>
            {
                for (var i = 0; i < perWorker; i++)
                {
                    var id = worker * 1000 + i;
                    _engine.InsertDocument(collection, new BsonDocument()
                        .Set("_id", id)
                        .Set("worker", worker)
                        .Set("value", i));
                }
            }))
            .ToArray();

        await Task.WhenAll(tasks);

        var all = _engine.FindAll(collection).ToList();
        await Assert.That(all).Count().IsEqualTo(workers * perWorker);

        for (var worker = 0; worker < workers; worker++)
        {
            for (var i = 0; i < perWorker; i++)
            {
                await Assert.That(_engine.FindById(collection, worker * 1000 + i)).IsNotNull();
            }
        }
    }

    [Test]
    public async Task ConcurrentWrites_SameId_ShouldRemainSingleDocument()
    {
        const string collection = "concurrent_same_id";
        var id = new BsonInt32(7);
        var exceptions = new List<Exception>();

        var tasks = Enumerable.Range(0, 48)
            .Select(i => Task.Run(() =>
            {
                try
                {
                    switch (i % 4)
                    {
                        case 0:
                            _engine.InsertDocument(collection, new BsonDocument().Set("_id", id).Set("v", i));
                            break;
                        case 1:
                            _engine.UpsertDocument(collection, new BsonDocument().Set("_id", id).Set("v", i));
                            break;
                        case 2:
                            _engine.UpdateDocument(collection, new BsonDocument().Set("_id", id).Set("v", i));
                            break;
                        default:
                            _engine.DeleteDocument(collection, id);
                            break;
                    }
                }
                catch (InvalidOperationException ex) when (ex.Message.Contains("Duplicate document id", StringComparison.Ordinal))
                {
                    lock (exceptions)
                    {
                        exceptions.Add(ex);
                    }
                }
            }))
            .ToArray();

        await Task.WhenAll(tasks);

        var matching = _engine.FindAll(collection)
            .Where(doc => doc.TryGetValue("_id", out var documentId) && BsonValueComparer.ValueEquals(documentId, id))
            .ToList();

        await Assert.That(matching.Count).IsLessThanOrEqualTo(1);
        await Assert.That(_engine.GetIndexManager(collection).ValidateAllIndexes().InvalidIndexes).IsEqualTo(0);
    }

    [Test]
    public async Task ConcurrentUpdates_DifferentIds_ShouldKeepSecondaryIndexValid()
    {
        const string collection = "concurrent_update_indexed";
        const int count = 120;

        _engine.GetIndexManager(collection).CreateIndex("idx_group", new[] { "group" });
        for (var i = 0; i < count; i++)
        {
            _engine.InsertDocument(collection, new BsonDocument()
                .Set("_id", i)
                .Set("group", "initial")
                .Set("payload", "seed"));
        }

        var tasks = Enumerable.Range(0, count)
            .Select(i => Task.Run(() =>
            {
                _engine.UpdateDocument(collection, new BsonDocument()
                    .Set("_id", i)
                    .Set("group", $"updated_{i:000}")
                    .Set("payload", new string((char)('a' + i % 26), 128 + i)));
            }))
            .ToArray();

        await Task.WhenAll(tasks);

        var all = _engine.FindAll(collection).ToList();
        await Assert.That(all).Count().IsEqualTo(count);
        await Assert.That(all.Select(doc => doc["group"].ToString()).Distinct(StringComparer.Ordinal).Count()).IsEqualTo(count);
        await Assert.That(_engine.GetIndexManager(collection).ValidateAllIndexes().InvalidIndexes).IsEqualTo(0);
    }

    [Test]
    public async Task ConcurrentDeleteAndUpdate_SameId_ShouldEndConsistently()
    {
        const string collection = "concurrent_delete_update_same";
        var id = new BsonInt32(42);
        _engine.InsertDocument(collection, new BsonDocument().Set("_id", id).Set("v", 0));

        var tasks = Enumerable.Range(0, 64)
            .Select(i => Task.Run(() =>
            {
                if ((i & 1) == 0)
                {
                    _engine.UpdateDocument(collection, new BsonDocument().Set("_id", id).Set("v", i).Set("payload", new string('x', 256)));
                }
                else
                {
                    _engine.DeleteDocument(collection, id);
                }
            }))
            .ToArray();

        await Task.WhenAll(tasks);

        var matching = _engine.FindAll(collection)
            .Where(doc => doc.TryGetValue("_id", out var documentId) && BsonValueComparer.ValueEquals(documentId, id))
            .ToList();

        await Assert.That(matching.Count).IsLessThanOrEqualTo(1);
        await Assert.That(_engine.GetIndexManager(collection).ValidateAllIndexes().InvalidIndexes).IsEqualTo(0);
    }

    [Test]
    public async Task ConcurrentPageAppendRewriteFree_ShouldReopenWithValidPages()
    {
        var path = Path.Combine(Path.GetTempPath(), $"tinydb_page_mutation_{Guid.NewGuid():N}.db");
        const string collection = "page_mutation";

        try
        {
            using (var engine = new TinyDbEngine(path, CreateConcurrencyOptions(pageSize: 4096)))
            {
                for (var i = 0; i < 80; i++)
                {
                    engine.InsertDocument(collection, CreateSizedDocument(i, 220));
                }

                var appendTask = Task.Run(() =>
                {
                    for (var i = 1000; i < 1060; i++)
                    {
                        engine.InsertDocument(collection, CreateSizedDocument(i, 180));
                    }
                });

                var rewriteTask = Task.Run(() =>
                {
                    for (var i = 0; i < 80; i += 2)
                    {
                        engine.UpdateDocument(collection, CreateSizedDocument(i, 900));
                    }
                });

                var freeTask = Task.Run(() =>
                {
                    for (var i = 1; i < 80; i += 2)
                    {
                        engine.DeleteDocument(collection, i);
                    }
                });

                await Task.WhenAll(appendTask, rewriteTask, freeTask);
                engine.Flush();
            }

            using var reopened = new TinyDbEngine(path, CreateConcurrencyOptions(pageSize: 4096));
            var documents = reopened.FindAll(collection).ToList();
            var ids = documents.Select(doc => doc["_id"]).ToList();

            await Assert.That(ids.Distinct(BsonValueComparer.EqualityComparer).Count()).IsEqualTo(ids.Count);
            foreach (var id in ids)
            {
                await Assert.That(reopened.FindById(collection, id)).IsNotNull();
            }
        }
        finally
        {
            DeleteDatabaseFiles(path);
        }
    }

    [Test]
    public async Task ConcurrentPageMutations_WithWal_ShouldNotDeadlock()
    {
        var path = Path.Combine(Path.GetTempPath(), $"tinydb_page_mutation_wal_{Guid.NewGuid():N}.db");
        const string collection = "page_mutation_wal";

        try
        {
            using var engine = new TinyDbEngine(path, new TinyDbOptions
            {
                PageSize = 4096,
                EnableJournaling = true,
                WriteConcern = WriteConcern.None,
                BackgroundFlushInterval = Timeout.InfiniteTimeSpan
            });

            for (var i = 0; i < 96; i++)
            {
                engine.InsertDocument(collection, CreateSizedDocument(i, 96));
            }

            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            var updateTask = Task.Run(() =>
            {
                for (var i = 0; i < 96; i += 2)
                {
                    timeout.Token.ThrowIfCancellationRequested();
                    engine.UpdateDocument(collection, CreateSizedDocument(i, 768));
                }
            }, timeout.Token);

            var deleteTask = Task.Run(() =>
            {
                for (var i = 1; i < 96; i += 2)
                {
                    timeout.Token.ThrowIfCancellationRequested();
                    engine.DeleteDocument(collection, i);
                }
            }, timeout.Token);

            var insertTask = Task.Run(() =>
            {
                for (var i = 1000; i < 1096; i++)
                {
                    timeout.Token.ThrowIfCancellationRequested();
                    engine.InsertDocument(collection, CreateSizedDocument(i, 128));
                }
            }, timeout.Token);

            var allTasks = Task.WhenAll(updateTask, deleteTask, insertTask);
            var completed = await Task.WhenAny(allTasks, Task.Delay(TimeSpan.FromSeconds(20)));
            await Assert.That(completed == allTasks).IsTrue();
            await allTasks;

            await Assert.That(engine.GetIndexManager(collection).ValidateAllIndexes().InvalidIndexes).IsEqualTo(0);
        }
        finally
        {
            DeleteDatabaseFiles(path);
        }
    }

    [Test]
    public async Task AsyncUpdateDelete_ColdPages_ShouldComplete()
    {
        var path = Path.Combine(Path.GetTempPath(), $"tinydb_async_cold_pages_{Guid.NewGuid():N}.db");
        const string collection = "async_cold_pages";
        const int count = 96;

        try
        {
            using (var engine = new TinyDbEngine(path, CreateConcurrencyOptions(pageSize: 4096, cacheSize: 2)))
            {
                for (var i = 0; i < count; i++)
                {
                    engine.InsertDocument(collection, CreateSizedDocument(i, 420));
                }

                engine.Flush();
            }

            using var reopened = new TinyDbEngine(path, CreateConcurrencyOptions(pageSize: 4096, cacheSize: 2));
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(20));

            var tasks = Enumerable.Range(0, count)
                .Select(i => (i & 1) == 0
                    ? reopened.UpdateDocumentAsync(collection, CreateSizedDocument(i, 680), timeout.Token)
                    : reopened.DeleteDocumentAsync(collection, i, timeout.Token))
                .ToArray();

            var allTasks = Task.WhenAll(tasks);
            var completed = await Task.WhenAny(allTasks, Task.Delay(TimeSpan.FromSeconds(25)));
            await Assert.That(completed == allTasks).IsTrue();
            var results = await allTasks;

            var zeroResultIds = results
                .Select((result, id) => (result, id))
                .Where(static item => item.result == 0)
                .Select(static item => item.id)
                .ToArray();
            var missingUpdatedIds = Enumerable.Range(0, count)
                .Where(static id => (id & 1) == 0)
                .Where(id => reopened.FindById(collection, id) == null)
                .ToArray();
            var remainingDeletedIds = Enumerable.Range(0, count)
                .Where(static id => (id & 1) != 0)
                .Where(id => reopened.FindById(collection, id) != null)
                .ToArray();

            if (zeroResultIds.Length != 0 || missingUpdatedIds.Length != 0 || remainingDeletedIds.Length != 0)
            {
                throw new InvalidOperationException(
                    $"Async cold-page mutation mismatch. ZeroResults=[{string.Join(",", zeroResultIds)}], " +
                    $"MissingUpdated=[{string.Join(",", missingUpdatedIds)}], RemainingDeleted=[{string.Join(",", remainingDeletedIds)}].");
            }
        }
        finally
        {
            DeleteDatabaseFiles(path);
        }
    }

    [Test]
    public async Task BatchWriteAndSingleDocumentWrite_ShouldNotDeadlock()
    {
        const string collection = "batch_single_no_deadlock";
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(20));

        var batchTask = Task.Run(() =>
        {
            for (var batch = 0; batch < 12; batch++)
            {
                timeout.Token.ThrowIfCancellationRequested();
                var docs = Enumerable.Range(0, 25)
                    .Select(i => new BsonDocument()
                        .Set("_id", batch * 1000 + i)
                        .Set("kind", "batch")
                        .Set("payload", new string('b', 96)))
                    .ToArray();

                _engine.InsertDocuments(collection, docs);
            }
        }, timeout.Token);

        var singleTask = Task.Run(() =>
        {
            for (var i = 0; i < 300; i++)
            {
                timeout.Token.ThrowIfCancellationRequested();
                var id = 100000 + i;
                _engine.UpsertDocument(collection, new BsonDocument()
                    .Set("_id", id)
                    .Set("kind", "single")
                    .Set("payload", new string('s', 64 + i % 32)));
            }
        }, timeout.Token);

        var allTasks = Task.WhenAll(batchTask, singleTask);
        var completed = await Task.WhenAny(allTasks, Task.Delay(TimeSpan.FromSeconds(25)));
        await Assert.That(completed == allTasks).IsTrue();
        await allTasks;

        await Assert.That(_engine.FindAll(collection).Count()).IsEqualTo(600);
    }

    [Test]
    public async Task BatchDocumentLocks_ShouldNotBlockUnrelatedDocumentId()
    {
        var state = new CollectionState();
        var batchIds = Enumerable.Range(0, 1000)
            .Select(static id => new BsonInt32(id))
            .ToArray();

        using var batchLock = state.EnterDocumentLocks(batchIds);

        var unrelatedLockTask = Task.Run(() =>
        {
            using var unrelatedLock = state.EnterDocumentLock(new BsonInt32(-1));
            return true;
        });

        var completed = await Task.WhenAny(unrelatedLockTask, Task.Delay(TimeSpan.FromSeconds(2)));
        await Assert.That(completed == unrelatedLockTask).IsTrue();
        await unrelatedLockTask;
    }

    [Test]
    public async Task DocumentLock_InheritedChildContext_ShouldWaitForSameDocumentId()
    {
        var state = new CollectionState();
        var id = new BsonInt32(42);
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var parentLock = state.EnterDocumentLock(id);
        var childTask = Task.Run(() =>
        {
            started.SetResult();
            using var childLock = state.EnterDocumentLock(id);
            return true;
        });

        await started.Task.WaitAsync(TimeSpan.FromSeconds(2));
        var completedWhileParentHeld = await Task.WhenAny(childTask, Task.Delay(TimeSpan.FromMilliseconds(200)));
        await Assert.That(completedWhileParentHeld == childTask).IsFalse();

        parentLock.Dispose();

        var completedAfterRelease = await Task.WhenAny(childTask, Task.Delay(TimeSpan.FromSeconds(2)));
        await Assert.That(completedAfterRelease == childTask).IsTrue();
        await Assert.That(await childTask).IsTrue();
    }

    [Test]
    public async Task AsyncDocumentLock_ShouldWaitForSameDocumentId()
    {
        var state = new CollectionState();
        var id = new BsonInt32(43);

        var parentLock = await state.EnterDocumentLockAsync(id);
        var childTask = Task.Run(async () =>
        {
            using var childLock = await state.EnterDocumentLockAsync(id);
            return true;
        });

        var completedWhileParentHeld = await Task.WhenAny(childTask, Task.Delay(TimeSpan.FromMilliseconds(200)));
        await Assert.That(completedWhileParentHeld == childTask).IsFalse();

        parentLock.Dispose();

        var completedAfterRelease = await Task.WhenAny(childTask, Task.Delay(TimeSpan.FromSeconds(2)));
        await Assert.That(completedAfterRelease == childTask).IsTrue();
        await Assert.That(await childTask).IsTrue();
    }

    private static BsonDocument CreateSizedDocument(int id, int payloadSize)
    {
        return new BsonDocument()
            .Set("_id", id)
            .Set("payload", new string((char)('a' + id % 26), payloadSize));
    }

    private static TinyDbOptions CreateConcurrencyOptions(uint pageSize = TinyDbOptions.DefaultPageSize, int cacheSize = TinyDbOptions.DefaultCacheSize)
    {
        return new TinyDbOptions
        {
            PageSize = pageSize,
            CacheSize = cacheSize,
            EnableJournaling = false,
            WriteConcern = WriteConcern.None,
            BackgroundFlushInterval = Timeout.InfiniteTimeSpan
        };
    }

    private static void DeleteDatabaseFiles(string path)
    {
        TryDelete(path);
        var directory = Path.GetDirectoryName(path) ?? string.Empty;
        var name = Path.GetFileNameWithoutExtension(path);
        var ext = Path.GetExtension(path).TrimStart('.');
        TryDelete(Path.Combine(directory, $"{name}-wal.{ext}"));
    }

    private static void TryDelete(string path)
    {
        try
        {
            if (File.Exists(path))
            {
                File.Delete(path);
            }
        }
        catch
        {
        }
    }
}
