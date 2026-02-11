using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics.Tracing;
using System.IO;
using System.Linq;
using System.Reflection;
using TinyDb.Bson;
using TinyDb.Core;
using TinyDb.Index;
using TinyDb.Serialization;
using TinyDb.Storage;

namespace TinyDb.Benchmark;

internal static class AllocationProbe
{
    private sealed class AllocationTickListener : EventListener
    {
        private readonly Dictionary<string, long> _bytesByType = new(StringComparer.Ordinal);
        private volatile bool _enabled;

        public void Start() => _enabled = true;
        public void StopListening() => _enabled = false;
        public IReadOnlyDictionary<string, long> BytesByType => _bytesByType;

        protected override void OnEventSourceCreated(EventSource eventSource)
        {
            if (eventSource.Name is "Microsoft-Windows-DotNETRuntime" or "System.Runtime")
            {
                try
                {
                    EnableEvents(eventSource, EventLevel.Verbose, EventKeywords.All);
                }
                catch
                {
                    // Ignore if runtime provider isn't available in this environment.
                }
            }
        }

        protected override void OnEventWritten(EventWrittenEventArgs eventData)
        {
            if (!_enabled) return;
            if (!string.Equals(eventData.EventName, "GCAllocationTick", StringComparison.Ordinal)) return;

            var typeName = GetPayloadString(eventData, "TypeName") ?? "Unknown";
            var amount = GetPayloadInt64(eventData, "AllocationAmount64")
                         ?? GetPayloadInt64(eventData, "AllocationAmount")
                         ?? 0L;

            if (amount <= 0) return;

            if (_bytesByType.TryGetValue(typeName, out var existing))
            {
                _bytesByType[typeName] = existing + amount;
            }
            else
            {
                _bytesByType[typeName] = amount;
            }
        }

        private static string? GetPayloadString(EventWrittenEventArgs eventData, string name)
        {
            if (eventData.PayloadNames == null || eventData.Payload == null) return null;

            for (int i = 0; i < eventData.PayloadNames.Count; i++)
            {
                if (!string.Equals(eventData.PayloadNames[i], name, StringComparison.Ordinal)) continue;
                return eventData.Payload[i] as string;
            }

            return null;
        }

        private static long? GetPayloadInt64(EventWrittenEventArgs eventData, string name)
        {
            if (eventData.PayloadNames == null || eventData.Payload == null) return null;

            for (int i = 0; i < eventData.PayloadNames.Count; i++)
            {
                if (!string.Equals(eventData.PayloadNames[i], name, StringComparison.Ordinal)) continue;

                var value = eventData.Payload[i];
                return value switch
                {
                    byte b => b,
                    short s => s,
                    int i32 => i32,
                    long i64 => i64,
                    _ => null
                };
            }

            return null;
        }
    }

    public static void Run()
    {
        Console.WriteLine("=== Allocation Probe ===");
        Console.WriteLine("说明：使用 GC.GetAllocatedBytesForCurrentThread() 估算单线程分配。");

        const int iterations = 1000;
        const string databaseFile = "alloc_probe.db";
        const string indexedCollection = "quick_users";
        const string noIndexCollection = "probe_noindex";

        if (File.Exists(databaseFile)) File.Delete(databaseFile);

        var options = new TinyDbOptions
        {
            DatabaseName = "AllocProbeDb",
            PageSize = 16384,
            CacheSize = 1000,
            EnableJournaling = false,
            WriteConcern = WriteConcern.Journaled,
        };

        var runPoolTest = string.Equals(
            Environment.GetEnvironmentVariable("TINYDB_POOL_TEST"),
            "1",
            StringComparison.OrdinalIgnoreCase);

        if (runPoolTest)
        {
            Console.WriteLine();
            Console.WriteLine("=== ArrayPool Smoke Test ===");
            Console.WriteLine("æ³¨æ„ï¼šè¿™ä¸ªæµ‹è¯•åªæ˜¯ç”¨æ¥åˆ¤æ–­ Rent/Return æ˜¯å¦å¯¼è‡´å¤§é‡ GC åˆ†é…ã€‚");

            const int poolIterations = 10_000;
            long alloc8k = 0;
            long alloc16k = 0;

            for (int i = 0; i < poolIterations; i++)
            {
                var before = GC.GetAllocatedBytesForCurrentThread();
                var buffer = ArrayPool<byte>.Shared.Rent(8 * 1024);
                ArrayPool<byte>.Shared.Return(buffer);
                alloc8k += GC.GetAllocatedBytesForCurrentThread() - before;

                before = GC.GetAllocatedBytesForCurrentThread();
                buffer = ArrayPool<byte>.Shared.Rent(16 * 1024);
                ArrayPool<byte>.Shared.Return(buffer);
                alloc16k += GC.GetAllocatedBytesForCurrentThread() - before;
            }

            Console.WriteLine($"Rent/Return 8KB å¹³å‡åˆ†é…: {alloc8k / (double)poolIterations:N2} bytes");
            Console.WriteLine($"Rent/Return 16KB å¹³å‡åˆ†é…: {alloc16k / (double)poolIterations:N2} bytes");
        }

        var runIndexOnly = string.Equals(
            Environment.GetEnvironmentVariable("TINYDB_INDEX_ONLY"),
            "1",
            StringComparison.OrdinalIgnoreCase);

        if (runIndexOnly)
        {
            Console.WriteLine();
            Console.WriteLine("=== Index Only Insert ===");

            const int indexIterations = 1000;
            const string indexDbFile = "alloc_index_only.db";

            if (File.Exists(indexDbFile)) File.Delete(indexDbFile);

            var ds = new DiskStream(indexDbFile, FileAccess.ReadWrite, FileShare.ReadWrite);
            using var pm = new PageManager(ds, options.PageSize, options.CacheSize);

            using var pk = new BTreeIndex(pm, "pk_id", new[] { "_id" }, unique: true);
            using var name = new BTreeIndex(pm, "idx_name", new[] { "name" }, unique: false);
            using var email = new BTreeIndex(pm, "uidx_email", new[] { "email" }, unique: true);
            using var age = new BTreeIndex(pm, "idx_age", new[] { "age" }, unique: false);

            var ids = new BsonValue[indexIterations];
            var pkKeys = new IndexKey[indexIterations];
            var nameKeys = new IndexKey[indexIterations];
            var emailKeys = new IndexKey[indexIterations];
            var ageKeys = new IndexKey[indexIterations];

            for (int i = 0; i < indexIterations; i++)
            {
                var oid = new BsonObjectId(ObjectId.NewObjectId());
                ids[i] = oid;
                pkKeys[i] = new IndexKey(oid);
                nameKeys[i] = new IndexKey(new BsonString($"User{i}"));
                emailKeys[i] = new IndexKey(new BsonString($"user{i}@quick.com"));
                ageKeys[i] = new IndexKey(BsonInt32.FromValue(20 + (i % 50)));
            }

            static double Measure(BTreeIndex idx, IndexKey[] keys, BsonValue[] values)
            {
                long allocated = 0;
                for (int i = 0; i < keys.Length; i++)
                {
                    var before = GC.GetAllocatedBytesForCurrentThread();
                    idx.Insert(keys[i], values[i]);
                    allocated += GC.GetAllocatedBytesForCurrentThread() - before;
                }
                return allocated / (double)keys.Length;
            }

            Console.WriteLine($"pk(unique, ObjectId) avg alloc: {Measure(pk, pkKeys, ids):N0} bytes");
            Console.WriteLine($"name(non-unique, string) avg alloc: {Measure(name, nameKeys, ids):N0} bytes");
            Console.WriteLine($"email(unique, string) avg alloc: {Measure(email, emailKeys, ids):N0} bytes");
            Console.WriteLine($"age(non-unique, int) avg alloc: {Measure(age, ageKeys, ids):N0} bytes");
        }

        using var engine = new TinyDbEngine(databaseFile, options);
        _ = engine.GetCollection<QuickIndexBenchmark.QuickUser>();
        _ = engine.GetCollection<BsonDocument>(noIndexCollection);

        var insertInternalMethod = typeof(TinyDbEngine).GetMethod(
            "InsertDocumentInternal",
            BindingFlags.Instance | BindingFlags.NonPublic);

        if (insertInternalMethod == null)
        {
            throw new InvalidOperationException("无法通过反射找到 TinyDbEngine.InsertDocumentInternal 方法。");
        }

        var insertInternal = (Func<string, BsonDocument, BsonValue>)insertInternalMethod.CreateDelegate(
            typeof(Func<string, BsonDocument, BsonValue>),
            engine);

        long toDocumentAllocated = 0;
        long insertAllocatedIndexed = 0;
        long insertAllocatedNoIndex = 0;

        for (int i = 0; i < iterations; i++)
        {
            var user = new QuickIndexBenchmark.QuickUser
            {
                Id = ObjectId.NewObjectId(),
                Name = $"User{i}",
                Email = $"user{i}@quick.com",
                Age = 20 + (i % 50),
                Salary = 30000 + (i % 100) * 100
            };

            long before = GC.GetAllocatedBytesForCurrentThread();
            var doc = AotBsonMapper.ToDocument(user);
            toDocumentAllocated += GC.GetAllocatedBytesForCurrentThread() - before;

            before = GC.GetAllocatedBytesForCurrentThread();
            insertInternal(indexedCollection, doc);
            insertAllocatedIndexed += GC.GetAllocatedBytesForCurrentThread() - before;

            var rawDoc = new BsonDocumentBuilder()
                .Set("_id", new BsonObjectId(ObjectId.NewObjectId()))
                .Set("_collection", noIndexCollection)
                .Set("name", new BsonString($"User{i}"))
                .Set("email", new BsonString($"user{i}@quick.com"))
                .Set("age", BsonInt32.FromValue(20 + (i % 50)))
                .Set("salary", new BsonDecimal128((decimal)(30000 + (i % 100) * 100)))
                .Build();

            before = GC.GetAllocatedBytesForCurrentThread();
            insertInternal(noIndexCollection, rawDoc);
            insertAllocatedNoIndex += GC.GetAllocatedBytesForCurrentThread() - before;
        }

        Console.WriteLine();

        var runAllocTicks = string.Equals(
            Environment.GetEnvironmentVariable("TINYDB_ALLOC_TICKS"),
            "1",
            StringComparison.OrdinalIgnoreCase);

        if (runAllocTicks)
        {
            Console.WriteLine("=== Allocation Ticks (Top Types) ===");
            Console.WriteLine("æ³¨æ„ï¼šGCAllocationTick æ˜¯é‡‡æ ·äº‹ä»¶ï¼Œç»“æžœä»…ç”¨äºŽå®šæ€§åˆ†æžã€‚");

            const int tickIterations = 200;
            const string tickDatabaseFile = "alloc_ticks.db";

            if (File.Exists(tickDatabaseFile)) File.Delete(tickDatabaseFile);

            using var tickEngine = new TinyDbEngine(tickDatabaseFile, options);
            _ = tickEngine.GetCollection<QuickIndexBenchmark.QuickUser>();

            var tickInsertInternalMethod = typeof(TinyDbEngine).GetMethod(
                "InsertDocumentInternal",
                BindingFlags.Instance | BindingFlags.NonPublic);

            if (tickInsertInternalMethod == null)
            {
                throw new InvalidOperationException("æ— æ³•é€šè¿‡åå°„æ‰¾åˆ° TinyDbEngine.InsertDocumentInternal æ–¹æ³•ã€‚");
            }

            var tickInsertInternal = (Func<string, BsonDocument, BsonValue>)tickInsertInternalMethod.CreateDelegate(
                typeof(Func<string, BsonDocument, BsonValue>),
                tickEngine);

            using var listener = new AllocationTickListener();
            listener.Start();

            for (int i = 0; i < tickIterations; i++)
            {
                var user = new QuickIndexBenchmark.QuickUser
                {
                    Id = ObjectId.NewObjectId(),
                    Name = $"User{i}",
                    Email = $"user{i}@quick.com",
                    Age = 20 + (i % 50),
                    Salary = 30000 + (i % 100) * 100
                };

                var doc = AotBsonMapper.ToDocument(user);
                tickInsertInternal(indexedCollection, doc);
            }

            listener.StopListening();

            foreach (var entry in listener.BytesByType
                         .OrderByDescending(kvp => kvp.Value)
                         .Take(15))
            {
                Console.WriteLine($"{entry.Key}: {entry.Value:N0} bytes");
            }

            Console.WriteLine();
        }
        Console.WriteLine($"迭代次数: {iterations}");
        Console.WriteLine($"AotBsonMapper.ToDocument 平均分配: {toDocumentAllocated / (double)iterations:N0} bytes");
        Console.WriteLine($"InsertDocumentInternal(含索引) 平均分配: {insertAllocatedIndexed / (double)iterations:N0} bytes");
        Console.WriteLine($"InsertDocumentInternal(无索引) 平均分配: {insertAllocatedNoIndex / (double)iterations:N0} bytes");
    }
}
