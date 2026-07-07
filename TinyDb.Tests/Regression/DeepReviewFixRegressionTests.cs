using System.Globalization;
using TinyDb.Attributes;
using TinyDb.Bson;
using TinyDb.Core;
using TinyDb.IdGeneration;
using TinyDb.Serialization;
using TinyDb.Storage;
using TinyDb.Tests.Regression.Systematic.Models;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Regression;

[NotInParallel]
public sealed class DeepReviewFixRegressionTests : IDisposable
{
    private readonly string _directory;

    public DeepReviewFixRegressionTests()
    {
        _directory = Path.Combine(Path.GetTempPath(), "TinyDbDeepReviewFixes", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_directory);
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_directory))
            {
                Directory.Delete(_directory, recursive: true);
            }
        }
        catch
        {
        }
    }

    [Test]
    public async Task GuidV7Fallback_ShouldExposeUnixTimestampInCanonicalBytes()
    {
        var before = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var guid = AutoIdGenerator.CreateGuidV7();
        var after = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        var timestamp = ReadGuidV7Timestamp(guid);

        await Assert.That(timestamp).IsGreaterThanOrEqualTo(before);
        await Assert.That(timestamp).IsLessThanOrEqualTo(after);
    }

    [Test]
    public async Task GuidV7Generator_ShouldExposeUnixTimestampInCanonicalBytes()
    {
        var generator = new GuidV7Generator();
        var property = typeof(GuidV7RegressionDocument).GetProperty(nameof(GuidV7RegressionDocument.Id))!;

        var before = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var id = generator.GenerateId(typeof(GuidV7RegressionDocument), property);
        var after = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        var timestamp = ReadGuidV7Timestamp(Guid.Parse(id.ToString()));

        await Assert.That(timestamp).IsGreaterThanOrEqualTo(before);
        await Assert.That(timestamp).IsLessThanOrEqualTo(after);
    }

    [Test]
    public async Task Transaction_UpdateAfterPendingInsert_ShouldPersistUpdatedDocument()
    {
        using var engine = new TinyDbEngine(Path.Combine(_directory, "tx-insert-update.db"));
        var collection = engine.GetCollection<TransactionInsertUpdateDocument>();

        using (var transaction = engine.BeginTransaction())
        {
            var document = new TransactionInsertUpdateDocument { Id = 1, Name = "inserted", Value = 1 };
            collection.Insert(document);

            document.Name = "updated";
            document.Value = 2;

            await Assert.That(collection.Update(document)).IsEqualTo(1);
            await Assert.That(collection.FindById(1)!.Name).IsEqualTo("updated");

            transaction.Commit();
        }

        var persisted = collection.FindById(1);
        await Assert.That(persisted).IsNotNull();
        await Assert.That(persisted!.Name).IsEqualTo("updated");
        await Assert.That(persisted.Value).IsEqualTo(2);
    }

    [Test]
    public async Task FindAll_OnDisposedCollection_ShouldThrowBeforeEnumeration()
    {
        using var engine = new TinyDbEngine(Path.Combine(_directory, "findall-disposed.db"));
        var collection = engine.GetCollection<TransactionInsertUpdateDocument>();

        ((IDisposable)collection).Dispose();

        await Assert.That(() => collection.FindAll()).Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task BsonConversion_ShouldReleaseCycleTrackingSetAfterTopLevelConversion()
    {
        _ = BsonConversion.ToBsonValue(new List<object> { "value" });

        await Assert.That(BsonConversion.HasActiveSerializationTracking).IsFalse();
    }

    [Test]
    public async Task FreePage_ShouldAppendWalOnlyForCommittedFreeListUpdate()
    {
        using var disk = new MemoryDiskStream();
        using var pageManager = new PageManager(disk, pageSize: 4096, maxCacheSize: 8);

        var target = pageManager.NewPage(PageType.Data);
        var existingFreeHead = pageManager.NewPage(PageType.Data);
        var competing = pageManager.NewPage(PageType.Data);

        pageManager.SavePage(target);
        pageManager.SavePage(existingFreeHead);
        pageManager.SavePage(competing);
        pageManager.FreePage(existingFreeHead.PageID);

        var targetWalAppends = 0;
        Task? competingFree = null;

        pageManager.RegisterWAL(
            (page, _) =>
            {
                if (page.PageID != target.PageID)
                {
                    return;
                }

                targetWalAppends++;
                if (competingFree != null)
                {
                    return;
                }

                competingFree = Task.Run(() => pageManager.FreePage(competing.PageID));
                Thread.Sleep(100);
            },
            _ => { });

        pageManager.FreePage(target.PageID);

        if (competingFree != null)
        {
            await competingFree.WaitAsync(TimeSpan.FromSeconds(5));
        }

        await Assert.That(targetWalAppends).IsEqualTo(1);
    }

    [Test]
    public async Task SourceGenerator_ShouldIncludePublicFieldsFromOtherPartialDeclaration()
    {
        var document = AotBsonMapper.ToDocument(new SplitPartialFieldEntity
        {
            Id = 1,
            Name = "from-field"
        });

        await Assert.That(document["name"].ToString()).IsEqualTo("from-field");
    }

    [Test]
    public async Task SourceGenerator_ShouldHandleUnderscoreInNestedTypeName()
    {
        var document = AotBsonMapper.ToDocument(new Outer_Class.NestedEntity
        {
            Id = 2,
            Name = "nested"
        });

        await Assert.That(document["name"].ToString()).IsEqualTo("nested");
    }

    [Test]
    public async Task SourceGenerator_ShouldNotSkipBusinessNamespaceContainingSystem()
    {
        var registered = AotHelperRegistry.TryGetAdapter<SystematicEntity>(out var adapter);

        await Assert.That(registered).IsTrue();

        var document = adapter!.ToDocument(new SystematicEntity
        {
            Id = 10,
            Name = "systematic"
        });

        await Assert.That(document["name"].ToString()).IsEqualTo("systematic");
    }

    [Test]
    public async Task SourceGenerator_ShouldCompileGenericEntityHelpersWithDistinctArities()
    {
        var oneArgumentType = typeof(GenericArityRegressionEntity<int>);
        var twoArgumentType = typeof(GenericArityRegressionEntity<int, string>);

        await Assert.That(oneArgumentType.Name).StartsWith("GenericArityRegressionEntity`1");
        await Assert.That(twoArgumentType.Name).StartsWith("GenericArityRegressionEntity`2");
        await Assert.That(AotHelperRegistry.TryGetAdapter<GenericArityRegressionEntity<int>>(out _)).IsFalse();
        await Assert.That(AotHelperRegistry.TryGetAdapter<GenericArityRegressionEntity<int, string>>(out _)).IsFalse();
    }

    [Test]
    public async Task Decimal128_Bytes_ShouldUseLittleEndianLayout()
    {
        var value = new Decimal128(0x0102030405060708UL, 0x1112131415161718UL);
        var bytes = value.ToBytes();
        var expected = new byte[]
        {
            0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01,
            0x18, 0x17, 0x16, 0x15, 0x14, 0x13, 0x12, 0x11
        };

        await Assert.That(bytes.SequenceEqual(expected)).IsTrue();
        await Assert.That(Decimal128.FromBytes(bytes).Equals(value)).IsTrue();
    }

    private static long ReadGuidV7Timestamp(Guid guid)
    {
        return long.Parse(guid.ToString("N")[..12], NumberStyles.HexNumber, CultureInfo.InvariantCulture);
    }

    [Entity("GuidV7RegressionDocuments")]
    public sealed class GuidV7RegressionDocument
    {
        [IdGeneration(IdGenerationStrategy.GuidV7)]
        public string Id { get; set; } = string.Empty;
    }

    [Entity("TransactionInsertUpdateDocuments")]
    public sealed class TransactionInsertUpdateDocument
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public int Value { get; set; }
    }

    [Entity("SplitPartialFieldEntities")]
    public partial class SplitPartialFieldEntity
    {
        public int Id { get; set; }
    }

    public partial class SplitPartialFieldEntity
    {
        public string Name = string.Empty;
    }

    public sealed class Outer_Class
    {
        [Entity("NestedUnderscoreEntities")]
        public partial class NestedEntity
        {
            public int Id { get; set; }
            public string Name { get; set; } = string.Empty;
        }
    }

    private sealed class MemoryDiskStream : IDiskStream
    {
        private readonly MemoryStream _stream = new();
        private readonly object _syncRoot = new();

        public string FilePath => "memory";
        public long Size
        {
            get
            {
                lock (_syncRoot)
                {
                    return _stream.Length;
                }
            }
        }

        public bool IsReadable => true;
        public bool IsWritable => true;

        public void Dispose()
        {
            _stream.Dispose();
        }

        public void Flush()
        {
        }

        public Task FlushAsync(CancellationToken cancellationToken = default)
        {
            return Task.CompletedTask;
        }

        public byte[] ReadPage(long pageOffset, int pageSize)
        {
            var buffer = new byte[pageSize];
            lock (_syncRoot)
            {
                if (pageOffset >= _stream.Length)
                {
                    return buffer;
                }

                _stream.Position = pageOffset;
                var read = _stream.Read(buffer, 0, pageSize);
                if (read < pageSize)
                {
                    Array.Clear(buffer, read, pageSize - read);
                }

                return buffer;
            }
        }

        public Task<byte[]> ReadPageAsync(long pageOffset, int pageSize, CancellationToken cancellationToken = default)
        {
            return Task.FromResult(ReadPage(pageOffset, pageSize));
        }

        public void WritePage(long pageOffset, byte[] pageData)
        {
            lock (_syncRoot)
            {
                if (pageOffset + pageData.Length > _stream.Length)
                {
                    _stream.SetLength(pageOffset + pageData.Length);
                }

                _stream.Position = pageOffset;
                _stream.Write(pageData, 0, pageData.Length);
            }
        }

        public Task WritePageAsync(long pageOffset, byte[] pageData, CancellationToken cancellationToken = default)
        {
            WritePage(pageOffset, pageData);
            return Task.CompletedTask;
        }

        public void SetLength(long length)
        {
            lock (_syncRoot)
            {
                _stream.SetLength(length);
            }
        }

        public DiskStreamStatistics GetStatistics()
        {
            return new DiskStreamStatistics
            {
                FilePath = FilePath,
                Size = Size,
                IsReadable = IsReadable,
                IsWritable = IsWritable,
                IsSeekable = true
            };
        }
    }
}
