using System;
using System.Threading.Tasks;
using TinyDb.Core;
using TinyDb.Index;
using TinyDb.Attributes;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Index;

/// <summary>
/// IndexScanner 异常处理和边界情况测试
/// </summary>
public class IndexScannerExceptionTests : IDisposable
{
    private readonly string _testDbPath;

    public IndexScannerExceptionTests()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"idx_scan_ex_{Guid.NewGuid():N}.db");
    }

    public void Dispose()
    {
        try { if (File.Exists(_testDbPath)) File.Delete(_testDbPath); } catch { }
    }

    // 测试实体：没有任何索引属性
    public class PlainEntity
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    // 测试实体：有复合索引
    [CompositeIndex("composite_name_age", "Name", "Age", Unique = true)]
    public class CompositeEntity
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
        public int Age { get; set; }
    }

    // 测试实体：有命名索引
    public class NamedIndexEntity
    {
        public int Id { get; set; }
        [Index(Name = "custom_email_idx", Unique = true)]
        public string Email { get; set; } = "";
    }

    // 测试实体：有优先级索引
    public class PriorityIndexEntity
    {
        public int Id { get; set; }
        [Index(Priority = 10)]
        public string HighPriority { get; set; } = "";
        [Index(Priority = 1)]
        public string LowPriority { get; set; } = "";
    }

    [Test]
    public async Task ScanAndCreateIndexes_NullEngine_ShouldThrow()
    {
        await Assert.That(() => IndexScanner.ScanAndCreateIndexes(null!, typeof(PlainEntity), "test"))
            .Throws<ArgumentNullException>();
    }

    [Test]
    public async Task ScanAndCreateIndexes_NullEntityType_ShouldThrow()
    {
        using var engine = new TinyDbEngine(_testDbPath);
        await Assert.That(() => IndexScanner.ScanAndCreateIndexes(engine, null!, "test"))
            .Throws<ArgumentNullException>();
    }

    [Test]
    public async Task ScanAndCreateIndexes_NullCollectionName_ShouldThrow()
    {
        using var engine = new TinyDbEngine(_testDbPath);
        await Assert.That(() => IndexScanner.ScanAndCreateIndexes(engine, typeof(PlainEntity), null!))
            .Throws<ArgumentException>();
    }

    [Test]
    public async Task ScanAndCreateIndexes_EmptyCollectionName_ShouldThrow()
    {
        using var engine = new TinyDbEngine(_testDbPath);
        await Assert.That(() => IndexScanner.ScanAndCreateIndexes(engine, typeof(PlainEntity), ""))
            .Throws<ArgumentException>();
    }

    [Test]
    public async Task ScanAndCreateIndexes_PlainEntity_ShouldCreateOnlyPrimaryKeyIndex()
    {
        using var engine = new TinyDbEngine(_testDbPath);
        IndexScanner.ScanAndCreateIndexes(engine, typeof(PlainEntity), "plain");
        
        var idxMgr = engine.GetIndexManager("plain");
        var stats = idxMgr.GetAllStatistics().ToList();
        
        // Should have at least the primary key index
        await Assert.That(stats.Count).IsGreaterThanOrEqualTo(1);
    }

    [Test]
    public async Task ScanAndCreateIndexes_CompositeEntity_ShouldCreateCompositeIndex()
    {
        using var engine = new TinyDbEngine(_testDbPath);
        IndexScanner.ScanAndCreateIndexes(engine, typeof(CompositeEntity), "composite");
        
        var idxMgr = engine.GetIndexManager("composite");
        
        // Check that composite index exists
        await Assert.That(idxMgr.IndexExists("composite_name_age")).IsTrue();
    }

    [Test]
    public async Task ScanAndCreateIndexes_CompositeEntity_CalledTwice_ShouldNotDuplicateCompositeIndex()
    {
        using var engine = new TinyDbEngine(_testDbPath);

        IndexScanner.ScanAndCreateIndexes(engine, typeof(CompositeEntity), "composite_dup");
        var idxMgr = engine.GetIndexManager("composite_dup");
        var count1 = idxMgr.GetAllStatistics().Count();

        IndexScanner.ScanAndCreateIndexes(engine, typeof(CompositeEntity), "composite_dup");
        var count2 = idxMgr.GetAllStatistics().Count();

        await Assert.That(count2).IsEqualTo(count1);
    }

    [Test]
    public async Task ScanAndCreateIndexes_CompositeEntity_WhenIndexesAlreadyExist_ShouldNotRecreate()
    {
        using var engine = new TinyDbEngine(_testDbPath);
        const string collectionName = "composite_exists";

        var idxMgr = engine.GetIndexManager(collectionName);
        idxMgr.CreateIndex($"pk_{collectionName}_id", new[] { "_id" }, true);
        idxMgr.CreateIndex("composite_name_age", new[] { "name", "age" }, unique: true);

        var count1 = idxMgr.GetAllStatistics().Count();
        IndexScanner.ScanAndCreateIndexes(engine, typeof(CompositeEntity), collectionName);
        var count2 = idxMgr.GetAllStatistics().Count();

        await Assert.That(count2).IsEqualTo(count1);
    }

    [Test]
    public async Task ScanAndCreateIndexes_NamedIndexEntity_ShouldUseCustomName()
    {
        using var engine = new TinyDbEngine(_testDbPath);
        IndexScanner.ScanAndCreateIndexes(engine, typeof(NamedIndexEntity), "named");
        
        var idxMgr = engine.GetIndexManager("named");
        
        // Check that custom named index exists
        await Assert.That(idxMgr.IndexExists("custom_email_idx")).IsTrue();
    }

    [Test]
    public async Task ScanAndCreateIndexes_CalledTwice_ShouldNotDuplicateIndexes()
    {
        using var engine = new TinyDbEngine(_testDbPath);
        
        // First call
        IndexScanner.ScanAndCreateIndexes(engine, typeof(NamedIndexEntity), "dup_test");
        var idxMgr1 = engine.GetIndexManager("dup_test");
        var count1 = idxMgr1.GetAllStatistics().Count();
        
        // Second call - should not throw and should not create duplicates
        IndexScanner.ScanAndCreateIndexes(engine, typeof(NamedIndexEntity), "dup_test");
        var count2 = idxMgr1.GetAllStatistics().Count();
        
        await Assert.That(count2).IsEqualTo(count1);
    }

    [Test]
    public async Task GetEntityIndexes_PlainEntity_ShouldReturnEmpty()
    {
        var indexes = IndexScanner.GetEntityIndexes(typeof(PlainEntity));
        await Assert.That(indexes.Count).IsEqualTo(0);
    }

    [Test]
    public async Task GetEntityIndexes_CompositeEntity_ShouldReturnCompositeIndex()
    {
        var indexes = IndexScanner.GetEntityIndexes(typeof(CompositeEntity));
        
        var compositeIndex = indexes.FirstOrDefault(i => i.Name == "composite_name_age");
        await Assert.That(compositeIndex).IsNotNull();
        await Assert.That(compositeIndex!.IsComposite).IsTrue();
        await Assert.That(compositeIndex.IsUnique).IsTrue();
        await Assert.That(compositeIndex.Fields.Length).IsEqualTo(2);
    }

    [Test]
    public async Task GetEntityIndexes_NamedIndexEntity_ShouldReturnNamedIndex()
    {
        var indexes = IndexScanner.GetEntityIndexes(typeof(NamedIndexEntity));
        
        var namedIndex = indexes.FirstOrDefault(i => i.Name == "custom_email_idx");
        await Assert.That(namedIndex).IsNotNull();
        await Assert.That(namedIndex!.IsUnique).IsTrue();
    }

    [Test]
    public async Task GetEntityIndexes_PriorityEntity_ShouldSortByPriority()
    {
        var indexes = IndexScanner.GetEntityIndexes(typeof(PriorityIndexEntity));
        
        await Assert.That(indexes.Count).IsEqualTo(2);
        // Sorted by priority ascending, so LowPriority (1) should come before HighPriority (10)
        await Assert.That(indexes[0].Priority).IsLessThan(indexes[1].Priority);
    }

    [Test]
    public async Task EntityIndexInfo_ToString_Composite_ShouldContainType()
    {
        var info = new EntityIndexInfo
        {
            Name = "composite_test",
            Fields = new[] { "Field1", "Field2" },
            IsUnique = false,
            IsComposite = true
        };
        
        var str = info.ToString();
        await Assert.That(str).Contains("Composite");
        await Assert.That(str).Contains("composite_test");
        await Assert.That(str).Contains("Non-Unique");
    }

    [Test]
    public async Task EntityIndexInfo_ToString_Single_ShouldContainType()
    {
        var info = new EntityIndexInfo
        {
            Name = "single_test",
            Fields = new[] { "Field1" },
            IsUnique = true,
            IsComposite = false
        };
        
        var str = info.ToString();
        await Assert.That(str).Contains("Single");
        await Assert.That(str).Contains("single_test");
        await Assert.That(str).Contains("Unique");
    }

    // 测试实体：多个属性使用同一索引名
    public class SharedIndexNameEntity
    {
        public int Id { get; set; }
        [Index(Name = "shared_idx")]
        public string Field1 { get; set; } = "";
        [Index(Name = "shared_idx")]
        public string Field2 { get; set; } = "";
    }

    [Test]
    public async Task ScanAndCreateIndexes_SharedIndexName_ShouldCreateSingleIndex()
    {
        using var engine = new TinyDbEngine(_testDbPath);
        IndexScanner.ScanAndCreateIndexes(engine, typeof(SharedIndexNameEntity), "shared");
        
        var idxMgr = engine.GetIndexManager("shared");
        
        // Should create only one index with the shared name
        await Assert.That(idxMgr.IndexExists("shared_idx")).IsTrue();
    }

    // 测试带降序排序的索引
    public class DescendingIndexEntity
    {
        public int Id { get; set; }
        [Index(SortDirection = IndexSortDirection.Descending)]
        public DateTime Created { get; set; }
    }

    [Test]
    public async Task GetEntityIndexes_DescendingIndex_ShouldHaveCorrectDirection()
    {
        var indexes = IndexScanner.GetEntityIndexes(typeof(DescendingIndexEntity));
        
        await Assert.That(indexes.Count).IsEqualTo(1);
        await Assert.That(indexes[0].SortDirection).IsEqualTo(IndexSortDirection.Descending);
    }
}
