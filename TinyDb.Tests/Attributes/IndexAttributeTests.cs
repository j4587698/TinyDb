using System.IO;
using System.Linq;
using System.Threading.Tasks;
using TinyDb.Attributes;
using TinyDb.Bson;
using TinyDb.Collections;
using TinyDb.Core;
using TinyDb.Index;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Attributes;

/// <summary>
/// IndexAttribute 测试
/// </summary>
public class IndexAttributeTests
{
    private string _databasePath = null!;
    private TinyDbEngine _engine = null!;

    [Before(Test)]
    public void Setup()
    {
        _databasePath = Path.Combine(Path.GetTempPath(), $"index_attr_{Guid.NewGuid():N}.db");
        var options = new TinyDbOptions
        {
            DatabaseName = "IndexAttributeTestDb",
            PageSize = 4096,
            CacheSize = 100
        };

        _engine = new TinyDbEngine(_databasePath, options);
    }

    [After(Test)]
    public void Cleanup()
    {
        _engine?.Dispose();
        if (File.Exists(_databasePath))
        {
            File.Delete(_databasePath);
        }
    }

    [Test]
    public async Task IndexAttribute_ShouldCreateSingleIndex()
    {
        var collection = _engine.GetCollection<TestUser>();

        var indexManager = collection.GetIndexManager();
        await Assert.That(indexManager.IndexExists("idx_name")).IsTrue();
        var index = indexManager.GetIndex("idx_name");
        await Assert.That(index).IsNotNull();
        await Assert.That(index!.IsUnique).IsFalse();
    }

    [Test]
    public async Task IndexAttribute_ShouldCreateUniqueIndex()
    {
        var collection = _engine.GetCollection<TestUserWithUniqueEmail>();

        var indexManager = collection.GetIndexManager();
        await Assert.That(indexManager.IndexExists("uidx_email")).IsTrue();
        var index = indexManager.GetIndex("uidx_email");
        await Assert.That(index).IsNotNull();
        await Assert.That(index!.IsUnique).IsTrue();
    }

    [Test]
    public async Task IndexAttribute_ShouldCreateIndexWithCustomName()
    {
        var collection = _engine.GetCollection<TestUserWithCustomIndex>();

        var indexManager = collection.GetIndexManager();
        await Assert.That(indexManager.IndexExists("custom_name_index")).IsTrue();
    }

    [Test]
    public async Task CompositeIndexAttribute_ShouldCreateCompositeIndex()
    {
        var collection = _engine.GetCollection<TestUserWithCompositeIndex>();

        var indexManager = collection.GetIndexManager();
        await Assert.That(indexManager.IndexExists("idx_name_age")).IsTrue();
        var index = indexManager.GetIndex("idx_name_age");
        await Assert.That(index).IsNotNull();
        await Assert.That(index!.Fields).Contains("name");
        await Assert.That(index.Fields).Contains("age");
    }

    [Test]
    public async Task IndexAttribute_ShouldRespectPriority()
    {
        var collection = _engine.GetCollection<TestUserWithMultipleIndexes>();

        var indexManager = collection.GetIndexManager();
        await Assert.That(indexManager.IndexExists("high_priority_idx")).IsTrue();
        await Assert.That(indexManager.IndexExists("normal_idx")).IsTrue();
        await Assert.That(indexManager.IndexExists("low_priority_idx")).IsTrue();
    }

    [Test]
    public async Task IndexScanner_ShouldGetEntityIndexes()
    {
        var indexes = IndexScanner.GetEntityIndexes(typeof(TestUserWithMultipleIndexes));

        await Assert.That(indexes.Count).IsEqualTo(3);

        var nameIndex = indexes.FirstOrDefault(i => i.Fields.Any(f => string.Equals(f, "name", StringComparison.OrdinalIgnoreCase)));
        var ageIndex = indexes.FirstOrDefault(i => i.Fields.Any(f => string.Equals(f, "age", StringComparison.OrdinalIgnoreCase)));
        var emailIndex = indexes.FirstOrDefault(i => i.Fields.Any(f => string.Equals(f, "email", StringComparison.OrdinalIgnoreCase)));

        await Assert.That(nameIndex).IsNotNull();
        await Assert.That(ageIndex).IsNotNull();
        await Assert.That(emailIndex).IsNotNull();

        await Assert.That(nameIndex!.Priority < ageIndex!.Priority).IsTrue();
        await Assert.That(ageIndex.Priority < emailIndex!.Priority).IsTrue();
    }

    [Test]
    public async Task AutoIndex_ShouldWorkWithInsertOperations()
    {
        var collection = _engine.GetCollection<TestUser>();
        var indexManager = collection.GetIndexManager();

        var user = new TestUser
        {
            Name = "Test User",
            Age = 25,
            Email = "test@example.com"
        };
        collection.Insert(user);

        await Assert.That(indexManager.IndexExists("idx_name")).IsTrue();
        await Assert.That(indexManager.IndexExists("idx_age")).IsTrue();
        await Assert.That(indexManager.IndexExists("idx_email")).IsTrue();

        var nameIndex = indexManager.GetIndex("idx_name");
        await Assert.That(nameIndex).IsNotNull();
        await Assert.That(nameIndex!.EntryCount).IsEqualTo(1);
    }

    [Test]
    public async Task CompositeIndex_ShouldSupportUniqueConstraint()
    {
        var collection = _engine.GetCollection<TestUserWithUniqueComposite>();

        var indexManager = collection.GetIndexManager();
        await Assert.That(indexManager.IndexExists("unique_name_email")).IsTrue();
        var index = indexManager.GetIndex("unique_name_email");
        await Assert.That(index).IsNotNull();
        await Assert.That(index!.IsUnique).IsTrue();
    }

    [Test]
    public async Task IndexAttribute_ShouldHandleDuplicateIndexNames()
    {
        var collection = _engine.GetCollection<TestUserWithDuplicateIndexes>();

        var indexManager = collection.GetIndexManager();
        var indexes = indexManager.IndexNames.Where(name => name.Contains("duplicate")).ToList();
        await Assert.That(indexes.Count).IsEqualTo(1);
    }
}

/// <summary>
/// 测试用户实体 - 基本索引
/// </summary>
[Entity("test_users")]
public class TestUser
{
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();

    [Index]
    public string Name { get; set; } = "";

    [Index]
    public int Age { get; set; }

    [Index]
    public string Email { get; set; } = "";

    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
}

/// <summary>
/// 测试用户实体 - 唯一邮箱索引
/// </summary>
[Entity("test_users")]
public class TestUserWithUniqueEmail
{
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();

    [Index(Unique = true)]
    public string Email { get; set; } = "";

    public string Name { get; set; } = "";
}

/// <summary>
/// 测试用户实体 - 自定义索引名称
/// </summary>
[Entity("test_users")]
public class TestUserWithCustomIndex
{
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();

    [Index(Name = "custom_name_index")]
    public string Name { get; set; } = "";
}

/// <summary>
/// 测试用户实体 - 复合索引
/// </summary>
[Entity("test_users")]
public class TestUserWithCompositeIndex
{
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();

    [Index(Name = "idx_name_age", Priority = 1)]
    public string Name { get; set; } = "";

    [Index(Name = "idx_name_age", Priority = 2)]
    public int Age { get; set; }
}

/// <summary>
/// 测试用户实体 - 多个索引，测试优先级
/// </summary>
[Entity("test_users")]
public class TestUserWithMultipleIndexes
{
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();

    [Index(Name = "high_priority_idx", Priority = 1)]
    public string Name { get; set; } = "";

    [Index(Name = "normal_idx", Priority = 5)]
    public int Age { get; set; }

    [Index(Name = "low_priority_idx", Priority = 10)]
    public string Email { get; set; } = "";
}

/// <summary>
/// 测试用户实体 - 唯一复合索引
/// </summary>
[Entity("test_users")]
public class TestUserWithUniqueComposite
{
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();

    [Index(Name = "unique_name_email", Priority = 1, Unique = true)]
    public string Name { get; set; } = "";

    [Index(Name = "unique_name_email", Priority = 2, Unique = true)]
    public string Email { get; set; } = "";
}

/// <summary>
/// 测试用户实体 - 重复索引名称
/// </summary>
[Entity("test_users")]
public class TestUserWithDuplicateIndexes
{
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();

    [Index(Name = "duplicate_index")]
    public string Field1 { get; set; } = "";

    [Index(Name = "duplicate_index")]
    public string Field2 { get; set; } = "";
}
