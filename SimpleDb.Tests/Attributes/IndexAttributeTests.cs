using SimpleDb.Attributes;
using SimpleDb.Core;
using SimpleDb.Collections;
using SimpleDb.Index;
using Xunit;

namespace SimpleDb.Tests.Attributes;

/// <summary>
/// IndexAttribute 测试
/// </summary>
public class IndexAttributeTests : IDisposable
{
    private readonly SimpleDbEngine _engine;

    public IndexAttributeTests()
    {
        // 创建临时数据库
        var testDbFile = Path.GetTempFileName();
        var options = new SimpleDbOptions
        {
            DatabaseName = "IndexAttributeTestDb",
            PageSize = 4096,
            CacheSize = 100
        };

        _engine = new SimpleDbEngine(testDbFile, options);
    }

    public void Dispose()
    {
        _engine?.Dispose();
    }

    [Fact]
    public void IndexAttribute_ShouldCreateSingleIndex()
    {
        // Act
        var collection = _engine.GetCollection<TestUser>("test_users");

        // Assert
        var indexManager = collection.GetIndexManager();
        Assert.True(indexManager.IndexExists("idx_name"));
        Assert.False(indexManager.IndexExists("idx_name").Unique);
    }

    [Fact]
    public void IndexAttribute_ShouldCreateUniqueIndex()
    {
        // Act
        var collection = _engine.GetCollection<TestUserWithUniqueEmail>("test_users");

        // Assert
        var indexManager = collection.GetIndexManager();
        Assert.True(indexManager.IndexExists("uidx_email"));
        var index = indexManager.GetIndex("uidx_email");
        Assert.True(index.IsUnique);
    }

    [Fact]
    public void IndexAttribute_ShouldCreateIndexWithCustomName()
    {
        // Act
        var collection = _engine.GetCollection<TestUserWithCustomIndex>("test_users");

        // Assert
        var indexManager = collection.GetIndexManager();
        Assert.True(indexManager.IndexExists("custom_name_index"));
    }

    [Fact]
    public void CompositeIndexAttribute_ShouldCreateCompositeIndex()
    {
        // Act
        var collection = _engine.GetCollection<TestUserWithCompositeIndex>("test_users");

        // Assert
        var indexManager = collection.GetIndexManager();
        Assert.True(indexManager.IndexExists("idx_name_age"));
        var index = indexManager.GetIndex("idx_name_age");
        Assert.Equal(2, index.Fields.Count);
        Assert.Contains("name", index.Fields);
        Assert.Contains("age", index.Fields);
    }

    [Fact]
    public void IndexAttribute_ShouldRespectPriority()
    {
        // Act
        var collection = _engine.GetCollection<TestUserWithMultipleIndexes>("test_users");

        // Assert
        var indexManager = collection.GetIndexManager();
        // 高优先级索引应该先创建
        Assert.True(indexManager.IndexExists("high_priority_idx"));
        Assert.True(indexManager.IndexExists("normal_idx"));
        Assert.True(indexManager.IndexExists("low_priority_idx"));
    }

    [Fact]
    public void IndexScanner_ShouldGetEntityIndexes()
    {
        // Act
        var indexes = IndexScanner.GetEntityIndexes(typeof(TestUserWithMultipleIndexes));

        // Assert
        Assert.Equal(3, indexes.Count);

        var nameIndex = indexes.FirstOrDefault(i => i.Fields.Contains("name"));
        var ageIndex = indexes.FirstOrDefault(i => i.Fields.Contains("age"));
        var emailIndex = indexes.FirstOrDefault(i => i.Fields.Contains("email"));

        Assert.NotNull(nameIndex);
        Assert.NotNull(ageIndex);
        Assert.NotNull(emailIndex);

        // 验证优先级排序
        Assert.True(nameIndex.Priority < ageIndex.Priority);
        Assert.True(ageIndex.Priority < emailIndex.Priority);
    }

    [Fact]
    public void AutoIndex_ShouldWorkWithInsertOperations()
    {
        // Arrange
        var collection = _engine.GetCollection<TestUser>("auto_test_users");
        var indexManager = collection.GetIndexManager();

        // Act
        var user = new TestUser
        {
            Name = "Test User",
            Age = 25,
            Email = "test@example.com"
        };
        collection.Insert(user);

        // Assert
        Assert.True(indexManager.IndexExists("idx_name"));
        Assert.True(indexManager.IndexExists("idx_age"));
        Assert.True(indexManager.IndexExists("idx_email"));

        // 验证索引数据
        var nameIndex = indexManager.GetIndex("idx_name");
        Assert.Equal(1, nameIndex.EntryCount);
    }

    [Fact]
    public void CompositeIndex_ShouldSupportUniqueConstraint()
    {
        // Act
        var collection = _engine.GetCollection<TestUserWithUniqueComposite>("test_users");

        // Assert
        var indexManager = collection.GetIndexManager();
        Assert.True(indexManager.IndexExists("unique_name_email"));
        var index = indexManager.GetIndex("unique_name_email");
        Assert.True(index.IsUnique);
    }

    [Fact]
    public void IndexAttribute_ShouldHandleDuplicateIndexNames()
    {
        // Act
        var collection = _engine.GetCollection<TestUserWithDuplicateIndexes>("test_users");

        // Assert
        var indexManager = collection.GetIndexManager();
        // 应该只创建一个索引，因为名称相同
        var indexes = indexManager.IndexNames.Where(name => name.Contains("duplicate")).ToList();
        Assert.Single(indexes);
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

    public string Name { get; set; } = "";

    [Index(Unique = true)]
    public string Email { get; set; } = "";

    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
}

/// <summary>
/// 测试用户实体 - 自定义索引名称
/// </summary>
[Entity("test_users")]
public class TestUserWithCustomIndex
{
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();

    [Index("custom_name_index")]
    public string Name { get; set; } = "";

    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
}

/// <summary>
/// 测试用户实体 - 复合索引
/// </summary>
[Entity("test_users")]
[CompositeIndex("idx_name_age")]
public class TestUserWithCompositeIndex
{
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();

    public string Name { get; set; } = "";

    public int Age { get; set; }

    public string Email { get; set; } = "";

    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
}

/// <summary>
/// 测试用户实体 - 多个索引
/// </summary>
[Entity("test_users")]
public class TestUserWithMultipleIndexes
{
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();

    [Index(Priority = 1)]
    public string Name { get; set; } = "";

    [Index(Priority = 5)]
    public int Age { get; set; }

    [Index(Priority = 10)]
    public string Email { get; set; } = "";

    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
}

/// <summary>
/// 测试用户实体 - 唯一复合索引
/// </summary>
[Entity("test_users")]
[CompositeIndex("unique_name_email", true, "Name", "Email")]
public class TestUserWithUniqueComposite
{
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();

    public string Name { get; set; } = "";

    public string Email { get; set; } = "";

    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
}

/// <summary>
/// 测试用户实体 - 重复索引名称
/// </summary>
[Entity("test_users")]
public class TestUserWithDuplicateIndexes
{
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();

    [Index("duplicate_index")]
    public string Name { get; set; } = "";

    [Index("duplicate_index")]
    public string Email { get; set; } = "";

    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
}