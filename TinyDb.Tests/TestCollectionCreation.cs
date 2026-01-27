using System;
using System.Threading.Tasks;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TinyDb.Core;

namespace TinyDb.Tests;

/// <summary>
/// 集合创建功能测试
/// </summary>
public class TestCollectionCreation : IDisposable
{
    private string _testDbPath = string.Empty;

    [Test]
    public async Task CreateCollectionAsync_ShouldCreateCollectionSuccessfully()
    {
        // Arrange
        _testDbPath = $"test_collection_creation_{Guid.NewGuid():N}.db";
        using var engine = new TinyDbEngine(_testDbPath, new TinyDbOptions
        {
            EnableJournaling = false,
            ReadOnly = false
        });

        // Act - 直接测试引擎级别的集合创建
        var collection = engine.GetCollection<TestDocument>("TestCollection");
        var testDoc = new TestDocument
        {
            Name = "Test Document",
            Created = DateTime.UtcNow,
            IsActive = true
        };
        var id = collection.Insert(testDoc);

        // Assert
        await Assert.That(id.ToString()).IsNotEmpty();
        await Assert.That(engine.CollectionExists("TestCollection")).IsTrue();

        // 验证集合数量
        var collectionNames = engine.GetCollectionNames();
        await Assert.That(collectionNames.Count()).IsGreaterThanOrEqualTo(1);

        // 验证文档计数
        var count = collection.Count();
        await Assert.That(count).IsEqualTo(1);

        // 清理测试数据
        collection.Delete(id);
    }

    [Test]
    public async Task CreateCollectionAsync_ShouldCreateMultipleCollections()
    {
        // Arrange
        _testDbPath = $"test_multiple_collections_{Guid.NewGuid():N}.db";
        using var engine = new TinyDbEngine(_testDbPath, new TinyDbOptions
        {
            EnableJournaling = false,
            ReadOnly = false
        });

        // Act
        var collection1 = engine.GetCollection<TestDocument>("Collection1");
        var collection2 = engine.GetCollection<TestDocument>("Collection2");
        var collection3 = engine.GetCollection<TestDocument>("Collection3");

        // 插入测试文档以确保集合被创建
        var doc1 = new TestDocument { Name = "Doc1", Created = DateTime.UtcNow, IsActive = true };
        var doc2 = new TestDocument { Name = "Doc2", Created = DateTime.UtcNow, IsActive = true };
        var doc3 = new TestDocument { Name = "Doc3", Created = DateTime.UtcNow, IsActive = true };

        var id1 = collection1.Insert(doc1);
        var id2 = collection2.Insert(doc2);
        var id3 = collection3.Insert(doc3);

        // Assert
        await Assert.That(engine.CollectionExists("Collection1")).IsTrue();
        await Assert.That(engine.CollectionExists("Collection2")).IsTrue();
        await Assert.That(engine.CollectionExists("Collection3")).IsTrue();

        // 验证集合数量
        var collectionNames = engine.GetCollectionNames();
        await Assert.That(collectionNames.Count()).IsGreaterThanOrEqualTo(3);

        // 清理测试数据
        collection1.Delete(id1);
        collection2.Delete(id2);
        collection3.Delete(id3);
    }

    [Test]
    public async Task CreateCollectionAsync_ShouldAllowDocumentInsertionAfterCreation()
    {
        // Arrange
        _testDbPath = $"test_document_insertion_{Guid.NewGuid():N}.db";
        using var engine = new TinyDbEngine(_testDbPath, new TinyDbOptions
        {
            EnableJournaling = false,
            ReadOnly = false
        });
        var collectionName = "DocumentTestCollection";

        // Act
        var collection = engine.GetCollection<TestDocument>(collectionName);
        var testDoc = new TestDocument
        {
            Name = "Test Document",
            Created = DateTime.UtcNow,
            IsActive = true
        };
        var documentId = collection.Insert(testDoc);

        // Assert
        await Assert.That(documentId.ToString()).IsNotEmpty();
        await Assert.That(engine.CollectionExists(collectionName)).IsTrue();

        // 验证文档可以被检索
        var retrievedDoc = collection.FindById(documentId);
        await Assert.That(retrievedDoc).IsNotNull();
        await Assert.That(retrievedDoc!.Name).IsEqualTo("Test Document");

        // 清理测试数据
        collection.Delete(documentId);
    }

    public void Dispose()
    {
        // 清理测试文件
        if (System.IO.File.Exists(_testDbPath))
        {
            System.IO.File.Delete(_testDbPath);
        }

        // 清理可能的WAL文件
        var walPath = _testDbPath.Replace(".db", "-wal.db");
        if (System.IO.File.Exists(walPath))
        {
            System.IO.File.Delete(walPath);
        }
    }
}

/// <summary>
/// 测试文档类
/// </summary>
public class TestDocument
{
    public string? Name { get; set; }
    public DateTime Created { get; set; }
    public bool IsActive { get; set; }
}