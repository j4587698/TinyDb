using SimpleDb.Index;
using SimpleDb.Bson;
using Xunit;

namespace SimpleDb.Tests.Index;

/// <summary>
/// 索引管理器测试
/// </summary>
public class IndexManagerTests : IDisposable
{
    private readonly IndexManager _manager;

    public IndexManagerTests()
    {
        _manager = new IndexManager("test_collection");
    }

    public void Dispose()
    {
        _manager?.Dispose();
    }

    [Fact]
    public void Constructor_ShouldInitializeCorrectly()
    {
        // Assert
        Assert.Equal("test_collection", _manager.CollectionName);
        Assert.Equal(0, _manager.IndexCount);
        Assert.Empty(_manager.IndexNames);
    }

    [Fact]
    public void CreateIndex_ShouldCreateIndexSuccessfully()
    {
        // Arrange
        var name = "test_index";
        var fields = new[] { "field1", "field2" };

        // Act
        var result = _manager.CreateIndex(name, fields, true);

        // Assert
        Assert.True(result);
        Assert.Equal(1, _manager.IndexCount);
        Assert.Contains(name, _manager.IndexNames);
        Assert.True(_manager.IndexExists(name));
    }

    [Fact]
    public void CreateIndex_ShouldReturnFalseForDuplicateIndex()
    {
        // Arrange
        var name = "test_index";
        var fields = new[] { "field1" };
        _manager.CreateIndex(name, fields);

        // Act
        var result = _manager.CreateIndex(name, fields);

        // Assert
        Assert.False(result);
        Assert.Equal(1, _manager.IndexCount);
    }

    [Fact]
    public void CreateIndex_ShouldThrowExceptionForInvalidParameters()
    {
        // Arrange
        var fields = new[] { "field1" };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => _manager.CreateIndex("", fields));
        Assert.Throws<ArgumentException>(() => _manager.CreateIndex(null!, fields));
        Assert.Throws<ArgumentException>(() => _manager.CreateIndex("test", Array.Empty<string>()));
        Assert.Throws<ArgumentException>(() => _manager.CreateIndex("test", null!));
    }

    [Fact]
    public void DropIndex_ShouldRemoveIndexSuccessfully()
    {
        // Arrange
        var name = "test_index";
        var fields = new[] { "field1" };
        _manager.CreateIndex(name, fields);

        // Act
        var result = _manager.DropIndex(name);

        // Assert
        Assert.True(result);
        Assert.Equal(0, _manager.IndexCount);
        Assert.DoesNotContain(name, _manager.IndexNames);
        Assert.False(_manager.IndexExists(name));
    }

    [Fact]
    public void DropIndex_ShouldReturnFalseForNonExistentIndex()
    {
        // Act
        var result = _manager.DropIndex("non_existent");

        // Assert
        Assert.False(result);
    }

    [Fact]
    public void GetIndex_ShouldReturnCorrectIndex()
    {
        // Arrange
        var name = "test_index";
        var fields = new[] { "field1" };
        _manager.CreateIndex(name, fields);

        // Act
        var index = _manager.GetIndex(name);

        // Assert
        Assert.NotNull(index);
        Assert.Equal(name, index.Name);
    }

    [Fact]
    public void GetIndex_ShouldReturnNullForNonExistentIndex()
    {
        // Act
        var index = _manager.GetIndex("non_existent");

        // Assert
        Assert.Null(index);
    }

    [Fact]
    public void GetBestIndex_ShouldReturnBestMatchingIndex()
    {
        // Arrange
        _manager.CreateIndex("index1", new[] { "field1" });
        _manager.CreateIndex("index2", new[] { "field1", "field2" });
        _manager.CreateIndex("index3", new[] { "field2" });

        // Act
        var bestIndex = _manager.GetBestIndex(new[] { "field1", "field2" });

        // Assert
        Assert.NotNull(bestIndex);
        Assert.Equal("index2", bestIndex.Name); // 应该选择匹配字段最多的索引
    }

    [Fact]
    public void GetBestIndex_ShouldReturnNullForNoMatch()
    {
        // Arrange
        _manager.CreateIndex("index1", new[] { "field1" });

        // Act
        var bestIndex = _manager.GetBestIndex(new[] { "field2" });

        // Assert
        Assert.Null(bestIndex);
    }

    [Fact]
    public void InsertDocument_ShouldAddToAllIndexes()
    {
        // Arrange
        _manager.CreateIndex("name_index", new[] { "name" });
        _manager.CreateIndex("age_index", new[] { "age" });

        var document = new BsonDocument
        {
            ["_id"] = new ObjectId(),
            ["name"] = "TestUser",
            ["age"] = 25
        };
        var documentId = document["_id"];

        // Act
        _manager.InsertDocument(document, documentId);

        // Assert
        var nameIndex = _manager.GetIndex("name_index");
        var ageIndex = _manager.GetIndex("age_index");

        Assert.NotNull(nameIndex);
        Assert.NotNull(ageIndex);

        var nameKey = new IndexKey(new[] { new BsonString("TestUser") });
        var ageKey = new IndexKey(new[] { new BsonInt32(25) });

        Assert.Contains(documentId, nameIndex.Find(nameKey));
        Assert.Contains(documentId, ageIndex.Find(ageKey));
    }

    [Fact]
    public void DeleteDocument_ShouldRemoveFromAllIndexes()
    {
        // Arrange
        _manager.CreateIndex("name_index", new[] { "name" });

        var document = new BsonDocument
        {
            ["_id"] = new ObjectId(),
            ["name"] = "TestUser"
        };
        var documentId = document["_id"];
        _manager.InsertDocument(document, documentId);

        // Act
        _manager.DeleteDocument(document, documentId);

        // Assert
        var nameIndex = _manager.GetIndex("name_index");
        Assert.NotNull(nameIndex);

        var nameKey = new IndexKey(new[] { new BsonString("TestUser") });
        Assert.DoesNotContain(documentId, nameIndex.Find(nameKey));
    }

    [Fact]
    public void UpdateDocument_ShouldUpdateAllIndexes()
    {
        // Arrange
        _manager.CreateIndex("name_index", new[] { "name" });

        var oldDocument = new BsonDocument
        {
            ["_id"] = new ObjectId(),
            ["name"] = "OldName"
        };
        var newDocument = new BsonDocument
        {
            ["_id"] = oldDocument["_id"],
            ["name"] = "NewName"
        };
        var documentId = oldDocument["_id"];
        _manager.InsertDocument(oldDocument, documentId);

        // Act
        _manager.UpdateDocument(oldDocument, newDocument, documentId);

        // Assert
        var nameIndex = _manager.GetIndex("name_index");
        Assert.NotNull(nameIndex);

        var oldKey = new IndexKey(new[] { new BsonString("OldName") });
        var newKey = new IndexKey(new[] { new BsonString("NewName") });

        Assert.DoesNotContain(documentId, nameIndex.Find(oldKey));
        Assert.Contains(documentId, nameIndex.Find(newKey));
    }

    [Fact]
    public void RebuildIndex_ShouldRebuildCorrectly()
    {
        // Arrange
        var indexName = "test_index";
        _manager.CreateIndex(indexName, new[] { "name" });

        var documents = new[]
        {
            new BsonDocument { ["_id"] = new ObjectId(), ["name"] = "User1" },
            new BsonDocument { ["_id"] = new ObjectId(), ["name"] = "User2" }
        };

        // 先插入文档
        foreach (var doc in documents)
        {
            _manager.InsertDocument(doc, doc["_id"]);
        }

        // Act
        var result = _manager.RebuildIndex(indexName, documents);

        // Assert
        Assert.True(result);
        var index = _manager.GetIndex(indexName);
        Assert.NotNull(index);
        Assert.Equal(2, index.EntryCount);
    }

    [Fact]
    public void GetAllStatistics_ShouldReturnAllIndexStatistics()
    {
        // Arrange
        _manager.CreateIndex("index1", new[] { "field1" });
        _manager.CreateIndex("index2", new[] { "field2" }, true);

        // Act
        var statistics = _manager.GetAllStatistics().ToList();

        // Assert
        Assert.Equal(2, statistics.Count);
        Assert.All(statistics, stat => Assert.NotNull(stat));
    }

    [Fact]
    public void ValidateAllIndexes_ShouldValidateCorrectly()
    {
        // Arrange
        _manager.CreateIndex("index1", new[] { "field1" });

        // Act
        var result = _manager.ValidateAllIndexes();

        // Assert
        Assert.True(result.IsValid);
        Assert.Equal(1, result.TotalIndexes);
        Assert.Equal(1, result.ValidIndexes);
        Assert.Equal(0, result.InvalidIndexes);
        Assert.Empty(result.Errors);
    }

    [Fact]
    public void ClearAllIndexes_ShouldClearAllIndexes()
    {
        // Arrange
        _manager.CreateIndex("index1", new[] { "field1" });
        _manager.CreateIndex("index2", new[] { "field2" });

        var document = new BsonDocument
        {
            ["_id"] = new ObjectId(),
            ["field1"] = "value1",
            ["field2"] = "value2"
        };
        _manager.InsertDocument(document, document["_id"]);

        // Act
        _manager.ClearAllIndexes();

        // Assert
        var index1 = _manager.GetIndex("index1");
        var index2 = _manager.GetIndex("index2");

        Assert.NotNull(index1);
        Assert.NotNull(index2);
        Assert.Equal(0, index1.EntryCount);
        Assert.Equal(0, index2.EntryCount);
    }

    [Fact]
    public void DropAllIndexes_ShouldRemoveAllIndexes()
    {
        // Arrange
        _manager.CreateIndex("index1", new[] { "field1" });
        _manager.CreateIndex("index2", new[] { "field2" });

        // Act
        _manager.DropAllIndexes();

        // Assert
        Assert.Equal(0, _manager.IndexCount);
        Assert.Empty(_manager.IndexNames);
    }

    [Fact]
    public void InsertDocumentWithMissingFields_ShouldSkipIndex()
    {
        // Arrange
        _manager.CreateIndex("name_index", new[] { "name" });

        var document = new BsonDocument
        {
            ["_id"] = new ObjectId(),
            ["age"] = 25 // 缺少name字段
        };
        var documentId = document["_id"];

        // Act - 不应该抛出异常
        _manager.InsertDocument(document, documentId);

        // Assert - 索引应该为空
        var nameIndex = _manager.GetIndex("name_index");
        Assert.NotNull(nameIndex);
        Assert.Equal(0, nameIndex.EntryCount);
    }
}