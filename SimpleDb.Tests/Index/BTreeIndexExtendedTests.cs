using SimpleDb.Index;
using SimpleDb.Bson;
using Xunit;

namespace SimpleDb.Tests.Index;

/// <summary>
/// B+ 树索引扩展测试
/// </summary>
public class BTreeIndexExtendedTests : IDisposable
{
    private readonly BTreeIndex _index;

    public BTreeIndexExtendedTests()
    {
        _index = new BTreeIndex("test_index", new[] { "field1" }, false, 4);
    }

    public void Dispose()
    {
        _index?.Dispose();
    }

    [Fact]
    public void Insert_ShouldInsertSingleDocument()
    {
        // Arrange
        var key = new IndexKey(new[] { new BsonString("test") });
        var documentId = new ObjectId();

        // Act
        var result = _index.Insert(key, documentId);

        // Assert
        Assert.True(result);
        Assert.Equal(1, _index.EntryCount);
        Assert.Contains(documentId, _index.Find(key));
    }

    [Fact]
    public void Insert_ShouldHandleDuplicateKeys()
    {
        // Arrange
        var key = new IndexKey(new[] { new BsonString("test") });
        var doc1 = new ObjectId();
        var doc2 = new ObjectId();

        // Act
        var result1 = _index.Insert(key, doc1);
        var result2 = _index.Insert(key, doc2);

        // Assert
        Assert.True(result1);
        Assert.True(result2);
        Assert.Equal(2, _index.EntryCount);

        var found = _index.Find(key).ToList();
        Assert.Equal(2, found.Count);
        Assert.Contains(doc1, found);
        Assert.Contains(doc2, found);
    }

    [Fact]
    public void Insert_ShouldRespectUniqueConstraint()
    {
        // Arrange
        var uniqueIndex = new BTreeIndex("unique_index", new[] { "field1" }, true, 4);
        var key = new IndexKey(new[] { new BsonString("test") });
        var doc1 = new ObjectId();
        var doc2 = new ObjectId();

        try
        {
            // Act
            var result1 = uniqueIndex.Insert(key, doc1);
            var result2 = uniqueIndex.Insert(key, doc2);

            // Assert
            Assert.True(result1);
            Assert.False(result2); // 第二次插入应该失败
            Assert.Equal(1, uniqueIndex.EntryCount);
        }
        finally
        {
            uniqueIndex.Dispose();
        }
    }

    [Fact]
    public void Delete_ShouldRemoveDocument()
    {
        // Arrange
        var key = new IndexKey(new[] { new BsonString("test") });
        var documentId = new ObjectId();
        _index.Insert(key, documentId);

        // Act
        var result = _index.Delete(key, documentId);

        // Assert
        Assert.True(result);
        Assert.Equal(0, _index.EntryCount);
        Assert.DoesNotContain(documentId, _index.Find(key));
    }

    [Fact]
    public void Delete_ShouldHandleNonExistentDocument()
    {
        // Arrange
        var key = new IndexKey(new[] { new BsonString("test") });
        var documentId = new ObjectId();

        // Act
        var result = _index.Delete(key, documentId);

        // Assert
        Assert.False(result);
    }

    [Fact]
    public void Find_ShouldReturnCorrectDocuments()
    {
        // Arrange
        var key = new IndexKey(new[] { new BsonString("test") });
        var doc1 = new ObjectId();
        var doc2 = new ObjectId();
        _index.Insert(key, doc1);
        _index.Insert(key, doc2);

        // Act
        var found = _index.Find(key).ToList();

        // Assert
        Assert.Equal(2, found.Count);
        Assert.Contains(doc1, found);
        Assert.Contains(doc2, found);
    }

    [Fact]
    public void FindRange_ShouldReturnCorrectRange()
    {
        // Arrange
        var keys = new[]
        {
            new IndexKey(new[] { new BsonString("a") }),
            new IndexKey(new[] { new BsonString("b") }),
            new IndexKey(new[] { new BsonString("c") }),
            new IndexKey(new[] { new BsonString("d") })
        };
        var docIds = keys.Select(_ => new ObjectId()).ToArray();

        for (int i = 0; i < keys.Length; i++)
        {
            _index.Insert(keys[i], docIds[i]);
        }

        // Act
        var rangeResult = _index.FindRange(
            new IndexKey(new[] { new BsonString("b") }),
            new IndexKey(new[] { new BsonString("c") })
        ).ToList();

        // Assert
        Assert.Equal(2, rangeResult.Count);
        Assert.Contains(docIds[1], rangeResult); // 'b'
        Assert.Contains(docIds[2], rangeResult); // 'c'
    }

    [Fact]
    public void Contains_ShouldWorkCorrectly()
    {
        // Arrange
        var key = new IndexKey(new[] { new BsonString("test") });
        var documentId = new ObjectId();
        _index.Insert(key, documentId);

        // Act & Assert
        Assert.True(_index.Contains(key, documentId));
        Assert.True(_index.Contains(key));
        Assert.False(_index.Contains(key, new ObjectId()));
        Assert.False(_index.Contains(new IndexKey(new[] { new BsonString("nonexistent") })));
    }

    [Fact]
    public void GetAll_ShouldReturnAllDocuments()
    {
        // Arrange
        var keys = new[]
        {
            new IndexKey(new[] { new BsonString("a") }),
            new IndexKey(new[] { new BsonString("b") }),
            new IndexKey(new[] { new BsonString("c") })
        };
        var docIds = keys.Select(_ => new ObjectId()).ToArray();

        foreach (var key in keys)
        {
            _index.Insert(key, new ObjectId());
        }

        // Act
        var allDocs = _index.GetAll().ToList();

        // Assert
        Assert.Equal(keys.Length, allDocs.Count);
    }

    [Fact]
    public void Clear_ShouldRemoveAllEntries()
    {
        // Arrange
        var key = new IndexKey(new[] { new BsonString("test") });
        _index.Insert(key, new ObjectId());
        Assert.Equal(1, _index.EntryCount);

        // Act
        _index.Clear();

        // Assert
        Assert.Equal(0, _index.EntryCount);
        Assert.Empty(_index.Find(key));
    }

    [Fact]
    public void GetStatistics_ShouldReturnCorrectInformation()
    {
        // Arrange
        var key = new IndexKey(new[] { new BsonString("test") });
        _index.Insert(key, new ObjectId());

        // Act
        var stats = _index.GetStatistics();

        // Assert
        Assert.Equal("test_index", stats.Name);
        Assert.Equal(IndexType.BTree, stats.Type);
        Assert.Equal(1, stats.EntryCount);
        Assert.True(stats.NodeCount > 0);
        Assert.Equal(4, stats.MaxKeysPerNode);
        Assert.False(stats.RootIsLeaf); // 根节点可能是内部节点
    }

    [Fact]
    public void Validate_ShouldReturnTrueForValidIndex()
    {
        // Arrange
        var keys = new[]
        {
            new IndexKey(new[] { new BsonString("a") }),
            new IndexKey(new[] { new BsonString("b") }),
            new IndexKey(new[] { new BsonString("c") })
        };
        foreach (var key in keys)
        {
            _index.Insert(key, new ObjectId());
        }

        // Act
        var isValid = _index.Validate();

        // Assert
        Assert.True(isValid);
    }

    [Fact]
    public void LargeDataSet_ShouldWorkCorrectly()
    {
        // Arrange
        var random = new Random(42); // 固定种子以确保可重复性
        var documents = new List<(IndexKey key, ObjectId id)>();

        // Act - 插入大量数据
        for (int i = 0; i < 1000; i++)
        {
            var key = new IndexKey(new[] { new BsonString($"key_{i:D4}") });
            var id = new ObjectId();
            documents.Add((key, id));
            _index.Insert(key, id);
        }

        // Assert
        Assert.Equal(1000, _index.EntryCount);

        // 验证所有文档都能找到
        foreach (var (key, id) in documents)
        {
            Assert.Contains(id, _index.Find(key));
        }

        // 随机删除一些文档
        var toDelete = documents.OrderBy(_ => random.Next()).Take(100).ToList();
        foreach (var (key, id) in toDelete)
        {
            var deleted = _index.Delete(key, id);
            Assert.True(deleted);
        }

        Assert.Equal(900, _index.EntryCount);

        // 验证删除的文档找不到了
        foreach (var (key, id) in toDelete)
        {
            Assert.DoesNotContain(id, _index.Find(key));
        }
    }

    [Fact]
    public void ComplexKeyTypes_ShouldWorkCorrectly()
    {
        // Arrange
        var stringKey = new IndexKey(new[] { new BsonString("test") });
        var intKey = new IndexKey(new[] { new BsonInt32(42) });
        var doubleKey = new IndexKey(new[] { new BsonDouble(3.14) });
        var boolKey = new IndexKey(new[] { new BsonBoolean(true) });
        var dateKey = new IndexKey(new[] { new BsonDateTime(DateTime.UtcNow) });

        var keys = new[] { stringKey, intKey, doubleKey, boolKey, dateKey };
        var ids = keys.Select(_ => new ObjectId()).ToArray();

        // Act
        for (int i = 0; i < keys.Length; i++)
        {
            var result = _index.Insert(keys[i], ids[i]);
            Assert.True(result);
        }

        // Assert
        for (int i = 0; i < keys.Length; i++)
        {
            Assert.Contains(ids[i], _index.Find(keys[i]));
        }

        Assert.Equal(5, _index.EntryCount);
    }

    [Fact]
    public void MultiFieldIndex_ShouldWorkCorrectly()
    {
        // Arrange
        var multiFieldIndex = new BTreeIndex("multi_field", new[] { "field1", "field2" }, false, 4);

        try
        {
            var key1 = new IndexKey(new[] { new BsonString("a"), new BsonInt32(1) });
            var key2 = new IndexKey(new[] { new BsonString("a"), new BsonInt32(2) });
            var key3 = new IndexKey(new[] { new BsonString("b"), new BsonInt32(1) });

            var id1 = new ObjectId();
            var id2 = new ObjectId();
            var id3 = new ObjectId();

            // Act
            multiFieldIndex.Insert(key1, id1);
            multiFieldIndex.Insert(key2, id2);
            multiFieldIndex.Insert(key3, id3);

            // Assert
            Assert.Contains(id1, multiFieldIndex.Find(key1));
            Assert.Contains(id2, multiFieldIndex.Find(key2));
            Assert.Contains(id3, multiFieldIndex.Find(key3));
            Assert.Equal(3, multiFieldIndex.EntryCount);
        }
        finally
        {
            multiFieldIndex.Dispose();
        }
    }

    [Fact]
    public void ToString_ShouldReturnUsefulInformation()
    {
        // Arrange
        var key = new IndexKey(new[] { new BsonString("test") });
        _index.Insert(key, new ObjectId());

        // Act
        var result = _index.ToString();

        // Assert
        Assert.Contains("BTreeIndex", result);
        Assert.Contains("test_index", result);
        Assert.Contains("1 fields", result);
        Assert.Contains("1 entries", result);
    }
}