using SimpleDb.Bson;
using SimpleDb.Index;
using System.Linq;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace SimpleDb.Tests.Index;

public class BTreeIndexTests
{
    [Test]
    public async Task Constructor_Should_Initialize_With_Valid_Parameters()
    {
        // Arrange
        var name = "test_index";
        var fields = new[] { "field1", "field2" };
        var unique = true;
        var maxKeys = 100;

        // Act
        var index = new BTreeIndex(name, fields, unique, maxKeys);

        // Assert
        await Assert.That(index.Name).IsEqualTo(name);
        await Assert.That(index.Fields).IsEqualTo(fields);
        await Assert.That(index.IsUnique).IsEqualTo(unique);
        await Assert.That(index.NodeCount).IsEqualTo(1);
        await Assert.That(index.EntryCount).IsEqualTo(0);
        await Assert.That(index.Type).IsEqualTo(IndexType.BTree);
    }

    [Test]
    public async Task Constructor_Should_Throw_ArgumentNullException_For_Null_Name()
    {
        // Arrange
        var fields = new[] { "field1" };

        // Act & Assert
        await Assert.That(() => new BTreeIndex(null!, fields)).Throws<ArgumentNullException>();
    }

    [Test]
    public async Task Constructor_Should_Throw_ArgumentNullException_For_Null_Fields()
    {
        // Act & Assert
        await Assert.That(() => new BTreeIndex("test", null!)).Throws<ArgumentNullException>();
    }

    [Test]
    public async Task Constructor_Should_Throw_ArgumentException_For_Empty_Fields()
    {
        // Act & Assert
        await Assert.That(() => new BTreeIndex("test", Array.Empty<string>())).Throws<ArgumentException>();
    }

    [Test]
    public async Task Insert_Should_Add_Entry_Successfully()
    {
        // Arrange
        var index = new BTreeIndex("test", new[] { "field1" });
        var key = new IndexKey(new BsonString("test"));
        var docId = new BsonObjectId(ObjectId.NewObjectId());

        // Act
        var result = index.Insert(key, docId);

        // Assert
        await Assert.That(result).IsTrue();
        await Assert.That(index.EntryCount).IsEqualTo(1);
    }

    [Test]
    public async Task Insert_Should_Return_False_For_Unique_Conflict()
    {
        // Arrange
        var index = new BTreeIndex("test", new[] { "field1" }, true);
        var key = new IndexKey(new BsonString("test"));
        var docId1 = new BsonObjectId(ObjectId.NewObjectId());
        var docId2 = new BsonObjectId(ObjectId.NewObjectId());

        index.Insert(key, docId1);

        // Act
        var result = index.Insert(key, docId2);

        // Assert
        await Assert.That(result).IsFalse();
        await Assert.That(index.EntryCount).IsEqualTo(1);
    }

    [Test]
    public async Task Insert_Should_Allow_Duplicates_For_Non_Unique_Index()
    {
        // Arrange
        var index = new BTreeIndex("test", new[] { "field1" }, false);
        var key = new IndexKey(new BsonString("test"));
        var docId1 = new BsonObjectId(ObjectId.NewObjectId());
        var docId2 = new BsonObjectId(ObjectId.NewObjectId());

        index.Insert(key, docId1);

        // Act
        var result = index.Insert(key, docId2);

        // Assert
        await Assert.That(result).IsTrue();
        await Assert.That(index.EntryCount).IsEqualTo(2);
    }

    [Test]
    public async Task Insert_Should_Trigger_Split_When_Capacity_Exceeded()
    {
        // Arrange
        var index = new BTreeIndex("test", new[] { "field1" }, false, 2); // Very small capacity
        for (int i = 0; i < 5; i++)
        {
            var key = new IndexKey(new BsonInt32(i));
            var docId = new BsonInt32(i);
            index.Insert(key, docId);
        }

        // Assert
        await Assert.That(index.NodeCount).IsGreaterThan(1);
        await Assert.That(index.EntryCount).IsEqualTo(5);
    }

    [Test]
    public async Task Insert_Should_Throw_ArgumentNullException_For_Null_Key()
    {
        // Arrange
        var index = new BTreeIndex("test", new[] { "field1" });
        var docId = new BsonObjectId(ObjectId.NewObjectId());

        // Act & Assert
        await Assert.That(() => index.Insert(null!, docId)).Throws<ArgumentNullException>();
    }

    [Test]
    public async Task Insert_Should_Throw_ArgumentNullException_For_Null_DocumentId()
    {
        // Arrange
        var index = new BTreeIndex("test", new[] { "field1" });
        var key = new IndexKey(new BsonString("test"));

        // Act & Assert
        await Assert.That(() => index.Insert(key, null!)).Throws<ArgumentNullException>();
    }

    [Test]
    public async Task Delete_Should_Remove_Entry_Successfully()
    {
        // Arrange
        var index = new BTreeIndex("test", new[] { "field1" });
        var key = new IndexKey(new BsonString("test"));
        var docId = new BsonObjectId(ObjectId.NewObjectId());
        index.Insert(key, docId);

        // Act
        var result = index.Delete(key, docId);

        // Assert
        await Assert.That(result).IsTrue();
        await Assert.That(index.EntryCount).IsEqualTo(0);
    }

    [Test]
    public async Task Delete_Should_Return_False_For_Non_Existing_Entry()
    {
        // Arrange
        var index = new BTreeIndex("test", new[] { "field1" });
        var key = new IndexKey(new BsonString("test"));
        var docId = new BsonObjectId(ObjectId.NewObjectId());

        // Act
        var result = index.Delete(key, docId);

        // Assert
        await Assert.That(result).IsFalse();
        await Assert.That(index.EntryCount).IsEqualTo(0);
    }

    [Test]
    public async Task Delete_Should_Throw_ArgumentNullException_For_Null_Key()
    {
        // Arrange
        var index = new BTreeIndex("test", new[] { "field1" });
        var docId = new BsonObjectId(ObjectId.NewObjectId());

        // Act & Assert
        await Assert.That(() => index.Delete(null!, docId)).Throws<ArgumentNullException>();
    }

    [Test]
    public async Task Find_Should_Return_Matching_DocumentIds()
    {
        // Arrange
        var index = new BTreeIndex("test", new[] { "field1" });
        var key = new IndexKey(new BsonString("test"));
        var docId1 = new BsonObjectId(ObjectId.NewObjectId());
        var docId2 = new BsonObjectId(ObjectId.NewObjectId());

        index.Insert(key, docId1);
        index.Insert(key, docId2);

        // Act
        var results = index.Find(key).ToList();

        // Assert
        await Assert.That(results.Count).IsEqualTo(2);
        await Assert.That(results).Contains(docId1);
        await Assert.That(results).Contains(docId2);
    }

    [Test]
    public async Task Find_Should_Return_Empty_For_Non_Existing_Key()
    {
        // Arrange
        var index = new BTreeIndex("test", new[] { "field1" });
        var key = new IndexKey(new BsonString("nonexistent"));

        // Act
        var results = index.Find(key).ToList();

        // Assert
        await Assert.That(results.Count).IsEqualTo(0);
    }

    [Test]
    public async Task Find_Should_Throw_ArgumentNullException_For_Null_Key()
    {
        // Arrange
        var index = new BTreeIndex("test", new[] { "field1" });

        // Act & Assert
        await Assert.That(() => index.Find(null!).ToList()).Throws<ArgumentNullException>();
    }

    [Test]
    public async Task FindRange_Should_Return_DocumentIds_In_Range()
    {
        // Arrange
        var index = new BTreeIndex("test", new[] { "field1" });
        for (int i = 1; i <= 5; i++)
        {
            var key = new IndexKey(new BsonInt32(i));
            var docId = new BsonInt32(i);
            index.Insert(key, docId);
        }

        var startKey = new IndexKey(new BsonInt32(2));
        var endKey = new IndexKey(new BsonInt32(4));

        // Act
        var results = index.FindRange(startKey, endKey).ToList();

        // Assert
        await Assert.That(results.Count).IsEqualTo(3);
        await Assert.That(results).Contains(new BsonInt32(2));
        await Assert.That(results).Contains(new BsonInt32(3));
        await Assert.That(results).Contains(new BsonInt32(4));
    }

    [Test]
    public async Task FindRange_Should_Respect_Exclude_Flags()
    {
        // Arrange
        var index = new BTreeIndex("test", new[] { "field1" });
        for (int i = 1; i <= 3; i++)
        {
            var key = new IndexKey(new BsonInt32(i));
            var docId = new BsonInt32(i);
            index.Insert(key, docId);
        }

        var startKey = new IndexKey(new BsonInt32(1));
        var endKey = new IndexKey(new BsonInt32(3));

        // Act
        var results = index.FindRange(startKey, endKey, false, false).ToList();

        // Assert
        await Assert.That(results.Count).IsEqualTo(1);
        await Assert.That(results).Contains(new BsonInt32(2));
    }

    [Test]
    public async Task Contains_Should_Return_True_For_Existing_Key_Document_Pair()
    {
        // Arrange
        var index = new BTreeIndex("test", new[] { "field1" });
        var key = new IndexKey(new BsonString("test"));
        var docId = new BsonObjectId(ObjectId.NewObjectId());
        index.Insert(key, docId);

        // Act
        var result = index.Contains(key, docId);

        // Assert
        await Assert.That(result).IsTrue();
    }

    [Test]
    public async Task Contains_Should_Return_False_For_Non_Existing_Key_Document_Pair()
    {
        // Arrange
        var index = new BTreeIndex("test", new[] { "field1" });
        var key = new IndexKey(new BsonString("test"));
        var docId = new BsonObjectId(ObjectId.NewObjectId());

        // Act
        var result = index.Contains(key, docId);

        // Assert
        await Assert.That(result).IsFalse();
    }

    [Test]
    public async Task Contains_Key_Only_Should_Return_True_For_Existing_Key()
    {
        // Arrange
        var index = new BTreeIndex("test", new[] { "field1" });
        var key = new IndexKey(new BsonString("test"));
        var docId = new BsonObjectId(ObjectId.NewObjectId());
        index.Insert(key, docId);

        // Act
        var result = index.Contains(key);

        // Assert
        await Assert.That(result).IsTrue();
    }

    [Test]
    public async Task GetAll_Should_Return_All_DocumentIds()
    {
        // Arrange
        var index = new BTreeIndex("test", new[] { "field1" });
        var docIds = new List<BsonValue>();
        for (int i = 0; i < 5; i++)
        {
            var key = new IndexKey(new BsonInt32(i));
            var docId = new BsonInt32(i);
            index.Insert(key, docId);
            docIds.Add(docId);
        }

        // Act
        var results = index.GetAll().ToList();

        // Assert
        await Assert.That(results.Count).IsEqualTo(5);
        foreach (var docId in docIds)
        {
            await Assert.That(results).Contains(docId);
        }
    }

    [Test]
    public async Task Clear_Should_Remove_All_Entries()
    {
        // Arrange
        var index = new BTreeIndex("test", new[] { "field1" });
        for (int i = 0; i < 5; i++)
        {
            var key = new IndexKey(new BsonInt32(i));
            var docId = new BsonInt32(i);
            index.Insert(key, docId);
        }

        // Act
        index.Clear();

        // Assert
        await Assert.That(index.EntryCount).IsEqualTo(0);
        await Assert.That(index.NodeCount).IsEqualTo(1);
    }

    [Test]
    public async Task GetStatistics_Should_Return_Correct_Information()
    {
        // Arrange
        var index = new BTreeIndex("test", new[] { "field1", "field2" }, true, 50);
        var key = new IndexKey(new BsonString("test"), new BsonInt32(42));
        var docId = new BsonObjectId(ObjectId.NewObjectId());
        index.Insert(key, docId);

        // Act
        var stats = index.GetStatistics();

        // Assert
        await Assert.That(stats.Name).IsEqualTo("test");
        await Assert.That(stats.Type).IsEqualTo(IndexType.BTree);
        await Assert.That(stats.Fields.SequenceEqual(new[] { "field1", "field2" })).IsTrue();
        await Assert.That(stats.IsUnique).IsTrue();
        await Assert.That(stats.EntryCount).IsEqualTo(1);
        await Assert.That(stats.MaxKeysPerNode).IsEqualTo(50);
        await Assert.That(stats.AverageKeysPerNode).IsGreaterThan(0);
    }

    [Test]
    public async Task Validate_Should_Return_True_For_Valid_Index()
    {
        // Arrange
        var index = new BTreeIndex("test", new[] { "field1" });
        for (int i = 0; i < 5; i++)
        {
            var key = new IndexKey(new BsonInt32(i));
            var docId = new BsonInt32(i);
            index.Insert(key, docId);
        }

        // Act
        var isValid = index.Validate();

        // Assert
        await Assert.That(isValid).IsTrue();
    }

    [Test]
    public async Task ToString_Should_Return_Correct_Format()
    {
        // Arrange
        var index = new BTreeIndex("test_index", new[] { "field1", "field2" });

        // Act
        var result = index.ToString();

        // Assert
        await Assert.That(result).Contains("BTreeIndex[test_index]");
        await Assert.That(result).Contains("2 fields");
        await Assert.That(result).Contains("0 entries");
    }

    [Test]
    public async Task Dispose_Should_Clean_Up_Resources()
    {
        // Arrange
        var index = new BTreeIndex("test", new[] { "field1" });

        // Act
        index.Dispose();

        // Assert - Should not throw when accessing disposed properties
        await Assert.That(() => index.GetStatistics()).Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task Insert_After_Dispose_Should_Throw_Exception()
    {
        // Arrange
        var index = new BTreeIndex("test", new[] { "field1" });
        index.Dispose();

        // Act & Assert
        await Assert.That(() => index.Insert(new IndexKey(new BsonString("test")), new BsonInt32(1)))
            .Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task Complex_IndexKey_Operations_Should_Work()
    {
        // Arrange
        var index = new BTreeIndex("test", new[] { "field1", "field2" });
        var key1 = new IndexKey(new BsonString("group1"), new BsonInt32(10));
        var key2 = new IndexKey(new BsonString("group1"), new BsonInt32(20));
        var key3 = new IndexKey(new BsonString("group2"), new BsonInt32(10));

        // Act
        var result1 = index.Insert(key1, new BsonInt32(1));
        var result2 = index.Insert(key2, new BsonInt32(2));
        var result3 = index.Insert(key3, new BsonInt32(3));

        // Assert
        await Assert.That(result1).IsTrue();
        await Assert.That(result2).IsTrue();
        await Assert.That(result3).IsTrue();
        await Assert.That(index.EntryCount).IsEqualTo(3);

        // Verify order
        var allResults = index.GetAll().ToList();
        await Assert.That(allResults.Count).IsEqualTo(3);
    }
}