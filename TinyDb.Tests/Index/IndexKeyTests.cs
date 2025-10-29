using TinyDb.Bson;
using TinyDb.Index;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Index;

public class IndexKeyTests
{
    [Test]
    public async Task Constructor_Should_Initialize_With_Values()
    {
        // Arrange
        var values = new BsonValue[] { new BsonString("test"), new BsonInt32(42) };

        // Act
        var indexKey = new IndexKey(values);

        // Assert
        await Assert.That(indexKey.Values).IsNotNull();
        await Assert.That(indexKey.Length).IsEqualTo(2);
        await Assert.That(indexKey[0]).IsEqualTo(values[0]);
        await Assert.That(indexKey[1]).IsEqualTo(values[1]);
    }

    [Test]
    public async Task Constructor_Should_Throw_ArgumentNullException_For_Null_Values()
    {
        // Act & Assert
        await Assert.That(() => new IndexKey(null!)).Throws<ArgumentNullException>();
    }

    [Test]
    public async Task CompareTo_Should_Return_Zero_For_Equal_Keys()
    {
        // Arrange
        var key1 = new IndexKey(new BsonString("test"), new BsonInt32(42));
        var key2 = new IndexKey(new BsonString("test"), new BsonInt32(42));

        // Act
        var result = key1.CompareTo(key2);

        // Assert
        await Assert.That(result).IsEqualTo(0);
    }

    [Test]
    public async Task CompareTo_Should_Compare_String_Values_Correctly()
    {
        // Arrange
        var key1 = new IndexKey(new BsonString("apple"));
        var key2 = new IndexKey(new BsonString("banana"));

        // Act
        var result = key1.CompareTo(key2);

        // Assert
        await Assert.That(result).IsNegative();
    }

    [Test]
    public async Task CompareTo_Should_Compare_Numeric_Values_Correctly()
    {
        // Arrange
        var key1 = new IndexKey(new BsonInt32(10));
        var key2 = new IndexKey(new BsonInt32(20));

        // Act
        var result = key1.CompareTo(key2);

        // Assert
        await Assert.That(result).IsNegative();
    }

    [Test]
    public async Task CompareTo_Should_Compare_Different_Types_By_Type_Order()
    {
        // Arrange
        var key1 = new IndexKey(new BsonString("test")); // String type comes after numeric types
        var key2 = new IndexKey(new BsonInt32(42));

        // Act
        var result = key1.CompareTo(key2);

        // Assert
        await Assert.That(result).IsPositive();
    }

    [Test]
    public async Task CompareTo_Should_Handle_Null_Values()
    {
        // Arrange
        var key1 = new IndexKey(BsonValue.Null);
        var key2 = new IndexKey(new BsonString("test"));

        // Act
        var result = key1.CompareTo(key2);

        // Assert
        await Assert.That(result).IsNegative();
    }

    [Test]
    public async Task CompareTo_Should_Compare_Multi_Field_Keys_Sequentially()
    {
        // Arrange
        var key1 = new IndexKey(new BsonString("test"), new BsonInt32(10));
        var key2 = new IndexKey(new BsonString("test"), new BsonInt32(20));

        // Act
        var result = key1.CompareTo(key2);

        // Assert
        await Assert.That(result).IsNegative(); // Second field differs
    }

    [Test]
    public async Task CompareTo_Should_Return_One_For_Null_Other()
    {
        // Arrange
        var key1 = new IndexKey(new BsonString("test"));

        // Act
        var result = key1.CompareTo(null);

        // Assert
        await Assert.That(result).IsEqualTo(1);
    }

    [Test]
    public async Task Equals_Should_Return_True_For_Equal_Keys()
    {
        // Arrange
        var key1 = new IndexKey(new BsonString("test"), new BsonInt32(42));
        var key2 = new IndexKey(new BsonString("test"), new BsonInt32(42));

        // Act & Assert
        await Assert.That(key1.Equals(key2)).IsTrue();
        await Assert.That(key1 == key2).IsTrue();
    }

    [Test]
    public async Task Equals_Should_Return_False_For_Different_Keys()
    {
        // Arrange
        var key1 = new IndexKey(new BsonString("test"), new BsonInt32(42));
        var key2 = new IndexKey(new BsonString("test"), new BsonInt32(43));

        // Act & Assert
        await Assert.That(key1.Equals(key2)).IsFalse();
        await Assert.That(key1 != key2).IsTrue();
    }

    [Test]
    public async Task Equals_Should_Return_False_For_Different_Lengths()
    {
        // Arrange
        var key1 = new IndexKey(new BsonString("test"));
        var key2 = new IndexKey(new BsonString("test"), new BsonInt32(42));

        // Act & Assert
        await Assert.That(key1.Equals(key2)).IsFalse();
    }

    [Test]
    public async Task Equals_Should_Return_False_For_Null_Other()
    {
        // Arrange
        var key1 = new IndexKey(new BsonString("test"));

        // Act & Assert
        await Assert.That(key1.Equals((IndexKey?)null)).IsFalse();
    }

    [Test]
    public async Task GetHashCode_Should_Be_Consistent_For_Equal_Keys()
    {
        // Arrange
        var key1 = new IndexKey(new BsonString("test"), new BsonInt32(42));
        var key2 = new IndexKey(new BsonString("test"), new BsonInt32(42));

        // Act
        var hash1 = key1.GetHashCode();
        var hash2 = key2.GetHashCode();

        // Assert
        await Assert.That(hash1).IsEqualTo(hash2);
    }

    [Test]
    public async Task GetHashCode_Should_Be_Different_For_Different_Keys()
    {
        // Arrange
        var key1 = new IndexKey(new BsonString("test"));
        var key2 = new IndexKey(new BsonString("different"));

        // Act
        var hash1 = key1.GetHashCode();
        var hash2 = key2.GetHashCode();

        // Assert
        await Assert.That(hash1).IsNotEqualTo(hash2);
    }

    [Test]
    public async Task Comparison_Operators_Should_Work_Correctly()
    {
        // Arrange
        var key1 = new IndexKey(new BsonInt32(10));
        var key2 = new IndexKey(new BsonInt32(20));

        // Act & Assert
        await Assert.That(key1 < key2).IsTrue();
        await Assert.That(key2 > key1).IsTrue();
        await Assert.That(key1 <= key2).IsTrue();
        await Assert.That(key2 >= key1).IsTrue();
        await Assert.That(key1.CompareTo(key1)).IsEqualTo(0); // Equal to itself
    }

    [Test]
    public async Task ToString_Should_Return_Correct_Format()
    {
        // Arrange
        var key = new IndexKey(new BsonString("test"), new BsonInt32(42));

        // Act
        var result = key.ToString();

        // Assert
        await Assert.That(result).IsEqualTo("IndexKey[test, 42]");
    }

    [Test]
    public async Task Clone_Should_Create_Separate_Instance()
    {
        // Arrange
        var original = new IndexKey(new BsonString("test"), new BsonInt32(42));

        // Act
        var cloned = original.Clone();

        // Assert
        await Assert.That(!ReferenceEquals(cloned, original)).IsTrue();
        await Assert.That(cloned.Equals(original)).IsTrue();
        await Assert.That(cloned.Length).IsEqualTo(original.Length);
    }

    [Test]
    public async Task Create_Single_Field_Should_Work()
    {
        // Arrange
        var value = new BsonString("test");

        // Act
        var key = IndexKey.Create(value);

        // Assert
        await Assert.That(key.Length).IsEqualTo(1);
        await Assert.That(key[0]).IsEqualTo(value);
    }

    [Test]
    public async Task Create_Multiple_Fields_Should_Work()
    {
        // Arrange
        var values = new BsonValue[] { new BsonString("test"), new BsonInt32(42) };

        // Act
        var key = IndexKey.Create(values);

        // Assert
        await Assert.That(key.Length).IsEqualTo(2);
        await Assert.That(key[0]).IsEqualTo(values[0]);
        await Assert.That(key[1]).IsEqualTo(values[1]);
    }

    [Test]
    public async Task CompareTo_Should_Handle_Complex_Bson_Types()
    {
        // Arrange
        var objectId1 = new BsonObjectId(ObjectId.NewObjectId());
        var objectId2 = new BsonObjectId(ObjectId.NewObjectId());
        var key1 = new IndexKey(objectId1);
        var key2 = new IndexKey(objectId2);

        // Act
        var result = key1.CompareTo(key2);

        // Assert
        await Assert.That(result).IsNotEqualTo(0); // Different ObjectIds should be different
    }

    [Test]
    public async Task CompareTo_Should_Handle_DateTime_Values()
    {
        // Arrange
        var date1 = new BsonDateTime(DateTime.UtcNow);
        var date2 = new BsonDateTime(DateTime.UtcNow.AddDays(1));
        var key1 = new IndexKey(date1);
        var key2 = new IndexKey(date2);

        // Act
        var result = key1.CompareTo(key2);

        // Assert
        await Assert.That(result).IsNegative();
    }
}