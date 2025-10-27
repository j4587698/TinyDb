using System.Diagnostics.CodeAnalysis;
using SimpleDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace SimpleDb.Tests.Bson;

public class BsonArrayTests
{
    [Test]
    public async Task BsonArray_Constructor_Should_Create_Empty_Array()
    {
        // Arrange & Act
        var array = new BsonArray();

        // Assert
        await Assert.That(array).IsNotNull();
        await Assert.That(array.Count).IsEqualTo(0);
        await Assert.That(array.BsonType).IsEqualTo(BsonType.Array);
        await Assert.That(array.IsArray).IsTrue();
    }

    [Test]
    public async Task BsonArray_AddValue_Should_Work_Correctly()
    {
        // Arrange
        var array = new BsonArray();

        // Act
        array = array.AddValue(new BsonString("item1"));
        array = array.AddValue(new BsonInt32(42));
        array = array.AddValue(new BsonBoolean(true));

        // Assert
        await Assert.That(array.Count).IsEqualTo(3);
        await Assert.That(array[0].ToString()).IsEqualTo("item1");
        await Assert.That(array[1].ToInt32(null)).IsEqualTo(42);
        await Assert.That(array[2].ToBoolean(null)).IsTrue();
    }

    [Test]
    public async Task BsonArray_AddValue_With_Primitive_Types_Should_Work()
    {
        // Arrange
        var array = new BsonArray();

        // Act
        array = array.AddValue("string value");
        array = array.AddValue(123);
        array = array.AddValue(456L);
        array = array.AddValue(78.9);
        array = array.AddValue(true);
        array = array.AddValue(ObjectId.NewObjectId());

        // Assert
        await Assert.That(array.Count).IsEqualTo(6);
        await Assert.That(array[0].ToString()).IsEqualTo("string value");
        await Assert.That(array[1].ToInt32(null)).IsEqualTo(123);
        await Assert.That(array[2].ToInt64(null)).IsEqualTo(456L);
        await Assert.That(array[3].ToDouble(null)).IsEqualTo(78.9);
        await Assert.That(array[4].ToBoolean(null)).IsTrue();
        await Assert.That(array[5].BsonType).IsEqualTo(BsonType.ObjectId);
    }

    [Test]
    public async Task BsonArray_Set_Should_Update_Value_At_Index()
    {
        // Arrange
        var array = new BsonArray();
        array = array.AddValue("original");

        // Act
        array = array.Set(0, "updated");

        // Assert
        await Assert.That(array.Count).IsEqualTo(1);
        await Assert.That(array[0].ToString()).IsEqualTo("updated");
    }

    [Test]
    public async Task BsonArray_Should_Handle_Null_Values()
    {
        // Arrange
        var array = new BsonArray();

        // Act
        array = array.AddValue(BsonNull.Value);

        // Assert
        await Assert.That(array.Count).IsEqualTo(1);
        await Assert.That(array[0].IsNull).IsTrue();
    }

    [Test]
    public async Task BsonArray_Should_Handle_Mixed_Types()
    {
        // Arrange
        var array = new BsonArray();

        // Act
        array = array.AddValue("string");
        array = array.AddValue(123);
        array = array.AddValue(true);
        array = array.AddValue(BsonNull.Value);

        // Assert
        await Assert.That(array.Count).IsEqualTo(4);
        await Assert.That(array[0].IsString).IsTrue();
        await Assert.That(array[1].IsNumeric).IsTrue();
        await Assert.That(array[2].IsBoolean).IsTrue();
        await Assert.That(array[3].IsNull).IsTrue();
    }

    [Test]
    public async Task BsonArray_Enumeration_Should_Work()
    {
        // Arrange
        var array = new BsonArray();
        array = array.AddValue("item1");
        array = array.AddValue("item2");
        array = array.AddValue("item3");

        // Act
        var enumeratedItems = array.ToList();

        // Assert
        await Assert.That(enumeratedItems.Count).IsEqualTo(3);
        await Assert.That(enumeratedItems[0].ToString()).IsEqualTo("item1");
        await Assert.That(enumeratedItems[1].ToString()).IsEqualTo("item2");
        await Assert.That(enumeratedItems[2].ToString()).IsEqualTo("item3");
    }

    [Test]
    public async Task BsonArray_Equality_Should_Work_Correctly()
    {
        // Arrange
        var array1 = new BsonArray();
        array1 = array1.AddValue("item1");
        array1 = array1.AddValue("item2");

        var array2 = new BsonArray();
        array2 = array2.AddValue("item1");
        array2 = array2.AddValue("item2");

        // Act & Assert
        await Assert.That(array1.Equals(array2)).IsTrue();
        await Assert.That(array1 == array2).IsTrue();
    }

    [Test]
    public async Task BsonArray_ToString_Should_Return_Representation()
    {
        // Arrange
        var array = new BsonArray();
        array = array.AddValue("item1");
        array = array.AddValue(42);

        // Act
        var stringRepresentation = array.ToString();

        // Assert
        await Assert.That(stringRepresentation).IsNotEmpty();
    }

    [Test]
    public async Task BsonArray_Nested_Documents_Should_Work()
    {
        // Arrange
        var array = new BsonArray();
        var nestedDocument = new BsonDocument();
        nestedDocument = nestedDocument.Set("name", new BsonString("test"));
        nestedDocument = nestedDocument.Set("value", new BsonInt32(42));

        // Act
        array = array.AddValue(nestedDocument);

        // Assert
        await Assert.That(array.Count).IsEqualTo(1);
        await Assert.That(array[0].IsDocument).IsTrue();
    }

    [Test]
    public async Task BsonArray_Nested_Arrays_Should_Work()
    {
        // Arrange
        var array = new BsonArray();
        var nestedArray = new BsonArray();
        nestedArray = nestedArray.AddValue(1);
        nestedArray = nestedArray.AddValue(2);
        nestedArray = nestedArray.AddValue(3);

        // Act
        array = array.AddValue(nestedArray);

        // Assert
        await Assert.That(array.Count).IsEqualTo(1);
        await Assert.That(array[0].IsArray).IsTrue();

        var retrievedArray = array[0] as BsonArray;
        await Assert.That(retrievedArray.Count).IsEqualTo(3);
        await Assert.That(retrievedArray[0].ToInt32(null)).IsEqualTo(1);
    }

    [Test]
    public async Task BsonArray_Should_Handle_Empty_Array()
    {
        // Arrange & Act
        var array = new BsonArray();

        // Assert
        await Assert.That(array.Count).IsEqualTo(0);
        await Assert.That(array.Count == 0).IsTrue();
    }

    [Test]
    public async Task BsonArray_Get_Should_Return_Correct_Value()
    {
        // Arrange
        var array = new BsonArray();
        array = array.AddValue("test");

        // Act
        var value = array.Get(0);

        // Assert
        await Assert.That(value.ToString()).IsEqualTo("test");
    }

    [Test]
    public async Task BsonArray_Get_Should_Throw_On_Invalid_Index()
    {
        // Arrange
        var array = new BsonArray();

        // Act & Assert
        await Assert.That(() => array.Get(0)).Throws<ArgumentOutOfRangeException>();
    }

    [Test]
    public async Task BsonArray_Set_Should_Throw_On_Invalid_Index()
    {
        // Arrange
        var array = new BsonArray();

        // Act & Assert
        await Assert.That(() => array.Set(0, "test")).Throws<ArgumentOutOfRangeException>();
    }

    [Test]
    public async Task BsonArray_IsReadOnly_Should_Return_True()
    {
        // Arrange & Act
        var array = new BsonArray();

        // Assert
        await Assert.That(array.IsReadOnly).IsTrue();
    }

    [Test]
    public async Task BsonArray_Indexer_Setter_Should_Throw()
    {
        // Arrange
        var array = new BsonArray();

        // Act & Assert
        await Assert.That(() => array[0] = "test").Throws<NotSupportedException>();
    }
}