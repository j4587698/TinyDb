using SimpleDb.Storage;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace SimpleDb.Tests.Storage;

public class PageTypeTests
{
    [Test]
    public async Task PageType_Should_Have_Correct_Values()
    {
        // Assert
        await Assert.That((byte)PageType.Empty == 0x00).IsTrue();
        await Assert.That((byte)PageType.Header == 0x01).IsTrue();
        await Assert.That((byte)PageType.Collection == 0x02).IsTrue();
        await Assert.That((byte)PageType.Data == 0x03).IsTrue();
        await Assert.That((byte)PageType.Index == 0x04).IsTrue();
        await Assert.That((byte)PageType.Journal == 0x05).IsTrue();
        await Assert.That((byte)PageType.Extension == 0x06).IsTrue();
    }

    [Test]
    public async Task PageType_Should_Be_Byte_Type()
    {
        // Arrange
        var pageType = PageType.Data;

        // Act
        var byteValue = (byte)pageType;

        // Assert
        await Assert.That(byteValue == 0x03).IsTrue();
    }

    [Test]
    public async Task PageType_Should_Support_Casting_From_Byte()
    {
        // Arrange
        byte byteValue = 0x04;

        // Act
        var pageType = (PageType)byteValue;

        // Assert
        await Assert.That(pageType == PageType.Index).IsTrue();
    }

    [Test]
    public async Task PageType_Should_Support_All_Values()
    {
        // Arrange
        var expectedTypes = new[]
        {
            PageType.Empty,
            PageType.Header,
            PageType.Collection,
            PageType.Data,
            PageType.Index,
            PageType.Journal,
            PageType.Extension
        };

        // Act & Assert
        var i = 0;
        foreach (var expectedType in expectedTypes)
        {
            var actualType = (PageType)i;
            await Assert.That(actualType).IsEqualTo(expectedType);
            i++;
        }
    }

    [Test]
    public async Task PageType_Should_Handle_Invalid_Byte_Values()
    {
        // Arrange
        byte invalidValue = 0xFF;

        // Act
        var pageType = (PageType)invalidValue;

        // Assert - Should handle gracefully (though it won't match any known type)
        await Assert.That((byte)pageType == 0xFF).IsTrue();
    }

    [Test]
    public async Task PageType_Comparison_Should_Work_Correctly()
    {
        // Arrange
        var type1 = PageType.Data;
        var type2 = PageType.Data;
        var type3 = PageType.Index;

        // Act & Assert
        await Assert.That(type1 == type2).IsTrue();
        await Assert.That(type1 != type3).IsTrue();
        await Assert.That(type1.Equals(type2)).IsTrue();
        await Assert.That(type1.Equals(type3)).IsFalse();
    }

    [Test]
    public async Task PageType_ToString_Should_Return_Type_Name()
    {
        // Arrange
        var pageType = PageType.Collection;

        // Act
        var result = pageType.ToString();

        // Assert
        await Assert.That(result).IsEqualTo("Collection");
    }

    [Test]
    public async Task PageType_GetHashCode_Should_Be_Consistent()
    {
        // Arrange
        var pageType = PageType.Journal;

        // Act
        var hash1 = pageType.GetHashCode();
        var hash2 = pageType.GetHashCode();

        // Assert
        await Assert.That(hash1 == hash2).IsTrue();
        await Assert.That(hash1 == (byte)PageType.Journal).IsTrue();
    }

    [Test]
    public async Task PageType_Should_Support_Switch_Statement()
    {
        // Arrange
        var testTypes = new[] { PageType.Empty, PageType.Data, PageType.Index };
        var results = new List<string>();

        // Act
        foreach (var type in testTypes)
        {
            switch (type)
            {
                case PageType.Empty:
                    results.Add("Empty");
                    break;
                case PageType.Data:
                    results.Add("Data");
                    break;
                case PageType.Index:
                    results.Add("Index");
                    break;
                default:
                    results.Add("Other");
                    break;
            }
        }

        // Assert
        await Assert.That(results.Count).IsEqualTo(3);
        await Assert.That(results[0]).IsEqualTo("Empty");
        await Assert.That(results[1]).IsEqualTo("Data");
        await Assert.That(results[2]).IsEqualTo("Index");
    }

    [Test]
    public async Task PageType_Should_Work_In_Collections()
    {
        // Arrange
        var pageTypes = new List<PageType>
        {
            PageType.Header,
            PageType.Data,
            PageType.Index,
            PageType.Collection
        };

        // Act
        var hasData = pageTypes.Contains(PageType.Data);
        var hasEmpty = pageTypes.Contains(PageType.Empty);
        var count = pageTypes.Count;

        // Assert
        await Assert.That(hasData).IsTrue();
        await Assert.That(hasEmpty).IsFalse();
        await Assert.That(count).IsEqualTo(4);
    }

    [Test]
    public async Task PageType_Should_Support_Enumeration()
    {
        // Arrange
        var allTypes = new List<PageType>();

        // Act
        for (byte i = 0; i <= 6; i++)
        {
            allTypes.Add((PageType)i);
        }

        // Assert
        await Assert.That(allTypes.Count).IsEqualTo(7);
        await Assert.That(allTypes).Contains(PageType.Empty);
        await Assert.That(allTypes).Contains(PageType.Header);
        await Assert.That(allTypes).Contains(PageType.Collection);
        await Assert.That(allTypes).Contains(PageType.Data);
        await Assert.That(allTypes).Contains(PageType.Index);
        await Assert.That(allTypes).Contains(PageType.Journal);
        await Assert.That(allTypes).Contains(PageType.Extension);
    }
}