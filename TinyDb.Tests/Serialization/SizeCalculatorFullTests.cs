using System.Text;
using System.Threading.Tasks;
using TinyDb.Bson;
using TinyDb.Serialization;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Serialization;

/// <summary>
/// Comprehensive tests for SizeCalculator to improve coverage
/// </summary>
public class SizeCalculatorFullTests
{
    private SizeCalculator _calculator = null!;

    [Before(Test)]
    public void Setup()
    {
        _calculator = new SizeCalculator();
    }

    #region BsonJavaScript Tests

    [Test]
    public async Task CalculateSize_BsonJavaScript_ShouldCalculateCorrectly()
    {
        var js = new BsonJavaScript("function() { return 1; }");
        var size = _calculator.CalculateSize(js);
        
        // String size = 4 (length) + UTF8 bytes + 1 (null terminator)
        var expectedCode = "function() { return 1; }";
        var expectedSize = 4 + Encoding.UTF8.GetByteCount(expectedCode) + 1;
        
        await Assert.That(size).IsEqualTo(expectedSize);
    }

    [Test]
    public async Task CalculateSize_BsonJavaScript_EmptyCode_ShouldCalculateCorrectly()
    {
        var js = new BsonJavaScript("");
        var size = _calculator.CalculateSize(js);
        
        // Empty string: 4 (length) + 0 (bytes) + 1 (null terminator) = 5
        await Assert.That(size).IsEqualTo(5);
    }

    [Test]
    public async Task CalculateSize_BsonJavaScript_UnicodeCode_ShouldCalculateCorrectly()
    {
        var js = new BsonJavaScript("var x = '\u4e2d\u6587';"); // Chinese characters
        var size = _calculator.CalculateSize(js);
        
        var expectedSize = 4 + Encoding.UTF8.GetByteCount("var x = '\u4e2d\u6587';") + 1;
        await Assert.That(size).IsEqualTo(expectedSize);
    }

    #endregion

    #region BsonSymbol Tests

    [Test]
    public async Task CalculateSize_BsonSymbol_ShouldCalculateCorrectly()
    {
        var symbol = new BsonSymbol("my_symbol");
        var size = _calculator.CalculateSize(symbol);
        
        // String size = 4 (length) + UTF8 bytes + 1 (null terminator)
        var expectedSize = 4 + Encoding.UTF8.GetByteCount("my_symbol") + 1;
        
        await Assert.That(size).IsEqualTo(expectedSize);
    }

    [Test]
    public async Task CalculateSize_BsonSymbol_EmptyName_ShouldCalculateCorrectly()
    {
        var symbol = new BsonSymbol("");
        var size = _calculator.CalculateSize(symbol);
        
        // Empty string: 4 (length) + 0 (bytes) + 1 (null terminator) = 5
        await Assert.That(size).IsEqualTo(5);
    }

    [Test]
    public async Task CalculateSize_BsonSymbol_LongName_ShouldCalculateCorrectly()
    {
        var longName = new string('a', 1000);
        var symbol = new BsonSymbol(longName);
        var size = _calculator.CalculateSize(symbol);
        
        var expectedSize = 4 + Encoding.UTF8.GetByteCount(longName) + 1;
        await Assert.That(size).IsEqualTo(expectedSize);
    }

    #endregion

    #region BsonJavaScriptWithScope Tests

    [Test]
    public async Task CalculateSize_BsonJavaScriptWithScope_ShouldCalculateCorrectly()
    {
        var scope = new BsonDocument().Set("x", 1);
        var jsWithScope = new BsonJavaScriptWithScope("function() { return x; }", scope);
        var size = _calculator.CalculateSize(jsWithScope);
        
        // Size = 4 (total length prefix) + string size + document size
        var codeSize = 4 + Encoding.UTF8.GetByteCount("function() { return x; }") + 1;
        var docSize = _calculator.CalculateDocumentSize(scope);
        var expectedSize = 4 + codeSize + docSize;
        
        await Assert.That(size).IsEqualTo(expectedSize);
    }

    [Test]
    public async Task CalculateSize_BsonJavaScriptWithScope_EmptyScope_ShouldCalculateCorrectly()
    {
        var scope = new BsonDocument();
        var jsWithScope = new BsonJavaScriptWithScope("return 1;", scope);
        var size = _calculator.CalculateSize(jsWithScope);
        
        var codeSize = 4 + Encoding.UTF8.GetByteCount("return 1;") + 1;
        var docSize = _calculator.CalculateDocumentSize(scope);
        var expectedSize = 4 + codeSize + docSize;
        
        await Assert.That(size).IsEqualTo(expectedSize);
    }

    [Test]
    public async Task CalculateSize_BsonJavaScriptWithScope_ComplexScope_ShouldCalculateCorrectly()
    {
        var scope = new BsonDocument()
            .Set("name", "test")
            .Set("count", 42)
            .Set("enabled", true)
            .Set("nested", new BsonDocument().Set("inner", "value"));
        var jsWithScope = new BsonJavaScriptWithScope("function() { return name + count; }", scope);
        var size = _calculator.CalculateSize(jsWithScope);
        
        var codeSize = 4 + Encoding.UTF8.GetByteCount("function() { return name + count; }") + 1;
        var docSize = _calculator.CalculateDocumentSize(scope);
        var expectedSize = 4 + codeSize + docSize;
        
        await Assert.That(size).IsEqualTo(expectedSize);
    }

    #endregion

    #region CalculateDocumentSize Edge Cases

    [Test]
    public async Task CalculateDocumentSize_WithNullDocument_ShouldThrowArgumentNullException()
    {
        await Assert.That(() => _calculator.CalculateDocumentSize(null!))
            .Throws<ArgumentNullException>();
    }

    [Test]
    public async Task CalculateDocumentSize_EmptyDocument_ShouldReturnMinSize()
    {
        var doc = new BsonDocument();
        var size = _calculator.CalculateDocumentSize(doc);
        
        // Empty document: 4 (size prefix) + 1 (end marker) = 5
        await Assert.That(size).IsEqualTo(5);
    }

    [Test]
    public async Task CalculateDocumentSize_WithAllBsonTypes_ShouldCalculateCorrectly()
    {
        var doc = new BsonDocument()
            .Set("null", BsonNull.Value)
            .Set("string", "test")
            .Set("int32", 42)
            .Set("int64", 123456789012345L)
            .Set("double", 3.14)
            .Set("bool", true)
            .Set("objectId", ObjectId.NewObjectId())
            .Set("dateTime", new BsonDateTime(DateTime.UtcNow))
            .Set("array", new BsonArray().AddValue(new BsonInt32(1)).AddValue(new BsonInt32(2)).AddValue(new BsonInt32(3)))
            .Set("nested", new BsonDocument().Set("inner", "value"));
        
        var size = _calculator.CalculateDocumentSize(doc);
        
        // Size should be positive and reasonable
        await Assert.That(size).IsGreaterThan(50);
    }

    [Test]
    public async Task CalculateDocumentSize_WithBinary_ShouldCalculateCorrectly()
    {
        var binaryData = new byte[] { 1, 2, 3, 4, 5 };
        var doc = new BsonDocument()
            .Set("data", new BsonBinary(binaryData));
        
        var size = _calculator.CalculateDocumentSize(doc);
        
        // Binary: 4 (length) + 1 (subtype) + data.Length
        // Plus document overhead
        await Assert.That(size).IsGreaterThan(10);
    }

    [Test]
    public async Task CalculateDocumentSize_WithRegex_ShouldCalculateCorrectly()
    {
        var doc = new BsonDocument()
            .Set("pattern", new BsonRegularExpression("^test.*$", "i"));
        
        var size = _calculator.CalculateDocumentSize(doc);
        
        // Regex: CString(pattern) + CString(options)
        await Assert.That(size).IsGreaterThan(15);
    }

    [Test]
    public async Task CalculateDocumentSize_WithTimestamp_ShouldCalculateCorrectly()
    {
        var doc = new BsonDocument()
            .Set("ts", new BsonTimestamp(1234567890, 1));
        
        var size = _calculator.CalculateDocumentSize(doc);
        
        // Timestamp: 8 bytes (fixed)
        await Assert.That(size).IsGreaterThan(12);
    }

    [Test]
    public async Task CalculateDocumentSize_WithDecimal128_ShouldCalculateCorrectly()
    {
        var doc = new BsonDocument()
            .Set("decimal", new BsonDecimal128(123.456m));
        
        var size = _calculator.CalculateDocumentSize(doc);
        
        // Decimal128: 16 bytes (fixed)
        await Assert.That(size).IsGreaterThan(20);
    }

    #endregion

    #region CalculateArraySize Edge Cases

    [Test]
    public async Task CalculateArraySize_WithNullArray_ShouldThrowArgumentNullException()
    {
        await Assert.That(() => _calculator.CalculateArraySize(null!))
            .Throws<ArgumentNullException>();
    }

    [Test]
    public async Task CalculateArraySize_EmptyArray_ShouldReturnMinSize()
    {
        var arr = new BsonArray();
        var size = _calculator.CalculateArraySize(arr);
        
        // Empty array: 4 (size prefix) + 1 (end marker) = 5
        await Assert.That(size).IsEqualTo(5);
    }

    [Test]
    public async Task CalculateArraySize_WithMixedTypes_ShouldCalculateCorrectly()
    {
        var arr = new BsonArray()
            .AddValue(BsonNull.Value)
            .AddValue(new BsonString("test"))
            .AddValue(new BsonInt32(42))
            .AddValue(new BsonDouble(3.14))
            .AddValue(BsonBoolean.True)
            .AddValue(new BsonArray().AddValue(new BsonInt32(1)).AddValue(new BsonInt32(2)))
            .AddValue(new BsonDocumentValue(new BsonDocument().Set("x", 1)));
        
        var size = _calculator.CalculateArraySize(arr);
        
        // Size should be positive and reasonable
        await Assert.That(size).IsGreaterThan(30);
    }

    [Test]
    public async Task CalculateArraySize_WithLargeIndices_ShouldCalculateCorrectly()
    {
        var arr = new BsonArray();
        for (int i = 0; i < 100; i++)
        {
            arr = arr.AddValue(new BsonInt32(i));
        }
        
        var size = _calculator.CalculateArraySize(arr);
        
        // Each element: 1 (type) + CString(index) + 4 (int32 value)
        // Indices > 10 take more bytes as strings
        await Assert.That(size).IsGreaterThan(500);
    }

    #endregion

    #region BsonValue Type Coverage

    [Test]
    public async Task CalculateSize_BsonNull_ShouldReturnZero()
    {
        var size = _calculator.CalculateSize(BsonNull.Value);
        await Assert.That(size).IsEqualTo(0);
    }

    [Test]
    public async Task CalculateSize_BsonInt32_ShouldReturn4()
    {
        var size = _calculator.CalculateSize(new BsonInt32(42));
        await Assert.That(size).IsEqualTo(4);
    }

    [Test]
    public async Task CalculateSize_BsonInt64_ShouldReturn8()
    {
        var size = _calculator.CalculateSize(new BsonInt64(42L));
        await Assert.That(size).IsEqualTo(8);
    }

    [Test]
    public async Task CalculateSize_BsonDouble_ShouldReturn8()
    {
        var size = _calculator.CalculateSize(new BsonDouble(3.14));
        await Assert.That(size).IsEqualTo(8);
    }

    [Test]
    public async Task CalculateSize_BsonBoolean_ShouldReturn1()
    {
        var size = _calculator.CalculateSize(BsonBoolean.True);
        await Assert.That(size).IsEqualTo(1);
    }

    [Test]
    public async Task CalculateSize_BsonObjectId_ShouldReturn12()
    {
        var size = _calculator.CalculateSize(ObjectId.NewObjectId());
        await Assert.That(size).IsEqualTo(12);
    }

    [Test]
    public async Task CalculateSize_BsonDateTime_ShouldReturn8()
    {
        var size = _calculator.CalculateSize(new BsonDateTime(DateTime.UtcNow));
        await Assert.That(size).IsEqualTo(8);
    }

    [Test]
    public async Task CalculateSize_BsonTimestamp_ShouldReturn8()
    {
        var size = _calculator.CalculateSize(new BsonTimestamp(1234567890, 1));
        await Assert.That(size).IsEqualTo(8);
    }

    [Test]
    public async Task CalculateSize_BsonDecimal128_ShouldReturn16()
    {
        var size = _calculator.CalculateSize(new BsonDecimal128(123.456m));
        await Assert.That(size).IsEqualTo(16);
    }

    [Test]
    public async Task CalculateSize_BsonString_ShouldCalculateCorrectly()
    {
        var str = new BsonString("hello");
        var size = _calculator.CalculateSize(str);
        
        // String: 4 (length) + 5 (bytes) + 1 (null) = 10
        await Assert.That(size).IsEqualTo(10);
    }

    [Test]
    public async Task CalculateSize_BsonBinary_ShouldCalculateCorrectly()
    {
        var binary = new BsonBinary(new byte[] { 1, 2, 3, 4, 5 });
        var size = _calculator.CalculateSize(binary);
        
        // Binary: 4 (length) + 1 (subtype) + 5 (data) = 10
        await Assert.That(size).IsEqualTo(10);
    }

    [Test]
    public async Task CalculateSize_BsonRegularExpression_ShouldCalculateCorrectly()
    {
        var regex = new BsonRegularExpression("^test$", "i");
        var size = _calculator.CalculateSize(regex);
        
        // CString(pattern) + CString(options)
        // "^test$" = 6 bytes + 1 null = 7
        // "i" = 1 byte + 1 null = 2
        await Assert.That(size).IsEqualTo(9);
    }

    #endregion

    #region BsonDocumentValue and BsonArrayValue Tests

    [Test]
    public async Task CalculateSize_BsonDocumentValue_ShouldCalculateCorrectly()
    {
        var innerDoc = new BsonDocument().Set("key", "value");
        var docValue = new BsonDocumentValue(innerDoc);
        var size = _calculator.CalculateSize(docValue);
        
        var expectedSize = _calculator.CalculateDocumentSize(innerDoc);
        await Assert.That(size).IsEqualTo(expectedSize);
    }

    [Test]
    public async Task CalculateSize_BsonArrayValue_ShouldCalculateCorrectly()
    {
        var innerArray = new BsonArray().AddValue(new BsonInt32(1)).AddValue(new BsonInt32(2)).AddValue(new BsonInt32(3));
        var arrValue = new BsonArrayValue(innerArray);
        var size = _calculator.CalculateSize(arrValue);
        
        var expectedSize = _calculator.CalculateArraySize(innerArray);
        await Assert.That(size).IsEqualTo(expectedSize);
    }

    #endregion

    #region Complex Nested Structures

    [Test]
    public async Task CalculateSize_DeeplyNestedDocument_ShouldCalculateCorrectly()
    {
        var doc = new BsonDocument()
            .Set("level1", new BsonDocument()
                .Set("level2", new BsonDocument()
                    .Set("level3", new BsonDocument()
                        .Set("level4", new BsonDocument()
                            .Set("value", "deep")))));
        
        var size = _calculator.CalculateDocumentSize(doc);
        
        // Should handle deep nesting without issues
        await Assert.That(size).IsGreaterThan(50);
    }

    [Test]
    public async Task CalculateSize_ArrayOfDocuments_ShouldCalculateCorrectly()
    {
        var arr = new BsonArray()
            .AddValue(new BsonDocumentValue(new BsonDocument().Set("a", 1)))
            .AddValue(new BsonDocumentValue(new BsonDocument().Set("b", 2)))
            .AddValue(new BsonDocumentValue(new BsonDocument().Set("c", 3)));
        
        var size = _calculator.CalculateArraySize(arr);
        
        await Assert.That(size).IsGreaterThan(30);
    }

    [Test]
    public async Task CalculateSize_DocumentWithArray_ShouldCalculateCorrectly()
    {
        var doc = new BsonDocument()
            .Set("items", new BsonArray()
                .AddValue(new BsonInt32(1))
                .AddValue(new BsonInt32(2))
                .AddValue(new BsonInt32(3))
                .AddValue(new BsonInt32(4))
                .AddValue(new BsonInt32(5)))
            .Set("count", 5);
        
        var size = _calculator.CalculateDocumentSize(doc);
        
        await Assert.That(size).IsGreaterThan(30);
    }

    #endregion

    #region Unicode and Special Characters

    [Test]
    public async Task CalculateSize_UnicodeString_ShouldCalculateCorrectly()
    {
        var str = new BsonString("\u4e2d\u6587\u6d4b\u8bd5"); // Chinese characters
        var size = _calculator.CalculateSize(str);
        
        var byteCount = Encoding.UTF8.GetByteCount("\u4e2d\u6587\u6d4b\u8bd5");
        var expectedSize = 4 + byteCount + 1;
        
        await Assert.That(size).IsEqualTo(expectedSize);
    }

    [Test]
    public async Task CalculateSize_StringWithNullCharacters_ShouldCalculateCorrectly()
    {
        var str = new BsonString("hello\0world");
        var size = _calculator.CalculateSize(str);
        
        var byteCount = Encoding.UTF8.GetByteCount("hello\0world");
        var expectedSize = 4 + byteCount + 1;
        
        await Assert.That(size).IsEqualTo(expectedSize);
    }

    [Test]
    public async Task CalculateSize_EmptyBinary_ShouldCalculateCorrectly()
    {
        var binary = new BsonBinary(Array.Empty<byte>());
        var size = _calculator.CalculateSize(binary);
        
        // Binary: 4 (length) + 1 (subtype) + 0 (data) = 5
        await Assert.That(size).IsEqualTo(5);
    }

    #endregion
}
