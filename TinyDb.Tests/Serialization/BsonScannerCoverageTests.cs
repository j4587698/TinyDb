using System;
using System.IO;
using System.Linq;
using System.Text;
using TinyDb.Bson;
using TinyDb.Serialization;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Serialization;

public class BsonScannerCoverageTests
{
    [Test]
    public async Task TryGetValue_Primitives_ShouldWork()
    {
        var doc = new BsonDocument()
            .Set("int", 123)
            .Set("str", "test")
            .Set("bool", true)
            .Set("double", 1.23);
            
        var bytes = BsonSerializer.SerializeDocument(doc);
        
        await Assert.That(BsonScanner.TryGetValue(bytes, "int", out var val)).IsTrue();
        await Assert.That(((BsonInt32)val!).Value).IsEqualTo(123);
        
        await Assert.That(BsonScanner.TryGetValue(bytes, "str", out val)).IsTrue();
        await Assert.That(((BsonString)val!).Value).IsEqualTo("test");
        
        await Assert.That(BsonScanner.TryGetValue(bytes, "bool", out val)).IsTrue();
        await Assert.That(((BsonBoolean)val!).Value).IsTrue();
        
        await Assert.That(BsonScanner.TryGetValue(bytes, "double", out val)).IsTrue();
        await Assert.That(((BsonDouble)val!).Value).IsEqualTo(1.23);
    }
    
    [Test]
    public async Task TryGetValue_NotFound_ShouldReturnFalse()
    {
        var doc = new BsonDocument().Set("a", 1);
        var bytes = BsonSerializer.SerializeDocument(doc);
        
        await Assert.That(BsonScanner.TryGetValue(bytes, "b", out _)).IsFalse();
    }
    
    [Test]
    public async Task TryGetValue_SkipComplexTypes_ShouldWork()
    {
        var doc = new BsonDocument()
            .Set("skip1", new BsonDocument().Set("sub", 1))
            .Set("skip2", new BsonArray(new BsonValue[] { 1, 2 }))
            .Set("target", 999);
            
        var bytes = BsonSerializer.SerializeDocument(doc);
        
        await Assert.That(BsonScanner.TryGetValue(bytes, "target", out var val)).IsTrue();
        await Assert.That(((BsonInt32)val!).Value).IsEqualTo(999);
    }

    [Test]
    public async Task TryGetValue_SkipDouble_ShouldWork()
    {
        using var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms);

        writer.Write(0); // placeholder length

        writer.Write((byte)BsonType.Double);
        writer.Write(Encoding.UTF8.GetBytes("skipDouble"));
        writer.Write((byte)0);
        writer.Write(1.23d);

        writer.Write((byte)BsonType.Int32);
        writer.Write(Encoding.UTF8.GetBytes("target"));
        writer.Write((byte)0);
        writer.Write(42);

        writer.Write((byte)0); // end

        var bytes = ms.ToArray();
        BitConverter.GetBytes(bytes.Length).CopyTo(bytes, 0);

        await Assert.That(BsonScanner.TryGetValue(bytes, "target", out var val)).IsTrue();
        await Assert.That(((BsonInt32)val!).Value).IsEqualTo(42);
    }

    [Test]
    public async Task TryGetValue_SkipBoolean_ShouldWork()
    {
        using var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms);

        writer.Write(0); // placeholder length

        writer.Write((byte)BsonType.Boolean);
        writer.Write(Encoding.UTF8.GetBytes("skipBool"));
        writer.Write((byte)0);
        writer.Write((byte)1);

        writer.Write((byte)BsonType.Int32);
        writer.Write(Encoding.UTF8.GetBytes("target"));
        writer.Write((byte)0);
        writer.Write(42);

        writer.Write((byte)0); // end

        var bytes = ms.ToArray();
        BitConverter.GetBytes(bytes.Length).CopyTo(bytes, 0);

        await Assert.That(BsonScanner.TryGetValue(bytes, "target", out var val)).IsTrue();
        await Assert.That(((BsonInt32)val!).Value).IsEqualTo(42);
    }

    [Test]
    public async Task TryGetValue_SkipString_ShouldWork()
    {
        using var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms);

        writer.Write(0); // placeholder length

        writer.Write((byte)BsonType.String);
        writer.Write(Encoding.UTF8.GetBytes("skipStr"));
        writer.Write((byte)0);

        var strBytes = Encoding.UTF8.GetBytes("hello");
        writer.Write(strBytes.Length + 1);
        writer.Write(strBytes);
        writer.Write((byte)0);

        writer.Write((byte)BsonType.Int32);
        writer.Write(Encoding.UTF8.GetBytes("target"));
        writer.Write((byte)0);
        writer.Write(42);

        writer.Write((byte)0); // end

        var bytes = ms.ToArray();
        BitConverter.GetBytes(bytes.Length).CopyTo(bytes, 0);

        await Assert.That(BsonScanner.TryGetValue(bytes, "target", out var val)).IsTrue();
        await Assert.That(((BsonInt32)val!).Value).IsEqualTo(42);
    }

    [Test]
    public async Task TryGetValue_SkipObjectId_ShouldWork()
    {
        var doc = new BsonDocument()
            .Set("oid", new BsonObjectId(ObjectId.NewObjectId()))
            .Set("target", 1);

        var bytes = BsonSerializer.SerializeDocument(doc);

        await Assert.That(BsonScanner.TryGetValue(bytes, "target", out var val)).IsTrue();
        await Assert.That(((BsonInt32)val!).Value).IsEqualTo(1);
    }

    [Test]
    public async Task TryGetValue_SkipRawObjectId_ShouldWork()
    {
        using var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms);

        writer.Write(0); // placeholder for length

        writer.Write((byte)BsonType.ObjectId);
        writer.Write((byte)'o');
        writer.Write((byte)'i');
        writer.Write((byte)'d');
        writer.Write((byte)0);
        writer.Write(new byte[12]);

        writer.Write((byte)BsonType.Int32);
        writer.Write((byte)'t');
        writer.Write((byte)'a');
        writer.Write((byte)'r');
        writer.Write((byte)'g');
        writer.Write((byte)'e');
        writer.Write((byte)'t');
        writer.Write((byte)0);
        writer.Write(42);

        writer.Write((byte)0); // end

        var bytes = ms.ToArray();
        BitConverter.GetBytes(bytes.Length).CopyTo(bytes, 0);

        await Assert.That(BsonScanner.TryGetValue(bytes, "target", out var val)).IsTrue();
        await Assert.That(((BsonInt32)val!).Value).IsEqualTo(42);
    }

    [Test]
    public async Task TryGetValue_SkipRawDecimal128_ShouldWork()
    {
        using var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms);

        writer.Write(0); // placeholder for length

        writer.Write((byte)BsonType.Decimal128);
        writer.Write((byte)'d');
        writer.Write((byte)'e');
        writer.Write((byte)'c');
        writer.Write((byte)0);
        writer.Write(new byte[16]);

        writer.Write((byte)BsonType.Int32);
        writer.Write((byte)'t');
        writer.Write((byte)'a');
        writer.Write((byte)'r');
        writer.Write((byte)'g');
        writer.Write((byte)'e');
        writer.Write((byte)'t');
        writer.Write((byte)0);
        writer.Write(7);

        writer.Write((byte)0); // end

        var bytes = ms.ToArray();
        BitConverter.GetBytes(bytes.Length).CopyTo(bytes, 0);

        await Assert.That(BsonScanner.TryGetValue(bytes, "target", out var val)).IsTrue();
        await Assert.That(((BsonInt32)val!).Value).IsEqualTo(7);
    }
    
    [Test]
    public async Task TryGetValue_SkipRegex_ShouldWork()
    {
        var doc = new BsonDocument()
            .Set("regex", new BsonRegularExpression("pattern", "options"))
            .Set("target", 1);
            
        var bytes = BsonSerializer.SerializeDocument(doc);
        
        await Assert.That(BsonScanner.TryGetValue(bytes, "target", out var val)).IsTrue();
    }
    
    [Test]
    public async Task TryGetValue_ComplexFallback_ShouldWork()
    {
        var bin = new byte[] { 1, 2, 3 };
        var doc = new BsonDocument()
            .Set("bin", new BsonBinary(bin));
            
        var bytes = BsonSerializer.SerializeDocument(doc);
        
        await Assert.That(BsonScanner.TryGetValue(bytes, "bin", out var val)).IsTrue();
        await Assert.That(val).IsTypeOf<BsonBinary>();
        await Assert.That(((BsonBinary)val!).Bytes.SequenceEqual(bin)).IsTrue();
    }

    [Test]
    public async Task TryGetValue_InvalidData_ShouldReturnFalse()
    {
        var bytes = new byte[0];
        await Assert.That(BsonScanner.TryGetValue(bytes, "a", out _)).IsFalse();
        
        bytes = new byte[4]; // header only (too short)
        await Assert.That(BsonScanner.TryGetValue(bytes, "a", out _)).IsFalse();
    }

    [Test]
    public async Task TryGetValue_MalformedName_ShouldReturnFalse()
    {
        // Construct malformed BSON where name is not null terminated
        using var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms);
        writer.Write(20); // Length
        writer.Write((byte)BsonType.Int32);
        writer.Write((byte)'a'); // 'a'
        // Missing null terminator for name
        writer.Write(123); // Value
        writer.Write((byte)0); // End
        
        var bytes = ms.ToArray();
        // The scanner loop checks nameEnd < document.Length
        await Assert.That(BsonScanner.TryGetValue(bytes, "a", out _)).IsFalse();
    }

    [Test]
    public async Task TryGetValue_UnknownType_ShouldThrow()
    {
        using var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms);

        writer.Write(0); // placeholder for length
        writer.Write((byte)250); // unknown type
        writer.Write((byte)'x');
        writer.Write((byte)0);
        writer.Write((byte)0); // end

        var bytes = ms.ToArray();
        BitConverter.GetBytes(bytes.Length).CopyTo(bytes, 0);

        await Assert.That(() => BsonScanner.TryGetValue(bytes, "target", out _))
            .Throws<NotSupportedException>();
    }
}
