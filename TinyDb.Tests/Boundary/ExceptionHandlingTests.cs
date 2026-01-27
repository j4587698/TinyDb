using TinyDb.Bson;
using TinyDb.Serialization;
using TinyDb.Storage;
using TinyDb.Core;
using TinyDb.Attributes;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Boundary;

public class ExceptionHandlingTests : IDisposable
{
    private readonly string _testDbPath;

    public ExceptionHandlingTests()
    {
        _testDbPath = Path.GetTempFileName();
    }

    public void Dispose()
    {
        if (File.Exists(_testDbPath)) File.Delete(_testDbPath);
    }

    [Test]
    public async Task Deserialize_Corrupt_Bson_Should_Throw()
    {
        // 只有 4 字节（长度），但内容缺失
        byte[] corruptBson = new byte[] { 0x08, 0x00, 0x00, 0x00 }; 
        
        await Assert.That(() => BsonSerializer.DeserializeDocument(corruptBson))
            .Throws<Exception>(); // 可能是 EndOfStreamException 或 IndexOutOfRangeException
    }

    [Test]
    public async Task DiskStream_Operations_After_Dispose_Should_Throw()
    {
        var stream = new DiskStream(_testDbPath);
        stream.Dispose();

        await Assert.That(() => stream.Read(new byte[1], 0, 1)).Throws<ObjectDisposedException>();
        await Assert.That(() => stream.Write(new byte[1], 0, 1)).Throws<ObjectDisposedException>();
        await Assert.That(() => stream.Size).Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task TinyDbEngine_With_Invalid_Header_Should_Throw()
    {
        // 创建一个包含垃圾数据的文件
        File.WriteAllBytes(_testDbPath, new byte[] { 0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x01 });

        await Assert.That(() => new TinyDbEngine(_testDbPath))
            .Throws<InvalidOperationException>();
    }

    [Test]
    public async Task FindById_With_Null_Or_NullBson_Should_Return_Null()
    {
        using var engine = new TinyDbEngine(_testDbPath);
        var collection = engine.GetCollection<ExceptionTestEntity>();

        var result1 = collection.FindById(null!);
        var result2 = collection.FindById(BsonNull.Value);

        await Assert.That(result1).IsNull();
        await Assert.That(result2).IsNull();
    }
}

[Entity("Test")]
public class ExceptionTestEntity
{
    public int Id { get; set; }
}
