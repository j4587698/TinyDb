using System;
using System.Globalization;
using TinyDb.Attributes;
using TinyDb.Bson;
using TinyDb.Serialization;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Serialization;

/// <summary>
/// A class with Id property for GetId/SetId tests
/// </summary>
public class MapperSimpleEntity
{
    public int Id { get; set; }
    public string Name { get; set; } = "";
}

/// <summary>
/// A class with [Entity] attribute specifying IdProperty
/// </summary>
[Entity(IdProperty = "UserId")]
public class EntityWithSpecifiedIdProperty
{
    public int UserId { get; set; }
    public string Name { get; set; } = "";
}

/// <summary>
/// A class with [Id] attribute on a property
/// </summary>
public class EntityWithIdAttribute
{
    [Id]
    public string MyCustomId { get; set; } = "";
    public string Name { get; set; } = "";
}

/// <summary>
/// A class without any Id property
/// </summary>
public class MapperNoIdEntity
{
    public string Name { get; set; } = "";
}

public class AotBsonMapperExtendedTests
{
    private enum TestEnum { A, B }

    [Test]
    public async Task ConvertPrimitiveValue_NumericConversions()
    {
        BsonValue i32 = 100;
        BsonValue i64 = 200L;
        BsonValue dbl = 300.5;
        BsonValue str = "400";

        // short
        await Assert.That(AotBsonMapper.ConvertValue(i32, typeof(short))).IsEqualTo((short)100);
        await Assert.That(AotBsonMapper.ConvertValue(i64, typeof(short))).IsEqualTo((short)200);
        await Assert.That(AotBsonMapper.ConvertValue(dbl, typeof(short))).IsEqualTo((short)300);
        await Assert.That(AotBsonMapper.ConvertValue(str, typeof(short))).IsEqualTo((short)400);

        // byte
        await Assert.That(AotBsonMapper.ConvertValue(i32, typeof(byte))).IsEqualTo((byte)100);
        
        // sbyte
        await Assert.That(AotBsonMapper.ConvertValue(i32, typeof(sbyte))).IsEqualTo((sbyte)100);
        
        // uint
        await Assert.That(AotBsonMapper.ConvertValue(i32, typeof(uint))).IsEqualTo(100u);
        
        // ulong
        await Assert.That(AotBsonMapper.ConvertValue(i64, typeof(ulong))).IsEqualTo(200ul);
        
        // ushort
        await Assert.That(AotBsonMapper.ConvertValue(i32, typeof(ushort))).IsEqualTo((ushort)100);
        
        // float
        await Assert.That(AotBsonMapper.ConvertValue(dbl, typeof(float))).IsEqualTo(300.5f);
    }

    [Test]
    public async Task ConvertPrimitiveValue_Enum_ShouldWork()
    {
        await Assert.That(AotBsonMapper.ConvertValue((BsonValue)"A", typeof(TestEnum))).IsEqualTo(TestEnum.A);
        await Assert.That(AotBsonMapper.ConvertValue((BsonValue)1, typeof(TestEnum))).IsEqualTo(TestEnum.B);
        await Assert.That(AotBsonMapper.ConvertValue((BsonValue)0L, typeof(TestEnum))).IsEqualTo(TestEnum.A);
    }

    [Test]
    public async Task ConvertPrimitiveValue_Guid_ShouldWork()
    {
        var guid = Guid.NewGuid();
        var bin = new BsonBinary(guid.ToByteArray(), BsonBinary.BinarySubType.Uuid);
        
        await Assert.That(AotBsonMapper.ConvertValue(bin, typeof(Guid))).IsEqualTo(guid);
        await Assert.That(AotBsonMapper.ConvertValue((BsonValue)guid.ToString(), typeof(Guid))).IsEqualTo(guid);
    }

    [Test]
    public async Task ConvertPrimitiveValue_ByteArray_ShouldWork()
    {
        byte[] data = { 1, 2, 3 };
        var bin = new BsonBinary(data);
        var base64 = Convert.ToBase64String(data);
        
        await Assert.That(AotBsonMapper.ConvertValue(bin, typeof(byte[]))).IsEquivalentTo(data);
        await Assert.That(AotBsonMapper.ConvertValue((BsonValue)base64, typeof(byte[]))).IsEquivalentTo(data);
    }

    [Test]
    public async Task ConvertPrimitiveValue_DateTime_ShouldWork()
    {
        var now = DateTime.UtcNow;
        // BsonDateTime stores ms precision
        var nowMs = new DateTime(now.Year, now.Month, now.Day, now.Hour, now.Minute, now.Second, now.Millisecond, DateTimeKind.Utc);
        
        await Assert.That(AotBsonMapper.ConvertValue(new BsonDateTime(nowMs), typeof(DateTime))).IsEqualTo(nowMs);
        await Assert.That(AotBsonMapper.ConvertValue((BsonValue)nowMs.ToString("o"), typeof(DateTime))).IsEqualTo(nowMs);
    }

    [Test]
    public async Task ConvertPrimitiveValue_Checked_Overflow_ShouldThrow()
    {
        BsonValue big = long.MaxValue;
        await Assert.That(() => AotBsonMapper.ConvertValue(big, typeof(int))).Throws<OverflowException>();
    }
    
    /// <summary>
    /// Test GetId with a regular entity
    /// </summary>
    [Test]
    public async Task GetId_SimpleEntity_ShouldReturnId()
    {
        var entity = new MapperSimpleEntity { Id = 42, Name = "Test" };
        var id = AotBsonMapper.GetId(entity);
        await Assert.That(id.ToInt32(null)).IsEqualTo(42);
    }
    
    /// <summary>
    /// Test GetId with BsonDocument
    /// </summary>
    [Test]
    public async Task GetId_BsonDocument_ShouldReturnId()
    {
        var doc = new BsonDocument().Set("_id", 123).Set("name", "Test");
        var id = AotBsonMapper.GetId(doc);
        await Assert.That(id.ToInt32(null)).IsEqualTo(123);
    }
    
    /// <summary>
    /// Test GetId with BsonDocument without _id
    /// </summary>
    [Test]
    public async Task GetId_BsonDocument_NoId_ShouldReturnNull()
    {
        var doc = new BsonDocument().Set("name", "Test");
        var id = AotBsonMapper.GetId(doc);
        await Assert.That(id.IsNull).IsTrue();
    }
    
    /// <summary>
    /// Test GetId throws on null entity
    /// </summary>
    [Test]
    public async Task GetId_NullEntity_ShouldThrow()
    {
        await Assert.That(() => AotBsonMapper.GetId<MapperSimpleEntity>(null!)).Throws<ArgumentNullException>();
    }
    
    /// <summary>
    /// Test SetId with a regular entity
    /// </summary>
    [Test]
    public async Task SetId_SimpleEntity_ShouldSetId()
    {
        var entity = new MapperSimpleEntity { Id = 0, Name = "Test" };
        AotBsonMapper.SetId(entity, new BsonInt32(99));
        await Assert.That(entity.Id).IsEqualTo(99);
    }
    
    /// <summary>
    /// Test SetId with BsonDocument (should do nothing, BsonDocument is immutable)
    /// </summary>
    [Test]
    public async Task SetId_BsonDocument_ShouldDoNothing()
    {
        var doc = new BsonDocument().Set("_id", 123);
        AotBsonMapper.SetId(doc, new BsonInt32(999));
        // BsonDocument is immutable, SetId does nothing
        await Assert.That(doc["_id"].ToInt32(null)).IsEqualTo(123);
    }
    
    /// <summary>
    /// Test SetId throws on null entity
    /// </summary>
    [Test]
    public async Task SetId_NullEntity_ShouldThrow()
    {
        await Assert.That(() => AotBsonMapper.SetId<MapperSimpleEntity>(null!, new BsonInt32(1))).Throws<ArgumentNullException>();
    }
    
    /// <summary>
    /// Test GetPropertyValue with adapter
    /// </summary>
    [Test]
    public async Task GetPropertyValue_WithAdapter_ShouldWork()
    {
        var entity = new MapperSimpleEntity { Id = 1, Name = "TestValue" };
        var value = AotBsonMapper.GetPropertyValue(entity, "Name");
        await Assert.That(value).IsEqualTo("TestValue");
    }
    
    /// <summary>
    /// Test GetPropertyValue with non-existent property
    /// </summary>
    [Test]
    public async Task GetPropertyValue_NonExistent_ShouldReturnNull()
    {
        var entity = new MapperSimpleEntity { Id = 1, Name = "Test" };
        var value = AotBsonMapper.GetPropertyValue(entity, "NonExistent");
        await Assert.That(value).IsNull();
    }
    
    /// <summary>
    /// Test GetPropertyValue throws on null entity
    /// </summary>
    [Test]
    public async Task GetPropertyValue_NullEntity_ShouldThrow()
    {
        await Assert.That(() => AotBsonMapper.GetPropertyValue<MapperSimpleEntity>(null!, "Name")).Throws<ArgumentNullException>();
    }
    
    /// <summary>
    /// Test GetPropertyValue throws on null propertyName
    /// </summary>
    [Test]
    public async Task GetPropertyValue_NullPropertyName_ShouldThrow()
    {
        var entity = new MapperSimpleEntity { Id = 1, Name = "Test" };
        await Assert.That(() => AotBsonMapper.GetPropertyValue(entity, null!)).Throws<ArgumentNullException>();
    }
    
    /// <summary>
    /// Test ResolveIdProperty with EntityAttribute.IdProperty specified
    /// </summary>
    [Test]
    public async Task ToDocument_EntityWithSpecifiedIdProperty_ShouldUseSpecifiedProperty()
    {
        var entity = new EntityWithSpecifiedIdProperty { UserId = 42, Name = "Test" };
        var doc = AotBsonMapper.ToDocument(entity);
        
        // The Id should be mapped to _id
        await Assert.That(doc.ContainsKey("_id")).IsTrue();
        await Assert.That(doc["_id"].ToInt32(null)).IsEqualTo(42);
    }
    
    /// <summary>
    /// Test ResolveIdProperty with [Id] attribute
    /// </summary>
    [Test]
    public async Task ToDocument_EntityWithIdAttribute_ShouldUseAttributedProperty()
    {
        var entity = new EntityWithIdAttribute { MyCustomId = "custom-id-123", Name = "Test" };
        var doc = AotBsonMapper.ToDocument(entity);
        
        // The MyCustomId should be mapped to _id
        await Assert.That(doc.ContainsKey("_id")).IsTrue();
        await Assert.That(doc["_id"].ToString()).IsEqualTo("custom-id-123");
    }
    
    /// <summary>
    /// Test FromDocument with EntityAttribute.IdProperty specified
    /// </summary>
    [Test]
    public async Task FromDocument_EntityWithSpecifiedIdProperty_ShouldRestoreCorrectly()
    {
        var doc = new BsonDocument().Set("_id", 42).Set("name", "Test");
        var entity = AotBsonMapper.FromDocument<EntityWithSpecifiedIdProperty>(doc);
        
        await Assert.That(entity.UserId).IsEqualTo(42);
        await Assert.That(entity.Name).IsEqualTo("Test");
    }
    
    /// <summary>
    /// Test GetId with entity that has no Id property
    /// </summary>
    [Test]
    public async Task GetId_NoIdProperty_ShouldReturnNull()
    {
        var entity = new MapperNoIdEntity { Name = "Test" };
        var id = AotBsonMapper.GetId(entity);
        await Assert.That(id.IsNull).IsTrue();
    }
}
