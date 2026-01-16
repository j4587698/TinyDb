using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading.Tasks;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TinyDb.Bson;
using TinyDb.Metadata;
using TinyDb.Serialization;

namespace TinyDb.Tests.Serialization;

public class AotAndMetadataCoverageTests
{
    private class TestEntity
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    [Test]
    public async Task AotEntityAdapter_Constructor_ShouldValidateArgs()
    {
        Func<TestEntity, BsonDocument> toDoc = _ => new BsonDocument();
        Func<BsonDocument, TestEntity> fromDoc = _ => new TestEntity();
        Func<TestEntity, BsonValue> getId = _ => BsonNull.Value;
        Action<TestEntity, BsonValue> setId = (_, _) => { };
        Func<TestEntity, bool> hasValidId = _ => true;
        Func<TestEntity, string, object?> getProp = (_, _) => null;

        await Assert.ThrowsAsync<ArgumentNullException>(() => { new AotEntityAdapter<TestEntity>(null!, fromDoc, getId, setId, hasValidId, getProp); return Task.CompletedTask; });
        await Assert.ThrowsAsync<ArgumentNullException>(() => { new AotEntityAdapter<TestEntity>(toDoc, null!, getId, setId, hasValidId, getProp); return Task.CompletedTask; });
        await Assert.ThrowsAsync<ArgumentNullException>(() => { new AotEntityAdapter<TestEntity>(toDoc, fromDoc, null!, setId, hasValidId, getProp); return Task.CompletedTask; });
        await Assert.ThrowsAsync<ArgumentNullException>(() => { new AotEntityAdapter<TestEntity>(toDoc, fromDoc, getId, null!, hasValidId, getProp); return Task.CompletedTask; });
        await Assert.ThrowsAsync<ArgumentNullException>(() => { new AotEntityAdapter<TestEntity>(toDoc, fromDoc, getId, setId, null!, getProp); return Task.CompletedTask; });
        await Assert.ThrowsAsync<ArgumentNullException>(() => { new AotEntityAdapter<TestEntity>(toDoc, fromDoc, getId, setId, hasValidId, null!); return Task.CompletedTask; });
    }

    [Test]
    public async Task AotHelperRegistry_Clear_ShouldWork()
    {
        // 注册一个假的适配器
        var adapter = new AotEntityAdapter<TestEntity>(
            _ => new BsonDocument(),
            _ => new TestEntity(),
            _ => BsonNull.Value,
            (_, _) => { },
            _ => true,
            (_, _) => null
        );
        
        AotHelperRegistry.Register(adapter);
        
        var found = AotHelperRegistry.TryGetAdapter<TestEntity>(out var retrieved);
        await Assert.That(found).IsTrue();
        await Assert.That(retrieved).IsSameReferenceAs(adapter);

        AotHelperRegistry.Clear();
        found = AotHelperRegistry.TryGetAdapter<TestEntity>(out retrieved);
        await Assert.That(found).IsFalse();
        await Assert.That(retrieved).IsNull();
    }

    [Test]
    public async Task AotIdAccessor_NullEntity_ShouldThrowOrReturnDefault()
    {
        await Assert.ThrowsAsync<ArgumentNullException>(() => { AotIdAccessor<TestEntity>.GetId(null!); return Task.CompletedTask; });
        // Use new BsonInt32(1) instead of Create to avoid compilation issues if Create doesn't exist
        await Assert.ThrowsAsync<ArgumentNullException>(() => { AotIdAccessor<TestEntity>.SetId(null!, new BsonInt32(1)); return Task.CompletedTask; });
        
        await Assert.That(AotIdAccessor<TestEntity>.HasValidId(null!)).IsFalse();
        await Assert.That(AotIdAccessor<TestEntity>.GenerateIdIfNeeded(null!)).IsFalse();
    }

    [Test]
    public async Task AotIdAccessor_NoIdProperty_ShouldHandleGracefully()
    {
        // NoIdEntity has no [Id] attribute or property named Id
        var entity = new NoIdEntity();
        
        // Ensure registry is clear for this type
        AotHelperRegistry.Clear();

        await Assert.That(AotIdAccessor<NoIdEntity>.GetId(entity)).IsEqualTo(BsonNull.Value);
        
        // SetId should do nothing
        AotIdAccessor<NoIdEntity>.SetId(entity, new BsonObjectId(ObjectId.NewObjectId()));
        
        await Assert.That(AotIdAccessor<NoIdEntity>.HasValidId(entity)).IsFalse();
        await Assert.That(AotIdAccessor<NoIdEntity>.GenerateIdIfNeeded(entity)).IsFalse();
    }

    [Test]
    public async Task AotIdAccessor_ConvertIdValue_Coverage()
    {
        // Use reflection to invoke private ConvertIdValue
        var method = typeof(AotIdAccessor<TestEntity>).GetMethod("ConvertIdValue", BindingFlags.NonPublic | BindingFlags.Static);
        
        // String to Guid
        var guid = Guid.NewGuid();
        var guidStr = new BsonString(guid.ToString());
        var resGuid = method!.Invoke(null, new object[] { guidStr, typeof(Guid) });
        await Assert.That(resGuid).IsEqualTo(guid);

        // String to ObjectId
        var oid = ObjectId.NewObjectId();
        var oidStr = new BsonString(oid.ToString());
        var resOid = method!.Invoke(null, new object[] { oidStr, typeof(ObjectId) });
        await Assert.That(resOid).IsEqualTo(oid);

        // String to Enum
        var enumStr = new BsonString("Synced");
        var resEnum = method!.Invoke(null, new object[] { enumStr, typeof(TinyDb.Core.WriteConcern) });
        await Assert.That(resEnum).IsEqualTo(TinyDb.Core.WriteConcern.Synced);
    }

    [Test]
    public async Task MetadataApi_UnregisteredType_ShouldReturnDefaults()
    {
        // Create a real engine for MetadataManager
        var dbPath = $"test_metadata_api_{Guid.NewGuid():N}.db";
        using var engine = new TinyDb.Core.TinyDbEngine(dbPath, new TinyDb.Core.TinyDbOptions { EnableJournaling = false });
        var manager = new MetadataManager(engine);
        
        // Unregistered type
        var type = typeof(string); 

        var props = manager.GetEntityProperties(type);
        await Assert.That(props).IsEmpty();

        var displayName = manager.GetEntityDisplayName(type);
        await Assert.That(displayName).IsEqualTo(type.Name);

        var propName = manager.GetPropertyDisplayName(type, "Length");
        await Assert.That(propName).IsEqualTo("Length"); // Fallback to property name

        var propType = manager.GetPropertyType(type, "Length");
        await Assert.That(propType).IsNull();

        var required = manager.IsPropertyRequired(type, "Length");
        await Assert.That(required).IsFalse();

        var order = manager.GetPropertyOrder(type, "Length");
        await Assert.That(order).IsEqualTo(0);
        
        // Cleanup
        if (System.IO.File.Exists(dbPath)) try { System.IO.File.Delete(dbPath); } catch { }
    }

    private class NoIdEntity
    {
        public string Name { get; set; } = "test";
    }
}