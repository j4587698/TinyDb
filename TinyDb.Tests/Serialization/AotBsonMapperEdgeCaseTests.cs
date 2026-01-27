using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading.Tasks;
using TinyDb.Serialization;
using TinyDb.Bson;
using TinyDb.Attributes;
using TUnit.Core;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TinyDb.Tests.Utils;

namespace TinyDb.Tests.Serialization;

/// <summary>
/// Additional coverage tests for AotBsonMapper edge cases and closures
/// </summary>
[SkipInAot("These tests use reflection-based fallback paths")]
public class AotBsonMapperEdgeCaseTests
{
    #region Test Entities

    internal class EntityWithIdAttribute
    {
        public int Key { get; set; }
        
        [Id]
        public string CustomId { get; set; } = "";
        
        public string Value { get; set; } = "";
    }

    // Note: EntityAttribute triggers source generator which doesn't support nested classes
    // Testing EntityAttribute functionality moved to top-level class test files
    internal class EntityWithCustomIdProperty
    {
        public int EntityKey { get; set; }
        public string Name { get; set; } = "";
    }

    internal class EntityWithCircularReference
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
        public EntityWithCircularReference? Parent { get; set; }
    }

    internal struct ValueTypeEntity
    {
        public int Id { get; set; }
        public string Name { get; set; }
    }

    internal class EntityWithNullableTypes
    {
        public int Id { get; set; }
        public int? NullableInt { get; set; }
        public DateTime? NullableDateTime { get; set; }
        public bool? NullableBool { get; set; }
    }

    internal class EntityWithPublicField
    {
        public int Id { get; set; }
        public string FieldValue = "";
    }

    internal class EntityWithArray
    {
        public int Id { get; set; }
        public int[] Numbers { get; set; } = Array.Empty<int>();
        public string[] Strings { get; set; } = Array.Empty<string>();
    }

    internal class EntityWithNestedObject
    {
        public int Id { get; set; }
        public NestedClass Nested { get; set; } = new();
    }

    internal class NestedClass
    {
        public string Value { get; set; } = "";
        public int Number { get; set; }
    }

    internal class EntityWithUnsignedTypes
    {
        public int Id { get; set; }
        public uint UInt32Val { get; set; }
        public ulong UInt64Val { get; set; }
        public ushort UInt16Val { get; set; }
        public sbyte SByteVal { get; set; }
    }

    internal class EntityWithIndexer
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
        
        // Indexer should be ignored by the mapper
        public string this[int index] => Name;
    }

    internal class EntityWithIgnoredProperty
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
        
        [BsonIgnore]
        public string IgnoredField { get; set; } = "";
    }

    #endregion

    #region Id Resolution Tests

    [Test]
    public async Task ToDocument_EntityWithIdAttribute_ShouldUseIdAttribute()
    {
        var entity = new EntityWithIdAttribute { Key = 100, CustomId = "custom-123", Value = "test" };
        
        var doc = AotBsonMapper.ToDocument(entity);
        
        await Assert.That(doc.ContainsKey("_id")).IsTrue();
        await Assert.That(doc["_id"].ToString()).IsEqualTo("custom-123");
    }

    [Test]
    public async Task ToDocument_EntityWithCustomIdProperty_ShouldFallbackToIdProperty()
    {
        var entity = new EntityWithCustomIdProperty { EntityKey = 42, Name = "Test" };
        
        // Without EntityAttribute, it will look for "Id", "_id", "ID" properties
        // EntityKey won't be used as _id without the attribute
        var doc = AotBsonMapper.ToDocument(entity);
        
        // EntityKey should be serialized as "entityKey" (camelCase)
        await Assert.That(doc.ContainsKey("entityKey")).IsTrue();
        await Assert.That(doc["entityKey"].ToInt32(null)).IsEqualTo(42);
    }

    #endregion

    #region Circular Reference Tests

    [Test]
    public async Task ToDocument_CircularReference_ShouldHandleGracefully()
    {
        var parent = new EntityWithCircularReference { Id = 1, Name = "Parent" };
        var child = new EntityWithCircularReference { Id = 2, Name = "Child", Parent = parent };
        parent.Parent = child; // Create circular reference
        
        // Should not throw and should handle circular reference
        var doc = AotBsonMapper.ToDocument(parent);
        
        await Assert.That(doc).IsNotNull();
        await Assert.That(doc["_id"].ToInt32(null)).IsEqualTo(1);
    }

    #endregion

    #region Value Type Tests

    [Test]
    public async Task ToDocument_ValueType_ShouldSerialize()
    {
        var entity = new ValueTypeEntity { Id = 1, Name = "Struct" };
        
        var doc = AotBsonMapper.ToDocument(entity);
        
        await Assert.That(doc["_id"].ToInt32(null)).IsEqualTo(1);
        await Assert.That(doc["name"].ToString()).IsEqualTo("Struct");
    }

    [Test]
    public async Task FromDocument_ValueType_ShouldDeserialize()
    {
        var doc = new BsonDocument()
            .Set("_id", new BsonInt32(1))
            .Set("name", new BsonString("Struct"));
        
        var entity = AotBsonMapper.FromDocument<ValueTypeEntity>(doc);
        
        await Assert.That(entity.Id).IsEqualTo(1);
        await Assert.That(entity.Name).IsEqualTo("Struct");
    }

    #endregion

    #region Nullable Type Tests

    [Test]
    public async Task ToDocument_NullableTypes_WithValues_ShouldSerialize()
    {
        var entity = new EntityWithNullableTypes
        {
            Id = 1,
            NullableInt = 42,
            NullableDateTime = new DateTime(2025, 1, 1),
            NullableBool = true
        };
        
        var doc = AotBsonMapper.ToDocument(entity);
        
        await Assert.That(doc["nullableInt"].ToInt32(null)).IsEqualTo(42);
        await Assert.That(doc["nullableBool"].ToBoolean(null)).IsTrue();
    }

    [Test]
    public async Task ToDocument_NullableTypes_WithNulls_ShouldSerializeAsNull()
    {
        var entity = new EntityWithNullableTypes
        {
            Id = 1,
            NullableInt = null,
            NullableDateTime = null,
            NullableBool = null
        };
        
        var doc = AotBsonMapper.ToDocument(entity);
        
        await Assert.That(doc["nullableInt"].IsNull).IsTrue();
        await Assert.That(doc["nullableDateTime"].IsNull).IsTrue();
        await Assert.That(doc["nullableBool"].IsNull).IsTrue();
    }

    [Test]
    public async Task FromDocument_NullToNullable_ShouldReturnNull()
    {
        var doc = new BsonDocument()
            .Set("_id", new BsonInt32(1))
            .Set("nullableInt", BsonNull.Value)
            .Set("nullableDateTime", BsonNull.Value)
            .Set("nullableBool", BsonNull.Value);
        
        var entity = AotBsonMapper.FromDocument<EntityWithNullableTypes>(doc);
        
        await Assert.That(entity.NullableInt).IsNull();
        await Assert.That(entity.NullableDateTime).IsNull();
        await Assert.That(entity.NullableBool).IsNull();
    }

    #endregion

    #region Field Tests

    [Test]
    public async Task ToDocument_PublicField_ShouldSerialize()
    {
        var entity = new EntityWithPublicField { Id = 1, FieldValue = "FieldTest" };
        
        var doc = AotBsonMapper.ToDocument(entity);
        
        await Assert.That(doc["fieldValue"].ToString()).IsEqualTo("FieldTest");
    }

    [Test]
    public async Task FromDocument_PublicField_ShouldDeserialize()
    {
        var doc = new BsonDocument()
            .Set("_id", new BsonInt32(1))
            .Set("fieldValue", new BsonString("FieldTest"));
        
        var entity = AotBsonMapper.FromDocument<EntityWithPublicField>(doc);
        
        await Assert.That(entity.FieldValue).IsEqualTo("FieldTest");
    }

    #endregion

    #region Array Tests

    [Test]
    public async Task ToDocument_WithArrays_ShouldSerialize()
    {
        var entity = new EntityWithArray
        {
            Id = 1,
            Numbers = new[] { 1, 2, 3 },
            Strings = new[] { "a", "b", "c" }
        };
        
        var doc = AotBsonMapper.ToDocument(entity);
        
        var numbers = doc["numbers"] as BsonArray;
        await Assert.That(numbers).IsNotNull();
        await Assert.That(numbers!.Count).IsEqualTo(3);
        
        var strings = doc["strings"] as BsonArray;
        await Assert.That(strings).IsNotNull();
        await Assert.That(strings!.Count).IsEqualTo(3);
    }

    [Test]
    public async Task FromDocument_WithArrays_ShouldDeserialize()
    {
        var doc = new BsonDocument()
            .Set("_id", new BsonInt32(1))
            .Set("numbers", new BsonArray().AddValue(1).AddValue(2).AddValue(3))
            .Set("strings", new BsonArray().AddValue("a").AddValue("b").AddValue("c"));
        
        var entity = AotBsonMapper.FromDocument<EntityWithArray>(doc);
        
        await Assert.That(entity.Numbers.Length).IsEqualTo(3);
        await Assert.That(entity.Strings.Length).IsEqualTo(3);
    }

    #endregion

    #region Nested Object Tests

    [Test]
    public async Task ToDocument_WithNestedObject_ShouldSerialize()
    {
        var entity = new EntityWithNestedObject
        {
            Id = 1,
            Nested = new NestedClass { Value = "NestedValue", Number = 42 }
        };
        
        var doc = AotBsonMapper.ToDocument(entity);
        
        var nested = doc["nested"] as BsonDocument;
        await Assert.That(nested).IsNotNull();
        await Assert.That(nested!["value"].ToString()).IsEqualTo("NestedValue");
        await Assert.That(nested["number"].ToInt32(null)).IsEqualTo(42);
    }

    [Test]
    public async Task FromDocument_WithNestedObject_ShouldDeserialize()
    {
        var doc = new BsonDocument()
            .Set("_id", new BsonInt32(1))
            .Set("nested", new BsonDocument()
                .Set("value", new BsonString("NestedValue"))
                .Set("number", new BsonInt32(42)));
        
        var entity = AotBsonMapper.FromDocument<EntityWithNestedObject>(doc);
        
        await Assert.That(entity.Nested.Value).IsEqualTo("NestedValue");
        await Assert.That(entity.Nested.Number).IsEqualTo(42);
    }

    #endregion

    #region Unsigned Type Tests

    [Test]
    public async Task ConvertValue_UnsignedTypes_ShouldConvert()
    {
        await Assert.That(AotBsonMapper.ConvertValue(new BsonInt32(100), typeof(uint))).IsEqualTo(100u);
        await Assert.That(AotBsonMapper.ConvertValue(new BsonInt64(100), typeof(ulong))).IsEqualTo(100ul);
        await Assert.That(AotBsonMapper.ConvertValue(new BsonInt32(100), typeof(ushort))).IsEqualTo((ushort)100);
        await Assert.That(AotBsonMapper.ConvertValue(new BsonInt32(-10), typeof(sbyte))).IsEqualTo((sbyte)-10);
    }

    [Test]
    public async Task ConvertValue_FromString_ToUnsignedTypes_ShouldConvert()
    {
        await Assert.That(AotBsonMapper.ConvertValue(new BsonString("100"), typeof(uint))).IsEqualTo(100u);
        await Assert.That(AotBsonMapper.ConvertValue(new BsonString("100"), typeof(ulong))).IsEqualTo(100ul);
        await Assert.That(AotBsonMapper.ConvertValue(new BsonString("100"), typeof(ushort))).IsEqualTo((ushort)100);
        await Assert.That(AotBsonMapper.ConvertValue(new BsonString("100"), typeof(byte))).IsEqualTo((byte)100);
        await Assert.That(AotBsonMapper.ConvertValue(new BsonString("-10"), typeof(sbyte))).IsEqualTo((sbyte)-10);
    }

    #endregion

    #region Indexer and BsonIgnore Tests

    [Test]
    public async Task ToDocument_WithIndexer_ShouldIgnoreIndexer()
    {
        var entity = new EntityWithIndexer { Id = 1, Name = "Test" };
        
        var doc = AotBsonMapper.ToDocument(entity);
        
        await Assert.That(doc.ContainsKey("_id")).IsTrue();
        await Assert.That(doc.ContainsKey("name")).IsTrue();
        // Indexer should not be serialized
    }

    [Test]
    public async Task ToDocument_WithBsonIgnore_ShouldIgnoreProperty()
    {
        var entity = new EntityWithIgnoredProperty { Id = 1, Name = "Test", IgnoredField = "ShouldBeIgnored" };
        
        var doc = AotBsonMapper.ToDocument(entity);
        
        await Assert.That(doc.ContainsKey("name")).IsTrue();
        await Assert.That(doc.ContainsKey("ignoredField")).IsFalse();
    }

    #endregion

    #region ConvertValue Additional Tests

    [Test]
    public async Task ConvertValue_BsonDecimal128_ToVariousTypes()
    {
        var dec = new BsonDecimal128(new Decimal128(123.45m));
        
        await Assert.That(AotBsonMapper.ConvertValue(dec, typeof(decimal))).IsEqualTo(123.45m);
        await Assert.That(AotBsonMapper.ConvertValue(dec, typeof(double))).IsEqualTo(123.45);
        await Assert.That(AotBsonMapper.ConvertValue(dec, typeof(int))).IsEqualTo(123);
        await Assert.That(AotBsonMapper.ConvertValue(dec, typeof(long))).IsEqualTo(123L);
    }

    [Test]
    public async Task ConvertValue_BsonBoolean_ToInt_ShouldConvert()
    {
        await Assert.That(AotBsonMapper.ConvertValue(new BsonBoolean(true), typeof(bool))).IsEqualTo(true);
        await Assert.That(AotBsonMapper.ConvertValue(new BsonBoolean(false), typeof(bool))).IsEqualTo(false);
    }

    [Test]
    public async Task ConvertValue_BsonInt_ToBool_ShouldConvert()
    {
        await Assert.That(AotBsonMapper.ConvertValue(new BsonInt32(1), typeof(bool))).IsEqualTo(true);
        await Assert.That(AotBsonMapper.ConvertValue(new BsonInt32(0), typeof(bool))).IsEqualTo(false);
        await Assert.That(AotBsonMapper.ConvertValue(new BsonInt64(1), typeof(bool))).IsEqualTo(true);
    }

    [Test]
    public async Task ConvertValue_BsonDouble_ToBool_ShouldConvert()
    {
        await Assert.That(AotBsonMapper.ConvertValue(new BsonDouble(1.0), typeof(bool))).IsEqualTo(true);
        await Assert.That(AotBsonMapper.ConvertValue(new BsonDouble(0.0), typeof(bool))).IsEqualTo(false);
    }

    [Test]
    public async Task ConvertValue_DateTime_FromBsonString_ShouldParse()
    {
        var dateStr = "2025-01-26T12:00:00Z";
        var result = AotBsonMapper.ConvertValue(new BsonString(dateStr), typeof(DateTime));
        
        await Assert.That(result).IsTypeOf<DateTime>();
    }

    [Test]
    public async Task ConvertValue_Guid_FromBsonString_ShouldParse()
    {
        var guid = Guid.NewGuid();
        var result = AotBsonMapper.ConvertValue(new BsonString(guid.ToString()), typeof(Guid));
        
        await Assert.That(result).IsEqualTo(guid);
    }

    [Test]
    public async Task ConvertValue_ByteArray_FromBase64String_ShouldConvert()
    {
        var bytes = new byte[] { 1, 2, 3, 4, 5 };
        var base64 = Convert.ToBase64String(bytes);
        
        var result = AotBsonMapper.ConvertValue(new BsonString(base64), typeof(byte[])) as byte[];
        
        await Assert.That(result).IsNotNull();
        await Assert.That(result!.Length).IsEqualTo(5);
    }

    [Test]
    public async Task ConvertValue_Enum_FromBsonInt64_ShouldConvert()
    {
        var result = AotBsonMapper.ConvertValue(new BsonInt64(2), typeof(DayOfWeek));
        
        await Assert.That(result).IsEqualTo(DayOfWeek.Tuesday);
    }

    #endregion

    #region Object Type Tests

    [Test]
    public async Task ConvertValue_ToObjectType_ShouldUnwrap()
    {
        // Test unwrapping for object target type
        await Assert.That(AotBsonMapper.ConvertValue(new BsonString("test"), typeof(object))).IsEqualTo("test");
        await Assert.That(AotBsonMapper.ConvertValue(new BsonInt32(42), typeof(object))).IsEqualTo(42);
        await Assert.That(AotBsonMapper.ConvertValue(new BsonInt64(123), typeof(object))).IsEqualTo(123L);
        await Assert.That(AotBsonMapper.ConvertValue(new BsonDouble(3.14), typeof(object))).IsEqualTo(3.14);
        await Assert.That(AotBsonMapper.ConvertValue(new BsonBoolean(true), typeof(object))).IsEqualTo(true);
        await Assert.That(AotBsonMapper.ConvertValue(BsonNull.Value, typeof(object))).IsNull();
    }

    #endregion

    #region BsonDocument Direct Tests

    [Test]
    public async Task ToDocument_BsonDocument_ShouldReturnSame()
    {
        var doc = new BsonDocument().Set("key", new BsonString("value"));
        
        var result = AotBsonMapper.ToDocument(doc);
        
        await Assert.That(ReferenceEquals(result, doc)).IsTrue();
    }

    [Test]
    public async Task FromDocument_ToBsonDocument_ShouldReturnSame()
    {
        var doc = new BsonDocument().Set("key", new BsonString("value"));
        
        var result = AotBsonMapper.FromDocument<BsonDocument>(doc);
        
        await Assert.That(ReferenceEquals(result, doc)).IsTrue();
    }

    #endregion

    #region GetId/SetId Tests

    internal class SimpleEntity
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    [Test]
    public async Task GetId_BsonDocument_ShouldReturnId()
    {
        var doc = new BsonDocument().Set("_id", new BsonInt32(42));
        
        var id = AotBsonMapper.GetId(doc);
        
        await Assert.That(id.ToInt32(null)).IsEqualTo(42);
    }

    [Test]
    public async Task GetId_BsonDocumentWithoutId_ShouldReturnNull()
    {
        var doc = new BsonDocument().Set("key", new BsonString("value"));
        
        var id = AotBsonMapper.GetId(doc);
        
        await Assert.That(id.IsNull).IsTrue();
    }

    [Test]
    public async Task SetId_BsonDocument_ShouldNotModify()
    {
        // BsonDocument is immutable, SetId should be a no-op
        var doc = new BsonDocument().Set("_id", new BsonInt32(1));
        
        AotBsonMapper.SetId(doc, new BsonInt32(999));
        
        // Should still be 1 (not modified)
        await Assert.That(doc["_id"].ToInt32(null)).IsEqualTo(1);
    }

    #endregion

    #region Error Handling Tests

    [Test]
    public async Task ToDocument_NullEntity_ShouldThrow()
    {
        SimpleEntity? entity = null;
        
        await Assert.That(() => AotBsonMapper.ToDocument(entity!))
            .Throws<ArgumentNullException>();
    }

    [Test]
    public async Task FromDocument_NullDocument_ShouldThrow()
    {
        BsonDocument? doc = null;
        
        await Assert.That(() => AotBsonMapper.FromDocument<SimpleEntity>(doc!))
            .Throws<ArgumentNullException>();
    }

    [Test]
    public async Task GetId_NullEntity_ShouldThrow()
    {
        SimpleEntity? entity = null;
        
        await Assert.That(() => AotBsonMapper.GetId(entity!))
            .Throws<ArgumentNullException>();
    }

    [Test]
    public async Task SetId_NullEntity_ShouldThrow()
    {
        SimpleEntity? entity = null;
        
        await Assert.That(() => AotBsonMapper.SetId(entity!, new BsonInt32(1)))
            .Throws<ArgumentNullException>();
    }

    [Test]
    public async Task GetPropertyValue_NullEntity_ShouldThrow()
    {
        SimpleEntity? entity = null;
        
        await Assert.That(() => AotBsonMapper.GetPropertyValue(entity!, "Name"))
            .Throws<ArgumentNullException>();
    }

    [Test]
    public async Task GetPropertyValue_NullPropertyName_ShouldThrow()
    {
        var entity = new SimpleEntity { Id = 1, Name = "Test" };
        
        await Assert.That(() => AotBsonMapper.GetPropertyValue(entity, null!))
            .Throws<ArgumentNullException>();
    }

    [Test]
    public async Task ConvertValue_NullBsonValue_ShouldThrow()
    {
        BsonValue? value = null;
        
        await Assert.That(() => AotBsonMapper.ConvertValue(value!, typeof(int)))
            .Throws<ArgumentNullException>();
    }

    [Test]
    public async Task ConvertValue_NullTargetType_ShouldThrow()
    {
        await Assert.That(() => AotBsonMapper.ConvertValue(new BsonInt32(1), null!))
            .Throws<ArgumentNullException>();
    }

    #endregion
}
