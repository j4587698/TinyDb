using System;
using System.Collections.Generic;
using System.Collections;
using System.Reflection;
using TinyDb.Bson;
using TinyDb.Metadata;
using TinyDb.Serialization;
using TinyDb.Attributes;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Serialization;

public class AotBsonMapperMissingLinesCoverageTests
{
    private static object? InvokePrivateStatic(string methodName, params object?[] args)
    {
        var method = typeof(AotBsonMapper)
            .GetMethod(methodName, BindingFlags.NonPublic | BindingFlags.Static);

        if (method == null)
        {
            throw new InvalidOperationException($"Method '{methodName}' not found on {nameof(AotBsonMapper)}.");
        }

        try
        {
            return method.Invoke(null, args);
        }
        catch (TargetInvocationException tie) when (tie.InnerException != null)
        {
            throw tie.InnerException;
        }
    }

    private static T InvokePrivateStatic<T>(string methodName, params object?[] args)
    {
        return (T)InvokePrivateStatic(methodName, args)!;
    }

    private struct StructWithPropertyAndField
    {
        public int X { get; set; }
        public int Y { get; set; }
    }

    private sealed class EntityWithoutIdProperty
    {
        public string Name { get; init; } = string.Empty;
    }

    private sealed class EntityWithProperties
    {
        public string Name { get; init; } = string.Empty;
    }

    private sealed class EntityWithNullableId
    {
        public int? Id { get; set; }
    }

    private abstract class AbstractStringIntDictionary : Dictionary<string, int>
    {
    }

    [Entity]
    internal sealed class EntityWithGeneratedAdapter
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    private sealed class StringListWrapper : IEnumerable<string?>
    {
        public StringListWrapper(List<string?> items) => Items = items;

        public List<string?> Items { get; }

        public IEnumerator<string?> GetEnumerator() => Items.GetEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }

    private sealed class ObjectListWrapper : IEnumerable<object?>
    {
        public ObjectListWrapper(List<object?> items) => Items = items;

        public List<object?> Items { get; }

        public IEnumerator<object?> GetEnumerator() => Items.GetEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }

    private enum NumericEnum
    {
        None = 0,
        A = 1,
        B = 2
    }

    private enum BoolNamedEnum
    {
        False = 0,
        True = 1
    }

    [Test]
    public async Task ConvertValue_Object_ShouldUnwrapKnownBsonTypes()
    {
        var dt = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var oid = ObjectId.NewObjectId();

        await Assert.That(AotBsonMapper.ConvertValue(BsonNull.Value, typeof(object))).IsNull();
        await Assert.That(AotBsonMapper.ConvertValue(new BsonDecimal128(12.34m), typeof(object))).IsNotNull();
        await Assert.That(AotBsonMapper.ConvertValue(new BsonDateTime(dt), typeof(object))).IsEqualTo(dt);
        await Assert.That(AotBsonMapper.ConvertValue(new BsonObjectId(oid), typeof(object))).IsEqualTo(oid);

        var arr = new BsonArray(new BsonValue[] { new BsonInt32(1) });
        var fallback = AotBsonMapper.ConvertValue(arr, typeof(object));
        await Assert.That(ReferenceEquals(fallback, arr)).IsTrue();
    }

    [Test]
    public async Task ConvertValue_TargetBsonValue_ShouldReturnOriginalInstance()
    {
        var value = new BsonInt32(123);
        var converted = AotBsonMapper.ConvertValue(value, typeof(BsonValue));

        await Assert.That(ReferenceEquals(converted, value)).IsTrue();
    }

    [Test]
    public async Task ConvertValue_ComplexObject_ShouldUseRegisteredAdapter()
    {
        var entity = new MetadataDocument
        {
            Id = ObjectId.NewObjectId(),
            TypeName = "T",
            CollectionName = "C",
            DisplayName = "D",
            Description = "Desc",
            PropertiesJson = "[]",
            CreatedAt = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc),
            UpdatedAt = new DateTime(2024, 1, 2, 0, 0, 0, DateTimeKind.Utc)
        };

        var doc = AotBsonMapper.ToDocument(entity);
        var converted = (MetadataDocument)AotBsonMapper.ConvertValue(doc, typeof(MetadataDocument))!;

        await Assert.That(converted.TypeName).IsEqualTo("T");
        await Assert.That(converted.CollectionName).IsEqualTo("C");
    }

    [Test]
    public async Task ConvertValue_DictionaryAndCollection_WithRegisteredAdapters_ShouldHitAdapterBranches()
    {
        var bsonDict = new BsonDocument().Set("a", 1).Set("b", 2);
        var dictObj = (Dictionary<string, int>)AotBsonMapper.ConvertValue(bsonDict, typeof(Dictionary<string, int>))!;
        await Assert.That(dictObj["a"]).IsEqualTo(1);

        var bsonArray = new BsonArray(new BsonValue[] { new BsonInt32(1) });
        var listObj = (List<int>)AotBsonMapper.ConvertValue(bsonArray, typeof(List<int>))!;
        await Assert.That(listObj.Count).IsEqualTo(1);
        await Assert.That(listObj[0]).IsEqualTo(1);
    }

    [Test]
    public async Task ConvertValue_InterfaceTargets_ShouldThrow()
    {
        var bsonDict = new BsonDocument().Set("a", 1);
        await Assert.That(() => AotBsonMapper.ConvertValue(bsonDict, typeof(IDictionary<string, int>)))
            .Throws<NotSupportedException>();

        var bsonArray = new BsonArray(new BsonValue[] { new BsonInt32(1), new BsonInt32(2) });
        await Assert.That(() => AotBsonMapper.ConvertValue(bsonArray, typeof(ICollection<int>)))
            .Throws<NotSupportedException>();
    }

    [Test]
    public async Task FromDocument_UnregisteredType_ShouldThrow()
    {
        var doc = new BsonDocument()
            .Set("x", 10)
            .Set("y", 20);

        await Assert.That(() => AotBsonMapper.FromDocument<StructWithPropertyAndField>(doc))
            .Throws<InvalidOperationException>();
    }

    [Test]
    public async Task ReflectionMetadata_PropertyMapGetter_ShouldBeCovered()
    {
        await Assert.That(EntityMetadata<MetadataDocument>.TryGetProperty(nameof(MetadataDocument.TypeName), out _))
            .IsTrue();
    }

    [Test]
    public async Task GetId_ForEntityWithoutIdProperty_ShouldReturnBsonNull()
    {
        var entity = new EntityWithoutIdProperty { Name = "n" };
        var id = AotBsonMapper.GetId(entity);

        await Assert.That(id).IsEqualTo(BsonNull.Value);
    }

    [Test]
    public async Task GetId_ForBsonDocument_ShouldReadIdOrReturnBsonNull()
    {
        var withoutId = new BsonDocument();
        var id1 = AotBsonMapper.GetId(withoutId);
        await Assert.That(id1).IsEqualTo(BsonNull.Value);

        var withId = new BsonDocument().Set("_id", 123);
        var id2 = AotBsonMapper.GetId(withId);
        await Assert.That(id2).IsEqualTo(new BsonInt32(123));
    }

    [Test]
    public async Task GetId_ForEntityWithNullableId_ShouldReturnNullOrValue()
    {
        var entity1 = new EntityWithNullableId { Id = null };
        await Assert.That(AotBsonMapper.GetId(entity1)).IsEqualTo(BsonNull.Value);

        var entity2 = new EntityWithNullableId { Id = 123 };
        await Assert.That(AotBsonMapper.GetId(entity2)).IsEqualTo(new BsonInt32(123));
    }

    [Test]
    public async Task SetId_ForBsonDocument_ShouldNoOp()
    {
        var doc = new BsonDocument();
        await Assert.That(() => AotBsonMapper.SetId(doc, new BsonInt32(1))).ThrowsNothing();
    }

    [Test]
    public async Task GetPropertyValue_ShouldReturnValueWhenFound_AndNullWhenMissing()
    {
        var entity = new EntityWithProperties { Name = "abc" };

        await Assert.That(AotBsonMapper.GetPropertyValue(entity, nameof(EntityWithProperties.Name))).IsEqualTo("abc");
        await Assert.That(AotBsonMapper.GetPropertyValue(entity, "Missing")).IsNull();
    }

    [Test]
    public async Task ConvertEnumValue_ShouldCoverAllBranches()
    {
        await Assert.That(() => AotBsonMapper.ConvertEnumValue<NumericEnum>(null!)).Throws<ArgumentNullException>();

        await Assert.That(AotBsonMapper.ConvertEnumValue<NumericEnum>(BsonNull.Value)).IsEqualTo(NumericEnum.None);
        await Assert.That(AotBsonMapper.ConvertEnumValue<NumericEnum>(new BsonString("B"))).IsEqualTo(NumericEnum.B);
        await Assert.That(AotBsonMapper.ConvertEnumValue<NumericEnum>(new BsonInt32(1))).IsEqualTo(NumericEnum.A);
        await Assert.That(AotBsonMapper.ConvertEnumValue<NumericEnum>(new BsonInt64(2))).IsEqualTo(NumericEnum.B);
        await Assert.That(AotBsonMapper.ConvertEnumValue<NumericEnum>(new BsonDouble(2.0))).IsEqualTo(NumericEnum.B);
        await Assert.That(AotBsonMapper.ConvertEnumValue<NumericEnum>(new BsonDecimal128(2m))).IsEqualTo(NumericEnum.B);

        await Assert.That(AotBsonMapper.ConvertEnumValue<BoolNamedEnum>(new BsonBoolean(true))).IsEqualTo(BoolNamedEnum.True);
    }

    [Test]
    public async Task ConvertPrimitiveValue_EnumBranches_ShouldBeCovered()
    {
        await Assert.That(AotBsonMapper.ConvertValue(new BsonDecimal128(2m), typeof(NumericEnum))).IsEqualTo(NumericEnum.B);

        await Assert.That(() => AotBsonMapper.ConvertValue(new BsonString("B"), typeof(NumericEnum)))
            .Throws<InvalidOperationException>();

        await Assert.That(() => AotBsonMapper.ConvertValue(new BsonBoolean(true), typeof(NumericEnum)))
            .Throws<InvalidOperationException>();
    }

    [Test]
    public async Task AotFallback_CollectionsAndDictionaries_ShouldCoverMissingBranches()
    {
        var array = new BsonArray(new BsonValue[] { new BsonString("a"), BsonNull.Value });
        var strings = (string[])AotBsonMapper.ConvertValue(array, typeof(string[]))!;
        await Assert.That(strings.Length).IsEqualTo(2);
        await Assert.That(strings[0]).IsEqualTo("a");
        await Assert.That(strings[1]).IsNull();

        await Assert.That(() => AotBsonMapper.ConvertValue(array, typeof(bool[]))).Throws<NotSupportedException>();

        var list = (List<string>)AotBsonMapper.ConvertValue(array, typeof(List<string>))!;
        await Assert.That(list.Count).IsEqualTo(2);
        await Assert.That(list[0]).IsEqualTo("a");
        await Assert.That(list[1]).IsNull();

        await Assert.That(() => AotBsonMapper.ConvertValue(array, typeof(List<DateTime>))).Throws<NotSupportedException>();

        var wrapper1 = (StringListWrapper)AotBsonMapper.ConvertValue(array, typeof(StringListWrapper))!;
        await Assert.That(wrapper1.Items.Count).IsEqualTo(2);

        var objArray = new BsonArray(new BsonValue[] { new BsonInt32(1), new BsonString("x") });
        var wrapper2 = (ObjectListWrapper)AotBsonMapper.ConvertValue(objArray, typeof(ObjectListWrapper))!;
        await Assert.That(wrapper2.Items.Count).IsEqualTo(2);

        var doc = new BsonDocument().Set("a", 1).Set("b", "x").Set("c", BsonNull.Value);

        var intDict = (Dictionary<string, int>)AotBsonMapper.ConvertValue(new BsonDocument().Set("a", 1), typeof(Dictionary<string, int>))!;
        await Assert.That(intDict["a"]).IsEqualTo(1);

        var stringDict = (Dictionary<string, string?>)AotBsonMapper.ConvertValue(new BsonDocument().Set("a", "x").Set("b", BsonNull.Value), typeof(Dictionary<string, string?>))!;
        await Assert.That(stringDict["a"]).IsEqualTo("x");
        await Assert.That(stringDict["b"]).IsNull();

        var objectDict = (Dictionary<string, object?>)AotBsonMapper.ConvertValue(doc, typeof(Dictionary<string, object?>))!;
        await Assert.That(objectDict["a"]).IsEqualTo(1);
        await Assert.That(objectDict["b"]).IsEqualTo("x");
        await Assert.That(objectDict["c"]).IsNull();

        await Assert.That(() => AotBsonMapper.ConvertValue(doc, typeof(Dictionary<string, DateTime>))).Throws<NotSupportedException>();
    }

    [Test]
    public async Task AotBsonMapper_PrivateHelpers_ShouldCoverRemainingBranches()
    {
        await Assert.That(InvokePrivateStatic<string>("ToCamelCase", string.Empty)).IsEqualTo(string.Empty);
        await Assert.That(InvokePrivateStatic<string>("ToCamelCase", "A")).IsEqualTo("a");
        await Assert.That(InvokePrivateStatic<string>("ToCamelCase", "Abc")).IsEqualTo("abc");

        await Assert.That(InvokePrivateStatic<object?>("UnwrapBsonValue", BsonNull.Value)).IsNull();

        await Assert.That(InvokePrivateStatic<bool>("IsComplexObjectType", typeof(List<int>))).IsFalse();
        await Assert.That(InvokePrivateStatic<bool>("IsComplexObjectType", typeof(EntityWithProperties))).IsTrue();
        await Assert.That(InvokePrivateStatic<bool>("IsComplexObjectType", typeof(StructWithPropertyAndField))).IsTrue();
        await Assert.That(InvokePrivateStatic<bool>("IsComplexObjectType", typeof(IDisposable))).IsFalse();

        // ConvertToBsonValue: null, passthrough, primitives, collections, dictionaries, complex objects
        await Assert.That(InvokePrivateStatic<BsonValue>("ConvertToBsonValue", (object?)null)).IsEqualTo(BsonNull.Value);

        var passthrough = new BsonInt32(7);
        var converted = InvokePrivateStatic<BsonValue>("ConvertToBsonValue", passthrough);
        await Assert.That(ReferenceEquals(converted, passthrough)).IsTrue();

        await Assert.That(InvokePrivateStatic<BsonValue>("ConvertToBsonValue", 123)).IsTypeOf<BsonInt32>();

        var array = (BsonArray)InvokePrivateStatic<BsonValue>("ConvertToBsonValue", (object)new object?[] { null, 1, "x" });
        await Assert.That(array.Count).IsEqualTo(3);

        var bsonDoc = (BsonDocument)InvokePrivateStatic<BsonValue>("ConvertToBsonValue", new Dictionary<string, object?> { ["a"] = 1, ["b"] = null });
        await Assert.That(bsonDoc.ContainsKey("a")).IsTrue();

        await Assert.That(() => InvokePrivateStatic<BsonValue>("ConvertToBsonValue", new Dictionary<int, int> { [1] = 2 }))
            .Throws<NotSupportedException>();

        // Complex object path: adapter success + adapter missing throw
        var entity = new EntityWithGeneratedAdapter { Id = 1, Name = "n" };
        _ = AotBsonMapper.ToDocument(entity);
        await Assert.That(InvokePrivateStatic<BsonValue>("ConvertToBsonValue", entity)).IsTypeOf<BsonDocument>();

        await Assert.That(() => InvokePrivateStatic<BsonValue>("ConvertToBsonValue", new EntityWithoutIdProperty()))
            .Throws<InvalidOperationException>();

        // ConvertCollectionToBsonArray / ConvertDictionaryToBsonDocument argument validation
        await Assert.That(() => InvokePrivateStatic<BsonArray>("ConvertCollectionToBsonArray", new object())).Throws<ArgumentException>();
        await Assert.That(() => InvokePrivateStatic<BsonDocument>("ConvertDictionaryToBsonDocument", new object())).Throws<ArgumentException>();
        await Assert.That(() => InvokePrivateStatic<BsonArray>("ConvertCollectionToBsonArray", (object?)null)).Throws<ArgumentNullException>();
        await Assert.That(() => InvokePrivateStatic<BsonDocument>("ConvertDictionaryToBsonDocument", (object?)null)).Throws<ArgumentNullException>();

        await Assert.That(InvokePrivateStatic<bool>("IsDictionaryType", (object?)null)).IsFalse();

        // ConvertDictionary: interface/abstract + invalid concrete type that isn't IDictionary
        var emptyDoc = new BsonDocument();
        await Assert.That(() => InvokePrivateStatic("ConvertDictionary", typeof(IDictionary), emptyDoc)).Throws<NotSupportedException>();
        await Assert.That(() => InvokePrivateStatic("ConvertDictionary", typeof(AbstractStringIntDictionary), emptyDoc)).Throws<NotSupportedException>();
        await Assert.That(() => InvokePrivateStatic("ConvertDictionary", typeof(KeyValuePair<string, int>), emptyDoc)).Throws<NotSupportedException>();
        await Assert.That(InvokePrivateStatic("ConvertDictionary", typeof(Dictionary<string, int>), (object?)null)).IsNull();

        // ConvertFromBsonValue argument validation
        await Assert.That(() => InvokePrivateStatic<object?>("ConvertFromBsonValue", BsonNull.Value, null!)).Throws<ArgumentNullException>();

        // TryWrapWithTargetCollection argument validation
        await Assert.That(() => InvokePrivateStatic("TryWrapWithTargetCollection", null!, new object())).Throws<ArgumentNullException>();
        await Assert.That(() => InvokePrivateStatic("TryWrapWithTargetCollection", typeof(List<int>), null!)).Throws<ArgumentNullException>();

        // TryCreateCollectionFromListCtor argument validation
        await Assert.That(() => InvokePrivateStatic<object?>("TryCreateCollectionFromListCtor", null!, new BsonArray())).Throws<ArgumentNullException>();
        await Assert.That(InvokePrivateStatic<object?>("TryCreateCollectionFromListCtor", typeof(List<int>), null!)).IsNull();

        // ConvertCollection argument validation
        await Assert.That(InvokePrivateStatic<object?>("ConvertCollection", typeof(int[]), null!)).IsNull();
    }
}
