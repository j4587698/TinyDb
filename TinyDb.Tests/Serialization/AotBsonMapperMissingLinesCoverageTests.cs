using System;
using System.Collections;
using System.Collections.Generic;
using System.Reflection;
using TinyDb.Bson;
using TinyDb.Metadata;
using TinyDb.Serialization;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Serialization;

public class AotBsonMapperMissingLinesCoverageTests
{
    private interface IHybridDictionary : System.Collections.IDictionary, IDictionary<string, int>
    {
    }

    private interface IMarker
    {
        int X { get; }
    }

    private struct StructWithPropertyAndField
    {
        public int X { get; set; }
        public int Y;
    }

    private sealed class EnumerableLongCtorOnly
    {
        public EnumerableLongCtorOnly(IEnumerable<long> values)
        {
        }
    }

    private sealed class NonGenericEnumerableInt : IEnumerable<int>
    {
        public IEnumerator<int> GetEnumerator() => ((IEnumerable<int>)Array.Empty<int>()).GetEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }

    private sealed class NonGenericCollectionInt : ICollection<int>
    {
        private readonly List<int> _items = new();

        public int Count => _items.Count;
        public bool IsReadOnly => false;
        public void Add(int item) => _items.Add(item);
        public void Clear() => _items.Clear();
        public bool Contains(int item) => _items.Contains(item);
        public void CopyTo(int[] array, int arrayIndex) => _items.CopyTo(array, arrayIndex);
        public IEnumerator<int> GetEnumerator() => _items.GetEnumerator();
        public bool Remove(int item) => _items.Remove(item);
        IEnumerator IEnumerable.GetEnumerator() => _items.GetEnumerator();
    }

    private sealed class NonGenericListInt : IList<int>
    {
        private readonly List<int> _items = new();

        public int Count => _items.Count;
        public bool IsReadOnly => false;
        public int this[int index] { get => _items[index]; set => _items[index] = value; }
        public void Add(int item) => _items.Add(item);
        public void Clear() => _items.Clear();
        public bool Contains(int item) => _items.Contains(item);
        public void CopyTo(int[] array, int arrayIndex) => _items.CopyTo(array, arrayIndex);
        public IEnumerator<int> GetEnumerator() => _items.GetEnumerator();
        public int IndexOf(int item) => _items.IndexOf(item);
        public void Insert(int index, int item) => _items.Insert(index, item);
        public bool Remove(int item) => _items.Remove(item);
        public void RemoveAt(int index) => _items.RemoveAt(index);
        IEnumerator IEnumerable.GetEnumerator() => _items.GetEnumerator();
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
    public async Task IsComplexObjectType_ShouldReturnFalseForCollections()
    {
        var method = typeof(AotBsonMapper).GetMethod("IsComplexObjectType", BindingFlags.NonPublic | BindingFlags.Static);
        await Assert.That(method).IsNotNull();

        var isComplexList = (bool)method!.Invoke(null, new object[] { typeof(List<int>) })!;
        await Assert.That(isComplexList).IsFalse();

        var isComplexEntity = (bool)method.Invoke(null, new object[] { typeof(MetadataDocument) })!;
        await Assert.That(isComplexEntity).IsTrue();

        var isComplexStruct = (bool)method.Invoke(null, new object[] { typeof(StructWithPropertyAndField) })!;
        await Assert.That(isComplexStruct).IsTrue();

        var isComplexInterface = (bool)method.Invoke(null, new object[] { typeof(IMarker) })!;
        await Assert.That(isComplexInterface).IsFalse();
    }

    [Test]
    public async Task FromDocument_Struct_ShouldUseValueTypeFallbackAndPopulateMembers()
    {
        var doc = new BsonDocument()
            .Set("x", 10)
            .Set("y", 20);

        var value = AotBsonMapper.FromDocument<StructWithPropertyAndField>(doc);

        await Assert.That(value.X).IsEqualTo(10);
        await Assert.That(value.Y).IsEqualTo(20);
    }

    [Test]
    public async Task ConvertValue_DictionaryInterfaceAndCollectionInterface_ShouldUseConcreteTypes()
    {
        var resolveDictionaryTypes = typeof(AotBsonMapper).GetMethod("ResolveDictionaryTypes", BindingFlags.NonPublic | BindingFlags.Static);
        await Assert.That(resolveDictionaryTypes).IsNotNull();

        var tuple = (ValueTuple<Type, Type, Type>)resolveDictionaryTypes!.Invoke(null, new object[] { typeof(IHybridDictionary) })!;
        await Assert.That(tuple.Item1).IsEqualTo(typeof(string));
        await Assert.That(tuple.Item2).IsEqualTo(typeof(int));
        await Assert.That(tuple.Item3).IsEqualTo(typeof(Dictionary<string, int>));

        var bsonArray = new BsonArray(new BsonValue[] { new BsonInt32(1), new BsonInt32(2) });
        var coll = (ICollection<int>)AotBsonMapper.ConvertValue(bsonArray, typeof(ICollection<int>))!;
        await Assert.That(coll.Count).IsEqualTo(2);
    }

    [Test]
    public async Task ReflectionMetadata_PropertyMapGetter_ShouldBeCovered()
    {
        var propertyMap = AotBsonMapper.GetPropertyMapForTests(typeof(MetadataDocument));

        await Assert.That(propertyMap.ContainsKey(nameof(MetadataDocument.TypeName))).IsTrue();
    }

    [Test]
    public async Task UnwrapBsonValue_BsonNull_ShouldReturnNull()
    {
        var unwrap = typeof(AotBsonMapper).GetMethod("UnwrapBsonValue", BindingFlags.NonPublic | BindingFlags.Static);
        await Assert.That(unwrap).IsNotNull();

        await Assert.That(unwrap!.Invoke(null, new object[] { BsonNull.Value })).IsNull();
    }

    [Test]
    public async Task ResolveCollectionElementType_Array_ShouldReturnElementType()
    {
        var method = typeof(AotBsonMapper).GetMethod("ResolveCollectionElementType", BindingFlags.NonPublic | BindingFlags.Static);
        await Assert.That(method).IsNotNull();

        var elementType = (Type)method!.Invoke(null, new object[] { typeof(int[]) })!;
        await Assert.That(elementType).IsEqualTo(typeof(int));
    }

    [Test]
    public async Task ResolveCollectionElementType_NonGenericInterfaces_ShouldInferElementType()
    {
        var method = typeof(AotBsonMapper).GetMethod("ResolveCollectionElementType", BindingFlags.NonPublic | BindingFlags.Static);
        await Assert.That(method).IsNotNull();

        var enumerableElement = (Type)method!.Invoke(null, new object[] { typeof(NonGenericEnumerableInt) })!;
        await Assert.That(enumerableElement).IsEqualTo(typeof(int));

        var collectionElement = (Type)method.Invoke(null, new object[] { typeof(NonGenericCollectionInt) })!;
        await Assert.That(collectionElement).IsEqualTo(typeof(int));

        var listElement = (Type)method.Invoke(null, new object[] { typeof(NonGenericListInt) })!;
        await Assert.That(listElement).IsEqualTo(typeof(int));

        var fallbackElement = (Type)method.Invoke(null, new object[] { typeof(ArrayList) })!;
        await Assert.That(fallbackElement).IsEqualTo(typeof(object));
    }

    [Test]
    public async Task TryWrapWithTargetCollection_NoMatchingCtor_ShouldReturnNull()
    {
        var method = typeof(AotBsonMapper).GetMethod("TryWrapWithTargetCollection", BindingFlags.NonPublic | BindingFlags.Static);
        await Assert.That(method).IsNotNull();

        var wrapped = method!.Invoke(null, new object[] { typeof(EnumerableLongCtorOnly), new List<int> { 1, 2 } });
        await Assert.That(wrapped).IsNull();
    }

    [Test]
    public async Task FallbackHelpers_NullArgs_ShouldThrow()
    {
        var fallbackToDocument = typeof(AotBsonMapper).GetMethod("FallbackToDocument", BindingFlags.NonPublic | BindingFlags.Static);
        await Assert.That(fallbackToDocument).IsNotNull();

        await Assert.That(() => fallbackToDocument!.Invoke(null, new object?[] { null, new object() }))
            .Throws<TargetInvocationException>();
        await Assert.That(() => fallbackToDocument!.Invoke(null, new object?[] { typeof(object), null }))
            .Throws<TargetInvocationException>();

        var fallbackFromDocument = typeof(AotBsonMapper).GetMethod("FallbackFromDocument", BindingFlags.NonPublic | BindingFlags.Static);
        await Assert.That(fallbackFromDocument).IsNotNull();

        await Assert.That(() => fallbackFromDocument!.Invoke(null, new object?[] { null, new BsonDocument() }))
            .Throws<TargetInvocationException>();
        await Assert.That(() => fallbackFromDocument!.Invoke(null, new object?[] { typeof(MetadataDocument), null }))
            .Throws<TargetInvocationException>();
    }

    [Test]
    public async Task FallbackToDocument_ShouldInitializeAndReuseSerializingSet()
    {
        var serializingField = typeof(AotBsonMapper).GetField("_serializingObjects", BindingFlags.NonPublic | BindingFlags.Static);
        await Assert.That(serializingField).IsNotNull();
        serializingField!.SetValue(null, null);

        var fallbackToDocument = typeof(AotBsonMapper).GetMethod("FallbackToDocument", BindingFlags.NonPublic | BindingFlags.Static);
        await Assert.That(fallbackToDocument).IsNotNull();

        var entityType = typeof(MetadataDocument);
        var entity = new MetadataDocument { TypeName = "T", CollectionName = "C" };

        var first = (BsonDocument)fallbackToDocument!.Invoke(null, new object[] { entityType, entity })!;
        await Assert.That(first).IsNotNull();

        var second = (BsonDocument)fallbackToDocument.Invoke(null, new object[] { entityType, entity })!;
        await Assert.That(second).IsNotNull();
    }

    [Test]
    public async Task PrivateHelpers_NullAndEdgeInputs_ShouldCoverBranches()
    {
        var toCamelCase = typeof(AotBsonMapper).GetMethod("ToCamelCase", BindingFlags.NonPublic | BindingFlags.Static);
        await Assert.That(toCamelCase).IsNotNull();
        await Assert.That(toCamelCase!.Invoke(null, new object[] { "" })).IsEqualTo("");

        var convertCollectionToBsonArray = typeof(AotBsonMapper).GetMethod("ConvertCollectionToBsonArray", BindingFlags.NonPublic | BindingFlags.Static);
        await Assert.That(convertCollectionToBsonArray).IsNotNull();
        await Assert.That(() => convertCollectionToBsonArray!.Invoke(null, new object?[] { null }))
            .Throws<TargetInvocationException>();

        var isDictionaryType = typeof(AotBsonMapper).GetMethod("IsDictionaryType", BindingFlags.NonPublic | BindingFlags.Static);
        await Assert.That(isDictionaryType).IsNotNull();
        await Assert.That((bool)isDictionaryType!.Invoke(null, new object?[] { null })!).IsFalse();

        var convertDictionary = typeof(AotBsonMapper).GetMethod("ConvertDictionary", BindingFlags.NonPublic | BindingFlags.Static);
        await Assert.That(convertDictionary).IsNotNull();
        await Assert.That(convertDictionary!.Invoke(null, new object?[] { typeof(Dictionary<string, int>), null })).IsNull();

        var convertCollection = typeof(AotBsonMapper).GetMethod("ConvertCollection", BindingFlags.NonPublic | BindingFlags.Static);
        await Assert.That(convertCollection).IsNotNull();
        await Assert.That(convertCollection!.Invoke(null, new object?[] { typeof(List<int>), null })).IsNull();

        var convertFromBsonValue = typeof(AotBsonMapper).GetMethod("ConvertFromBsonValue", BindingFlags.NonPublic | BindingFlags.Static);
        await Assert.That(convertFromBsonValue).IsNotNull();
        await Assert.That(() => convertFromBsonValue!.Invoke(null, new object?[] { new BsonInt32(1), null }))
            .Throws<TargetInvocationException>();
    }

    [Test]
    public async Task ConvertDictionaryToBsonDocument_NullArgument_ShouldThrow()
    {
        var method = typeof(AotBsonMapper).GetMethod("ConvertDictionaryToBsonDocument", BindingFlags.NonPublic | BindingFlags.Static);
        await Assert.That(method).IsNotNull();

        await Assert.That(() => method!.Invoke(null, new object?[] { null }))
            .Throws<TargetInvocationException>();
    }
}
