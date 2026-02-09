using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using TinyDb.Attributes;
using TinyDb.Bson;
using TinyDb.Serialization;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Serialization;

public class AotBsonMapperFallbackBranchCoverageTests
{
    [Entity]
    internal sealed class MetadataEdgeEntity
    {
        public int Id { get; set; }

        public string Included { get; set; } = "ok";

        [BsonIgnore]
        public string Ignored { get; set; } = "no";

        public static string StaticProp { get; set; } = "static";

        public string WriteOnly { set => _writeOnly = value; }
        private string _writeOnly = "";

        public string this[int index]
        {
            get => index.ToString();
            set { }
        }

        public int X { get; set; }

        public int FieldIncluded;

        public const int ConstField = 123;

        public BsonValue AlreadyBson { get; set; } = new BsonInt32(7);

        public object?[] Items { get; set; } = new object?[] { null, 1 };

        public Dictionary<string, object?> MapWithNull { get; set; } = new()
        {
            ["a"] = 1,
            ["b"] = null
        };
    }

    [Entity]
    internal sealed class EntityWithIdAttribute
    {
        [Id]
        public Guid Key { get; set; }

        public int Id { get; set; }
    }

    [Entity]
    internal sealed class EntityWithoutId
    {
        public string Name { get; set; } = "";
    }

    [Entity]
    internal sealed class CircularNode
    {
        public int Id { get; set; }
        public CircularNode? Next { get; set; }
    }

    [Entity]
    internal sealed class ForeignKeyEntity
    {
        public int Id { get; set; }

        [ForeignKey("other_collection")]
        public BsonDocument? Other { get; set; }

        public string Name { get; set; } = "";
    }

    [Entity]
    internal sealed class HashtableKeyEntity
    {
        public Hashtable Table { get; set; } = new();
    }

    private sealed class QueueEntity
    {
        public Queue<int> Values { get; set; } = new();
    }

    private sealed class ListWrappingCollection<T> : IEnumerable<T>
    {
        private readonly List<T> _items;

        public ListWrappingCollection(List<T> items)
        {
            _items = items;
        }

        public IEnumerator<T> GetEnumerator() => _items.GetEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }

    private sealed class NoPublicCtorCollection<T> : IEnumerable<T>
    {
        private NoPublicCtorCollection()
        {
        }

        public IEnumerator<T> GetEnumerator()
        {
            yield break;
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }

    [Test]
    public async Task ToDocument_ShouldHandle_MetadataFilters_Fields_And_Collections()
    {
        var entity = new MetadataEdgeEntity
        {
            Id = 1,
            Included = "yes",
            Ignored = "ignore",
            X = 5,
            FieldIncluded = 9,
            AlreadyBson = new BsonInt32(123),
            Items = new object?[] { null, 1 },
            MapWithNull = new Dictionary<string, object?> { ["a"] = 1, ["b"] = null }
        };

        var doc = AotBsonMapper.ToDocument(entity);

        await Assert.That(doc.ContainsKey("_id")).IsTrue();
        await Assert.That(doc["_id"].ToInt32(null)).IsEqualTo(1);

        // Id property itself should be skipped (only stored as _id)
        await Assert.That(doc.ContainsKey("id")).IsFalse();

        await Assert.That(doc["included"].ToString()).IsEqualTo("yes");
        await Assert.That(doc.ContainsKey("ignored")).IsFalse();
        await Assert.That(doc.ContainsKey("staticProp")).IsFalse();
        await Assert.That(doc.ContainsKey("writeOnly")).IsFalse();

        // single-letter property name -> lowercased key
        await Assert.That(doc["x"].ToInt32(null)).IsEqualTo(5);

        // public fields are mapped too; literal fields are ignored by metadata.
        await Assert.That(doc["fieldIncluded"].ToInt32(null)).IsEqualTo(9);

        // Value already a BsonValue should pass through unchanged.
        await Assert.That(doc["alreadyBson"].ToInt32(null)).IsEqualTo(123);

        // Collection with null element should contain BsonNull.
        var items = (BsonArray)doc["items"];
        await Assert.That(items.Count).IsEqualTo(2);
        await Assert.That(items[0].IsNull).IsTrue();
        await Assert.That(items[1].ToInt32(null)).IsEqualTo(1);

        // Dictionary with null value should store BsonNull.
        var map = (BsonDocument)doc["mapWithNull"];
        await Assert.That(map["a"].ToInt32(null)).IsEqualTo(1);
        await Assert.That(map["b"].IsNull).IsTrue();
    }

    [Test]
    public async Task GetMetadata_ShouldHonor_EntityAttribute_IdProperty()
    {
        var entity = new EntityWithSpecifiedIdProperty { UserId = 123, Name = "n" };
        var doc = AotBsonMapper.ToDocument(entity);

        await Assert.That(doc.ContainsKey("_id")).IsTrue();
        await Assert.That(doc["_id"].ToInt32(null)).IsEqualTo(123);
    }

    [Test]
    public async Task ToDocument_ShouldPrefer_IdAttribute()
    {
        var key = Guid.NewGuid();
        var entity = new EntityWithIdAttribute { Key = key, Id = 9 };

        var doc = AotBsonMapper.ToDocument(entity);

        await Assert.That(doc.ContainsKey("_id")).IsTrue();

        var idValue = doc["_id"];
        var actualKey = idValue switch
        {
            BsonBinary bin => new Guid(bin.Bytes),
            BsonString str => Guid.Parse(str.Value),
            _ => throw new InvalidOperationException($"Unexpected _id BSON type: {idValue.GetType().Name}")
        };

        await Assert.That(actualKey).IsEqualTo(key);
        await Assert.That(doc.ContainsKey("key")).IsFalse();
        await Assert.That(doc["id"].ToInt32(null)).IsEqualTo(9);
    }

    [Test]
    public async Task ToDocument_ShouldHandle_CircularReference_ByReturning_IdOnlyDocument()
    {
        var node = new CircularNode { Id = 1 };
        node.Next = node;

        var doc = AotBsonMapper.ToDocument(node);
        var next = (BsonDocument)doc["next"];

        await Assert.That(next.ContainsKey("_id")).IsTrue();
        await Assert.That(next["_id"].ToInt32(null)).IsEqualTo(1);
    }

    [Test]
    public async Task ToDocument_ShouldSerialize_ForeignKeyProperty_AsDocument()
    {
        var entity = new ForeignKeyEntity
        {
            Id = 1,
            Name = "n",
            Other = new BsonDocument().Set("_id", 7)
        };

        var doc = AotBsonMapper.ToDocument(entity);

        var other = (BsonDocument)doc["other"];
        await Assert.That(other["_id"].ToInt32(null)).IsEqualTo(7);
        await Assert.That(doc["name"].ToString()).IsEqualTo("n");
    }

    [Test]
    public async Task GetId_ShouldReturnNull_WhenEntityHasNoIdProperty()
    {
        var entity = new EntityWithoutId { Name = "x" };
        await Assert.That(AotBsonMapper.GetId(entity).IsNull).IsTrue();
    }

    [Test]
    public async Task GetId_ShouldUseEntityMetadata_WhenEntityHasIdProperty()
    {
        var entity = new MetadataEdgeEntity { Id = 123 };
        await Assert.That(AotBsonMapper.GetId(entity).ToInt32(null)).IsEqualTo(123);
    }

    [Test]
    public async Task GetId_ShouldHandle_BsonDocument()
    {
        var doc = new BsonDocument().Set("_id", 9);
        await Assert.That(AotBsonMapper.GetId(doc).ToInt32(null)).IsEqualTo(9);
    }

    [Test]
    public async Task GetPropertyValue_ShouldFallback_ToEntityMetadata()
    {
        var entity = new EntityWithoutId { Name = "hello" };
        await Assert.That(AotBsonMapper.GetPropertyValue(entity, nameof(EntityWithoutId.Name))).IsEqualTo("hello");
        await Assert.That(AotBsonMapper.GetPropertyValue(entity, "DoesNotExist")).IsNull();
    }

    [Test]
    public async Task ConvertValue_ShouldThrow_WhenDictionaryOrCollectionBsonTypeIsWrong()
    {
        await Assert.That(() => AotBsonMapper.ConvertValue(new BsonArray(), typeof(Dictionary<string, int>)))
            .Throws<NotSupportedException>();

        var doc = new BsonDocument().Set("a", 1);
        await Assert.That(() => AotBsonMapper.ConvertValue(doc, typeof(List<int>)))
            .Throws<NotSupportedException>();
    }

    [Test]
    public async Task ConvertDictionaryToBsonDocument_ShouldThrow_WhenKeyNotString()
    {
        var entity = new HashtableKeyEntity();
        entity.Table[1] = "value";

        await Assert.That(() => AotBsonMapper.ToDocument(entity))
            .Throws<NotSupportedException>();
    }

    [Test]
    public async Task ConvertValue_DictionaryWithNonStringKeyType_ShouldThrow()
    {
        var doc = new BsonDocument().Set("1", 1);
        await Assert.That(() => AotBsonMapper.ConvertValue(doc, typeof(Dictionary<int, int>)))
            .Throws<NotSupportedException>();
    }

    [Test]
    public async Task ConvertValue_CollectionWithoutPublicAdd_ShouldThrow()
    {
        var arr = new BsonArray(new BsonValue[] { new BsonInt32(1) });
        await Assert.That(() => AotBsonMapper.ConvertValue(arr, typeof(Queue<int>)))
            .Throws<NotSupportedException>();
    }

    [Test]
    public async Task ConvertValue_ShouldWrap_ToTargetCollection_WhenNoDefaultCtor()
    {
        var arr = new BsonArray(new BsonValue[] { new BsonInt32(1), new BsonInt32(2) });
        var wrapped = (ListWrappingCollection<int>)AotBsonMapper.ConvertValue(arr, typeof(ListWrappingCollection<int>))!;
        var list = wrapped.ToList();

        await Assert.That(list.Count).IsEqualTo(2);
        await Assert.That(list[0]).IsEqualTo(1);
        await Assert.That(list[1]).IsEqualTo(2);
    }

    [Test]
    public async Task ConvertValue_ShouldThrow_WhenTargetCollectionCannotBeWrapped()
    {
        var arr = new BsonArray(new BsonValue[] { new BsonInt32(1) });
        await Assert.That(() => AotBsonMapper.ConvertValue(arr, typeof(NoPublicCtorCollection<int>)))
            .Throws<NotSupportedException>();
    }

    [Test]
    public async Task PrivateHelpers_ShouldThrow_ForInvalidInputs()
    {
        var convertCollection = typeof(AotBsonMapper).GetMethod("ConvertCollectionToBsonArray", BindingFlags.NonPublic | BindingFlags.Static);
        await Assert.That(convertCollection).IsNotNull();
        await Assert.That(() => convertCollection!.Invoke(null, new object?[] { new object() }))
            .Throws<TargetInvocationException>();

        var convertDictionary = typeof(AotBsonMapper).GetMethod("ConvertDictionaryToBsonDocument", BindingFlags.NonPublic | BindingFlags.Static);
        await Assert.That(convertDictionary).IsNotNull();
        await Assert.That(() => convertDictionary!.Invoke(null, new object?[] { new object() }))
            .Throws<TargetInvocationException>();
    }
}
