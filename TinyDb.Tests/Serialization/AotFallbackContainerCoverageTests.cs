using System.Collections;
using System.Collections.Concurrent;
using System.Collections.ObjectModel;
using TinyDb.Attributes;
using TinyDb.Bson;
using TinyDb.Serialization;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Serialization;

public class AotFallbackContainerCoverageTests
{
    [Test]
    public async Task NestedContainers_ShouldConvert_ThroughFallback()
    {
        var nestedArray = new BsonArray(new BsonValue[]
        {
            new BsonArray(new BsonValue[] { new BsonInt32(1), new BsonInt32(2) }),
            new BsonArray(new BsonValue[] { new BsonInt32(3) })
        });

        var aotNested = (List<List<int>>)AotBsonMapper.ConvertValue(nestedArray, typeof(List<List<int>>))!;
        await Assert.That(aotNested[0][1]).IsEqualTo(2);
        await Assert.That(aotNested[1][0]).IsEqualTo(3);

        var convertedNested = (List<List<int>>)BsonConversion.FromBsonValue(nestedArray, typeof(List<List<int>>))!;
        await Assert.That(convertedNested[0][1]).IsEqualTo(2);

        var nestedDocument = new BsonDocument().Set("a", new BsonArray(new BsonValue[] { 1, 2 }));
        var aotDictionary = (Dictionary<string, List<int>>)AotBsonMapper.ConvertValue(
            nestedDocument, typeof(Dictionary<string, List<int>>))!;
        await Assert.That(aotDictionary["a"].Count).IsEqualTo(2);
        await Assert.That(aotDictionary["a"][1]).IsEqualTo(2);

        var convertedDictionary = (Dictionary<string, List<int>>)BsonConversion.FromBsonValue(
            nestedDocument, typeof(Dictionary<string, List<int>>))!;
        await Assert.That(convertedDictionary["a"][0]).IsEqualTo(1);
    }

    [Test]
    public async Task HashSet_ShouldConvert_ThroughFallback()
    {
        var array = new BsonArray(new BsonValue[] { new BsonInt32(1), new BsonInt32(2) });

        var genericSet = BsonConversion.FromBsonValue<HashSet<int>>(array)!;
        await Assert.That(genericSet.Count).IsEqualTo(2);
        await Assert.That(genericSet.Contains(1)).IsTrue();
        await Assert.That(genericSet.Contains(2)).IsTrue();

        var aotSet = (HashSet<int>)AotBsonMapper.ConvertValue(array, typeof(HashSet<int>))!;
        await Assert.That(aotSet.Count).IsEqualTo(2);
        await Assert.That(aotSet.Contains(2)).IsTrue();

        var untypedSet = (HashSet<int>)BsonConversion.FromBsonValue(array, typeof(HashSet<int>))!;
        await Assert.That(untypedSet.Contains(1)).IsTrue();
    }

    [Test]
    public async Task BsonValueElements_ShouldConvert_ThroughFallback()
    {
        var array = new BsonArray(new BsonValue[]
        {
            new BsonInt32(1),
            new BsonDocument().Set("a", 2)
        });

        var aotValues = (List<BsonValue>)AotBsonMapper.ConvertValue(array, typeof(List<BsonValue>))!;
        await Assert.That(aotValues[0]).IsTypeOf<BsonInt32>();
        await Assert.That(aotValues[1]).IsTypeOf<BsonDocument>();

        var convertedValues = (List<BsonValue>)BsonConversion.FromBsonValue(array, typeof(List<BsonValue>))!;
        await Assert.That(convertedValues[1]).IsTypeOf<BsonDocument>();
    }

    [Test]
    public async Task BsonValue_ToType_NullableScalars_ShouldReturnUnderlyingValue()
    {
        var intValue = new BsonInt32(5).ToType(typeof(int?), null);
        await Assert.That(intValue).IsTypeOf<int>();
        await Assert.That((int)intValue!).IsEqualTo(5);

        var dateValue = new BsonDateTime(DateTime.UnixEpoch).ToType(typeof(DateTime?), null);
        await Assert.That(dateValue).IsTypeOf<DateTime>();
        await Assert.That((DateTime)dateValue!).IsEqualTo(DateTime.UnixEpoch);

        var decimalValue = new BsonDecimal128(1.5m).ToType(typeof(decimal?), null);
        await Assert.That(decimalValue).IsTypeOf<decimal>();

        var arrayCount = new BsonArray(new BsonValue[] { 1, 2 }).ToType(typeof(int?), null);
        await Assert.That((int)arrayCount!).IsEqualTo(2);
    }

    [Test]
    public async Task GeneratedEntity_NestedList_ShouldRoundTrip()
    {
        var entity = new NestedListEntity { Id = 1, Matrix = [[1, 2], [3]] };

        var document = AotBsonMapper.ToDocument(entity);
        var restored = AotBsonMapper.FromDocument<NestedListEntity>(document);

        await Assert.That(restored.Matrix[0][1]).IsEqualTo(2);
        await Assert.That(restored.Matrix[1][0]).IsEqualTo(3);
    }

    [Test]
    public async Task GeneratedEntity_NestedHashSet_ShouldRoundTrip()
    {
        var entity = new NestedHashSetEntity
        {
            Id = 1,
            Sets = [new HashSet<int> { 1, 2 }, new HashSet<int> { 3 }],
            Map = new Dictionary<string, HashSet<int>> { ["a"] = new HashSet<int> { 4 } }
        };

        var document = AotBsonMapper.ToDocument(entity);
        var restored = AotBsonMapper.FromDocument<NestedHashSetEntity>(document);

        await Assert.That(restored.Sets[0].Count).IsEqualTo(2);
        await Assert.That(restored.Sets[0].Contains(2)).IsTrue();
        await Assert.That(restored.Map["a"].Contains(4)).IsTrue();
    }

    [Test]
    public async Task GeneratedEntity_ConcreteCollections_ShouldRoundTrip()
    {
        var entity = new ConcreteCollectionEntity
        {
            Id = 1,
            Queue = new Queue<int>([1, 2, 3]),
            Stack = new Stack<int>([1, 2, 3]),
            Concurrent = new ConcurrentStack<int>([1, 2, 3]),
            Observable = new ObservableCollection<int> { 4, 5 },
            Linked = new LinkedList<int>([6, 7]),
            ReadOnlyList = [8, 9],
            InterfaceSet = new HashSet<int> { 10, 11 },
            ReadOnlySet = new HashSet<int> { 12, 13 }
        };

        var document = AotBsonMapper.ToDocument(entity);
        var restored = AotBsonMapper.FromDocument<ConcreteCollectionEntity>(document);

        await AssertSequenceAsync(restored.Queue, 1, 2, 3);
        await AssertSequenceAsync(restored.Stack, 3, 2, 1);
        await AssertSequenceAsync(restored.Concurrent, 3, 2, 1);
        await AssertSequenceAsync(restored.Observable, 4, 5);
        await AssertSequenceAsync(restored.Linked, 6, 7);
        await Assert.That(restored.ReadOnlyList).IsTypeOf<List<int>>();
        await AssertSequenceAsync(restored.ReadOnlyList, 8, 9);
        await Assert.That(restored.InterfaceSet).IsTypeOf<HashSet<int>>();
        await Assert.That(restored.InterfaceSet.SetEquals([10, 11])).IsTrue();
        await Assert.That(restored.ReadOnlySet.SetEquals([12, 13])).IsTrue();
    }

    [Test]
    public async Task GeneratedDependentType_QueueProperty_ShouldRoundTrip()
    {
        var entity = new DependentCollectionEntity
        {
            Id = 1,
            Holder = new DependentCollectionHolder { Queue = new Queue<string>(["a", "b"]) }
        };

        var document = AotBsonMapper.ToDocument(entity);
        var restored = AotBsonMapper.FromDocument<DependentCollectionEntity>(document);

        await AssertSequenceAsync(restored.Holder.Queue, "a", "b");
    }

    [Test]
    public async Task RuntimeFallback_ConcreteCollections_ShouldConvert()
    {
        var array = new BsonArray(new BsonValue[] { new BsonInt32(1), new BsonInt32(2), new BsonInt32(3) });

        var aotQueue = (Queue<int>)AotBsonMapper.ConvertValue(array, typeof(Queue<int>))!;
        await AssertSequenceAsync(aotQueue, 1, 2, 3);

        var genericQueue = BsonConversion.FromBsonValue<Queue<int>>(array)!;
        await AssertSequenceAsync(genericQueue, 1, 2, 3);

        var observable = (ObservableCollection<int>)BsonConversion.FromBsonValue(array, typeof(ObservableCollection<int>))!;
        await AssertSequenceAsync(observable, 1, 2, 3);

        var aotStack = (Stack<int>)AotBsonMapper.ConvertValue(array, typeof(Stack<int>))!;
        await AssertSequenceAsync(aotStack, 1, 2, 3);

        var genericStack = BsonConversion.FromBsonValue<Stack<int>>(array)!;
        await AssertSequenceAsync(genericStack, 1, 2, 3);

        var untypedStack = (ConcurrentStack<int>)BsonConversion.FromBsonValue(array, typeof(ConcurrentStack<int>))!;
        await AssertSequenceAsync(untypedStack, 1, 2, 3);
    }

    [Test]
    public async Task GeneratedEntity_NestedQueue_ShouldRoundTrip()
    {
        var entity = new NestedQueueEntity
        {
            Id = 1,
            Queues = [new Queue<int>([1, 2]), new Queue<int>([3])]
        };

        var document = AotBsonMapper.ToDocument(entity);
        var restored = AotBsonMapper.FromDocument<NestedQueueEntity>(document);

        await AssertSequenceAsync(restored.Queues[0], 1, 2);
        await AssertSequenceAsync(restored.Queues[1], 3);
    }

    [Test]
    public async Task AotCollectionFactory_ParameterlessConstructorWithAdd_ShouldMaterialize()
    {
        var created = AotCollectionFactory.Create<AddOnlyCollection<int>, int>([1, 2, 3]);

        await Assert.That(created).IsTypeOf<AddOnlyCollection<int>>();
        await AssertSequenceAsync(created, 1, 2, 3);
    }

    [Test]
    public async Task AotCollectionFactory_AssignableInterface_ShouldReturnConcreteInstance()
    {
        var asList = AotCollectionFactory.Create<IReadOnlyList<int>, int>([1, 2]);
        await Assert.That(asList).IsTypeOf<List<int>>();

        var asSet = AotCollectionFactory.Create<ISet<int>, int>([1, 2]);
        await Assert.That(asSet).IsTypeOf<HashSet<int>>();
    }

    [Test]
    public async Task AotCollectionFactory_UnsupportedCollection_ShouldThrow()
    {
        await Assert.That(() => AotCollectionFactory.Create<UnsupportedCollection<int>, int>([1]))
            .Throws<NotSupportedException>();
    }

    private static async Task AssertSequenceAsync<T>(IEnumerable<T> actual, params T[] expected)
    {
        await Assert.That(actual.SequenceEqual(expected)).IsTrue();
    }

    [Entity]
    internal sealed class ConcreteCollectionEntity
    {
        public int Id { get; set; }
        public Queue<int> Queue { get; set; } = new();
        public Stack<int> Stack { get; set; } = new();
        public ConcurrentStack<int> Concurrent { get; set; } = new();
        public ObservableCollection<int> Observable { get; set; } = new();
        public LinkedList<int> Linked { get; set; } = new();
        public IReadOnlyList<int> ReadOnlyList { get; set; } = [];
        public ISet<int> InterfaceSet { get; set; } = new HashSet<int>();
        public IReadOnlySet<int> ReadOnlySet { get; set; } = new HashSet<int>();
    }

    [Entity]
    internal sealed class NestedQueueEntity
    {
        public int Id { get; set; }
        public List<Queue<int>> Queues { get; set; } = [];
    }

    private sealed class AddOnlyCollection<T> : ICollection<T>
    {
        private readonly List<T> _items = new();

        public int Count => _items.Count;
        public bool IsReadOnly => false;

        public void Add(T item) => _items.Add(item);
        public void Clear() => _items.Clear();
        public bool Contains(T item) => _items.Contains(item);
        public void CopyTo(T[] array, int arrayIndex) => _items.CopyTo(array, arrayIndex);
        public bool Remove(T item) => _items.Remove(item);
        public IEnumerator<T> GetEnumerator() => _items.GetEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }

    private sealed class UnsupportedCollection<T> : IEnumerable<T>
    {
        private UnsupportedCollection()
        {
        }

        public IEnumerator<T> GetEnumerator() => Enumerable.Empty<T>().GetEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }

    [Entity]
    internal sealed class DependentCollectionEntity
    {
        public int Id { get; set; }
        public DependentCollectionHolder Holder { get; set; } = new();
    }

    internal sealed class DependentCollectionHolder
    {
        public Queue<string> Queue { get; set; } = new();
    }

    [Entity]
    internal sealed class NestedListEntity
    {
        public int Id { get; set; }
        public List<List<int>> Matrix { get; set; } = [];
    }

    [Entity]
    internal sealed class NestedHashSetEntity
    {
        public int Id { get; set; }
        public List<HashSet<int>> Sets { get; set; } = [];
        public Dictionary<string, HashSet<int>> Map { get; set; } = new();
    }
}
