using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using TinyDb.Attributes;
using TinyDb.Bson;
using TinyDb.Serialization;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Serialization;

public class AotBsonMapperBranchCoverageTests
{
    [Test]
    public async Task ToDocument_ShouldSkipIgnoredStaticWriteOnlyAndIndexerMembers()
    {
        var entity = new WeirdEntity
        {
            Id = 1,
            Normal = 2,
            Ignored = 3,
            PublicField = 4
        };

        var doc = AotBsonMapper.ToDocument(entity);

        await Assert.That(doc.ContainsKey("_id")).IsTrue();
        await Assert.That(doc.ContainsKey("normal")).IsTrue();
        await Assert.That(doc.ContainsKey("publicField")).IsTrue();

        await Assert.That(doc.ContainsKey("ignored")).IsFalse();
        await Assert.That(doc.ContainsKey("staticProp")).IsFalse();
        await Assert.That(doc.ContainsKey("writeOnly")).IsFalse();
        await Assert.That(doc.ContainsKey("item")).IsFalse(); // indexer
    }

    [Test]
    public async Task FromDocument_ForStruct_ShouldPopulateMembers()
    {
        var original = new SimpleStruct { Id = 7, Name = "hello" };
        var doc = AotBsonMapper.ToDocument(original);

        var roundtrip = AotBsonMapper.FromDocument<SimpleStruct>(doc);

        await Assert.That(roundtrip.Id).IsEqualTo(7);
        await Assert.That(roundtrip.Name).IsEqualTo("hello");
    }

    [Test]
    public async Task ConvertValue_Guid_FromBsonBinary_UuidLegacy_ShouldWork()
    {
        var guid = Guid.NewGuid();
        var bin = new BsonBinary(guid.ToByteArray(), BsonBinary.BinarySubType.UuidLegacy);

        var converted = (Guid)AotBsonMapper.ConvertValue(bin, typeof(Guid))!;

        await Assert.That(converted).IsEqualTo(guid);
    }

    [Test]
    public async Task ConvertValue_BsonArray_ToCustomCollectionWithoutDefaultCtor_ShouldUseListCtor()
    {
        var array = new BsonArray(new BsonValue[] { 1, 2, 3 });
        var result = (IntCollectionWithListCtor)AotBsonMapper.ConvertValue(array, typeof(IntCollectionWithListCtor))!;

        await Assert.That(result.ToArray().SequenceEqual(new[] { 1, 2, 3 })).IsTrue();
    }

    [Test]
    public async Task ConvertValue_BsonArray_ToCustomCollectionWithoutMatchingCtor_ShouldThrow()
    {
        var array = new BsonArray(new BsonValue[] { 1, 2, 3 });

        await Assert.That(() => AotBsonMapper.ConvertValue(array, typeof(IntCollectionWithBadCtor)))
            .Throws<NotSupportedException>();
    }

    [Test]
    public async Task ConvertValue_BsonArray_ToCustomCollectionWithOnlyNonSingleParameterCtors_ShouldThrow()
    {
        var array = new BsonArray(new BsonValue[] { 1, 2, 3 });

        await Assert.That(() => AotBsonMapper.ConvertValue(array, typeof(IntCollectionWithTwoParamCtor)))
            .Throws<NotSupportedException>();
    }

    [Entity]
    internal sealed class WeirdEntity
    {
        public int Id { get; set; }
        public int Normal { get; set; }

        [BsonIgnore]
        public int Ignored { get; set; }

        public static int StaticProp { get; set; }

        public int WriteOnly
        {
            set { _ = value; }
        }

        public int this[int index]
        {
            get => index;
            set { _ = value; }
        }

        public int PublicField;
    }

    [Entity]
    internal struct SimpleStruct
    {
        public int Id { get; set; }
        public string Name { get; set; }
    }

    internal sealed class IntCollectionWithListCtor : IEnumerable<int>
    {
        private readonly List<int> _inner;

        public IntCollectionWithListCtor(List<int> inner)
        {
            _inner = inner;
        }

        public IEnumerator<int> GetEnumerator() => _inner.GetEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }

    internal sealed class IntCollectionWithBadCtor : IEnumerable<int>
    {
        public IntCollectionWithBadCtor(int notACollection)
        {
            _ = notACollection;
        }

        public IEnumerator<int> GetEnumerator() => Enumerable.Empty<int>().GetEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }

    internal sealed class IntCollectionWithTwoParamCtor : IEnumerable<int>
    {
        public IntCollectionWithTwoParamCtor(int a, int b)
        {
            _ = a;
            _ = b;
        }

        public IEnumerator<int> GetEnumerator() => Enumerable.Empty<int>().GetEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
