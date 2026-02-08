using System;
using System.Collections.Generic;
using System.Linq;
using TinyDb.Bson;
using TinyDb.Serialization;
using TinyDb.Attributes;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Serialization;

public class MapperExhaustiveTests
{
    [Entity]
    public class AllTypesEntity
    {
        public string String { get; set; } = "";
        public int Int { get; set; }
        public long Long { get; set; }
        public double Double { get; set; }
        public decimal Decimal { get; set; }
        public bool Bool { get; set; }
        public DateTime DateTime { get; set; }
        public Guid Guid { get; set; }
        public ObjectId ObjectId { get; set; }
        public byte[] Bytes { get; set; } = Array.Empty<byte>();
        public List<int> List { get; set; } = new();
        public Dictionary<string, string> Dict { get; set; } = new();
    }

    [Test]
    public async Task AotBsonMapper_AllTypes_RoundTrip_ShouldWork()
    {
        var entity = new AllTypesEntity
        {
            String = "test",
            Int = 42,
            Long = 1234567890L,
            Double = 3.14,
            Decimal = 123.45m,
            Bool = true,
            DateTime = DateTime.Now,
            Guid = Guid.NewGuid(),
            ObjectId = ObjectId.NewObjectId(),
            Bytes = new byte[] { 1, 2, 3 },
            List = new List<int> { 1, 2, 3 },
            Dict = new Dictionary<string, string> { ["k"] = "v" }
        };

        var doc = AotBsonMapper.ToDocument(entity);
        var back = AotBsonMapper.FromDocument<AllTypesEntity>(doc);

        await Assert.That(back.String).IsEqualTo(entity.String);
        await Assert.That(back.Int).IsEqualTo(entity.Int);
        await Assert.That(back.Long).IsEqualTo(entity.Long);
        await Assert.That(back.Double).IsEqualTo(entity.Double);
        await Assert.That(back.Decimal).IsEqualTo(entity.Decimal);
        await Assert.That(back.Bool).IsEqualTo(entity.Bool);
        await Assert.That(Math.Abs((back.DateTime - entity.DateTime).TotalSeconds)).IsLessThan(1.0);
        await Assert.That(back.Guid).IsEqualTo(entity.Guid);
        await Assert.That(back.ObjectId).IsEqualTo(entity.ObjectId);
        await Assert.That(back.Bytes.SequenceEqual(entity.Bytes)).IsTrue();
        await Assert.That(back.List.SequenceEqual(entity.List)).IsTrue();
        await Assert.That(back.Dict["k"]).IsEqualTo("v");
    }

    [Test]
    public async Task AotBsonMapper_ConvertValue_ShouldHandlePrimitives()
    {
        await Assert.That(AotBsonMapper.ConvertValue(new BsonInt32(1), typeof(bool))).IsEqualTo(true);
        await Assert.That(AotBsonMapper.ConvertValue(new BsonInt32(0), typeof(bool))).IsEqualTo(false);
        await Assert.That(AotBsonMapper.ConvertValue(new BsonString("123"), typeof(int))).IsEqualTo(123);
        await Assert.That(AotBsonMapper.ConvertValue(new BsonDouble(3.14), typeof(decimal))).IsEqualTo(3.14m);
    }

    [Test]
    public async Task AotBsonMapper_UnwrapBsonValue_ShouldWork()
    {
        await Assert.That(BsonMapper.ConvertFromBsonValue(new BsonInt32(42), typeof(object))).IsEqualTo(42);
        await Assert.That(BsonMapper.ConvertFromBsonValue(new BsonString("hi"), typeof(object))).IsEqualTo("hi");
        await Assert.That(BsonMapper.ConvertFromBsonValue(BsonNull.Value, typeof(object))).IsNull();
    }

    [Entity]
    public class CyclicEntity
    {
        public string Name { get; set; } = "";
        public CyclicEntity? Self { get; set; }
    }

    [Test]
    public async Task AotBsonMapper_CyclicReference_ShouldHandle()
    {
        var entity = new CyclicEntity { Name = "Parent" };
        entity.Self = entity;
        
        // Should not throw StackOverflowException
        var doc = AotBsonMapper.ToDocument(entity);
        await Assert.That(doc["name"].ToString()).IsEqualTo("Parent");
        // Circular reference should be detected and handled (usually by skipping or inserting a placeholder)
    }
}
