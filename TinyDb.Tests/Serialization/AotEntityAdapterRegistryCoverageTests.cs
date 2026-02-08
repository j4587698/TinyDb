using System;
using TinyDb.Bson;
using TinyDb.Serialization;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Serialization;

[NotInParallel]
public class AotEntityAdapterRegistryCoverageTests
{
    private sealed class TestEntity
    {
        public int X { get; set; }
    }

    [Test]
    public async Task Registry_ShouldExpose_UntypedAdapter_And_Clear()
    {
        AotHelperRegistry.Clear();

        var adapter = new AotEntityAdapter<TestEntity>(
            toDocument: e => new BsonDocument().Set("x", e.X),
            fromDocument: d => new TestEntity { X = d["x"].ToInt32(null) },
            getId: _ => BsonNull.Value,
            setId: (_, _) => { },
            hasValidId: _ => false,
            getPropertyValue: (e, name) => name == "X" ? e.X : null);

        AotHelperRegistry.Register(adapter);

        await Assert.That(AotHelperRegistry.TryGetAdapter(typeof(TestEntity), out var untyped)).IsTrue();
        await Assert.That(untyped).IsNotNull();

        var entity = untyped!.FromDocumentObject(new BsonDocument().Set("x", 123));
        await Assert.That(entity).IsTypeOf<TestEntity>();
        await Assert.That(((TestEntity)entity!).X).IsEqualTo(123);

        AotHelperRegistry.Clear();
        await Assert.That(AotHelperRegistry.TryGetAdapter(typeof(TestEntity), out _)).IsFalse();
    }
}

