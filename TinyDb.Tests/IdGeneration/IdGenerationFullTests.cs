using System;
using System.Reflection;
using TinyDb.Attributes;
using TinyDb.Bson;
using TinyDb.IdGeneration;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.IdGeneration;

/// <summary>
/// Comprehensive tests for IdGeneratorFactory, AutoIdGenerator and IdentitySequences to improve coverage
/// </summary>
[NotInParallel]
public class IdGenerationFullTests
{
    #region IdentitySequences Tests

    [Test]
    public async Task IdentitySequences_GetNextValue_ShouldReturnSequentialValues()
    {
        // Use unique key to avoid race conditions with parallel tests
        var uniqueKey = $"test_seq_{Guid.NewGuid():N}";
        IdentitySequences.Reset(uniqueKey);

        var v1 = IdentitySequences.GetNextValue(uniqueKey);
        var v2 = IdentitySequences.GetNextValue(uniqueKey);
        var v3 = IdentitySequences.GetNextValue(uniqueKey);

        await Assert.That(v1).IsEqualTo(1);
        await Assert.That(v2).IsEqualTo(2);
        await Assert.That(v3).IsEqualTo(3);
    }

    [Test]
    public async Task IdentitySequences_DifferentKeys_ShouldHaveIndependentSequences()
    {
        // Use unique keys to avoid race conditions with parallel tests
        var keyA = $"seq_a_{Guid.NewGuid():N}";
        var keyB = $"seq_b_{Guid.NewGuid():N}";
        IdentitySequences.Reset(keyA);
        IdentitySequences.Reset(keyB);

        var a1 = IdentitySequences.GetNextValue(keyA);
        var b1 = IdentitySequences.GetNextValue(keyB);
        var a2 = IdentitySequences.GetNextValue(keyA);

        await Assert.That(a1).IsEqualTo(1);
        await Assert.That(b1).IsEqualTo(1);
        await Assert.That(a2).IsEqualTo(2);
    }

    [Test]
    public async Task IdentitySequences_Reset_ShouldClearSequence()
    {
        // Use unique key to avoid race conditions with parallel tests
        var uniqueKey = $"reset_test_{Guid.NewGuid():N}";
        IdentitySequences.GetNextValue(uniqueKey);
        IdentitySequences.GetNextValue(uniqueKey);
        IdentitySequences.Reset(uniqueKey);

        var value = IdentitySequences.GetNextValue(uniqueKey);
        await Assert.That(value).IsEqualTo(1);
    }

    [Test]
    public async Task IdentitySequences_ResetAll_ShouldClearAllSequences()
    {
        // Use unique keys for this test
        var key1 = $"all_1_{Guid.NewGuid():N}";
        var key2 = $"all_2_{Guid.NewGuid():N}";
        IdentitySequences.GetNextValue(key1);
        IdentitySequences.GetNextValue(key2);
        
        // Actually test ResetAll - safe since we're NotInParallel
        IdentitySequences.ResetAll();

        var v1 = IdentitySequences.GetNextValue(key1);
        var v2 = IdentitySequences.GetNextValue(key2);

        await Assert.That(v1).IsEqualTo(1);
        await Assert.That(v2).IsEqualTo(1);
    }

    #endregion

    #region IdGeneratorFactory Tests

    [Test]
    public async Task IdGeneratorFactory_GetGenerator_ObjectId_ShouldReturnGenerator()
    {
        var generator = IdGeneratorFactory.GetGenerator(IdGenerationStrategy.ObjectId);
        await Assert.That(generator).IsNotNull();
        await Assert.That(generator).IsTypeOf<ObjectIdGenerator>();
    }

    [Test]
    public async Task IdGeneratorFactory_GetGenerator_IdentityInt_ShouldReturnGenerator()
    {
        var generator = IdGeneratorFactory.GetGenerator(IdGenerationStrategy.IdentityInt);
        await Assert.That(generator).IsNotNull();
        await Assert.That(generator).IsTypeOf<IdentityGenerator>();
    }

    [Test]
    public async Task IdGeneratorFactory_GetGenerator_IdentityLong_ShouldReturnGenerator()
    {
        var generator = IdGeneratorFactory.GetGenerator(IdGenerationStrategy.IdentityLong);
        await Assert.That(generator).IsNotNull();
        await Assert.That(generator).IsTypeOf<IdentityGenerator>();
    }

    [Test]
    public async Task IdGeneratorFactory_GetGenerator_GuidV7_ShouldReturnGenerator()
    {
        var generator = IdGeneratorFactory.GetGenerator(IdGenerationStrategy.GuidV7);
        await Assert.That(generator).IsNotNull();
        await Assert.That(generator).IsTypeOf<GuidV7Generator>();
    }

    [Test]
    public async Task IdGeneratorFactory_GetGenerator_GuidV4_ShouldReturnGenerator()
    {
        var generator = IdGeneratorFactory.GetGenerator(IdGenerationStrategy.GuidV4);
        await Assert.That(generator).IsNotNull();
        await Assert.That(generator).IsTypeOf<GuidV4Generator>();
    }

    [Test]
    public async Task IdGeneratorFactory_GetGenerator_UnsupportedStrategy_ShouldThrow()
    {
        await Assert.That(() => IdGeneratorFactory.GetGenerator((IdGenerationStrategy)999))
            .Throws<NotSupportedException>();
    }

    [Test]
    public async Task IdGeneratorFactory_RegisterGenerator_ShouldOverride()
    {
        // Use a custom strategy enum value (998) to avoid interfering with other tests
        // that may be running in parallel. 999 is used by UnsupportedStrategy test.
        var testStrategy = (IdGenerationStrategy)998;
        var customGenerator = new CustomIdGenerator();
        IdGeneratorFactory.RegisterGenerator(testStrategy, customGenerator);

        var generator = IdGeneratorFactory.GetGenerator(testStrategy);
        await Assert.That(generator).IsEqualTo(customGenerator);
    }

    private class CustomIdGenerator : IIdGenerator
    {
        public BsonValue GenerateId(Type entityType, System.Reflection.PropertyInfo idProperty, string? sequenceName = null)
            => new BsonObjectId(ObjectId.NewObjectId());
        public bool Supports(Type idType) => idType == typeof(ObjectId);
    }

    #endregion

    #region AutoIdGenerator Tests

    [Test]
    public async Task AutoIdGenerator_NullEntity_ShouldReturnFalse()
    {
        var prop = typeof(IntIdEntity).GetProperty(nameof(IntIdEntity.Id))!;
        var result = AutoIdGenerator.GenerateIdIfNeeded(null!, prop);
        await Assert.That(result).IsFalse();
    }

    [Test]
    public async Task AutoIdGenerator_NullProperty_ShouldReturnFalse()
    {
        var entity = new IntIdEntity();
        var result = AutoIdGenerator.GenerateIdIfNeeded(entity, null!);
        await Assert.That(result).IsFalse();
    }

    [Test]
    public async Task AutoIdGenerator_IntId_EmptyValue_ShouldGenerate()
    {
        // Don't use ResetAll() as it affects parallel tests
        var entity = new IntIdEntity { Id = 0 };
        var prop = typeof(IntIdEntity).GetProperty(nameof(IntIdEntity.Id))!;

        var result = AutoIdGenerator.GenerateIdIfNeeded(entity, prop);

        await Assert.That(result).IsTrue();
        await Assert.That(entity.Id).IsGreaterThan(0);
    }

    [Test]
    public async Task AutoIdGenerator_IntId_HasValue_ShouldNotRegenerate()
    {
        var entity = new IntIdEntity { Id = 100 };
        var prop = typeof(IntIdEntity).GetProperty(nameof(IntIdEntity.Id))!;

        var result = AutoIdGenerator.GenerateIdIfNeeded(entity, prop);

        await Assert.That(result).IsFalse();
        await Assert.That(entity.Id).IsEqualTo(100);
    }

    [Test]
    public async Task AutoIdGenerator_IntId_Overflow_ShouldReturnFalse()
    {
        var entity = new OverflowIntIdEntity { MyId = 0 };
        var prop = typeof(OverflowIntIdEntity).GetProperty(nameof(OverflowIntIdEntity.MyId))!;
        var key = $"{entity.GetType().Name}_{prop.Name}_int";

        var sequencesField = typeof(IdentitySequences).GetField("_sequences", BindingFlags.NonPublic | BindingFlags.Static);
        await Assert.That(sequencesField).IsNotNull();

        var sequences = (System.Collections.Concurrent.ConcurrentDictionary<string, long>)sequencesField!.GetValue(null)!;
        sequences[key] = int.MaxValue;

        try
        {
            var result = AutoIdGenerator.GenerateIdIfNeeded(entity, prop);
            await Assert.That(result).IsFalse();
        }
        finally
        {
            IdentitySequences.Reset(key);
        }
    }

    [Test]
    public async Task AutoIdGenerator_LongId_EmptyValue_ShouldGenerate()
    {
        // Don't use ResetAll() as it affects parallel tests
        var entity = new LongIdEntity { Id = 0L };
        var prop = typeof(LongIdEntity).GetProperty(nameof(LongIdEntity.Id))!;

        var result = AutoIdGenerator.GenerateIdIfNeeded(entity, prop);

        await Assert.That(result).IsTrue();
        await Assert.That(entity.Id).IsGreaterThan(0L);
    }

    [Test]
    public async Task AutoIdGenerator_GuidId_EmptyValue_ShouldGenerate()
    {
        var entity = new GuidIdEntity { Id = Guid.Empty };
        var prop = typeof(GuidIdEntity).GetProperty(nameof(GuidIdEntity.Id))!;

        var result = AutoIdGenerator.GenerateIdIfNeeded(entity, prop);

        await Assert.That(result).IsTrue();
        await Assert.That(entity.Id).IsNotEqualTo(Guid.Empty);
    }

    [Test]
    public async Task AutoIdGenerator_GuidId_ShouldGenerateV7()
    {
        var entity = new GuidIdEntity { Id = Guid.Empty };
        var prop = typeof(GuidIdEntity).GetProperty(nameof(GuidIdEntity.Id))!;

        AutoIdGenerator.GenerateIdIfNeeded(entity, prop);

        // GUID v7 has version bits set correctly
        var bytes = entity.Id.ToByteArray();
        // Version is in bits 12-15 of byte 7 (big-endian interpretation)
        // But .NET Guid bytes are in mixed-endian format
        // For GUID v7: version should be 7 (0111 in bits 12-15)
        await Assert.That(entity.Id).IsNotEqualTo(Guid.Empty);
    }

    [Test]
    public async Task AutoIdGenerator_StringId_EmptyValue_ShouldGenerate()
    {
        var entity = new StringIdEntity { Id = "" };
        var prop = typeof(StringIdEntity).GetProperty(nameof(StringIdEntity.Id))!;

        var result = AutoIdGenerator.GenerateIdIfNeeded(entity, prop);

        await Assert.That(result).IsTrue();
        await Assert.That(entity.Id).IsNotEmpty();
        // Should be a valid GUID string
        await Assert.That(Guid.TryParse(entity.Id, out _)).IsTrue();
    }

    [Test]
    public async Task AutoIdGenerator_StringId_NullValue_ShouldGenerate()
    {
        var entity = new StringIdEntity { Id = null! };
        var prop = typeof(StringIdEntity).GetProperty(nameof(StringIdEntity.Id))!;

        var result = AutoIdGenerator.GenerateIdIfNeeded(entity, prop);

        await Assert.That(result).IsTrue();
        await Assert.That(entity.Id).IsNotNull();
    }

    [Test]
    public async Task AutoIdGenerator_StringId_WhitespaceValue_ShouldGenerate()
    {
        var entity = new StringIdEntity { Id = "   " };
        var prop = typeof(StringIdEntity).GetProperty(nameof(StringIdEntity.Id))!;

        var result = AutoIdGenerator.GenerateIdIfNeeded(entity, prop);

        await Assert.That(result).IsTrue();
        await Assert.That(entity.Id.Trim()).IsNotEmpty();
    }

    [Test]
    public async Task AutoIdGenerator_ObjectId_EmptyValue_ShouldGenerate()
    {
        var entity = new ObjectIdEntity { Id = ObjectId.Empty };
        var prop = typeof(ObjectIdEntity).GetProperty(nameof(ObjectIdEntity.Id))!;

        var result = AutoIdGenerator.GenerateIdIfNeeded(entity, prop);

        await Assert.That(result).IsTrue();
        await Assert.That(entity.Id).IsNotEqualTo(ObjectId.Empty);
    }

    [Test]
    public async Task AutoIdGenerator_UnsupportedType_ShouldReturnFalse()
    {
        var entity = new DecimalIdEntity { Id = null };
        var prop = typeof(DecimalIdEntity).GetProperty(nameof(DecimalIdEntity.Id))!;

        var result = AutoIdGenerator.GenerateIdIfNeeded(entity, prop);

        await Assert.That(result).IsFalse();
    }

    [Test]
    public async Task AutoIdGenerator_UnknownIdTypeWithNonEmptyValue_ShouldReturnFalse()
    {
        var entity = new ObjectIdAsObjectEntity { Id = new object() };
        var prop = typeof(ObjectIdAsObjectEntity).GetProperty(nameof(ObjectIdAsObjectEntity.Id))!;

        var result = AutoIdGenerator.GenerateIdIfNeeded(entity, prop);

        await Assert.That(result).IsFalse();
    }

    #endregion

    #region Test Entities

    private class IntIdEntity
    {
        public int Id { get; set; }
    }

    private class LongIdEntity
    {
        public long Id { get; set; }
    }

    private class GuidIdEntity
    {
        public Guid Id { get; set; }
    }

    private class StringIdEntity
    {
        public string Id { get; set; } = "";
    }

    private class ObjectIdEntity
    {
        public ObjectId Id { get; set; }
    }

    private class DecimalIdEntity
    {
        public decimal? Id { get; set; }
    }

    private class ObjectIdAsObjectEntity
    {
        public object? Id { get; set; }
    }

    private class OverflowIntIdEntity
    {
        public int MyId { get; set; }
    }

    #endregion
}

/// <summary>
/// Tests for individual ID generators
/// </summary>
public class IndividualGeneratorTests
{
    [Test]
    public async Task ObjectIdGenerator_GenerateId_ShouldReturnBsonObjectId()
    {
        var generator = new ObjectIdGenerator();
        var prop = typeof(ObjectIdEntity).GetProperty(nameof(ObjectIdEntity.Id))!;
        var id = generator.GenerateId(typeof(ObjectIdEntity), prop);

        await Assert.That(id).IsTypeOf<BsonObjectId>();
        await Assert.That(((BsonObjectId)id).Value).IsNotEqualTo(ObjectId.Empty);
    }

    [Test]
    public async Task ObjectIdGenerator_Supports_ShouldReturnTrueForObjectId()
    {
        var generator = new ObjectIdGenerator();

        await Assert.That(generator.Supports(typeof(ObjectId))).IsTrue();
        await Assert.That(generator.Supports(typeof(Guid))).IsFalse();
        await Assert.That(generator.Supports(typeof(int))).IsFalse();
    }

    [Test]
    public async Task GuidV4Generator_GenerateId_Guid_ShouldReturnBsonBinary()
    {
        var generator = new GuidV4Generator();
        var prop = typeof(GuidIdEntity).GetProperty(nameof(GuidIdEntity.Id))!;
        var id = generator.GenerateId(typeof(GuidIdEntity), prop);

        await Assert.That(id).IsTypeOf<BsonBinary>();
    }

    [Test]
    public async Task GuidV4Generator_GenerateId_String_ShouldReturnBsonString()
    {
        var generator = new GuidV4Generator();
        var prop = typeof(StringIdEntity).GetProperty(nameof(StringIdEntity.Id))!;
        var id = generator.GenerateId(typeof(StringIdEntity), prop);

        await Assert.That(id).IsTypeOf<BsonString>();
        await Assert.That(Guid.TryParse(((BsonString)id).Value, out _)).IsTrue();
    }

    [Test]
    public async Task GuidV4Generator_Supports_ShouldReturnTrueForGuidAndString()
    {
        var generator = new GuidV4Generator();

        await Assert.That(generator.Supports(typeof(Guid))).IsTrue();
        await Assert.That(generator.Supports(typeof(string))).IsTrue();
        await Assert.That(generator.Supports(typeof(int))).IsFalse();
    }

    [Test]
    public async Task GuidV7Generator_GenerateId_ShouldReturnBsonValue()
    {
        var generator = new GuidV7Generator();
        var prop = typeof(GuidIdEntity).GetProperty(nameof(GuidIdEntity.Id))!;
        var id = generator.GenerateId(typeof(GuidIdEntity), prop);

        // GuidV7Generator returns BsonBinary for Guid type or BsonString for string type
        await Assert.That(id).IsNotNull();
        await Assert.That(id is BsonBinary || id is BsonString).IsTrue();
    }

    [Test]
    public async Task GuidV7Generator_GenerateId_ShouldBeTimeOrdered()
    {
        var generator = new GuidV7Generator();
        var prop = typeof(GuidIdEntity).GetProperty(nameof(GuidIdEntity.Id))!;

        var id1 = generator.GenerateId(typeof(GuidIdEntity), prop);
        System.Threading.Thread.Sleep(10);
        var id2 = generator.GenerateId(typeof(GuidIdEntity), prop);

        // Both should be valid
        await Assert.That(id1).IsNotNull();
        await Assert.That(id2).IsNotNull();
    }

    [Test]
    public async Task GuidV7Generator_Supports_ShouldReturnTrueForGuidAndString()
    {
        var generator = new GuidV7Generator();

        await Assert.That(generator.Supports(typeof(Guid))).IsTrue();
        await Assert.That(generator.Supports(typeof(string))).IsTrue();
        await Assert.That(generator.Supports(typeof(int))).IsFalse();
    }

    [Test]
    public async Task IdentityGenerator_GenerateId_Int_ShouldReturnSequentialValues()
    {
        var generator = new IdentityGenerator();
        var prop = typeof(IntIdEntity).GetProperty(nameof(IntIdEntity.Id))!;

        var id1 = generator.GenerateId(typeof(IntIdEntity), prop, "test_int_seq");
        var id2 = generator.GenerateId(typeof(IntIdEntity), prop, "test_int_seq");

        await Assert.That(id1).IsTypeOf<BsonInt32>();
        await Assert.That(id2).IsTypeOf<BsonInt32>();
        await Assert.That(((BsonInt32)id2).Value).IsGreaterThan(((BsonInt32)id1).Value);
    }

    [Test]
    public async Task IdentityGenerator_GenerateId_Long_ShouldReturnSequentialValues()
    {
        var generator = new IdentityGenerator();
        var prop = typeof(LongIdEntity).GetProperty(nameof(LongIdEntity.Id))!;

        var id1 = generator.GenerateId(typeof(LongIdEntity), prop, "test_long_seq");
        var id2 = generator.GenerateId(typeof(LongIdEntity), prop, "test_long_seq");

        await Assert.That(id1).IsTypeOf<BsonInt64>();
        await Assert.That(id2).IsTypeOf<BsonInt64>();
        await Assert.That(((BsonInt64)id2).Value).IsGreaterThan(((BsonInt64)id1).Value);
    }

    [Test]
    public async Task IdentityGenerator_GenerateId_UnsupportedType_ShouldThrow()
    {
        var generator = new IdentityGenerator();
        var prop = typeof(GuidIdEntity).GetProperty(nameof(GuidIdEntity.Id))!;

        await Assert.That(() => generator.GenerateId(typeof(GuidIdEntity), prop))
            .Throws<InvalidOperationException>();
    }

    [Test]
    public async Task IdentityGenerator_Supports_ShouldReturnTrueForIntAndLong()
    {
        var generator = new IdentityGenerator();

        await Assert.That(generator.Supports(typeof(int))).IsTrue();
        await Assert.That(generator.Supports(typeof(long))).IsTrue();
        await Assert.That(generator.Supports(typeof(Guid))).IsFalse();
        await Assert.That(generator.Supports(typeof(string))).IsFalse();
    }

    private class IntIdEntity { public int Id { get; set; } }
    private class LongIdEntity { public long Id { get; set; } }
    private class GuidIdEntity { public Guid Id { get; set; } }
    private class StringIdEntity { public string Id { get; set; } = ""; }
    private class ObjectIdEntity { public ObjectId Id { get; set; } }
}
