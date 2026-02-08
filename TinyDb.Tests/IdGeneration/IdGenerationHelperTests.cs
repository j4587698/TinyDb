using System;
using TinyDb.Attributes;
using TinyDb.Bson;
using TinyDb.IdGeneration;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.IdGeneration;

public class IdGenerationHelperTests
{
    public class EntityWithIntAutoId
    {
        [IdGeneration(IdGenerationStrategy.IdentityInt)]
        public int Id { get; set; }
    }
    
    public class EntityWithLongAutoId
    {
        [IdGeneration(IdGenerationStrategy.IdentityLong)]
        public long Id { get; set; }
    }
    
    public class EntityWithGuidV4
    {
        [IdGeneration(IdGenerationStrategy.GuidV4)]
        public Guid Id { get; set; }
    }

    public class EntityWithGuidV7
    {
        [IdGeneration(IdGenerationStrategy.GuidV7)]
        public Guid Id { get; set; }
    }
    
    public class EntityWithStringGuid
    {
        [IdGeneration(IdGenerationStrategy.GuidV4)]
        public string? Id { get; set; }
    }
    
    public class EntityWithObjectId
    {
        [IdGeneration(IdGenerationStrategy.ObjectId)]
        public ObjectId Id { get; set; }
    }
    
    public class EntityWithNone
    {
        [IdGeneration(IdGenerationStrategy.None)]
        public int Id { get; set; }
    }

    public class EntityWithIntIdNoGenerator
    {
        public int Id { get; set; }
    }

    public class EntityWithDecimalId
    {
        [IdGeneration(IdGenerationStrategy.IdentityInt)]
        public decimal Id { get; set; }
    }
    
    public class EntityNoId
    {
        public string? Name { get; set; }
    }

    [Test]
    public async Task ShouldGenerateId_VariousTypes()
    {
        // Int
        await Assert.That(IdGenerationHelper<EntityWithIntAutoId>.ShouldGenerateId(new EntityWithIntAutoId { Id = 0 })).IsTrue();
        await Assert.That(IdGenerationHelper<EntityWithIntAutoId>.ShouldGenerateId(new EntityWithIntAutoId { Id = 1 })).IsFalse();

        // Long
        await Assert.That(IdGenerationHelper<EntityWithLongAutoId>.ShouldGenerateId(new EntityWithLongAutoId { Id = 0L })).IsTrue();
        await Assert.That(IdGenerationHelper<EntityWithLongAutoId>.ShouldGenerateId(new EntityWithLongAutoId { Id = 1L })).IsFalse();
        
        // Guid
        await Assert.That(IdGenerationHelper<EntityWithGuidV4>.ShouldGenerateId(new EntityWithGuidV4 { Id = Guid.Empty })).IsTrue();
        await Assert.That(IdGenerationHelper<EntityWithGuidV4>.ShouldGenerateId(new EntityWithGuidV4 { Id = Guid.NewGuid() })).IsFalse();
        
        // String
        await Assert.That(IdGenerationHelper<EntityWithStringGuid>.ShouldGenerateId(new EntityWithStringGuid { Id = null })).IsTrue();
        await Assert.That(IdGenerationHelper<EntityWithStringGuid>.ShouldGenerateId(new EntityWithStringGuid { Id = "" })).IsTrue();
        await Assert.That(IdGenerationHelper<EntityWithStringGuid>.ShouldGenerateId(new EntityWithStringGuid { Id = "abc" })).IsFalse();
        
        // ObjectId
        await Assert.That(IdGenerationHelper<EntityWithObjectId>.ShouldGenerateId(new EntityWithObjectId { Id = ObjectId.Empty })).IsTrue();
        await Assert.That(IdGenerationHelper<EntityWithObjectId>.ShouldGenerateId(new EntityWithObjectId { Id = ObjectId.NewObjectId() })).IsFalse();
        
        // Null entity
        await Assert.That(IdGenerationHelper<EntityWithIntAutoId>.ShouldGenerateId(null!)).IsFalse();
        
        // No ID property
        await Assert.That(IdGenerationHelper<EntityNoId>.ShouldGenerateId(new EntityNoId())).IsFalse();

        // ID property without generator attribute
        await Assert.That(IdGenerationHelper<EntityWithIntIdNoGenerator>.ShouldGenerateId(new EntityWithIntIdNoGenerator { Id = 0 }))
            .IsFalse();
        
        // Strategy None
        await Assert.That(IdGenerationHelper<EntityWithNone>.ShouldGenerateId(new EntityWithNone { Id = 0 })).IsFalse();

        // Non-empty value not handled by IsEmptyValue switch
        await Assert.That(IdGenerationHelper<EntityWithDecimalId>.ShouldGenerateId(new EntityWithDecimalId { Id = 1m }))
            .IsFalse();
    }
    
    [Test]
    public async Task GenerateIdForEntity_Execution()
    {
        // Guid V4
        var eGuid = new EntityWithGuidV4();
        var res1 = IdGenerationHelper<EntityWithGuidV4>.GenerateIdForEntity(eGuid);
        await Assert.That(res1).IsTrue();
        await Assert.That(eGuid.Id).IsNotEqualTo(Guid.Empty);

        // Guid V7 (string generator + Guid target type conversion)
        var eGuidV7 = new EntityWithGuidV7();
        var resV7 = IdGenerationHelper<EntityWithGuidV7>.GenerateIdForEntity(eGuidV7);
        await Assert.That(resV7).IsTrue();
        await Assert.That(eGuidV7.Id).IsNotEqualTo(Guid.Empty);
        
        // String Guid
        var eStr = new EntityWithStringGuid();
        var res2 = IdGenerationHelper<EntityWithStringGuid>.GenerateIdForEntity(eStr);
        await Assert.That(res2).IsTrue();
        await Assert.That(Guid.TryParse(eStr.Id, out _)).IsTrue();
        
        // ObjectId
        var eObj = new EntityWithObjectId();
        var res3 = IdGenerationHelper<EntityWithObjectId>.GenerateIdForEntity(eObj);
        await Assert.That(res3).IsTrue();
        await Assert.That(eObj.Id).IsNotEqualTo(ObjectId.Empty);

        // Identity long
        var eLong = new EntityWithLongAutoId();
        var resLong = IdGenerationHelper<EntityWithLongAutoId>.GenerateIdForEntity(eLong);
        await Assert.That(resLong).IsTrue();
        await Assert.That(eLong.Id).IsNotEqualTo(0L);
         
        // None
        var eNone = new EntityWithNone();
        var res4 = IdGenerationHelper<EntityWithNone>.GenerateIdForEntity(eNone);
        await Assert.That(res4).IsFalse();
        
        // No ID
        var eNoId = new EntityNoId();
        var res5 = IdGenerationHelper<EntityNoId>.GenerateIdForEntity(eNoId);
        await Assert.That(res5).IsFalse();
        
        // Null
        await Assert.That(IdGenerationHelper<EntityWithIntAutoId>.GenerateIdForEntity(null!)).IsFalse();
    }

    [Test]
    public async Task GetIdGenerationStrategy_Test()
    {
        await Assert.That(IdGenerationHelper<EntityWithIntAutoId>.GetIdGenerationStrategy())
            .IsEqualTo(IdGenerationStrategy.IdentityInt);
            
        await Assert.That(IdGenerationHelper<EntityWithGuidV4>.GetIdGenerationStrategy())
            .IsEqualTo(IdGenerationStrategy.GuidV4);

        // ID property exists but has no [IdGeneration] attribute
        await Assert.That(IdGenerationHelper<EntityWithIntIdNoGenerator>.GetIdGenerationStrategy())
            .IsEqualTo(IdGenerationStrategy.None);
             
        await Assert.That(IdGenerationHelper<EntityNoId>.GetIdGenerationStrategy())
            .IsEqualTo(IdGenerationStrategy.None);
    }
}
