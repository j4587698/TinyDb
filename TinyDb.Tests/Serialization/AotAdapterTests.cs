using System;
using TinyDb.Bson;
using TinyDb.Serialization;
using TinyDb.Tests.TestEntities;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Serialization;

public class AotAdapterTests
{
    [Test]
    public async Task AotHelperRegistry_Should_RegisterAndRetrieve()
    {
        try 
        {
            var adapter = new AotEntityAdapter<UserWithIntId>(
                toDocument: u => new BsonDocument().Set("_id", u.Id).Set("name", u.Name),
                fromDocument: d => new UserWithIntId { Id = d["_id"].ToInt32(null), Name = d["name"].ToString() },
                getId: u => u.Id,
                setId: (u, id) => u.Id = id.ToInt32(null),
                hasValidId: u => u.Id > 0,
                getPropertyValue: (u, prop) => prop == "Name" ? u.Name : null
            );

            AotHelperRegistry.Register(adapter);

            await Assert.That(AotHelperRegistry.TryGetAdapter<UserWithIntId>(out var retrieved)).IsTrue();
            await Assert.That(retrieved).IsSameReferenceAs(adapter);
            
            // Test AotBsonMapper integration
            var user = new UserWithIntId { Id = 1, Name = "AOT" };
            var doc = AotBsonMapper.ToDocument(user);
            await Assert.That(doc["name"].ToString()).IsEqualTo("AOT");
            
            var back = AotBsonMapper.FromDocument<UserWithIntId>(doc);
            await Assert.That(back.Name).IsEqualTo("AOT");
        }
        finally
        {
            AotHelperRegistry.Clear();
        }
    }
}
