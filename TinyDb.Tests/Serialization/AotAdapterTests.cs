using System;
using TinyDb.Bson;
using TinyDb.Serialization;
using TinyDb.Tests.TestEntities;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Serialization;

/// <summary>
/// 本地测试实体，用于测试 AOT 适配器而不影响全局注册
/// </summary>
file class LocalTestUser
{
    public int Id { get; set; }
    public string Name { get; set; } = "";
}

public class AotAdapterTests
{
    [Test]
    public async Task AotHelperRegistry_Should_RegisterAndRetrieve()
    {
        // 使用文件局部类型进行测试，避免影响其他已注册的适配器
        var adapter = new AotEntityAdapter<LocalTestUser>(
            toDocument: u => new BsonDocument().Set("_id", u.Id).Set("name", u.Name),
            fromDocument: d => new LocalTestUser { Id = d["_id"].ToInt32(null), Name = d["name"].ToString() },
            getId: u => u.Id,
            setId: (u, id) => u.Id = id.ToInt32(null),
            hasValidId: u => u.Id > 0,
            getPropertyValue: (u, prop) => prop == "Name" ? u.Name : null
        );

        AotHelperRegistry.Register(adapter);

        await Assert.That(AotHelperRegistry.TryGetAdapter<LocalTestUser>(out var retrieved)).IsTrue();
        await Assert.That(retrieved).IsSameReferenceAs(adapter);
        
        // Test AotBsonMapper integration
        var user = new LocalTestUser { Id = 1, Name = "AOT" };
        var doc = AotBsonMapper.ToDocument(user);
        await Assert.That(doc["name"].ToString()).IsEqualTo("AOT");
        
        var back = AotBsonMapper.FromDocument<LocalTestUser>(doc);
        await Assert.That(back.Name).IsEqualTo("AOT");
        
        // 注意：不再调用 Clear()，因为这会影响其他测试
        // 由于 LocalTestUser 是文件局部类型，不会与其他测试冲突
    }
}
