using TinyDb.Attributes;
using TinyDb.Serialization;
using TinyDb.Bson;
using TinyDb.Tests.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Serialization;

public class AotMapperReflectionTests
{
    public struct SubItem
    {
        public bool Flag { get; set; }
    }

    /// <summary>
    /// 嵌套的非Entity类型（用于测试多级嵌套）
    /// </summary>
    public class NestedNonEntity
    {
        public string Name { get; set; } = "";
        public InnerNonEntity? Inner { get; set; }
    }

    /// <summary>
    /// 内部非Entity类型
    /// </summary>
    public class InnerNonEntity
    {
        public int Value { get; set; }
        public string Description { get; set; } = "";
    }

    [Test]
    public async Task Reflection_Fallback_Should_Handle_Complex_Graph()
    {
        var doc = new AotReflectionComplexDoc
        {
            Id = Guid.NewGuid(),
            Nested = new AotReflectionNestedDoc { Name = "N", Value = 1 },
            List = new List<SubItem> { new SubItem { Flag = true }, new SubItem { Flag = false } },
            Dict = new Dictionary<string, int> { { "k1", 10 }, { "k2", 20 } }
        };

        var bson = AotBsonMapper.ToDocument(doc);
        var restored = AotBsonMapper.FromDocument<AotReflectionComplexDoc>(bson);

        await Assert.That(restored.Id).IsEqualTo(doc.Id);
        await Assert.That(restored.Nested.Name).IsEqualTo("N");
        await Assert.That(restored.List.Count).IsEqualTo(2);
        await Assert.That(restored.List[0].Flag).IsTrue();
        await Assert.That(restored.Dict["k1"]).IsEqualTo(10);
    }

    [Test]
    public async Task Should_Handle_Nested_NonEntity_Types()
    {
        // 测试多级嵌套的非Entity类型
        var doc = new AotNestedNonEntityDoc
        {
            Id = Guid.NewGuid(),
            TopLevel = new NestedNonEntity
            {
                Name = "Top",
                Inner = new InnerNonEntity
                {
                    Value = 42,
                    Description = "Inner value"
                }
            }
        };

        var bson = AotBsonMapper.ToDocument(doc);
        var restored = AotBsonMapper.FromDocument<AotNestedNonEntityDoc>(bson);

        await Assert.That(restored.Id).IsEqualTo(doc.Id);
        await Assert.That(restored.TopLevel).IsNotNull();
        await Assert.That(restored.TopLevel!.Name).IsEqualTo("Top");
        await Assert.That(restored.TopLevel.Inner).IsNotNull();
        await Assert.That(restored.TopLevel.Inner!.Value).IsEqualTo(42);
        await Assert.That(restored.TopLevel.Inner.Description).IsEqualTo("Inner value");
    }

    [Test]
    public async Task Should_Handle_Nullable_NonEntity_Types()
    {
        // 测试可空的非Entity类型 - 有值
        var doc = new AotNullableNonEntityDoc
        {
            Id = Guid.NewGuid(),
            OptionalItem = new SubItem { Flag = true }
        };

        var bson = AotBsonMapper.ToDocument(doc);
        var restored = AotBsonMapper.FromDocument<AotNullableNonEntityDoc>(bson);

        await Assert.That(restored.Id).IsEqualTo(doc.Id);
        await Assert.That(restored.OptionalItem.HasValue).IsTrue();
        await Assert.That(restored.OptionalItem!.Value.Flag).IsTrue();
    }

    [Test]
    public async Task Should_Handle_Null_NonEntity_Types()
    {
        // 测试可空的非Entity类型 - null值
        var doc = new AotNullableNonEntityDoc
        {
            Id = Guid.NewGuid(),
            OptionalItem = null
        };

        var bson = AotBsonMapper.ToDocument(doc);
        var restored = AotBsonMapper.FromDocument<AotNullableNonEntityDoc>(bson);

        await Assert.That(restored.Id).IsEqualTo(doc.Id);
        await Assert.That(restored.OptionalItem.HasValue).IsFalse();
    }

    [Test]
    public async Task Should_Handle_Dictionary_With_NonEntity_Values()
    {
        // 测试字典值是非Entity类型
        var doc = new AotDictNonEntityDoc
        {
            Id = Guid.NewGuid(),
            Items = new Dictionary<string, SubItem>
            {
                { "first", new SubItem { Flag = true } },
                { "second", new SubItem { Flag = false } }
            }
        };

        var bson = AotBsonMapper.ToDocument(doc);
        var restored = AotBsonMapper.FromDocument<AotDictNonEntityDoc>(bson);

        await Assert.That(restored.Id).IsEqualTo(doc.Id);
        await Assert.That(restored.Items.Count).IsEqualTo(2);
        await Assert.That(restored.Items["first"].Flag).IsTrue();
        await Assert.That(restored.Items["second"].Flag).IsFalse();
    }

    [Test]
    public async Task Should_Handle_Array_Of_NonEntity_Types()
    {
        // 测试数组元素是非Entity类型
        var doc = new AotArrayNonEntityDoc
        {
            Id = Guid.NewGuid(),
            Items = new[] 
            { 
                new SubItem { Flag = true }, 
                new SubItem { Flag = false },
                new SubItem { Flag = true }
            }
        };

        var bson = AotBsonMapper.ToDocument(doc);
        var restored = AotBsonMapper.FromDocument<AotArrayNonEntityDoc>(bson);

        await Assert.That(restored.Id).IsEqualTo(doc.Id);
        await Assert.That(restored.Items.Length).IsEqualTo(3);
        await Assert.That(restored.Items[0].Flag).IsTrue();
        await Assert.That(restored.Items[1].Flag).IsFalse();
        await Assert.That(restored.Items[2].Flag).IsTrue();
    }

    [Test]
    public async Task Should_Handle_Circular_Reference_Without_StackOverflow()
    {
        // 测试循环引用类型 - 应该不会导致栈溢出
        // 循环引用属性会被设置为 null
        var doc = new AotCircularRefDoc
        {
            Id = Guid.NewGuid(),
            TypeA = new CircularTypeA
            {
                Name = "TypeA",
                RefToB = new CircularTypeB
                {
                    Value = 42,
                    RefToA = new CircularTypeA { Name = "Nested A" } // 这里形成循环
                }
            }
        };

        // 序列化不应该抛出栈溢出异常
        var bson = AotBsonMapper.ToDocument(doc);
        
        // 验证基本序列化成功
        await Assert.That(bson).IsNotNull();
        await Assert.That(bson["_id"]).IsNotNull();
        
        // 反序列化
        var restored = AotBsonMapper.FromDocument<AotCircularRefDoc>(bson);
        
        await Assert.That(restored.Id).IsEqualTo(doc.Id);
        await Assert.That(restored.TypeA).IsNotNull();
        await Assert.That(restored.TypeA!.Name).IsEqualTo("TypeA");
        // 注意：由于循环引用被中断，RefToB.RefToA 应该是 null 或 default
    }

    [Test]
    public async Task Should_Handle_Self_Reference_Without_StackOverflow()
    {
        // 测试自引用类型 - 应该不会导致栈溢出
        var doc = new AotSelfRefDoc
        {
            Id = Guid.NewGuid(),
            Node = new SelfReferencingType
            {
                Name = "Root",
                Parent = new SelfReferencingType
                {
                    Name = "Parent",
                    Parent = new SelfReferencingType { Name = "GrandParent" } // 这里形成自引用
                }
            }
        };

        // 序列化不应该抛出栈溢出异常
        var bson = AotBsonMapper.ToDocument(doc);
        
        // 验证基本序列化成功
        await Assert.That(bson).IsNotNull();
        
        // 反序列化
        var restored = AotBsonMapper.FromDocument<AotSelfRefDoc>(bson);
        
        await Assert.That(restored.Id).IsEqualTo(doc.Id);
        await Assert.That(restored.Node).IsNotNull();
        await Assert.That(restored.Node!.Name).IsEqualTo("Root");
        // 注意：由于自引用被中断，深层嵌套的 Parent 应该是 null
    }
}

[Entity]
public class AotReflectionComplexDoc
{
    public Guid Id { get; set; }
    public AotReflectionNestedDoc Nested { get; set; } = new();
    public List<AotMapperReflectionTests.SubItem> List { get; set; } = new();
    public Dictionary<string, int> Dict { get; set; } = new();
}

[Entity]
public class AotReflectionNestedDoc
{
    public string Name { get; set; } = "";
    public int Value { get; set; }
}

/// <summary>
/// 测试多级嵌套非Entity类型的Entity
/// </summary>
[Entity]
public class AotNestedNonEntityDoc
{
    public Guid Id { get; set; }
    public AotMapperReflectionTests.NestedNonEntity? TopLevel { get; set; }
}

/// <summary>
/// 测试可空非Entity值类型的Entity
/// </summary>
[Entity]
public class AotNullableNonEntityDoc
{
    public Guid Id { get; set; }
    public AotMapperReflectionTests.SubItem? OptionalItem { get; set; }
}

/// <summary>
/// 测试字典值为非Entity类型的Entity
/// </summary>
[Entity]
public class AotDictNonEntityDoc
{
    public Guid Id { get; set; }
    public Dictionary<string, AotMapperReflectionTests.SubItem> Items { get; set; } = new();
}

/// <summary>
/// 测试数组元素为非Entity类型的Entity
/// </summary>
[Entity]
public class AotArrayNonEntityDoc
{
    public Guid Id { get; set; }
    public AotMapperReflectionTests.SubItem[] Items { get; set; } = Array.Empty<AotMapperReflectionTests.SubItem>();
}

// ========== 循环引用测试类型 ==========

/// <summary>
/// 循环引用类型A - 引用类型B
/// </summary>
public class CircularTypeA
{
    public string Name { get; set; } = "";
    public CircularTypeB? RefToB { get; set; }
}

/// <summary>
/// 循环引用类型B - 引用类型A（形成 A -> B -> A 循环）
/// </summary>
public class CircularTypeB
{
    public int Value { get; set; }
    public CircularTypeA? RefToA { get; set; }
}

/// <summary>
/// 自引用类型 - 引用自身（形成 Self -> Self 循环）
/// </summary>
public class SelfReferencingType
{
    public string Name { get; set; } = "";
    public SelfReferencingType? Parent { get; set; }
}

/// <summary>
/// 测试循环引用检测的Entity
/// </summary>
[Entity]
public class AotCircularRefDoc
{
    public Guid Id { get; set; }
    public CircularTypeA? TypeA { get; set; }
}

/// <summary>
/// 测试自引用检测的Entity
/// </summary>
[Entity]
public class AotSelfRefDoc
{
    public Guid Id { get; set; }
    public SelfReferencingType? Node { get; set; }
}
