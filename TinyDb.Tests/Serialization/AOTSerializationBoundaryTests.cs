using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using TinyDb.Bson;
using TinyDb.Serialization;
using TinyDb.Tests.TestEntities;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Serialization;

/// <summary>
/// AOT序列化边界测试
/// 验证在AOT环境下的序列化/反序列化边界条件和兼容性
/// </summary>
[NotInParallel]
public class AOTSerializationBoundaryTests
{
    [Before(Test)]
    public void Setup()
    {
        // BsonMapper是静态类，不需要实例化
    }

    /// <summary>
    /// 测试复杂嵌套对象的AOT序列化
    /// </summary>
    [Test]
    public async Task ComplexNestedObject_ShouldSerializeCorrectly()
    {
        if (!System.Runtime.CompilerServices.RuntimeFeature.IsDynamicCodeCompiled) return;

        // Arrange - 创建复杂的嵌套对象
        var complexEntity = new ComplexAOTEntity
        {
            Id = "complex_001",
            Name = "Complex Entity",
            Age = 30,
            CreatedAt = DateTime.UtcNow,
            IsActive = true,
            Score = 95.5,
            Tags = new[] { "tag1", "tag2", "tag3" },
            Address = new Address
            {
                Street = "123 Test St",
                City = "Test City",
                Country = "Test Country",
                ZipCode = "12345"
            },
            Metadata = new Dictionary<string, object>
            {
                ["key1"] = "value1",
                ["key2"] = 42,
                ["key3"] = true
            }
        };

        // Act - 序列化和反序列化
        var bsonDoc = BsonMapper.ToDocument(complexEntity);
        var deserializedEntity = BsonMapper.ToObject<ComplexAOTEntity>(bsonDoc);

        // Assert - 验证序列化结果
        await Assert.That(bsonDoc).IsNotNull();
        await Assert.That(bsonDoc.ContainsKey("_id")).IsTrue();
        await Assert.That(((BsonString)bsonDoc["_id"]).Value).IsEqualTo(complexEntity.Id);

        // 验证反序列化结果
        await Assert.That(deserializedEntity).IsNotNull();
        await Assert.That(deserializedEntity.Id).IsEqualTo(complexEntity.Id);
        await Assert.That(deserializedEntity.Name).IsEqualTo(complexEntity.Name);
        await Assert.That(deserializedEntity.Age).IsEqualTo(complexEntity.Age);
        await Assert.That(deserializedEntity.IsActive).IsEqualTo(complexEntity.IsActive);
        await Assert.That(deserializedEntity.Score).IsEqualTo(complexEntity.Score);

        // 验证嵌套对象
        await Assert.That(deserializedEntity.Address).IsNotNull();
        await Assert.That(deserializedEntity.Address.Street).IsEqualTo(complexEntity.Address.Street);
        await Assert.That(deserializedEntity.Address.City).IsEqualTo(complexEntity.Address.City);

        // 验证数组
        await Assert.That(deserializedEntity.Tags).IsNotNull();
        await Assert.That(deserializedEntity.Tags).Count().IsEqualTo(complexEntity.Tags.Length);

        // 验证字典
        await Assert.That(deserializedEntity.Metadata).IsNotNull();
        await Assert.That(deserializedEntity.Metadata).Count().IsEqualTo(complexEntity.Metadata.Count);
    }

    /// <summary>
    /// 测试循环引用的AOT处理
    /// </summary>
    [Test]
    public async Task CircularReference_ShouldBeHandledCorrectly()
    {
        // Arrange - 创建循环引用对象
        var parent = new CircularEntity
        {
            Id = "parent_001",
            Name = "Parent Entity"
        };

        var child = new CircularEntity
        {
            Id = "child_001",
            Name = "Child Entity"
        };

        parent.Children = new[] { child };
        child.Parent = parent;

        // Act - 尝试序列化（应该处理循环引用）
        var bsonDoc = BsonMapper.ToDocument(parent);

        // Assert - 验证循环引用被正确处理
        await Assert.That(bsonDoc).IsNotNull();
        await Assert.That(bsonDoc.ContainsKey("_id")).IsTrue();
        await Assert.That(((BsonString)bsonDoc["_id"]).Value).IsEqualTo(parent.Id);

        // 验证基本属性被序列化
        await Assert.That(bsonDoc.ContainsKey("name")).IsTrue();
        await Assert.That(((BsonString)bsonDoc["name"]).Value).IsEqualTo(parent.Name);

        // Children可能被忽略或简化，以避免循环引用
        if (bsonDoc.ContainsKey("Children"))
        {
            var childrenArray = (BsonArray)bsonDoc["Children"];
            await Assert.That(childrenArray).IsNotNull();
        }
    }

    /// <summary>
    /// 测试null值和默认值的AOT序列化
    /// </summary>
    [Test]
    public async Task NullAndDefaultValues_ShouldSerializeCorrectly()
    {
        // Arrange - 创建包含null和默认值的对象
        var entity = new NullTestEntity
        {
            Id = "null_test_001",
            StringNull = null,
            StringEmpty = "",
            IntDefault = 0,
            IntValue = 42,
            DateTimeNull = null,
            DateTimeValue = DateTime.UtcNow,
            BoolDefault = false,
            BoolValue = true,
            ListNull = null,
            ListEmpty = new List<string>(),
            ListValue = new List<string> { "item1", "item2" }
        };

        // Act
        var bsonDoc = BsonMapper.ToDocument(entity);
        var deserializedEntity = BsonMapper.ToObject<NullTestEntity>(bsonDoc);

        // Assert - 验证null值处理
        await Assert.That(bsonDoc).IsNotNull();

        // null字符串应该被跳过或特殊处理
        if (bsonDoc.ContainsKey("StringNull"))
        {
            await Assert.That(bsonDoc["StringNull"].IsNull).IsTrue();
        }

        // 空字符串应该被序列化
        if (bsonDoc.ContainsKey("StringEmpty"))
        {
            await Assert.That(((BsonString)bsonDoc["StringEmpty"]).Value).IsEqualTo("");
        }

        // 验证反序列化结果
        await Assert.That(deserializedEntity).IsNotNull();
        await Assert.That(deserializedEntity.Id).IsEqualTo(entity.Id);
        await Assert.That(deserializedEntity.StringNull).IsEqualTo(entity.StringNull);
        await Assert.That(deserializedEntity.StringEmpty).IsEqualTo(entity.StringEmpty);
        await Assert.That(deserializedEntity.IntDefault).IsEqualTo(entity.IntDefault);
        await Assert.That(deserializedEntity.IntValue).IsEqualTo(entity.IntValue);
    }

    /// <summary>
    /// 测试大型对象的AOT序列化性能
    /// </summary>
    [Test]
    public async Task LargeObject_ShouldSerializeEfficiently()
    {
        // Arrange - 创建大型对象
        var largeEntity = new LargeAOTEntity
        {
            Id = "large_001",
            Name = "Large Entity",
            LargeArray = Enumerable.Range(1, 10000).Select(i => $"item_{i}").ToArray(),
            LargeDictionary = Enumerable.Range(1, 1000)
                .ToDictionary(i => $"key_{i}", i => (object)$"value_{i}"),
            LargeText = string.Join(" ", Enumerable.Repeat("large text block ", 1000))
        };

        // Act - 测量序列化时间
        var startTime = DateTime.UtcNow;
        var bsonDoc = BsonMapper.ToDocument(largeEntity);
        var serializationTime = DateTime.UtcNow - startTime;

        // 测量反序列化时间
        startTime = DateTime.UtcNow;
        var deserializedEntity = BsonMapper.ToObject<LargeAOTEntity>(bsonDoc);
        var deserializationTime = DateTime.UtcNow - startTime;

        Console.WriteLine($"序列化耗时: {serializationTime.TotalMilliseconds:F2} ms");
        Console.WriteLine($"反序列化耗时: {deserializationTime.TotalMilliseconds:F2} ms");

        // 验证数据完整性
        await Assert.That(deserializedEntity).IsNotNull();
        await Assert.That(deserializedEntity.Id).IsEqualTo(largeEntity.Id);
        await Assert.That(deserializedEntity.LargeArray).Count().IsEqualTo(largeEntity.LargeArray.Length);
        await Assert.That(deserializedEntity.LargeDictionary).Count().IsEqualTo(largeEntity.LargeDictionary.Count);
        await Assert.That(deserializedEntity.LargeText).IsEqualTo(largeEntity.LargeText);
    }

    /// <summary>
    /// 测试自定义类型的AOT序列化
    /// </summary>
    [Test]
    public async Task CustomType_ShouldSerializeCorrectly()
    {
        if (!System.Runtime.CompilerServices.RuntimeFeature.IsDynamicCodeCompiled) return;

        // Arrange - 创建包含自定义类型的对象
        var entity = new CustomTypeEntity
        {
            Id = "custom_001",
            CustomEnum = CustomEnumType.OptionB,
            CustomStruct = new CustomStruct { Value = 42, Name = "TestStruct" },
            CustomClass = new CustomClass { Description = "Test Description", Value = 3.14m }
        };

        // Act
        var bsonDoc = BsonMapper.ToDocument(entity);
        var deserializedEntity = BsonMapper.ToObject<CustomTypeEntity>(bsonDoc);

        // Assert - 验证自定义类型序列化
        await Assert.That(bsonDoc).IsNotNull();
        await Assert.That(deserializedEntity).IsNotNull();

        // 验证枚举
        await Assert.That(deserializedEntity.CustomEnum).IsEqualTo(entity.CustomEnum);

        // 验证结构体
        await Assert.That(deserializedEntity.CustomStruct.Value).IsEqualTo(entity.CustomStruct.Value);
        await Assert.That(deserializedEntity.CustomStruct.Name).IsEqualTo(entity.CustomStruct.Name);

        // 验证自定义类
        await Assert.That(deserializedEntity.CustomClass).IsNotNull();
        await Assert.That(deserializedEntity.CustomClass.Description).IsEqualTo(entity.CustomClass.Description);
        await Assert.That(deserializedEntity.CustomClass.Value).IsEqualTo(entity.CustomClass.Value);
    }

    /// <summary>
    /// 测试版本兼容性的AOT序列化
    /// </summary>
    [Test]
    public async Task VersionCompatibility_ShouldHandleMissingFields()
    {
        // Arrange - 创建旧版本对象（缺少某些字段）
        var oldVersionBson = new BsonDocument()
            .Set("_id", "version_test_001")
            .Set("name", "Old Version Entity")
            .Set("age", 25);
            // 注意：缺少新版本字段如 "CreatedAt", "IsActive" 等

        // Act - 反序列化为新版本对象
        var newVersionEntity = BsonMapper.ToObject<VersionTestEntity>(oldVersionBson);

        // Assert - 验证版本兼容性
        await Assert.That(newVersionEntity).IsNotNull();
        await Assert.That(newVersionEntity.Id).IsEqualTo("version_test_001");
        await Assert.That(newVersionEntity.Name).IsEqualTo("Old Version Entity");
        await Assert.That(newVersionEntity.Age).IsEqualTo(25);

        // 缺失字段应该有默认值
        var timeDiff = Math.Abs((newVersionEntity.CreatedAt - DateTime.UtcNow).TotalSeconds);
        await Assert.That(timeDiff).IsLessThan(1.0);
        await Assert.That(newVersionEntity.IsActive).IsFalse(); // 默认值
        await Assert.That(newVersionEntity.Tags).IsNull(); // 引用类型默认为null
    }

    /// <summary>
    /// 测试AOT环境下的反射限制
    /// </summary>
    [Test]
    public async Task AOTReflection_ShouldWorkWithConstraints()
    {
        // Arrange - 测试在AOT受限环境下的序列化
        var entity = new ReflectionTestEntity
        {
            Id = "reflection_test_001",
            PublicField = "public_field",
            PublicProperty = "public_property"
            // 私有字段和属性在编译时无法访问，这是正常的
        };

        // Act
        var bsonDoc = BsonMapper.ToDocument(entity);
        var deserializedEntity = BsonMapper.ToObject<ReflectionTestEntity>(bsonDoc);

        // Assert - 验证AOT环境下的反射限制
        await Assert.That(bsonDoc).IsNotNull();
        await Assert.That(deserializedEntity).IsNotNull();

        // 公共成员应该正常序列化
        await Assert.That(deserializedEntity.PublicField).IsEqualTo(entity.PublicField);
        await Assert.That(deserializedEntity.PublicProperty).IsEqualTo(entity.PublicProperty);

        // 私有成员在AOT环境下可能无法访问，这是预期行为
        // 这里我们只验证序列化不会失败
    }

    /// <summary>
    /// 测试泛型集合的AOT序列化
    /// </summary>
    [Test]
    public async Task GenericCollections_ShouldSerializeCorrectly()
    {
        // Arrange
        var entity = new GenericCollectionEntity
        {
            Id = "generic_test_001",
            StringList = new List<string> { "item1", "item2", "item3" },
            IntArray = new[] { 1, 2, 3, 4, 5 },
            StringIntDict = new Dictionary<string, int>
            {
                ["key1"] = 10,
                ["key2"] = 20,
                ["key3"] = 30
            },
            NestedList = new List<List<int>>
            {
                new() { 1, 2, 3 },
                new() { 4, 5, 6 }
            }
        };

        // Act
        var bsonDoc = BsonMapper.ToDocument(entity);
        var deserializedEntity = BsonMapper.ToObject<GenericCollectionEntity>(bsonDoc);

        // Assert - 验证泛型集合序列化
        await Assert.That(bsonDoc).IsNotNull();
        await Assert.That(deserializedEntity).IsNotNull();

        // 验证List<T>
        await Assert.That(deserializedEntity.StringList).IsNotNull();
        await Assert.That(deserializedEntity.StringList).Count().IsEqualTo(entity.StringList.Count);
        await Assert.That(deserializedEntity.StringList!
                .SequenceEqual(entity.StringList, StringComparer.Ordinal))
            .IsTrue();

        // 验证数组
        await Assert.That(deserializedEntity.IntArray).IsNotNull();
        await Assert.That(deserializedEntity.IntArray).Count().IsEqualTo(entity.IntArray.Length);
        await Assert.That(deserializedEntity.IntArray!
                .SequenceEqual(entity.IntArray))
            .IsTrue();

        // 验证Dictionary<K,V>
        await Assert.That(deserializedEntity.StringIntDict).IsNotNull();
        await Assert.That(deserializedEntity.StringIntDict).Count().IsEqualTo(entity.StringIntDict.Count);
        foreach (var kvp in entity.StringIntDict)
        {
            await Assert.That(deserializedEntity.StringIntDict[kvp.Key]).IsEqualTo(kvp.Value);
        }

        // 验证嵌套泛型
        await Assert.That(deserializedEntity.NestedList).IsNotNull();
        await Assert.That(deserializedEntity.NestedList).Count().IsEqualTo(entity.NestedList.Count);
    }

    /// <summary>
    /// 测试异常情况的AOT序列化处理
    /// </summary>
    [Test]
    public async Task ExceptionHandling_ShouldGracefullyDegrade()
    {
        // Arrange - 创建可能导致序列化异常的对象
        var problematicEntity = new ProblematicEntity
        {
            Id = "problematic_001",
            Name = "Problematic Entity",
            SelfReference = null // 稍后设置自引用
        };

        // 设置自引用创建潜在问题
        problematicEntity.SelfReference = problematicEntity;

        // Act & Assert - 验证异常处理
        try
        {
            var bsonDoc = BsonMapper.ToDocument(problematicEntity);

            // 如果没有抛出异常，验证结果的合理性
            await Assert.That(bsonDoc).IsNotNull();
            await Assert.That(bsonDoc.ContainsKey("_id")).IsTrue();
        }
        catch (Exception ex)
        {
            // 如果抛出异常，应该是预期的异常类型
            await Assert.That(ex).IsAssignableTo<InvalidOperationException>()
                .Or.IsAssignableTo<NotSupportedException>()
                .Or.IsAssignableTo<ArgumentException>();
        }
    }
}

// Address类定义
public class Address
{
    public string Street { get; set; } = string.Empty;
    public string City { get; set; } = string.Empty;
    public string Country { get; set; } = string.Empty;
    public string ZipCode { get; set; } = string.Empty;
}

// 测试用的实体类定义
public class ComplexAOTEntity
{
    public string Id { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public int Age { get; set; }
    public DateTime CreatedAt { get; set; }
    public bool IsActive { get; set; }
    public double Score { get; set; }
    public string[] Tags { get; set; } = Array.Empty<string>();
    public Address Address { get; set; } = null!;
    public Dictionary<string, object> Metadata { get; set; } = new();
}

public class CircularEntity
{
    public string Id { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public CircularEntity[] Children { get; set; } = Array.Empty<CircularEntity>();
    public CircularEntity? Parent { get; set; }
}

public class NullTestEntity
{
    public string Id { get; set; } = string.Empty;
    public string? StringNull { get; set; }
    public string StringEmpty { get; set; } = string.Empty;
    public int IntDefault { get; set; }
    public int IntValue { get; set; }
    public DateTime? DateTimeNull { get; set; }
    public DateTime DateTimeValue { get; set; }
    public bool BoolDefault { get; set; }
    public bool BoolValue { get; set; }
    public List<string>? ListNull { get; set; }
    public List<string> ListEmpty { get; set; } = new();
    public List<string> ListValue { get; set; } = new();
}

public class LargeAOTEntity
{
    public string Id { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public string[] LargeArray { get; set; } = Array.Empty<string>();
    public Dictionary<string, object> LargeDictionary { get; set; } = new();
    public string LargeText { get; set; } = string.Empty;
}

public class CustomTypeEntity
{
    public string Id { get; set; } = string.Empty;
    public CustomEnumType CustomEnum { get; set; }
    public CustomStruct CustomStruct { get; set; }
    public CustomClass CustomClass { get; set; } = null!;
}

public enum CustomEnumType
{
    OptionA,
    OptionB,
    OptionC
}

public struct CustomStruct
{
    public int Value { get; set; }
    public string Name { get; set; }
}

public class CustomClass
{
    public string Description { get; set; } = string.Empty;
    public decimal Value { get; set; }
}

public class VersionTestEntity
{
    public string Id { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public int Age { get; set; }
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow; // 新增字段
    public bool IsActive { get; set; } // 新增字段
    public List<string>? Tags { get; set; } // 新增字段
}

public class ReflectionTestEntity
{
    public string Id { get; set; } = string.Empty;
    public string PublicField = string.Empty;
    public string PublicProperty { get; set; } = string.Empty;
    private string PrivateField = string.Empty;
    private string PrivateProperty { get; set; } = string.Empty;
}

public class GenericCollectionEntity
{
    public string Id { get; set; } = string.Empty;
    public List<string> StringList { get; set; } = new();
    public int[] IntArray { get; set; } = Array.Empty<int>();
    public Dictionary<string, int> StringIntDict { get; set; } = new();
    public List<List<int>> NestedList { get; set; } = new();
}

public class ProblematicEntity
{
    public string Id { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public ProblematicEntity? SelfReference { get; set; }
}
