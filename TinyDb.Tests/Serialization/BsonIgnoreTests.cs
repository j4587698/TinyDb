using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using TinyDb.Serialization;
using TinyDb.Bson;
using TinyDb.Attributes;
using TUnit.Core;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TinyDb.Tests.Utils;

namespace TinyDb.Tests.Serialization;

/// <summary>
/// BsonIgnore 属性的全面测试，验证序列化时忽略指定字段
/// </summary>
[SkipInAot("These tests use reflection-based fallback paths")]
public class BsonIgnoreTests
{
    #region Test Entities

    /// <summary>
    /// 基本的忽略属性测试实体
    /// </summary>
    internal class BasicIgnoreEntity
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
        
        [BsonIgnore]
        public string IgnoredProperty { get; set; } = "";
        
        [BsonIgnore]
        public int IgnoredNumber { get; set; }
    }

    /// <summary>
    /// 多个忽略属性的实体
    /// </summary>
    internal class MultipleIgnoreEntity
    {
        public int Id { get; set; }
        public string VisibleName { get; set; } = "";
        
        [BsonIgnore]
        public string Ignored1 { get; set; } = "";
        
        [BsonIgnore]
        public string Ignored2 { get; set; } = "";
        
        [BsonIgnore]
        public int Ignored3 { get; set; }
        
        public decimal VisiblePrice { get; set; }
    }

    /// <summary>
    /// 忽略复杂类型的实体
    /// </summary>
    internal class IgnoreComplexTypeEntity
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
        
        [BsonIgnore]
        public NestedObject? IgnoredNested { get; set; }
        
        [BsonIgnore]
        public List<string>? IgnoredList { get; set; }
        
        [BsonIgnore]
        public Dictionary<string, int>? IgnoredDict { get; set; }
    }

    internal class NestedObject
    {
        public string Value { get; set; } = "";
        public int Number { get; set; }
    }

    /// <summary>
    /// 忽略可空类型的实体
    /// </summary>
    internal class IgnoreNullableEntity
    {
        public int Id { get; set; }
        
        [BsonIgnore]
        public int? IgnoredNullableInt { get; set; }
        
        [BsonIgnore]
        public DateTime? IgnoredNullableDateTime { get; set; }
        
        [BsonIgnore]
        public bool? IgnoredNullableBool { get; set; }
        
        public string? VisibleNullableString { get; set; }
    }

    /// <summary>
    /// 忽略计算属性的实体
    /// </summary>
    internal class IgnoreComputedPropertyEntity
    {
        public int Id { get; set; }
        public decimal Price { get; set; }
        public int Quantity { get; set; }
        
        /// <summary>
        /// 计算属性，不应该被序列化
        /// </summary>
        [BsonIgnore]
        public decimal TotalPrice => Price * Quantity;
        
        /// <summary>
        /// 另一个计算属性
        /// </summary>
        [BsonIgnore]
        public string DisplayName => $"Item #{Id}";
    }

    /// <summary>
    /// 忽略敏感数据的实体
    /// </summary>
    internal class IgnoreSensitiveDataEntity
    {
        public int Id { get; set; }
        public string Username { get; set; } = "";
        
        [BsonIgnore]
        public string Password { get; set; } = "";
        
        [BsonIgnore]
        public string SecurityToken { get; set; } = "";
        
        public string Email { get; set; } = "";
    }

    /// <summary>
    /// 忽略缓存属性的实体
    /// </summary>
    internal class IgnoreCacheEntity
    {
        public int Id { get; set; }
        public string Data { get; set; } = "";
        
        [BsonIgnore]
        public object? CachedResult { get; set; }
        
        [BsonIgnore]
        public DateTime? CacheTimestamp { get; set; }
    }

    /// <summary>
    /// 带有公共字段和BsonIgnore的实体
    /// </summary>
    internal class IgnoreFieldEntity
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
        
        [BsonIgnore]
        public string IgnoredField { get; set; } = "";
    }

    #endregion

    #region Basic BsonIgnore Tests

    [Test]
    public async Task ToDocument_BasicIgnore_ShouldNotContainIgnoredProperty()
    {
        var entity = new BasicIgnoreEntity
        {
            Id = 1,
            Name = "Test",
            IgnoredProperty = "Should be ignored",
            IgnoredNumber = 999
        };

        var doc = AotBsonMapper.ToDocument(entity);

        await Assert.That(doc.ContainsKey("_id")).IsTrue();
        await Assert.That(doc.ContainsKey("name")).IsTrue();
        await Assert.That(doc.ContainsKey("ignoredProperty")).IsFalse();
        await Assert.That(doc.ContainsKey("ignoredNumber")).IsFalse();
        await Assert.That(doc["_id"].ToInt32(null)).IsEqualTo(1);
        await Assert.That(doc["name"].ToString()).IsEqualTo("Test");
    }

    [Test]
    public async Task FromDocument_BasicIgnore_ShouldLeaveIgnoredPropertyDefault()
    {
        var doc = new BsonDocument()
            .Set("_id", new BsonInt32(1))
            .Set("name", new BsonString("Test"))
            .Set("ignoredProperty", new BsonString("Trying to set ignored"))
            .Set("ignoredNumber", new BsonInt32(999));

        var entity = AotBsonMapper.FromDocument<BasicIgnoreEntity>(doc);

        await Assert.That(entity.Id).IsEqualTo(1);
        await Assert.That(entity.Name).IsEqualTo("Test");
        // Ignored properties should have default values
        await Assert.That(entity.IgnoredProperty).IsEqualTo("");
        await Assert.That(entity.IgnoredNumber).IsEqualTo(0);
    }

    #endregion

    #region Multiple Ignore Tests

    [Test]
    public async Task ToDocument_MultipleIgnore_ShouldNotContainAnyIgnoredProperties()
    {
        var entity = new MultipleIgnoreEntity
        {
            Id = 1,
            VisibleName = "Visible",
            Ignored1 = "Ignored Value 1",
            Ignored2 = "Ignored Value 2",
            Ignored3 = 123,
            VisiblePrice = 99.99m
        };

        var doc = AotBsonMapper.ToDocument(entity);

        await Assert.That(doc.ContainsKey("_id")).IsTrue();
        await Assert.That(doc.ContainsKey("visibleName")).IsTrue();
        await Assert.That(doc.ContainsKey("visiblePrice")).IsTrue();
        await Assert.That(doc.ContainsKey("ignored1")).IsFalse();
        await Assert.That(doc.ContainsKey("ignored2")).IsFalse();
        await Assert.That(doc.ContainsKey("ignored3")).IsFalse();
    }

    #endregion

    #region Complex Type Ignore Tests

    [Test]
    public async Task ToDocument_IgnoreComplexType_ShouldNotContainIgnoredComplexProperties()
    {
        var entity = new IgnoreComplexTypeEntity
        {
            Id = 1,
            Name = "Test",
            IgnoredNested = new NestedObject { Value = "Nested", Number = 42 },
            IgnoredList = new List<string> { "a", "b", "c" },
            IgnoredDict = new Dictionary<string, int> { { "key", 100 } }
        };

        var doc = AotBsonMapper.ToDocument(entity);

        await Assert.That(doc.ContainsKey("_id")).IsTrue();
        await Assert.That(doc.ContainsKey("name")).IsTrue();
        await Assert.That(doc.ContainsKey("ignoredNested")).IsFalse();
        await Assert.That(doc.ContainsKey("ignoredList")).IsFalse();
        await Assert.That(doc.ContainsKey("ignoredDict")).IsFalse();
    }

    #endregion

    #region Nullable Ignore Tests

    [Test]
    public async Task ToDocument_IgnoreNullable_ShouldNotContainIgnoredNullableProperties()
    {
        var entity = new IgnoreNullableEntity
        {
            Id = 1,
            IgnoredNullableInt = 42,
            IgnoredNullableDateTime = DateTime.Now,
            IgnoredNullableBool = true,
            VisibleNullableString = "Visible"
        };

        var doc = AotBsonMapper.ToDocument(entity);

        await Assert.That(doc.ContainsKey("_id")).IsTrue();
        await Assert.That(doc.ContainsKey("visibleNullableString")).IsTrue();
        await Assert.That(doc.ContainsKey("ignoredNullableInt")).IsFalse();
        await Assert.That(doc.ContainsKey("ignoredNullableDateTime")).IsFalse();
        await Assert.That(doc.ContainsKey("ignoredNullableBool")).IsFalse();
    }

    #endregion

    #region Computed Property Ignore Tests

    [Test]
    public async Task ToDocument_ComputedProperty_ShouldNotSerializeComputedProperty()
    {
        var entity = new IgnoreComputedPropertyEntity
        {
            Id = 1,
            Price = 10.0m,
            Quantity = 5
        };

        var doc = AotBsonMapper.ToDocument(entity);

        await Assert.That(doc.ContainsKey("_id")).IsTrue();
        await Assert.That(doc.ContainsKey("price")).IsTrue();
        await Assert.That(doc.ContainsKey("quantity")).IsTrue();
        await Assert.That(doc.ContainsKey("totalPrice")).IsFalse();
        await Assert.That(doc.ContainsKey("displayName")).IsFalse();
    }

    #endregion

    #region Sensitive Data Ignore Tests

    [Test]
    public async Task ToDocument_SensitiveData_ShouldNotSerializeSensitiveProperties()
    {
        var entity = new IgnoreSensitiveDataEntity
        {
            Id = 1,
            Username = "user123",
            Password = "secret_password",
            SecurityToken = "abc123token",
            Email = "user@example.com"
        };

        var doc = AotBsonMapper.ToDocument(entity);

        await Assert.That(doc.ContainsKey("_id")).IsTrue();
        await Assert.That(doc.ContainsKey("username")).IsTrue();
        await Assert.That(doc.ContainsKey("email")).IsTrue();
        await Assert.That(doc.ContainsKey("password")).IsFalse();
        await Assert.That(doc.ContainsKey("securityToken")).IsFalse();
    }

    [Test]
    public async Task FromDocument_SensitiveData_ShouldNotDeserializeSensitiveProperties()
    {
        var doc = new BsonDocument()
            .Set("_id", new BsonInt32(1))
            .Set("username", new BsonString("user123"))
            .Set("password", new BsonString("hacked_password")) // Attacker trying to inject
            .Set("securityToken", new BsonString("fake_token"))
            .Set("email", new BsonString("user@example.com"));

        var entity = AotBsonMapper.FromDocument<IgnoreSensitiveDataEntity>(doc);

        await Assert.That(entity.Id).IsEqualTo(1);
        await Assert.That(entity.Username).IsEqualTo("user123");
        await Assert.That(entity.Email).IsEqualTo("user@example.com");
        // Ignored properties should have default values
        await Assert.That(entity.Password).IsEqualTo("");
        await Assert.That(entity.SecurityToken).IsEqualTo("");
    }

    #endregion

    #region Cache Property Ignore Tests

    [Test]
    public async Task ToDocument_CacheProperties_ShouldNotSerializeCacheProperties()
    {
        var entity = new IgnoreCacheEntity
        {
            Id = 1,
            Data = "Important Data",
            CachedResult = new { Temp = "Value" },
            CacheTimestamp = DateTime.Now
        };

        var doc = AotBsonMapper.ToDocument(entity);

        await Assert.That(doc.ContainsKey("_id")).IsTrue();
        await Assert.That(doc.ContainsKey("data")).IsTrue();
        await Assert.That(doc.ContainsKey("cachedResult")).IsFalse();
        await Assert.That(doc.ContainsKey("cacheTimestamp")).IsFalse();
    }

    #endregion

    #region Round Trip Tests

    [Test]
    public async Task RoundTrip_BasicIgnore_ShouldPreserveVisibleProperties()
    {
        var original = new BasicIgnoreEntity
        {
            Id = 42,
            Name = "Round Trip Test",
            IgnoredProperty = "Will be lost",
            IgnoredNumber = 999
        };

        var doc = AotBsonMapper.ToDocument(original);
        var restored = AotBsonMapper.FromDocument<BasicIgnoreEntity>(doc);

        await Assert.That(restored.Id).IsEqualTo(original.Id);
        await Assert.That(restored.Name).IsEqualTo(original.Name);
        // Ignored properties should have default values after round trip
        await Assert.That(restored.IgnoredProperty).IsEqualTo("");
        await Assert.That(restored.IgnoredNumber).IsEqualTo(0);
    }

    [Test]
    public async Task RoundTrip_MultipleIgnore_ShouldPreserveVisibleProperties()
    {
        var original = new MultipleIgnoreEntity
        {
            Id = 100,
            VisibleName = "Visible Name",
            Ignored1 = "Lost 1",
            Ignored2 = "Lost 2",
            Ignored3 = 123,
            VisiblePrice = 199.99m
        };

        var doc = AotBsonMapper.ToDocument(original);
        var restored = AotBsonMapper.FromDocument<MultipleIgnoreEntity>(doc);

        await Assert.That(restored.Id).IsEqualTo(original.Id);
        await Assert.That(restored.VisibleName).IsEqualTo(original.VisibleName);
        await Assert.That(restored.VisiblePrice).IsEqualTo(original.VisiblePrice);
        // All ignored properties should have default values
        await Assert.That(restored.Ignored1).IsEqualTo("");
        await Assert.That(restored.Ignored2).IsEqualTo("");
        await Assert.That(restored.Ignored3).IsEqualTo(0);
    }

    #endregion

    #region Document Key Count Tests

    [Test]
    public async Task ToDocument_BasicIgnore_ShouldHaveCorrectKeyCount()
    {
        var entity = new BasicIgnoreEntity
        {
            Id = 1,
            Name = "Test",
            IgnoredProperty = "Ignored",
            IgnoredNumber = 999
        };

        var doc = AotBsonMapper.ToDocument(entity);

        // Should only have _id and name keys (2 keys)
        await Assert.That(doc.Keys.Count).IsEqualTo(2);
    }

    [Test]
    public async Task ToDocument_MultipleIgnore_ShouldHaveCorrectKeyCount()
    {
        var entity = new MultipleIgnoreEntity
        {
            Id = 1,
            VisibleName = "Visible",
            Ignored1 = "Ignored",
            Ignored2 = "Ignored",
            Ignored3 = 123,
            VisiblePrice = 99.99m
        };

        var doc = AotBsonMapper.ToDocument(entity);

        // Should only have _id, visibleName, visiblePrice keys (3 keys)
        await Assert.That(doc.Keys.Count).IsEqualTo(3);
    }

    #endregion
}
