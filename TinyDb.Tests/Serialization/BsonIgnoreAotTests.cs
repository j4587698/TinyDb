using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using TinyDb.Serialization;
using TinyDb.Bson;
using TinyDb.Attributes;
using TinyDb.Core;
using TinyDb.Tests.TestEntities;
using TUnit.Core;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Serialization;

/// <summary>
/// BsonIgnore 属性的 AOT 兼容测试
/// 使用带有 [Entity] 特性的顶层类，通过源生成器生成序列化代码
/// </summary>
public class BsonIgnoreAotTests
{
    #region Basic Tests with Source Generator

    [Test]
    public async Task ToDocument_UserWithIgnoredFields_ShouldNotContainIgnoredProperties()
    {
        var entity = new UserWithIgnoredFields
        {
            Username = "testuser",
            Email = "test@example.com",
            IsActive = true,
            Password = "secret_password",
            TempToken = "temp_abc123"
        };

        var doc = AotBsonMapper.ToDocument(entity);

        await Assert.That(doc.ContainsKey("_id")).IsTrue();
        await Assert.That(doc.ContainsKey("username")).IsTrue();
        await Assert.That(doc.ContainsKey("email")).IsTrue();
        await Assert.That(doc.ContainsKey("isActive")).IsTrue();
        await Assert.That(doc.ContainsKey("password")).IsFalse();
        await Assert.That(doc.ContainsKey("tempToken")).IsFalse();
    }

    [Test]
    public async Task FromDocument_UserWithIgnoredFields_ShouldNotPopulateIgnoredProperties()
    {
        var doc = new BsonDocument()
            .Set("_id", new BsonObjectId(ObjectId.NewObjectId()))
            .Set("username", new BsonString("testuser"))
            .Set("email", new BsonString("test@example.com"))
            .Set("isActive", new BsonBoolean(true))
            .Set("password", new BsonString("injected_password")) // Trying to inject
            .Set("tempToken", new BsonString("injected_token"));

        var entity = AotBsonMapper.FromDocument<UserWithIgnoredFields>(doc);

        await Assert.That(entity.Username).IsEqualTo("testuser");
        await Assert.That(entity.Email).IsEqualTo("test@example.com");
        await Assert.That(entity.IsActive).IsTrue();
        // Ignored properties should have default values
        await Assert.That(entity.Password).IsEqualTo("");
        await Assert.That(entity.TempToken).IsEqualTo("");
    }

    #endregion

    #region Computed Property Tests

    [Test]
    public async Task ToDocument_ProductWithComputedFields_ShouldNotContainComputedProperties()
    {
        var entity = new ProductWithIgnoredComputed
        {
            Name = "Test Product",
            Price = 100.0m,
            Quantity = 5
        };

        var doc = AotBsonMapper.ToDocument(entity);

        await Assert.That(doc.ContainsKey("_id")).IsTrue();
        await Assert.That(doc.ContainsKey("name")).IsTrue();
        await Assert.That(doc.ContainsKey("price")).IsTrue();
        await Assert.That(doc.ContainsKey("quantity")).IsTrue();
        await Assert.That(doc.ContainsKey("totalValue")).IsFalse();
        await Assert.That(doc.ContainsKey("displayInfo")).IsFalse();
    }

    [Test]
    public async Task RoundTrip_ProductWithComputedFields_ComputedPropertiesShouldWork()
    {
        var original = new ProductWithIgnoredComputed
        {
            Name = "Test Product",
            Price = 100.0m,
            Quantity = 5
        };

        // Verify computed properties work before serialization
        await Assert.That(original.TotalValue).IsEqualTo(500.0m);
        await Assert.That(original.DisplayInfo).IsEqualTo("Test Product x 5");

        var doc = AotBsonMapper.ToDocument(original);
        var restored = AotBsonMapper.FromDocument<ProductWithIgnoredComputed>(doc);

        // Verify basic properties are restored
        await Assert.That(restored.Name).IsEqualTo(original.Name);
        await Assert.That(restored.Price).IsEqualTo(original.Price);
        await Assert.That(restored.Quantity).IsEqualTo(original.Quantity);
        
        // Verify computed properties still work after deserialization
        await Assert.That(restored.TotalValue).IsEqualTo(500.0m);
        await Assert.That(restored.DisplayInfo).IsEqualTo("Test Product x 5");
    }

    #endregion

    #region Cache Property Tests

    [Test]
    public async Task ToDocument_ArticleWithCache_ShouldNotContainCacheProperties()
    {
        var entity = new ArticleWithIgnoredCache
        {
            Title = "Test Article",
            Content = "Article content here...",
            CachedWordCount = 100,
            CacheTimestamp = DateTime.Now
        };

        var doc = AotBsonMapper.ToDocument(entity);

        await Assert.That(doc.ContainsKey("_id")).IsTrue();
        await Assert.That(doc.ContainsKey("title")).IsTrue();
        await Assert.That(doc.ContainsKey("content")).IsTrue();
        await Assert.That(doc.ContainsKey("createdAt")).IsTrue();
        await Assert.That(doc.ContainsKey("cachedWordCount")).IsFalse();
        await Assert.That(doc.ContainsKey("cacheTimestamp")).IsFalse();
    }

    [Test]
    public async Task FromDocument_ArticleWithCache_ShouldHaveDefaultCacheValues()
    {
        var doc = new BsonDocument()
            .Set("_id", new BsonObjectId(ObjectId.NewObjectId()))
            .Set("title", new BsonString("Test Article"))
            .Set("content", new BsonString("Content"))
            .Set("createdAt", new BsonDateTime(DateTime.UtcNow))
            .Set("cachedWordCount", new BsonInt32(999)) // Trying to set cache
            .Set("cacheTimestamp", new BsonDateTime(DateTime.Now));

        var entity = AotBsonMapper.FromDocument<ArticleWithIgnoredCache>(doc);

        await Assert.That(entity.Title).IsEqualTo("Test Article");
        await Assert.That(entity.Content).IsEqualTo("Content");
        // Cache properties should have default values
        await Assert.That(entity.CachedWordCount).IsEqualTo(0);
        await Assert.That(entity.CacheTimestamp).IsNull();
    }

    #endregion

    #region Complex Type Ignore Tests

    [Test]
    public async Task ToDocument_EntityWithIgnoredComplex_ShouldNotContainIgnoredComplexProperties()
    {
        var entity = new EntityWithIgnoredComplex
        {
            Name = "Test Entity",
            IgnoredList = new List<string> { "a", "b", "c" },
            IgnoredDict = new Dictionary<string, int> { { "key", 100 } }
        };

        var doc = AotBsonMapper.ToDocument(entity);

        await Assert.That(doc.ContainsKey("_id")).IsTrue();
        await Assert.That(doc.ContainsKey("name")).IsTrue();
        await Assert.That(doc.ContainsKey("ignoredList")).IsFalse();
        await Assert.That(doc.ContainsKey("ignoredDict")).IsFalse();
    }

    #endregion

    #region Database Integration Tests

    [Test]
    public async Task Database_UserWithIgnoredFields_RoundTrip()
    {
        const string dbPath = "bson_ignore_aot_test.db";
        if (File.Exists(dbPath)) File.Delete(dbPath);

        try
        {
            using var engine = new TinyDbEngine(dbPath);
            var users = engine.GetCollection<UserWithIgnoredFields>();

            var original = new UserWithIgnoredFields
            {
                Username = "dbuser",
                Email = "db@test.com",
                IsActive = true,
                Password = "db_secret_password",
                TempToken = "db_temp_token"
            };

            var insertedId = users.Insert(original);
            var loaded = users.FindById(insertedId);

            await Assert.That(loaded).IsNotNull();
            await Assert.That(loaded!.Username).IsEqualTo("dbuser");
            await Assert.That(loaded.Email).IsEqualTo("db@test.com");
            await Assert.That(loaded.IsActive).IsTrue();
            // Ignored fields should have default values
            await Assert.That(loaded.Password).IsEqualTo("");
            await Assert.That(loaded.TempToken).IsEqualTo("");
        }
        finally
        {
            if (File.Exists(dbPath)) File.Delete(dbPath);
        }
    }

    [Test]
    public async Task Database_ProductWithComputedFields_RoundTrip()
    {
        const string dbPath = "bson_ignore_computed_aot_test.db";
        if (File.Exists(dbPath)) File.Delete(dbPath);

        try
        {
            using var engine = new TinyDbEngine(dbPath);
            var products = engine.GetCollection<ProductWithIgnoredComputed>();

            var original = new ProductWithIgnoredComputed
            {
                Name = "DB Product",
                Price = 50.0m,
                Quantity = 10
            };

            var insertedId = products.Insert(original);
            var loaded = products.FindById(insertedId);

            await Assert.That(loaded).IsNotNull();
            await Assert.That(loaded!.Name).IsEqualTo("DB Product");
            await Assert.That(loaded.Price).IsEqualTo(50.0m);
            await Assert.That(loaded.Quantity).IsEqualTo(10);
            // Computed properties should still work
            await Assert.That(loaded.TotalValue).IsEqualTo(500.0m);
            await Assert.That(loaded.DisplayInfo).IsEqualTo("DB Product x 10");
        }
        finally
        {
            if (File.Exists(dbPath)) File.Delete(dbPath);
        }
    }

    #endregion

    #region Key Count Tests

    [Test]
    public async Task ToDocument_UserWithIgnoredFields_ShouldHaveCorrectKeyCount()
    {
        var entity = new UserWithIgnoredFields
        {
            Username = "test",
            Email = "test@test.com",
            IsActive = true,
            Password = "secret",
            TempToken = "token"
        };

        var doc = AotBsonMapper.ToDocument(entity);

        // Should have: _id, _collection, username, email, isActive (5 keys)
        // Should NOT have: password, tempToken
        await Assert.That(doc.Keys.Count).IsEqualTo(5);
    }

    [Test]
    public async Task ToDocument_ProductWithComputedFields_ShouldHaveCorrectKeyCount()
    {
        var entity = new ProductWithIgnoredComputed
        {
            Name = "Product",
            Price = 100.0m,
            Quantity = 5
        };

        var doc = AotBsonMapper.ToDocument(entity);

        // Should have: _id, _collection, name, price, quantity (5 keys)
        // Should NOT have: totalValue, displayInfo
        await Assert.That(doc.Keys.Count).IsEqualTo(5);
    }

    #endregion
}
