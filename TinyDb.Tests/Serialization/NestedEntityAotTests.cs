using TinyDb.Attributes;
using TinyDb.Bson;
using TinyDb.Serialization;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Serialization;

/// <summary>
/// 测试嵌套类 Entity 的 AOT 源代码生成器支持
/// </summary>
public class NestedEntityAotTests
{
    #region 测试用嵌套实体类定义

    /// <summary>
    /// 外部容器类 - 单层嵌套测试
    /// </summary>
    public class SingleLevelContainer
    {
        /// <summary>
        /// 单层嵌套的 Entity 类
        /// </summary>
        [Entity("single_nested_items")]
        public class NestedItem
        {
            public int Id { get; set; }
            public string Name { get; set; } = string.Empty;
            public decimal Price { get; set; }
        }
    }

    /// <summary>
    /// 外部容器类 - 多层嵌套测试
    /// </summary>
    public class MultiLevelContainer
    {
        public class MiddleContainer
        {
            /// <summary>
            /// 多层嵌套的 Entity 类
            /// </summary>
            [Entity("deeply_nested_items")]
            public class DeeplyNestedItem
            {
                public int Id { get; set; }
                public string Description { get; set; } = string.Empty;
                public DateTime CreatedAt { get; set; }
            }
        }
    }

    /// <summary>
    /// 包含复杂类型属性的嵌套 Entity 测试
    /// </summary>
    public class ComplexNestedContainer
    {
        /// <summary>
        /// 嵌套的复杂类型（非 Entity）
        /// </summary>
        public class AddressInfo
        {
            public string Street { get; set; } = string.Empty;
            public string City { get; set; } = string.Empty;
            public string ZipCode { get; set; } = string.Empty;
        }

        /// <summary>
        /// 包含嵌套复杂类型的嵌套 Entity
        /// </summary>
        [Entity("nested_customers")]
        public class NestedCustomer
        {
            public int Id { get; set; }
            public string Name { get; set; } = string.Empty;
            public AddressInfo? Address { get; set; }
        }
    }

    #endregion

    #region 单层嵌套 Entity 测试

    [Test]
    public async Task SingleNestedEntity_Should_Have_AotHelper_Generated()
    {
        // 验证 AOT 帮助器类已生成 - 通过 AotHelperRegistry 检查
        var registered = AotHelperRegistry.TryGetAdapter<SingleLevelContainer.NestedItem>(out _);
        await Assert.That(registered).IsTrue();
    }

    [Test]
    public async Task SingleNestedEntity_ToDocument_Should_Serialize_Correctly()
    {
        var item = new SingleLevelContainer.NestedItem
        {
            Id = 1,
            Name = "Test Item",
            Price = 99.99m
        };

        var doc = AotBsonMapper.ToDocument(item);

        await Assert.That(doc).IsNotNull();
        // Id 字段被序列化为 _id（BSON 标准）
        await Assert.That(doc["_id"].ToInt32(null)).IsEqualTo(1);
        await Assert.That(doc["name"].ToString()).IsEqualTo("Test Item");
        await Assert.That(doc["price"].ToDecimal(null)).IsEqualTo(99.99m);
    }

    [Test]
    public async Task SingleNestedEntity_FromDocument_Should_Deserialize_Correctly()
    {
        // 使用 Set 方法创建不可变文档
        var doc = new BsonDocument()
            .Set("_id", new BsonInt32(2))
            .Set("name", new BsonString("Deserialized Item"))
            .Set("price", new BsonDecimal128(199.99m));

        var item = AotBsonMapper.FromDocument<SingleLevelContainer.NestedItem>(doc);

        await Assert.That(item).IsNotNull();
        await Assert.That(item!.Id).IsEqualTo(2);
        await Assert.That(item.Name).IsEqualTo("Deserialized Item");
        await Assert.That(item.Price).IsEqualTo(199.99m);
    }

    [Test]
    public async Task SingleNestedEntity_GetId_Should_Work()
    {
        var item = new SingleLevelContainer.NestedItem { Id = 42, Name = "Test" };

        var id = AotIdAccessor<SingleLevelContainer.NestedItem>.GetId(item);

        await Assert.That(id).IsNotNull();
        await Assert.That(id.ToInt32(null)).IsEqualTo(42);
    }

    [Test]
    public async Task SingleNestedEntity_SetId_Should_Work()
    {
        var item = new SingleLevelContainer.NestedItem { Name = "Test" };

        AotIdAccessor<SingleLevelContainer.NestedItem>.SetId(item, new BsonInt32(100));

        await Assert.That(item.Id).IsEqualTo(100);
    }

    #endregion

    #region 多层嵌套 Entity 测试

    [Test]
    public async Task DeeplyNestedEntity_Should_Have_AotHelper_Generated()
    {
        // 验证 AOT 帮助器类已生成
        var registered = AotHelperRegistry.TryGetAdapter<MultiLevelContainer.MiddleContainer.DeeplyNestedItem>(out _);
        await Assert.That(registered).IsTrue();
    }

    [Test]
    public async Task DeeplyNestedEntity_ToDocument_Should_Serialize_Correctly()
    {
        var item = new MultiLevelContainer.MiddleContainer.DeeplyNestedItem
        {
            Id = 1,
            Description = "Deep nested item",
            CreatedAt = new DateTime(2024, 1, 15, 10, 30, 0)
        };

        var doc = AotBsonMapper.ToDocument(item);

        await Assert.That(doc).IsNotNull();
        // Id 字段被序列化为 _id（BSON 标准）
        await Assert.That(doc["_id"].ToInt32(null)).IsEqualTo(1);
        await Assert.That(doc["description"].ToString()).IsEqualTo("Deep nested item");
        await Assert.That(doc["createdAt"].ToDateTime(null)).IsEqualTo(new DateTime(2024, 1, 15, 10, 30, 0));
    }

    [Test]
    public async Task DeeplyNestedEntity_FromDocument_Should_Deserialize_Correctly()
    {
        // 使用 Set 方法创建不可变文档
        var doc = new BsonDocument()
            .Set("_id", new BsonInt32(5))
            .Set("description", new BsonString("From document"))
            .Set("createdAt", new BsonDateTime(new DateTime(2024, 6, 20)));

        var item = AotBsonMapper.FromDocument<MultiLevelContainer.MiddleContainer.DeeplyNestedItem>(doc);

        await Assert.That(item).IsNotNull();
        await Assert.That(item!.Id).IsEqualTo(5);
        await Assert.That(item.Description).IsEqualTo("From document");
        await Assert.That(item.CreatedAt).IsEqualTo(new DateTime(2024, 6, 20));
    }

    [Test]
    public async Task DeeplyNestedEntity_GetId_Should_Work()
    {
        var item = new MultiLevelContainer.MiddleContainer.DeeplyNestedItem
        {
            Id = 77,
            Description = "Test"
        };

        var id = AotIdAccessor<MultiLevelContainer.MiddleContainer.DeeplyNestedItem>.GetId(item);

        await Assert.That(id).IsNotNull();
        await Assert.That(id.ToInt32(null)).IsEqualTo(77);
    }

    #endregion

    #region 包含复杂类型的嵌套 Entity 测试

    [Test]
    public async Task NestedEntityWithComplexType_Should_Have_AotHelper_Generated()
    {
        var registered = AotHelperRegistry.TryGetAdapter<ComplexNestedContainer.NestedCustomer>(out _);
        await Assert.That(registered).IsTrue();
    }

    [Test]
    public async Task NestedEntityWithComplexType_ToDocument_Should_Serialize_ComplexProperty()
    {
        var customer = new ComplexNestedContainer.NestedCustomer
        {
            Id = 1,
            Name = "John Doe",
            Address = new ComplexNestedContainer.AddressInfo
            {
                Street = "123 Main St",
                City = "New York",
                ZipCode = "10001"
            }
        };

        var doc = AotBsonMapper.ToDocument(customer);

        await Assert.That(doc).IsNotNull();
        // Id 字段被序列化为 _id（BSON 标准）
        await Assert.That(doc["_id"].ToInt32(null)).IsEqualTo(1);
        await Assert.That(doc["name"].ToString()).IsEqualTo("John Doe");
        await Assert.That(doc["address"]).IsNotNull();
        await Assert.That(doc["address"].IsDocument).IsTrue();

        var addressDoc = (BsonDocument)doc["address"];
        await Assert.That(addressDoc["street"].ToString()).IsEqualTo("123 Main St");
        await Assert.That(addressDoc["city"].ToString()).IsEqualTo("New York");
        await Assert.That(addressDoc["zipCode"].ToString()).IsEqualTo("10001");
    }

    [Test]
    public async Task NestedEntityWithComplexType_FromDocument_Should_Deserialize_ComplexProperty()
    {
        // 使用 Set 方法创建不可变文档
        var addressDoc = new BsonDocument()
            .Set("street", new BsonString("456 Oak Ave"))
            .Set("city", new BsonString("Los Angeles"))
            .Set("zipCode", new BsonString("90001"));

        var doc = new BsonDocument()
            .Set("_id", new BsonInt32(2))
            .Set("name", new BsonString("Jane Smith"))
            .Set("address", addressDoc);

        var customer = AotBsonMapper.FromDocument<ComplexNestedContainer.NestedCustomer>(doc);

        await Assert.That(customer).IsNotNull();
        await Assert.That(customer!.Id).IsEqualTo(2);
        await Assert.That(customer.Name).IsEqualTo("Jane Smith");
        await Assert.That(customer.Address).IsNotNull();
        await Assert.That(customer.Address!.Street).IsEqualTo("456 Oak Ave");
        await Assert.That(customer.Address.City).IsEqualTo("Los Angeles");
        await Assert.That(customer.Address.ZipCode).IsEqualTo("90001");
    }

    [Test]
    public async Task NestedEntityWithNullComplexType_Should_Handle_Null()
    {
        var customer = new ComplexNestedContainer.NestedCustomer
        {
            Id = 3,
            Name = "No Address",
            Address = null
        };

        var doc = AotBsonMapper.ToDocument(customer);

        await Assert.That(doc).IsNotNull();
        // Id 字段被序列化为 _id（BSON 标准）
        await Assert.That(doc["_id"].ToInt32(null)).IsEqualTo(3);
        await Assert.That(doc["name"].ToString()).IsEqualTo("No Address");
        // Address 应该是 null 或不存在
        await Assert.That(doc["address"].IsNull || !doc.ContainsKey("address")).IsTrue();
    }

    #endregion

    #region AotHelperRegistry 注册测试

    [Test]
    public async Task NestedEntities_Should_Be_Registered_In_AotHelperRegistry()
    {
        // 单层嵌套
        var singleNestedRegistered = AotHelperRegistry.TryGetAdapter<SingleLevelContainer.NestedItem>(out _);
        await Assert.That(singleNestedRegistered).IsTrue();

        // 多层嵌套
        var deeplyNestedRegistered = AotHelperRegistry.TryGetAdapter<MultiLevelContainer.MiddleContainer.DeeplyNestedItem>(out _);
        await Assert.That(deeplyNestedRegistered).IsTrue();

        // 包含复杂类型的嵌套
        var complexNestedRegistered = AotHelperRegistry.TryGetAdapter<ComplexNestedContainer.NestedCustomer>(out _);
        await Assert.That(complexNestedRegistered).IsTrue();
    }

    #endregion

    #region 往返序列化测试

    [Test]
    public async Task SingleNestedEntity_RoundTrip_Should_Preserve_Data()
    {
        var original = new SingleLevelContainer.NestedItem
        {
            Id = 999,
            Name = "Round Trip Test",
            Price = 123.45m
        };

        var doc = AotBsonMapper.ToDocument(original);
        var restored = AotBsonMapper.FromDocument<SingleLevelContainer.NestedItem>(doc);

        await Assert.That(restored).IsNotNull();
        await Assert.That(restored!.Id).IsEqualTo(original.Id);
        await Assert.That(restored.Name).IsEqualTo(original.Name);
        await Assert.That(restored.Price).IsEqualTo(original.Price);
    }

    [Test]
    public async Task DeeplyNestedEntity_RoundTrip_Should_Preserve_Data()
    {
        var original = new MultiLevelContainer.MiddleContainer.DeeplyNestedItem
        {
            Id = 888,
            Description = "Deep Round Trip",
            CreatedAt = DateTime.Now
        };

        var doc = AotBsonMapper.ToDocument(original);
        var restored = AotBsonMapper.FromDocument<MultiLevelContainer.MiddleContainer.DeeplyNestedItem>(doc);

        await Assert.That(restored).IsNotNull();
        await Assert.That(restored!.Id).IsEqualTo(original.Id);
        await Assert.That(restored.Description).IsEqualTo(original.Description);
        // DateTime 精度可能有差异，只比较到秒
        await Assert.That(restored.CreatedAt.ToString("yyyy-MM-dd HH:mm:ss"))
            .IsEqualTo(original.CreatedAt.ToString("yyyy-MM-dd HH:mm:ss"));
    }

    [Test]
    public async Task NestedEntityWithComplexType_RoundTrip_Should_Preserve_Data()
    {
        var original = new ComplexNestedContainer.NestedCustomer
        {
            Id = 777,
            Name = "Complex Round Trip",
            Address = new ComplexNestedContainer.AddressInfo
            {
                Street = "789 Pine Rd",
                City = "Chicago",
                ZipCode = "60601"
            }
        };

        var doc = AotBsonMapper.ToDocument(original);
        var restored = AotBsonMapper.FromDocument<ComplexNestedContainer.NestedCustomer>(doc);

        await Assert.That(restored).IsNotNull();
        await Assert.That(restored!.Id).IsEqualTo(original.Id);
        await Assert.That(restored.Name).IsEqualTo(original.Name);
        await Assert.That(restored.Address).IsNotNull();
        await Assert.That(restored.Address!.Street).IsEqualTo(original.Address!.Street);
        await Assert.That(restored.Address.City).IsEqualTo(original.Address.City);
        await Assert.That(restored.Address.ZipCode).IsEqualTo(original.Address.ZipCode);
    }

    #endregion
}
