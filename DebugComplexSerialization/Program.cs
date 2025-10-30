using System;
using System.Collections.Generic;
using TinyDb.Bson;
using TinyDb.Serialization;

// Address类定义
public class Address
{
    public string Street { get; set; } = string.Empty;
    public string City { get; set; } = string.Empty;
    public string Country { get; set; } = string.Empty;
    public string ZipCode { get; set; } = string.Empty;
}

// 复杂实体类
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

class Program
{
    static void Main()
    {
        Console.WriteLine("调试复杂对象AOT序列化问题...");

        // 创建复杂的嵌套对象
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

        Console.WriteLine($"原始实体:");
        Console.WriteLine($"  Id: {complexEntity.Id}");
        Console.WriteLine($"  Name: {complexEntity.Name}");
        Console.WriteLine($"  Age: {complexEntity.Age}");
        Console.WriteLine($"  Address: {complexEntity.Address?.Street}, {complexEntity.Address?.City}");
        Console.WriteLine($"  Metadata count: {complexEntity.Metadata.Count}");

        try
        {
            // 序列化
            var bsonDoc = BsonMapper.ToDocument(complexEntity);
            Console.WriteLine($"\n序列化成功，BsonDocument包含 {bsonDoc.Count} 个字段:");

            foreach (var element in bsonDoc)
            {
                Console.WriteLine($"  {element.Key}: {element.Value} (类型: {element.Value.GetType().Name})");

                // 如果是复杂对象，显示其内容
                if (element.Value is BsonDocument nestedDoc && element.Key != "metadata")
                {
                    Console.WriteLine($"    嵌套文档内容:");
                    foreach (var nested in nestedDoc)
                    {
                        Console.WriteLine($"      {nested.Key}: {nested.Value}");
                    }
                }
            }

            // 检查Address字段是否存在和类型
            Console.WriteLine($"\n检查Address字段:");
            if (bsonDoc.ContainsKey("address"))
            {
                var addressValue = bsonDoc["address"];
                Console.WriteLine($"  address类型: {addressValue.GetType().Name}");
                if (addressValue is BsonDocument addressDoc)
                {
                    Console.WriteLine($"  address内容:");
                    foreach (var addrField in addressDoc)
                    {
                        Console.WriteLine($"    {addrField.Key}: {addrField.Value}");
                    }
                }
            }

            // 反序列化
            var deserializedEntity = BsonMapper.ToObject<ComplexAOTEntity>(bsonDoc);
            Console.WriteLine($"\n反序列化成功:");
            Console.WriteLine($"  Id: {deserializedEntity.Id}");
            Console.WriteLine($"  Name: {deserializedEntity.Name}");
            Console.WriteLine($"  Age: {deserializedEntity.Age}");
            Console.WriteLine($"  Address: {deserializedEntity.Address?.Street}, {deserializedEntity.Address?.City}");
            Console.WriteLine($"  Metadata count: {deserializedEntity.Metadata.Count}");

            // 验证
            Console.WriteLine($"\n验证结果:");
            Console.WriteLine($"  Id匹配: {complexEntity.Id == deserializedEntity.Id}");
            Console.WriteLine($"  Name匹配: {complexEntity.Name == deserializedEntity.Name}");
            Console.WriteLine($"  Age匹配: {complexEntity.Age == deserializedEntity.Age}");
            Console.WriteLine($"  Address匹配: {complexEntity.Address?.Street == deserializedEntity.Address?.Street}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"错误: {ex.Message}");
            Console.WriteLine($"堆栈跟踪:\n{ex.StackTrace}");
        }
    }
}