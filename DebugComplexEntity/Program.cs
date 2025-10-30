using System;
using System.Collections.Generic;
using TinyDb.Serialization;

namespace DebugComplexEntity
{
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

    public class Address
    {
        public string Street { get; set; } = string.Empty;
        public string City { get; set; } = string.Empty;
        public string Country { get; set; } = string.Empty;
        public string ZipCode { get; set; } = string.Empty;
    }

    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("=== ComplexAOTEntity AOT序列化调试测试 ===");

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

            Console.WriteLine("原始对象:");
            Console.WriteLine($"  Id: {complexEntity.Id}");
            Console.WriteLine($"  Name: {complexEntity.Name}");
            Console.WriteLine($"  Age: {complexEntity.Age}");
            Console.WriteLine($"  Tags.Length: {complexEntity.Tags.Length}");
            Console.WriteLine($"  Tags内容: [{string.Join(", ", complexEntity.Tags)}]");
            Console.WriteLine($"  Address: {complexEntity.Address?.Street ?? "null"}");
            Console.WriteLine($"  Metadata.Count: {complexEntity.Metadata.Count}");

            // 直接测试AOT序列化
            Console.WriteLine("\n=== 序列化测试 ===");
            var bsonDoc = AotBsonMapper.ToDocument(complexEntity);

            Console.WriteLine($"BsonDocument字段数量: {bsonDoc.Count}");
            Console.WriteLine($"使用Contains(_id): {bsonDoc.Contains("_id")}");
            Console.WriteLine($"使用ContainsKey(_id): {bsonDoc.ContainsKey("_id")}");
            Console.WriteLine($"使用索引器访问_id: {bsonDoc["_id"]} (类型: {bsonDoc["_id"].GetType().Name})");

            if (bsonDoc.Contains("_id"))
            {
                Console.WriteLine("  _id: 字段存在!");
            }
            else
            {
                Console.WriteLine("  _id: 字段缺失! (但索引器能访问到)");
            }

            foreach (var element in bsonDoc)
            {
                Console.WriteLine($"  字段名: '{element.Key}', 类型: {element.Value.GetType().Name}, 值: {element.Value}");
            }

            // 测试反序列化
            Console.WriteLine("\n=== 反序列化测试 ===");
            try
            {
                var deserialized = AotBsonMapper.FromDocument<ComplexAOTEntity>(bsonDoc);
                Console.WriteLine("反序列化成功:");
                Console.WriteLine($"  Id: {deserialized.Id}");
                Console.WriteLine($"  Name: {deserialized.Name}");
                Console.WriteLine($"  Age: {deserialized.Age}");
                Console.WriteLine($"  Tags.Length: {deserialized.Tags.Length}");
                if (deserialized.Address != null)
                {
                    Console.WriteLine($"  Address: {deserialized.Address.Street}, {deserialized.Address.City}");
                }
                Console.WriteLine($"  Metadata.Count: {deserialized.Metadata.Count}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"反序列化失败: {ex.Message}");
                Console.WriteLine($"错误类型: {ex.GetType().Name}");
                Console.WriteLine($"堆栈跟踪:\n{ex.StackTrace}");
            }
        }
    }
}