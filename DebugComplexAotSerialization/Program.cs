using System;
using System.Collections.Generic;
using TinyDb.Serialization;
using TinyDb.Bson;

namespace DebugComplexAotSerialization
{
    class Program
    {
        static void Main()
        {
            Console.WriteLine("调试复杂AOT序列化问题...");

            try
            {
                // 测试1: 枚举类型序列化
                Console.WriteLine("\n=== 测试1: 枚举类型序列化 ===");
                TestEnumSerialization();

                // 测试2: 嵌套对象序列化
                Console.WriteLine("\n=== 测试2: 嵌套对象序列化 ===");
                TestNestedObjectSerialization();

                // 测试3: _id字段问题
                Console.WriteLine("\n=== 测试3: _id字段问题 ===");
                TestIdFieldIssue();

                // 测试4: 循环引用处理
                Console.WriteLine("\n=== 测试4: 循环引用处理 ===");
                TestCircularReference();

                Console.WriteLine("\n✅ 所有复杂AOT序列化测试完成！");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"\n❌ 发生异常: {ex.Message}");
                Console.WriteLine($"异常类型: {ex.GetType().Name}");
                Console.WriteLine($"堆栈跟踪:\n{ex.StackTrace}");
            }
        }

        static void TestEnumSerialization()
        {
            var entity = new EntityWithEnum
            {
                Id = "enum_test_001",
                Name = "Enum Test",
                Status = CustomStatus.Active,
                Priority = Priority.High
            };

            Console.WriteLine($"原始对象:");
            Console.WriteLine($"  Id: {entity.Id}");
            Console.WriteLine($"  Name: {entity.Name}");
            Console.WriteLine($"  Status: {entity.Status} (值: {(int)entity.Status})");
            Console.WriteLine($"  Priority: {entity.Priority} (值: {(int)entity.Priority})");

            // 序列化为BsonDocument
            var bsonDoc = BsonMapper.ToDocument(entity);

            Console.WriteLine($"\n序列化后的BsonDocument:");
            Console.WriteLine($"  包含_id字段: {bsonDoc.ContainsKey("_id")}");
            Console.WriteLine($"  包含name字段: {bsonDoc.ContainsKey("name")}");
            Console.WriteLine($"  包含status字段: {bsonDoc.ContainsKey("status")}");
            Console.WriteLine($"  包含priority字段: {bsonDoc.ContainsKey("priority")}");
            Console.WriteLine(bsonDoc.ToString());

            // 反序列化
            var deserializedEntity = BsonMapper.ToObject<EntityWithEnum>(bsonDoc);

            Console.WriteLine($"\n反序列化后的对象:");
            Console.WriteLine($"  Id: {deserializedEntity.Id}");
            Console.WriteLine($"  Name: {deserializedEntity.Name}");
            Console.WriteLine($"  Status: {deserializedEntity.Status} (值: {(int)deserializedEntity.Status})");
            Console.WriteLine($"  Priority: {deserializedEntity.Priority} (值: {(int)deserializedEntity.Priority})");

            // 验证
            var success = entity.Id == deserializedEntity.Id &&
                         entity.Name == deserializedEntity.Name &&
                         entity.Status == deserializedEntity.Status &&
                         entity.Priority == deserializedEntity.Priority;

            Console.WriteLine($"验证结果: {(success ? "✅ 通过" : "❌ 失败")}");
        }

        static void TestNestedObjectSerialization()
        {
            var entity = new ComplexEntity
            {
                Id = "complex_test_001",
                Name = "Complex Test",
                Age = 30,
                Address = new Address
                {
                    Street = "123 Test St",
                    City = "Test City",
                    Country = "Test Country",
                    ZipCode = "12345"
                },
                Tags = new[] { "tag1", "tag2", "tag3" },
                Metadata = new Dictionary<string, object>
                {
                    ["key1"] = "value1",
                    ["key2"] = 42,
                    ["key3"] = true
                }
            };

            Console.WriteLine($"原始对象:");
            Console.WriteLine($"  Id: {entity.Id}");
            Console.WriteLine($"  Name: {entity.Name}");
            Console.WriteLine($"  Age: {entity.Age}");
            Console.WriteLine($"  Address: {entity.Address?.Street}, {entity.Address?.City}");
            Console.WriteLine($"  Tags: [{string.Join(", ", entity.Tags ?? new string[0])}]");
            Console.WriteLine($"  Metadata: {entity.Metadata?.Count} 个条目");

            // 序列化为BsonDocument
            var bsonDoc = BsonMapper.ToDocument(entity);

            Console.WriteLine($"\n序列化后的BsonDocument:");
            Console.WriteLine($"  包含_id字段: {bsonDoc.ContainsKey("_id")}");
            Console.WriteLine($"  包含address字段: {bsonDoc.ContainsKey("address")}");
            Console.WriteLine($"  包含tags字段: {bsonDoc.ContainsKey("tags")}");
            Console.WriteLine($"  包含metadata字段: {bsonDoc.ContainsKey("metadata")}");
            Console.WriteLine(bsonDoc.ToString());

            // 反序列化
            var deserializedEntity = BsonMapper.ToObject<ComplexEntity>(bsonDoc);

            Console.WriteLine($"\n反序列化后的对象:");
            Console.WriteLine($"  Id: {deserializedEntity.Id}");
            Console.WriteLine($"  Name: {deserializedEntity.Name}");
            Console.WriteLine($"  Age: {deserializedEntity.Age}");
            Console.WriteLine($"  Address: {deserializedEntity.Address?.Street}, {deserializedEntity.Address?.City}");
            Console.WriteLine($"  Tags: [{string.Join(", ", deserializedEntity.Tags ?? new string[0])}]");
            Console.WriteLine($"  Metadata: {deserializedEntity.Metadata?.Count} 个条目");

            // 验证
            var success = entity.Id == deserializedEntity.Id &&
                         entity.Name == deserializedEntity.Name &&
                         entity.Age == deserializedEntity.Age &&
                         entity.Address?.Street == deserializedEntity.Address?.Street &&
                         entity.Tags?.Length == deserializedEntity.Tags?.Length &&
                         entity.Metadata?.Count == deserializedEntity.Metadata?.Count;

            Console.WriteLine($"验证结果: {(success ? "✅ 通过" : "❌ 失败")}");
        }

        static void TestIdFieldIssue()
        {
            var entity = new SimpleEntity
            {
                Id = "id_test_001",
                Name = "ID Test",
                Value = 123
            };

            Console.WriteLine($"原始对象:");
            Console.WriteLine($"  Id: {entity.Id}");
            Console.WriteLine($"  Name: {entity.Name}");
            Console.WriteLine($"  Value: {entity.Value}");

            // 序列化为BsonDocument
            var bsonDoc = BsonMapper.ToDocument(entity);

            Console.WriteLine($"\n序列化后的BsonDocument:");
            Console.WriteLine($"  包含_id字段: {bsonDoc.ContainsKey("_id")}");
            Console.WriteLine($"  包含name字段: {bsonDoc.ContainsKey("name")}");
            Console.WriteLine($"  包含value字段: {bsonDoc.ContainsKey("value")}");
            Console.WriteLine($"  所有键: [{string.Join(", ", bsonDoc.Keys)}]");
            Console.WriteLine(bsonDoc.ToString());

            // 验证_id字段的值
            if (bsonDoc.ContainsKey("_id"))
            {
                var idValue = bsonDoc["_id"];
                Console.WriteLine($"_id字段类型: {idValue.GetType().Name}");
                Console.WriteLine($"_id字段值: {idValue}");
            }

            // 反序列化
            var deserializedEntity = BsonMapper.ToObject<SimpleEntity>(bsonDoc);

            Console.WriteLine($"\n反序列化后的对象:");
            Console.WriteLine($"  Id: {deserializedEntity.Id}");
            Console.WriteLine($"  Name: {deserializedEntity.Name}");
            Console.WriteLine($"  Value: {deserializedEntity.Value}");

            // 验证
            var success = entity.Id == deserializedEntity.Id &&
                         entity.Name == deserializedEntity.Name &&
                         entity.Value == deserializedEntity.Value;

            Console.WriteLine($"验证结果: {(success ? "✅ 通过" : "❌ 失败")}");
        }

        static void TestCircularReference()
        {
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

            Console.WriteLine($"创建循环引用对象:");
            Console.WriteLine($"  Parent: {parent.Name}, Children数量: {parent.Children?.Length ?? 0}");
            Console.WriteLine($"  Child: {child.Name}, Parent: {child.Parent?.Name}");

            // 尝试序列化（应该处理循环引用）
            var bsonDoc = BsonMapper.ToDocument(parent);

            Console.WriteLine($"\n序列化后的BsonDocument:");
            Console.WriteLine($"  包含_id字段: {bsonDoc.ContainsKey("_id")}");
            Console.WriteLine($"  包含name字段: {bsonDoc.ContainsKey("name")}");
            Console.WriteLine($"  包含children字段: {bsonDoc.ContainsKey("children")}");
            Console.WriteLine($"  所有键: [{string.Join(", ", bsonDoc.Keys)}]");
            Console.WriteLine(bsonDoc.ToString());

            Console.WriteLine($"验证结果: ✅ 循环引用处理成功（无异常抛出）");
        }
    }

    // 测试用的实体类

    public enum CustomStatus
    {
        Inactive = 0,
        Active = 1,
        Suspended = 2
    }

    public enum Priority
    {
        Low = 1,
        Medium = 2,
        High = 3,
        Critical = 4
    }

    public class EntityWithEnum
    {
        public string Id { get; set; } = string.Empty;
        public string Name { get; set; } = string.Empty;
        public CustomStatus Status { get; set; }
        public Priority Priority { get; set; }
    }

    public class SimpleEntity
    {
        public string Id { get; set; } = string.Empty;
        public string Name { get; set; } = string.Empty;
        public int Value { get; set; }
    }

    public class Address
    {
        public string Street { get; set; } = string.Empty;
        public string City { get; set; } = string.Empty;
        public string Country { get; set; } = string.Empty;
        public string ZipCode { get; set; } = string.Empty;
    }

    public class ComplexEntity
    {
        public string Id { get; set; } = string.Empty;
        public string Name { get; set; } = string.Empty;
        public int Age { get; set; }
        public Address Address { get; set; } = null!;
        public string[] Tags { get; set; } = Array.Empty<string>();
        public Dictionary<string, object> Metadata { get; set; } = new();
    }

    public class CircularEntity
    {
        public string Id { get; set; } = string.Empty;
        public string Name { get; set; } = string.Empty;
        public CircularEntity[] Children { get; set; } = Array.Empty<CircularEntity>();
        public CircularEntity? Parent { get; set; }
    }
}