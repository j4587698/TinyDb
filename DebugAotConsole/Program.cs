using System;
using System.Collections.Generic;
using TinyDb.Serialization;
using TinyDb.Bson;

namespace DebugAotConsole
{
    class TestEntity
    {
        public string Id { get; set; } = string.Empty;
        public List<string> ListValue { get; set; } = new();
    }

    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("=== AOT反序列化调试测试 ===");

            // 创建测试实体
            var entity = new TestEntity
            {
                Id = "test123",
                ListValue = new List<string> { "item1", "item2", "item3" }
            };

            Console.WriteLine($"原始实体: Id={entity.Id}, ListValue.Count={entity.ListValue.Count}");
            Console.WriteLine($"ListValue内容: [{string.Join(", ", entity.ListValue)}]");

            try
            {
                // 序列化为BsonDocument
                var bsonDoc = AotBsonMapper.ToDocument(entity);
                Console.WriteLine($"\n序列化成功，BsonDocument包含 {bsonDoc.Count} 个字段:");
                foreach (var element in bsonDoc)
                {
                    Console.WriteLine($"  {element.Key}: {element.Value.GetType().Name} = {element.Value}");
                }

                // 检查ListValue字段的具体类型
                if (bsonDoc.Contains("listValue"))
                {
                    var listValue = bsonDoc["listValue"];
                    Console.WriteLine($"listValue字段类型: {listValue.GetType().FullName}");
                    Console.WriteLine($"listValue是BsonArray: {listValue is BsonArray}");
                    if (listValue is BsonArray array)
                    {
                        Console.WriteLine($"BsonArray包含 {array.Count} 个元素:");
                        for (int i = 0; i < array.Count; i++)
                        {
                            Console.WriteLine($"  [{i}]: {array[i].GetType().Name} = {array[i]}");
                        }
                    }
                }

                // 反序列化
                Console.WriteLine("\n开始反序列化...");
                var deserialized = AotBsonMapper.FromDocument<TestEntity>(bsonDoc);
                Console.WriteLine($"反序列化成功: Id={deserialized.Id}, ListValue.Count={deserialized.ListValue.Count}");
                Console.WriteLine($"ListValue内容: [{string.Join(", ", deserialized.ListValue)}]");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"\n❌ 错误: {ex.Message}");
                Console.WriteLine($"堆栈跟踪:\n{ex.StackTrace}");
            }
        }
    }
}