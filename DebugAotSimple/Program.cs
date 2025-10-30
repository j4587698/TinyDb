using System;
using System.IO;
using System.Linq;
using TinyDb.Serialization;

namespace DebugAotSimple
{
    class SimpleObject
    {
        public string Name { get; set; } = string.Empty;
        public System.Collections.Generic.List<string> Tags { get; set; } = new();
    }

    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("=== 简单AOT序列化调试测试 ===");

            var obj = new SimpleObject
            {
                Name = "Test",
                Tags = new System.Collections.Generic.List<string> { "tag1", "tag2" }
            };

            Console.WriteLine("原始对象:");
            Console.WriteLine($"  Name: {obj.Name}");
            Console.WriteLine($"  Tags.Count: {obj.Tags.Count}");
            Console.WriteLine($"  Tags内容: [{string.Join(", ", obj.Tags)}]");

            // 直接测试AOT序列化
            Console.WriteLine("\n=== 序列化测试 ===");
            var bsonDoc = AotBsonMapper.ToDocument(obj);

            Console.WriteLine($"BsonDocument字段数量: {bsonDoc.Count}");
            foreach (var element in bsonDoc)
            {
                Console.WriteLine($"  字段名: '{element.Key}', 类型: {element.Value.GetType().Name}, 值: {element.Value}");

                if (element.Value is TinyDb.Bson.BsonArray array)
                {
                    Console.WriteLine($"    数组内容: [{string.Join(", ", array.Select(v => $"'{v}'"))}]");
                }
            }

            // 测试反序列化
            Console.WriteLine("\n=== 反序列化测试 ===");
            try
            {
                var deserialized = AotBsonMapper.FromDocument<SimpleObject>(bsonDoc);
                Console.WriteLine("反序列化成功:");
                Console.WriteLine($"  Name: {deserialized.Name}");
                Console.WriteLine($"  Tags类型: {deserialized.Tags?.GetType().FullName ?? "null"}");
                Console.WriteLine($"  Tags.Count: {deserialized.Tags?.Count ?? 0}");
                if (deserialized.Tags != null)
                {
                    Console.WriteLine($"  Tags内容: [{string.Join(", ", deserialized.Tags)}]");
                }
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