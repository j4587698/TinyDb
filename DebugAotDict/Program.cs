using System;
using System.IO;
using System.Linq;
using TinyDb.Serialization;

namespace DebugAotDict
{
    class DictObject
    {
        public string Name { get; set; } = string.Empty;
        public System.Collections.Generic.Dictionary<string, object> Metadata { get; set; } = new();
    }

    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("=== Dictionary AOT序列化调试测试 ===");

            var obj = new DictObject
            {
                Name = "Test",
                Metadata = new System.Collections.Generic.Dictionary<string, object>
                {
                    { "key1", "value1" },
                    { "key2", 42 }
                }
            };

            Console.WriteLine("原始对象:");
            Console.WriteLine($"  Name: {obj.Name}");
            Console.WriteLine($"  Metadata.Count: {obj.Metadata.Count}");
            Console.WriteLine($"  Metadata内容: {string.Join(", ", obj.Metadata.Select(kvp => $"{kvp.Key}={kvp.Value}"))}");

            // 直接测试AOT序列化
            Console.WriteLine("\n=== 序列化测试 ===");
            var bsonDoc = AotBsonMapper.ToDocument(obj);

            Console.WriteLine($"BsonDocument字段数量: {bsonDoc.Count}");
            foreach (var element in bsonDoc)
            {
                Console.WriteLine($"  字段名: '{element.Key}', 类型: {element.Value.GetType().Name}, 值: {element.Value}");

                if (element.Value is TinyDb.Bson.BsonDocument doc)
                {
                    Console.WriteLine($"    文档内容: [{string.Join(", ", doc.Select(e => $"'{e.Key}': '{e.Value}'"))}]");
                }
            }

            // 测试反序列化
            Console.WriteLine("\n=== 反序列化测试 ===");
            try
            {
                var deserialized = AotBsonMapper.FromDocument<DictObject>(bsonDoc);
                Console.WriteLine("反序列化成功:");
                Console.WriteLine($"  Name: {deserialized.Name}");
                Console.WriteLine($"  Metadata类型: {deserialized.Metadata?.GetType().FullName ?? "null"}");
                Console.WriteLine($"  Metadata.Count: {deserialized.Metadata?.Count ?? 0}");
                if (deserialized.Metadata != null)
                {
                    Console.WriteLine($"  Metadata内容: {string.Join(", ", deserialized.Metadata.Select(kvp => $"{kvp.Key}={kvp.Value}"))}");
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