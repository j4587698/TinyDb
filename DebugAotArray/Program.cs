using System;
using System.Linq;
using TinyDb.Serialization;

namespace DebugAotArray
{
    class ArrayObject
    {
        public string Name { get; set; } = string.Empty;
        public string[] Tags { get; set; } = Array.Empty<string>();
        public int[] Numbers { get; set; } = Array.Empty<int>();
    }

    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("=== 数组AOT序列化调试测试 ===");

            var obj = new ArrayObject
            {
                Name = "Test",
                Tags = new string[] { "tag1", "tag2", "tag3" },
                Numbers = new int[] { 1, 2, 3, 4, 5 }
            };

            Console.WriteLine("原始对象:");
            Console.WriteLine($"  Name: {obj.Name}");
            Console.WriteLine($"  Tags.Length: {obj.Tags.Length}");
            Console.WriteLine($"  Tags内容: [{string.Join(", ", obj.Tags)}]");
            Console.WriteLine($"  Numbers.Length: {obj.Numbers.Length}");
            Console.WriteLine($"  Numbers内容: [{string.Join(", ", obj.Numbers)}]");

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
                var deserialized = AotBsonMapper.FromDocument<ArrayObject>(bsonDoc);
                Console.WriteLine("反序列化成功:");
                Console.WriteLine($"  Name: {deserialized.Name}");
                Console.WriteLine($"  Tags类型: {deserialized.Tags?.GetType().FullName ?? "null"}");
                Console.WriteLine($"  Tags.Length: {deserialized.Tags?.Length ?? 0}");
                if (deserialized.Tags != null)
                {
                    Console.WriteLine($"  Tags内容: [{string.Join(", ", deserialized.Tags)}]");
                }
                Console.WriteLine($"  Numbers类型: {deserialized.Numbers?.GetType().FullName ?? "null"}");
                Console.WriteLine($"  Numbers.Length: {deserialized.Numbers?.Length ?? 0}");
                if (deserialized.Numbers != null)
                {
                    Console.WriteLine($"  Numbers内容: [{string.Join(", ", deserialized.Numbers)}]");
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