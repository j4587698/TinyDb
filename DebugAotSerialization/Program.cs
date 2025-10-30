using System;
using System.IO;
using System.Linq;
using TinyDb.Core;
using TinyDb.Collections;
using TinyDb.Attributes;
using TinyDb.IdGeneration;
using TinyDb.Serialization;

namespace DebugAotSerialization
{
    [Entity("complex_test")]
    class ComplexObject
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public System.Collections.Generic.List<string> Tags { get; set; } = new();
        public System.Collections.Generic.Dictionary<string, object> Metadata { get; set; } = new();
    }

    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("=== AOT序列化调试测试 ===");

            string testFile = Path.GetTempFileName();
            Console.WriteLine($"测试文件: {testFile}");

            try
            {
                using var engine = new TinyDbEngine(testFile);
                var collection = engine.GetCollection<ComplexObject>();

                // 创建复杂对象
                var obj = new ComplexObject
                {
                    Name = "Test Object",
                    Tags = new System.Collections.Generic.List<string> { "tag1", "tag2", "tag3" },
                    Metadata = new System.Collections.Generic.Dictionary<string, object>
                    {
                        { "key1", "value1" },
                        { "key2", 42 }
                    }
                };

                Console.WriteLine("\n=== 序列化测试 ===");
                Console.WriteLine($"原始对象:");
                Console.WriteLine($"  Name: {obj.Name}");
                Console.WriteLine($"  Tags.Count: {obj.Tags.Count}");
                Console.WriteLine($"  Tags内容: [{string.Join(", ", obj.Tags)}]");
                Console.WriteLine($"  Metadata.Count: {obj.Metadata.Count}");
                var originalMetadataContent = string.Join(", ", obj.Metadata.Select(kvp => $"{kvp.Key}={kvp.Value}"));
                Console.WriteLine($"  Metadata内容: {originalMetadataContent}");

                // 插入到数据库（会触发序列化）
                var id = collection.Insert(obj);
                Console.WriteLine($"\n插入成功，ID: {id}");

                // 从数据库读取（会触发反序列化）
                var retrieved = collection.FindById(id);
                if (retrieved != null)
                {
                    Console.WriteLine($"\n=== 反序列化测试 ===");
                    Console.WriteLine($"检索对象:");
                    Console.WriteLine($"  Name: {retrieved.Name}");
                    Console.WriteLine($"  Tags类型: {retrieved.Tags?.GetType().FullName ?? "null"}");
                    Console.WriteLine($"  Tags.Count: {retrieved.Tags?.Count ?? 0}");
                    if (retrieved.Tags != null)
                    {
                        Console.WriteLine($"  Tags内容: [{string.Join(", ", retrieved.Tags)}]");
                    }

                    Console.WriteLine($"  Metadata类型: {retrieved.Metadata?.GetType().FullName ?? "null"}");
                    Console.WriteLine($"  Metadata.Count: {retrieved.Metadata?.Count ?? 0}");
                    if (retrieved.Metadata != null)
                    {
                        var metadataContent = string.Join(", ", retrieved.Metadata.Select(kvp => $"{kvp.Key}={kvp.Value}"));
                        Console.WriteLine($"  Metadata内容: {metadataContent}");
                    }

                    // 验证数据完整性
                    bool tagsMatch = retrieved.Tags != null && obj.Tags.SequenceEqual(retrieved.Tags);
                    bool metadataMatch = retrieved.Metadata != null &&
                        obj.Metadata.All(kvp => retrieved.Metadata.ContainsKey(kvp.Key) &&
                            Equals(retrieved.Metadata[kvp.Key], kvp.Value));

                    Console.WriteLine($"\n=== 数据完整性验证 ===");
                    Console.WriteLine($"Tags匹配: {tagsMatch}");
                    Console.WriteLine($"Metadata匹配: {metadataMatch}");
                    Console.WriteLine($"整体成功: {tagsMatch && metadataMatch}");
                }
                else
                {
                    Console.WriteLine("错误：无法检索插入的对象");
                }

                // 直接测试AOT序列化
                Console.WriteLine("\n=== 直接AOT序列化测试 ===");
                var bsonDoc = AotBsonMapper.ToDocument(obj);

                Console.WriteLine($"BsonDocument字段数量: {bsonDoc.Count}");
                foreach (var element in bsonDoc)
                {
                    Console.WriteLine($"  {element.Key}: {element.Value.GetType().Name} = {element.Value}");
                    if (element.Value is TinyDb.Bson.BsonArray array)
                    {
                        Console.WriteLine($"    数组内容: [{string.Join(", ", array.Select(v => v.ToString()))}]");
                    }
                }

                // 测试反序列化
                Console.WriteLine("\n=== 直接AOT反序列化测试 ===");
                try
                {
                    var deserialized = AotBsonMapper.FromDocument<ComplexObject>(bsonDoc);
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
            catch (Exception ex)
            {
                Console.WriteLine($"\n❌ 错误: {ex.Message}");
                Console.WriteLine($"\n堆栈跟踪:\n{ex.StackTrace}");
            }
            finally
            {
                if (File.Exists(testFile))
                {
                    File.Delete(testFile);
                    Console.WriteLine($"\n🧹 已清理测试文件: {testFile}");
                }
            }
        }
    }
}