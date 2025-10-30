using System;
using System.Collections.Generic;
using TinyDb.Serialization;
using TinyDb.Bson;

namespace DebugAotSerialization
{
    class Program
    {
        static void Main()
        {
            Console.WriteLine("调试AOT序列化问题...");

            try
            {
                // 创建简单的测试对象
                var testObj = new TestClass
                {
                    Name = "TestObject",
                    Age = 25,
                    Tags = new List<string> { "tag1", "tag2" }
                };

                Console.WriteLine("原始对象:");
                Console.WriteLine($"  Name: {testObj.Name}");
                Console.WriteLine($"  Age: {testObj.Age}");
                Console.WriteLine($"  Tags: {string.Join(", ", testObj.Tags ?? new List<string>())}");

                // 序列化为BsonDocument
                var bsonDoc = BsonMapper.ToDocument(testObj);

                Console.WriteLine("\n序列化后的BsonDocument:");
                Console.WriteLine(bsonDoc.ToString());

                // 反序列化
                var deserializedObj = BsonMapper.ToObject<TestClass>(bsonDoc);

                Console.WriteLine("\n反序列化后的对象:");
                Console.WriteLine($"  Name: {deserializedObj.Name}");
                Console.WriteLine($"  Age: {deserializedObj.Age}");
                Console.WriteLine($"  Tags: {string.Join(", ", deserializedObj.Tags ?? new List<string>())}");

                Console.WriteLine("\n✅ AOT序列化测试成功！");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"\n❌ 发生异常: {ex.Message}");
                Console.WriteLine($"异常类型: {ex.GetType().Name}");
                Console.WriteLine($"堆栈跟踪:\n{ex.StackTrace}");
            }
        }
    }

    public class TestClass
    {
        public string? Name { get; set; }
        public int Age { get; set; }
        public List<string>? Tags { get; set; }
    }
}
