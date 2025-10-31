using System;
using TinyDb.Bson;
using TinyDb.Serialization;

namespace DebugAotIssue
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("🔍 调试AOT序列化问题");

            try
            {
                // 测试简单对象
                Console.WriteLine("\n1. 测试简单对象序列化:");
                var simpleObj = new SimpleClass { Name = "Test", Age = 25 };
                var simpleDoc = BsonMapper.ToDocument(simpleObj);
                Console.WriteLine($"✅ 简单对象序列化成功: {simpleDoc}");

                // 测试Address对象
                Console.WriteLine("\n2. 测试Address对象序列化:");
                var address = new Address
                {
                    Street = "123 Main St",
                    City = "Test City",
                    Country = "Test Country"
                };
                var addressDoc = BsonMapper.ToDocument(address);
                Console.WriteLine($"✅ Address序列化成功: {addressDoc}");

                // 测试Address反序列化 - 这里应该出错
                Console.WriteLine("\n3. 测试Address对象反序列化:");
                var deserializedAddress = BsonMapper.ToObject<Address>(addressDoc);
                Console.WriteLine($"✅ Address反序列化成功: {deserializedAddress?.Street}");

                // 测试复杂对象
                Console.WriteLine("\n4. 测试复杂对象序列化:");
                var complexObj = new PersonWithAddress
                {
                    Name = "John",
                    Age = 30,
                    Address = address,
                    Tags = new List<string> { "tag1", "tag2" }
                };
                var complexDoc = BsonMapper.ToDocument(complexObj);
                Console.WriteLine($"✅ 复杂对象序列化成功: {complexDoc}");

                // 测试复杂对象反序列化
                Console.WriteLine("\n5. 测试复杂对象反序列化:");
                var deserializedComplex = BsonMapper.ToObject<PersonWithAddress>(complexDoc);
                Console.WriteLine($"✅ 复杂对象反序列化成功: {deserializedComplex?.Address?.Street}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ 错误: {ex.GetType().Name}: {ex.Message}");
                Console.WriteLine($"堆栈跟踪:\n{ex.StackTrace}");
            }
        }
    }

    // 简单类定义
    public class SimpleClass
    {
        public string Name { get; set; } = "";
        public int Age { get; set; }
    }

    public class Address
    {
        public string Street { get; set; } = "";
        public string City { get; set; } = "";
        public string Country { get; set; } = "";
    }

    public class PersonWithAddress
    {
        public string Name { get; set; } = "";
        public int Age { get; set; }
        public Address Address { get; set; } = new Address();
        public List<string> Tags { get; set; } = new List<string>();
    }
}