using System;
using TinyDb.Bson;
using TinyDb.Serialization;

namespace TestAotIssue
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("🔍 快速测试AOT序列化问题");

            try
            {
                // 测试Address对象
                Console.WriteLine("\n1. 测试Address对象序列化:");
                var address = new Address
                {
                    Street = "123 Main St",
                    City = "Test City",
                    Country = "Test Country"
                };
                var addressDoc = BsonMapper.ToDocument(address);
                Console.WriteLine($"✅ Address序列化成功: {addressDoc}");

                // 测试Address反序列化 - 这里应该出错
                Console.WriteLine("\n2. 测试Address对象反序列化:");
                var deserializedAddress = BsonMapper.ToObject<Address>(addressDoc);
                Console.WriteLine($"✅ Address反序列化成功: {deserializedAddress?.Street}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ 错误: {ex.GetType().Name}: {ex.Message}");
                Console.WriteLine($"堆栈跟踪:\n{ex.StackTrace}");
            }
        }
    }

    public class Address
    {
        public string Street { get; set; } = string.Empty;
        public string City { get; set; } = string.Empty;
        public string Country { get; set; } = string.Empty;
    }
}