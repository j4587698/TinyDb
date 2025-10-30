using System;
using TinyDb.Bson;
using TinyDb.Serialization;

public class DebugDictTest
{
    public static void Main()
    {
        Console.WriteLine("=== 调试Dictionary序列化问题 ===");

        // 1. 创建Dictionary
        var dict = new Dictionary<string, object>
        {
            ["key1"] = "value1",
            ["key2"] = 42,
            ["key3"] = true
        };

        // 2. 序列化为BsonValue
        var bsonValue = BsonConversion.ToBsonValue(dict);
        Console.WriteLine($"序列化结果类型: {bsonValue.GetType().Name}");
        Console.WriteLine($"序列化结果: {bsonValue}");

        if (bsonValue is BsonDocument doc)
        {
            Console.WriteLine("是BsonDocument，枚举内容:");
            foreach (var kvp in doc)
            {
                Console.WriteLine($"  {kvp.Key}: {kvp.Value} ({kvp.Value.GetType().Name})");
            }

            // 3. 尝试反序列化
            try
            {
                var result = BsonConversion.FromBsonValue(doc, typeof(Dictionary<string, object>));
                Console.WriteLine($"反序列化成功: {result?.GetType().Name}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"反序列化失败: {ex.Message}");
            }
        }
        else
        {
            Console.WriteLine("不是BsonDocument!");
        }
    }
}