using System;
using TinyDb.Bson;
using TinyDb.Serialization;

public class ReflectionTestEntity
{
    public string Id { get; set; } = string.Empty;
    public string PublicField = string.Empty;
    public string PublicProperty { get; set; } = string.Empty;
    private string PrivateField = string.Empty;
    private string PrivateProperty { get; set; } = string.Empty;
}

class Program
{
    static void Main()
    {
        Console.WriteLine("调试公共字段序列化问题...");

        // 创建测试实体
        var entity = new ReflectionTestEntity
        {
            Id = "reflection_test_001",
            PublicField = "public_field",
            PublicProperty = "public_property"
        };

        Console.WriteLine($"原始实体:");
        Console.WriteLine($"  Id: {entity.Id}");
        Console.WriteLine($"  PublicField: '{entity.PublicField}'");
        Console.WriteLine($"  PublicProperty: '{entity.PublicProperty}'");

        try
        {
            // 序列化
            var bsonDoc = BsonMapper.ToDocument(entity);
            Console.WriteLine($"\n序列化成功，BsonDocument包含 {bsonDoc.Count} 个字段:");

            foreach (var element in bsonDoc)
            {
                Console.WriteLine($"  {element.Key}: {element.Value} (类型: {element.Value.GetType().Name})");
            }

            // 检查字段是否存在
            Console.WriteLine($"\n检查字段:");
            Console.WriteLine($"  包含_id: {bsonDoc.ContainsKey("_id")}");
            Console.WriteLine($"  包含publicField: {bsonDoc.ContainsKey("publicField")}");
            Console.WriteLine($"  包含publicProperty: {bsonDoc.ContainsKey("publicProperty")}");

            if (bsonDoc.ContainsKey("publicField"))
            {
                var publicFieldValue = bsonDoc["publicField"];
                Console.WriteLine($"  publicField值: '{publicFieldValue}' (类型: {publicFieldValue.GetType().Name})");
                if (publicFieldValue is BsonString bsonString)
                {
                    Console.WriteLine($"  BsonString实际值: '{bsonString.Value}'");
                }
            }

            // 反序列化
            var deserializedEntity = BsonMapper.ToObject<ReflectionTestEntity>(bsonDoc);
            Console.WriteLine($"\n反序列化成功:");
            Console.WriteLine($"  Id: {deserializedEntity.Id}");
            Console.WriteLine($"  PublicField: '{deserializedEntity.PublicField}'");
            Console.WriteLine($"  PublicProperty: '{deserializedEntity.PublicProperty}'");

            // 验证
            Console.WriteLine($"\n验证结果:");
            Console.WriteLine($"  PublicField匹配: {entity.PublicField == deserializedEntity.PublicField}");
            Console.WriteLine($"  PublicProperty匹配: {entity.PublicProperty == deserializedEntity.PublicProperty}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"错误: {ex.Message}");
            Console.WriteLine($"堆栈跟踪:\n{ex.StackTrace}");
        }
    }
}