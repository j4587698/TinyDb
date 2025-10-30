using System;
using TinyDb.Bson;
using TinyDb.Serialization;

public struct CustomStruct
{
    public int Value { get; set; }
    public string Name { get; set; }
}

public class CustomClass
{
    public string Description { get; set; } = string.Empty;
    public decimal Value { get; set; }
}

public class CustomTypeEntity
{
    public string Id { get; set; } = string.Empty;
    public CustomEnumType CustomEnum { get; set; }
    public CustomStruct CustomStruct { get; set; }
    public CustomClass CustomClass { get; set; } = null!;
}

public enum CustomEnumType
{
    OptionA,
    OptionB,
    OptionC
}

class Program
{
    static void Main()
    {
        Console.WriteLine("调试结构体序列化问题...");

        // 创建测试实体
        var entity = new CustomTypeEntity
        {
            Id = "custom_001",
            CustomEnum = CustomEnumType.OptionB,
            CustomStruct = new CustomStruct { Value = 42, Name = "TestStruct" },
            CustomClass = new CustomClass { Description = "Test Description", Value = 3.14m }
        };

        Console.WriteLine($"原始实体:");
        Console.WriteLine($"  CustomStruct.Value: {entity.CustomStruct.Value}");
        Console.WriteLine($"  CustomStruct.Name: {entity.CustomStruct.Name}");
        Console.WriteLine($"  CustomEnum: {entity.CustomEnum}");
        Console.WriteLine($"  CustomClass.Description: {entity.CustomClass.Description}");
        Console.WriteLine($"  CustomClass.Value: {entity.CustomClass.Value}");

        try
        {
            // 序列化
            var bsonDoc = BsonMapper.ToDocument(entity);
            Console.WriteLine($"\n序列化成功，BsonDocument包含 {bsonDoc.Count} 个字段:");

            foreach (var element in bsonDoc)
            {
                Console.WriteLine($"  {element.Key}: {element.Value} (类型: {element.Value.GetType().Name})");

                if (element.Key == "CustomStruct" && element.Value is BsonDocument structDoc)
                {
                    Console.WriteLine($"    CustomStruct内部字段:");
                    foreach (var structElement in structDoc)
                    {
                        Console.WriteLine($"      {structElement.Key}: {structElement.Value} (类型: {structElement.Value.GetType().Name})");
                    }
                }
            }

            // 反序列化
            var deserializedEntity = BsonMapper.ToObject<CustomTypeEntity>(bsonDoc);
            Console.WriteLine($"\n反序列化成功:");
            Console.WriteLine($"  CustomStruct.Value: {deserializedEntity.CustomStruct.Value}");
            Console.WriteLine($"  CustomStruct.Name: {deserializedEntity.CustomStruct.Name}");
            Console.WriteLine($"  CustomEnum: {deserializedEntity.CustomEnum}");
            Console.WriteLine($"  CustomClass.Description: {deserializedEntity.CustomClass.Description}");
            Console.WriteLine($"  CustomClass.Value: {deserializedEntity.CustomClass.Value}");

            // 验证
            Console.WriteLine($"\n验证结果:");
            Console.WriteLine($"  Value匹配: {entity.CustomStruct.Value == deserializedEntity.CustomStruct.Value}");
            Console.WriteLine($"  Name匹配: {entity.CustomStruct.Name == deserializedEntity.CustomStruct.Name}");
            Console.WriteLine($"  Enum匹配: {entity.CustomEnum == deserializedEntity.CustomEnum}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"错误: {ex.Message}");
            Console.WriteLine($"堆栈跟踪:\n{ex.StackTrace}");
        }
    }
}