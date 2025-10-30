using System;
using TinyDb.Bson;
using TinyDb.Serialization;

public class CircularEntity
{
    public string Id { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public CircularEntity[] Children { get; set; } = Array.Empty<CircularEntity>();
    public CircularEntity? Parent { get; set; }
}

class Program
{
    static void Main()
    {
        Console.WriteLine("调试循环引用序列化问题...");

        // 创建循环引用对象
        var parent = new CircularEntity
        {
            Id = "parent_001",
            Name = "Parent Entity"
        };

        var child = new CircularEntity
        {
            Id = "child_001",
            Name = "Child Entity"
        };

        parent.Children = new[] { child };
        child.Parent = parent;

        Console.WriteLine($"原始对象:");
        Console.WriteLine($"  Parent.Name: {parent.Name}");
        Console.WriteLine($"  Parent.Id: {parent.Id}");
        Console.WriteLine($"  Child.Name: {child.Name}");
        Console.WriteLine($"  Child.Id: {child.Id}");

        try
        {
            // 序列化parent对象
            var bsonDoc = BsonMapper.ToDocument(parent);
            Console.WriteLine($"\n序列化成功，BsonDocument包含 {bsonDoc.Count} 个字段:");

            foreach (var element in bsonDoc)
            {
                Console.WriteLine($"  {element.Key}: {element.Value} (类型: {element.Value.GetType().Name})");
            }

            // 检查Name字段是否存在
            Console.WriteLine($"\n检查字段:");
            Console.WriteLine($"  包含_id: {bsonDoc.ContainsKey("_id")}");
            Console.WriteLine($"  包含Name: {bsonDoc.ContainsKey("Name")}");
            Console.WriteLine($"  包含Children: {bsonDoc.ContainsKey("Children")}");
            Console.WriteLine($"  包含Parent: {bsonDoc.ContainsKey("Parent")}");

            if (bsonDoc.ContainsKey("Name"))
            {
                Console.WriteLine($"  Name值: {bsonDoc["Name"]}");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"错误: {ex.Message}");
            Console.WriteLine($"堆栈跟踪:\n{ex.StackTrace}");
        }
    }
}