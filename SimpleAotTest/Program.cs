using System;
using System.Linq;
using TinyDb.Core;
using TinyDb.Attributes;
using TinyDb.Bson;

namespace SimpleAotTest;

[Entity("tests")]
public class Test
{
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();
    public string Name { get; set; } = "";
}

class Program
{
    static void Main()
    {
        var engine = new TinyDbEngine("test.db");
        var tests = engine.GetCollection<Test>();

        tests.Insert(new Test(){Name = "张三"});
        tests.Insert(new Test(){Name = "李四"});

        var names = tests.Find(x => x.Name == "张三");
        Console.WriteLine(names.Count());
        Console.WriteLine(names.First().Id);

        engine.Dispose();
    }
}