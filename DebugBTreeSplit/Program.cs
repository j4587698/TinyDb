using System;
using System.Linq;
using TinyDb.Bson;
using TinyDb.Index;

class Program
{
    static void Main()
    {
        Console.WriteLine("调试B+树节点分裂和数据丢失问题...");

        // 创建B+树索引 - 匹配测试环境设置
        const int SmallMaxKeys = 4; // 与测试环境相同
        using var index = new BTreeIndex("test_index", new[] { "key" }, false, SmallMaxKeys);

        // 测试极值键 - 与BoundaryValues_ShouldHandleCorrectly测试相同
        var extremeKeys = new[]
        {
            new IndexKey(new BsonInt32(int.MinValue)),
            new IndexKey(new BsonInt32(int.MaxValue)),
            new IndexKey(new BsonInt32(0)),
            new IndexKey(new BsonInt32(-1)),
            new IndexKey(new BsonInt32(1))
        };

        var docIds = extremeKeys.Select((k, i) => new BsonString($"doc_extreme_{i}")).ToArray();

        Console.WriteLine($"准备插入 {extremeKeys.Length} 个键值对:");
        for (int i = 0; i < extremeKeys.Length; i++)
        {
            Console.WriteLine($"  {i}: Key={extremeKeys[i]}, DocId={docIds[i]}");
        }

        // 插入极值键
        Console.WriteLine("\n开始插入...");
        for (int i = 0; i < extremeKeys.Length; i++)
        {
            var inserted = index.Insert(extremeKeys[i], docIds[i]);
            Console.WriteLine($"  插入 {i}: {extremeKeys[i]} -> {docIds[i]}, 成功={inserted}, EntryCount={index.EntryCount}");
        }

        // 验证查找
        Console.WriteLine("\n验证每个键的查找:");
        for (int i = 0; i < extremeKeys.Length; i++)
        {
            var found = index.FindExact(extremeKeys[i]);
            Console.WriteLine($"  查找 {i}: {extremeKeys[i]} -> {(found != null ? found.ToString() : "NULL")}");
        }

        // 获取所有文档
        Console.WriteLine("\n获取所有文档:");
        var allDocs = index.GetAll().ToList();
        Console.WriteLine($"  预期数量: {extremeKeys.Length}");
        Console.WriteLine($"  实际数量: {allDocs.Count}");
        Console.WriteLine($"  丢失文档数: {extremeKeys.Length - allDocs.Count}");

        Console.WriteLine("\n所有文档列表:");
        for (int i = 0; i < allDocs.Count; i++)
        {
            Console.WriteLine($"  {i}: {allDocs[i]}");
        }

        // 检查索引完整性
        Console.WriteLine($"\n索引验证: {index.Validate()}");
        Console.WriteLine($"节点数量: {index.NodeCount}");
        Console.WriteLine($"条目数量: {index.EntryCount}");

        // 尝试范围查询
        var minKey = new IndexKey(new BsonInt32(int.MinValue));
        var maxKey = new IndexKey(new BsonInt32(int.MaxValue));
        var rangeResults = index.FindRange(minKey, maxKey).ToList();
        Console.WriteLine($"\n范围查询结果: {rangeResults.Count} 个文档");

        Console.WriteLine("\n调试完成。");
    }
}