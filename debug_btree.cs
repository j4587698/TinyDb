using System;
using TinyDb.Bson;
using TinyDb.Index;

// 简单的B+树调试程序
class Program
{
    static void Main()
    {
        Console.WriteLine("调试B+树分裂逻辑...");

        // 创建小容量索引便于调试
        var index = new BTreeIndex("test", new[] { "test" }, false, 4);

        // 插入数据直到触发分裂
        for (int i = 1; i <= 6; i++)
        {
            var key = new IndexKey(new BsonInt32(i));
            var docId = new BsonString($"doc_{i}");
            var inserted = index.Insert(key, docId);
            Console.WriteLine($"插入 key={i}, success={inserted}, nodes={index.NodeCount}, entries={index.EntryCount}");

            // 验证树结构
            var isValid = index.Validate();
            Console.WriteLine($"  树结构有效: {isValid}");

            if (!isValid)
            {
                Console.WriteLine("发现无效结构，停止插入");
                break;
            }
        }

        // 测试查找
        Console.WriteLine("\n测试查找:");
        for (int i = 1; i <= 6; i++)
        {
            var key = new IndexKey(new BsonInt32(i));
            var found = index.FindExact(key);
            Console.WriteLine($"查找 key={i}, found: {(found != null ? found.ToString() : "null")}");
        }

        // 输出统计信息
        Console.WriteLine("\n统计信息:");
        var stats = index.GetStatistics();
        Console.WriteLine($"  节点数: {stats.NodeCount}");
        Console.WriteLine($"  条目数: {stats.EntryCount}");
        Console.WriteLine($"  树高度: {stats.TreeHeight}");
        Console.WriteLine($"  根是叶子: {stats.RootIsLeaf}");
        Console.WriteLine($"  平均键/节点: {stats.AverageKeysPerNode:F2}");

        index.Dispose();
    }
}