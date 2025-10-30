using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using TinyDb.Bson;
using TinyDb.Index;

class Program
{
    static async Task Main()
    {
        Console.WriteLine("调试B+树并发操作问题...");

        // 创建B+树索引 - 匹配测试环境设置
        const int SmallMaxKeys = 4; // 与测试环境相同
        using var index = new BTreeIndex("test_index", new[] { "key" }, false, SmallMaxKeys);

        Console.WriteLine("开始并发操作测试...");

        var tasks = new List<Task>();
        var exceptions = new List<Exception>();
        const int operationsPerTask = 50;

        // Act - 并发执行插入和删除操作（与测试相同）
        for (int i = 0; i < 4; i++)
        {
            var taskId = i;
            tasks.Add(Task.Run(() =>
            {
                try
                {
                    for (int j = 0; j < operationsPerTask; j++)
                    {
                        var key = new IndexKey(new BsonInt32(taskId * 1000 + j));
                        var docId = new BsonString($"doc_{taskId}_{j}");

                        // 插入操作
                        index.Insert(key, docId);

                        // 随机删除一些之前插入的数据
                        if (j > 10)
                        {
                            var deleteKey = new IndexKey(new BsonInt32(taskId * 1000 + j - 10));
                            index.Delete(deleteKey, new BsonString($"doc_{taskId}_{j - 10}"));
                        }
                    }
                }
                catch (Exception ex)
                {
                    lock (exceptions)
                    {
                        exceptions.Add(ex);
                    }
                }
            }));
        }

        await Task.WhenAll(tasks);

        Console.WriteLine($"并发操作完成，异常数: {exceptions.Count}");
        if (exceptions.Any())
        {
            foreach (var ex in exceptions.Take(3)) // 只显示前3个异常
            {
                Console.WriteLine($"异常: {ex.Message}");
            }
        }

        // 验证索引
        Console.WriteLine($"EntryCount: {index.EntryCount}");
        Console.WriteLine($"NodeCount: {index.NodeCount}");

        var isValid = index.Validate();
        Console.WriteLine($"索引验证结果: {isValid}");

        if (!isValid)
        {
            Console.WriteLine("索引验证失败，尝试分析具体原因...");
            await AnalyzeIndexStructure(index);

            // 添加更详细的调试信息
            Console.WriteLine("\n=== 详细调试信息 ===");
            await DebugValidateProcess(index);
        }

        Console.WriteLine("调试完成。");
    }

    static async Task AnalyzeIndexStructure(BTreeIndex index)
    {
        Console.WriteLine("分析索引结构...");

        // 获取所有条目
        var allEntries = index.GetAll().ToList();
        Console.WriteLine($"总条目数: {allEntries.Count}");

        // 检查键的顺序
        var keys = new List<IndexKey>();
        foreach (var entry in allEntries.Take(20)) // 只检查前20个
        {
            if (entry is BsonString str)
            {
                // 从doc_id中提取key
                var parts = str.Value.Split('_');
                if (parts.Length >= 2 && int.TryParse(parts[1], out var num))
                {
                    keys.Add(new IndexKey(new BsonInt32(num)));
                }
            }
        }

        Console.WriteLine($"检查前{keys.Count}个键的顺序:");
        for (int i = 1; i < keys.Count; i++)
        {
            var comparison = keys[i-1].CompareTo(keys[i]);
            Console.WriteLine($"  {keys[i-1]} vs {keys[i]}: {comparison}");
            if (comparison >= 0)
            {
                Console.WriteLine($"    ❌ 顺序错误: {keys[i-1]} >= {keys[i]}");
            }
        }

        // 检查是否有重复键
        var duplicateKeys = keys.GroupBy(k => k)
                               .Where(g => g.Count() > 1)
                               .Select(g => g.Key);

        foreach (var dupKey in duplicateKeys.Take(5))
        {
            Console.WriteLine($"❌ 发现重复键: {dupKey}");
        }
    }

    static async Task DebugValidateProcess(BTreeIndex index)
    {
        Console.WriteLine("开始详细验证过程分析...");

        // 使用反射来访问ValidateNode方法
        var validateMethod = typeof(BTreeIndex).GetMethod("ValidateNode",
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);

        if (validateMethod == null)
        {
            Console.WriteLine("无法找到ValidateNode方法");
            return;
        }

        // 获取根节点
        var rootField = typeof(BTreeIndex).GetField("_root",
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);

        if (rootField == null)
        {
            Console.WriteLine("无法找到_root字段");
            return;
        }

        var root = rootField.GetValue(index);
        if (root == null)
        {
            Console.WriteLine("根节点为空");
            return;
        }

        Console.WriteLine($"根节点类型: {root.GetType().Name}");

        // 递归验证所有节点
        await ValidateNodeRecursively(root, validateMethod, index, 0);
    }

    static async Task ValidateNodeRecursively(object node, System.Reflection.MethodInfo validateMethod, BTreeIndex index, int depth)
    {
        var indent = new string(' ', depth * 2);
        Console.WriteLine($"{indent}验证节点 (深度 {depth}):");

        var nodeType = node.GetType();
        var isLeaf = (bool)nodeType.GetProperty("IsLeaf")!.GetValue(node)!;
        var keyCount = (int)nodeType.GetProperty("KeyCount")!.GetValue(node)!;

        Console.WriteLine($"{indent}  IsLeaf: {isLeaf}, KeyCount: {keyCount}");

        // 检查键的顺序
        if (keyCount > 1)
        {
            var getKeyMethod = nodeType.GetMethod("GetKey")!;
            var keys = new List<IndexKey>();

            for (int i = 0; i < keyCount; i++)
            {
                var key = (IndexKey)getKeyMethod.Invoke(node, new object[] { i })!;
                keys.Add(key);
                Console.WriteLine($"{indent}    Key[{i}]: {key}");
            }

            // 检查顺序
            for (int i = 1; i < keys.Count; i++)
            {
                var comparison = keys[i - 1].CompareTo(keys[i]);
                Console.WriteLine($"{indent}    比较 {keys[i-1]} vs {keys[i]}: {comparison}");

                if (comparison >= 0)
                {
                    Console.WriteLine($"{indent}    ❌ 顺序错误: {keys[i-1]} >= {keys[i]}");
                }
            }
        }

        // 递归检查子节点
        if (!isLeaf)
        {
            var childCount = (int)nodeType.GetProperty("ChildCount")!.GetValue(node)!;
            var getChildMethod = nodeType.GetMethod("GetChild")!;

            for (int i = 0; i < childCount; i++)
            {
                var child = getChildMethod.Invoke(node, new object[] { i });
                await ValidateNodeRecursively(child!, validateMethod, index, depth + 1);
            }
        }
    }
}