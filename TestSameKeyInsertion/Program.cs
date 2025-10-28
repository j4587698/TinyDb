using System;
using SimpleDb.Bson;
using SimpleDb.Index;

namespace TestSameKeyInsertion
{
    public class Program
    {
        public static void Main()
        {
            Console.WriteLine("=== 测试相同键插入逻辑 ===");

            // 创建B+树节点
            var node = new BTreeNode(100, true);
            var key = new IndexKey(new BsonString("test"));
            var docId1 = new BsonInt32(1);
            var docId2 = new BsonInt32(2);

            Console.WriteLine($"初始状态: KeyCount={node.KeyCount}, DocumentCount={node.DocumentCount}");

            // 插入第一个文档
            Console.WriteLine($"\n插入第一个文档: docId={docId1}");
            var needSplit1 = node.Insert(key, docId1);
            Console.WriteLine($"插入后: KeyCount={node.KeyCount}, DocumentCount={node.DocumentCount}, NeedSplit={needSplit1}");
            Console.WriteLine($"GetDocumentId(0)={node.GetDocumentId(0)}");

            // 插入第二个文档（相同键）
            Console.WriteLine($"\n插入第二个文档（相同键）: docId={docId2}");
            var needSplit2 = node.Insert(key, docId2);
            Console.WriteLine($"插入后: KeyCount={node.KeyCount}, DocumentCount={node.DocumentCount}, NeedSplit={needSplit2}");
            Console.WriteLine($"GetDocumentId(0)={node.GetDocumentId(0)}");
            if (node.DocumentCount > 1)
            {
                Console.WriteLine($"GetDocumentId(1)={node.GetDocumentId(1)}");
            }

            // 验证测试期望
            Console.WriteLine($"\n=== 测试验证 ===");
            Console.WriteLine($"期望: KeyCount=1, 实际: {node.KeyCount}");
            Console.WriteLine($"期望: GetDocumentId(0)=docId2, 实际: {node.GetDocumentId(0)}");

            bool testPasses = node.KeyCount == 1 && node.GetDocumentId(0).Equals(docId2);
            Console.WriteLine($"测试结果: {(testPasses ? "✅ 通过" : "❌ 失败")}");
        }
    }
}
