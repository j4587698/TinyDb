using System;
using System.Linq;
using SimpleDb.Bson;
using SimpleDb.Index;

namespace TestBorrowFromSibling
{
    public class Program
    {
        public static void Main()
        {
            Console.WriteLine("=== 测试BorrowFromSibling逻辑 ===");

            // Arrange
            var node1 = new BTreeNode(100, true);
            var node2 = new BTreeNode(100, true);
            var separatorKey = new IndexKey(new BsonString("separator"));

            node1.Insert(new IndexKey(new BsonString("a")), new BsonInt32(1));
            node2.Insert(new IndexKey(new BsonString("b")), new BsonInt32(2));
            node2.Insert(new IndexKey(new BsonString("c")), new BsonInt32(3));

            Console.WriteLine("借之前:");
            Console.WriteLine($"node1 keys: [{string.Join(", ", Enumerable.Range(0, node1.KeyCount).Select(i => node1.GetKey(i)))}]");
            Console.WriteLine($"node2 keys: [{string.Join(", ", Enumerable.Range(0, node2.KeyCount).Select(i => node2.GetKey(i)))}]");

            // Act - 从node2借键给node1 (isLeftSibling = false, 表示node2是左兄弟)
            var newSeparator = node1.BorrowFromSibling(node2, false, separatorKey);

            Console.WriteLine("\n借之后:");
            Console.WriteLine($"node1 keys: [{string.Join(", ", Enumerable.Range(0, node1.KeyCount).Select(i => node1.GetKey(i)))}]");
            Console.WriteLine($"node2 keys: [{string.Join(", ", Enumerable.Range(0, node2.KeyCount).Select(i => node2.GetKey(i)))}]");
            Console.WriteLine($"newSeparator: {newSeparator}");

            // 测试期望
            Console.WriteLine($"\n=== 测试验证 ===");
            Console.WriteLine($"期望: node1.KeyCount=2, 实际: {node1.KeyCount}");
            Console.WriteLine($"期望: node2.KeyCount=1, 实际: {node2.KeyCount}");
            Console.WriteLine($"期望: newSeparator=\"b\", 实际: {newSeparator}");

            // 检查实际借的键
            var expectedBorrowedKey = new IndexKey(new BsonString("b"));
            bool testPasses = node1.KeyCount == 2 && node2.KeyCount == 1 && newSeparator.Equals(expectedBorrowedKey);
            Console.WriteLine($"测试结果: {(testPasses ? "✅ 通过" : "❌ 失败")}");

            if (!testPasses)
            {
                Console.WriteLine("\n分析:");
                Console.WriteLine("当前实现：从左兄弟借最后一个键（c）");
                Console.WriteLine("测试期望：从左兄弟借第一个键（b）");
                Console.WriteLine("需要修改实现以满足测试期望");
            }
        }
    }
}
