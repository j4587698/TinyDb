using System;
using System.Linq;
using SimpleDb.Bson;
using SimpleDb.Index;

namespace TestBTreeIndexFind
{
    public class Program
    {
        public static void Main()
        {
            Console.WriteLine("=== 测试BTreeNode Find逻辑 ===");

            // 直接测试BTreeNode
            var node = new BTreeNode(100, true);
            var key = new IndexKey(new BsonString("test"));
            var docId1 = new BsonObjectId(ObjectId.NewObjectId());
            var docId2 = new BsonObjectId(ObjectId.NewObjectId());

            Console.WriteLine($"插入第一个文档: docId={docId1}");
            node.Insert(key, docId1);
            Console.WriteLine($"插入后: KeyCount={node.KeyCount}, DocumentCount={node.DocumentCount}");

            Console.WriteLine($"\n插入第二个文档（相同键）: docId={docId2}");
            node.Insert(key, docId2);
            Console.WriteLine($"插入后: KeyCount={node.KeyCount}, DocumentCount={node.DocumentCount}");

            // 测试FindKeyPosition
            Console.WriteLine($"\n=== 测试FindKeyPosition ===");
            var position = node.FindKeyPosition(key);
            Console.WriteLine($"FindKeyPosition({key}) = {position}");
            Console.WriteLine($"KeyCount = {node.KeyCount}");

            // 检查所有键
            Console.WriteLine($"\n=== 检查所有键 ===");
            for (int i = 0; i < node.KeyCount; i++)
            {
                var nodeKey = node.GetKey(i);
                var docId = node.GetDocumentId(i);
                Console.WriteLine($"位置{i}: Key={nodeKey}, DocId={docId}, Equals(key)={nodeKey.Equals(key)}");
            }

            // 测试范围查找
            Console.WriteLine($"\n=== 测试范围查找逻辑 ===");
            if (position < node.KeyCount && node.GetKey(position).Equals(key))
            {
                Console.WriteLine($"找到匹配的起始位置: {position}");
                var endPosition = position;
                while (endPosition < node.KeyCount && node.GetKey(endPosition).Equals(key))
                {
                    Console.WriteLine($"  endPosition={endPosition}, Key={node.GetKey(endPosition)}, 继续检查下一个");
                    endPosition++;
                }
                Console.WriteLine($"最终范围: [{position}, {endPosition})");

                Console.WriteLine($"范围内的文档ID:");
                for (int i = position; i < endPosition; i++)
                {
                    Console.WriteLine($"  位置{i}: {node.GetDocumentId(i)}");
                }
            }
            else
            {
                Console.WriteLine($"未找到匹配的键: position={position}, KeyCount={node.KeyCount}");
            }
        }
    }
}