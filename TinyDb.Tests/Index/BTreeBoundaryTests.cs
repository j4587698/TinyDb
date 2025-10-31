using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using TinyDb.Bson;
using TinyDb.Index;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Index;

/// <summary>
/// B+树边界条件测试 - 防止索引BUG复发
/// 专门测试节点分裂、合并、重平衡的边界情况
/// </summary>
[NotInParallel]
public class BTreeBoundaryTests
{
    private BTreeIndex _index = null!;
    private const int SmallMaxKeys = 4; // 使用小的键数量便于测试边界条件

    [Before(Test)]
    public void Setup()
    {
        _index = new BTreeIndex("test", new[] { "testField" }, false, SmallMaxKeys);
    }

    [After(Test)]
    public void Cleanup()
    {
        _index?.Dispose();
    }

    /// <summary>
    /// 测试节点分裂的边界条件 - 当节点刚好达到最大键数量时的行为
    /// </summary>
    [Test]
    public async Task NodeSplit_ShouldOccurExactlyAtMaxKeys()
    {
        // Arrange - 插入刚好达到最大键数量的数据
        var keys = Enumerable.Range(1, SmallMaxKeys).Select(i => new IndexKey(new BsonInt32(i))).ToArray();
        var docIds = keys.Select(k => new BsonString($"doc_{((BsonInt32)k.Values.First()).Value}")).ToArray();

        // Act - 插入前SmallMaxKeys个键，应该不需要分裂
        for (int i = 0; i < SmallMaxKeys; i++)
        {
            var inserted = _index.Insert(keys[i], docIds[i]);
            await Assert.That(inserted).IsTrue();
        }

        // 验证插入后节点状态
        await Assert.That(_index.NodeCount).IsEqualTo(1); // 仍然是单个节点
        await Assert.That(_index.EntryCount).IsEqualTo(SmallMaxKeys);

        // 插入第SmallMaxKeys + 1个键，应该触发分裂
        var extraKey = new IndexKey(new BsonInt32(SmallMaxKeys + 1));
        var extraDocId = new BsonString($"doc_{SmallMaxKeys + 1}");

        var splitInserted = _index.Insert(extraKey, extraDocId);
        await Assert.That(splitInserted).IsTrue();

        // Assert - 验证分裂后的状态
        await Assert.That(_index.NodeCount).IsGreaterThan(1); // 应该有多个节点
        await Assert.That(_index.EntryCount).IsEqualTo(SmallMaxKeys + 1);
        await Assert.That(_index.Validate()).IsTrue(); // 索引应该仍然有效

        // 验证所有键都能正确查找
        for (int i = 1; i <= SmallMaxKeys + 1; i++)
        {
            var searchKey = new IndexKey(new BsonInt32(i));
            var found = _index.FindExact(searchKey);
            await Assert.That(found).IsNotNull();
            await Assert.That(found).IsEqualTo(new BsonString($"doc_{i}"));
        }
    }

    /// <summary>
    /// 测试节点合并的边界条件 - 当节点删除到最小键数量以下时的行为
    /// </summary>
    [Test]
    public async Task NodeMerge_ShouldOccurBelowMinKeys()
    {
        // Arrange - 首先创建足够多的数据触发多次分裂
        var totalKeys = SmallMaxKeys * 3; // 足够触发多次分裂
        var keys = Enumerable.Range(1, totalKeys).Select(i => new IndexKey(new BsonInt32(i))).ToArray();
        var docIds = keys.Select(k => new BsonString($"doc_{((BsonInt32)k.Values.First()).Value}")).ToArray();

        // 插入所有数据
        for (int i = 0; i < totalKeys; i++)
        {
            _index.Insert(keys[i], docIds[i]);
        }

        var nodeCountAfterInserts = _index.NodeCount;
        await Assert.That(nodeCountAfterInserts).IsGreaterThan(1); // 应该有多个节点

        // Act - 逐步删除数据，观察合并行为
        var keysToDelete = keys.Skip(totalKeys / 2).ToArray(); // 删除后半部分的数据

        foreach (var key in keysToDelete)
        {
            var deleted = _index.Delete(key, docIds[keys.ToList().IndexOf(key)]);
            await Assert.That(deleted).IsTrue();
        }

        // Assert - 验证合并后的状态
        await Assert.That(_index.NodeCount).IsLessThanOrEqualTo(nodeCountAfterInserts); // 节点数应该减少或保持
        await Assert.That(_index.Validate()).IsTrue(); // 索引应该仍然有效

        // 验证剩余键都能正确查找
        var remainingKeys = keys.Take(totalKeys / 2);
        foreach (var key in remainingKeys)
        {
            var found = _index.FindExact(key);
            await Assert.That(found).IsNotNull();
        }
    }

    /// <summary>
    /// 测试重平衡的边界条件 - 兄弟节点借键的情况
    /// </summary>
    [Test]
    public async Task Rebalance_ShouldBorrowFromSibling()
    {
        // Arrange - 创建特定的数据分布来测试重平衡
        // 先插入数据创建多个节点
        var keys = Enumerable.Range(1, SmallMaxKeys * 2).Select(i => new IndexKey(new BsonInt32(i))).ToArray();
        var docIds = keys.Select(k => new BsonString($"doc_{((BsonInt32)k.Values.First()).Value}")).ToArray();

        foreach (var key in keys)
        {
            _index.Insert(key, docIds[keys.ToList().IndexOf(key)]);
        }

        // 删除部分数据使某些节点低于最小键数量，但仍有兄弟节点可以借键
        var keysToDelete = keys.Skip(SmallMaxKeys / 2).Take(SmallMaxKeys / 2).ToArray();

        foreach (var key in keysToDelete)
        {
            _index.Delete(key, docIds[keys.ToList().IndexOf(key)]);
        }

        // Assert - 验证重平衡后的索引状态
        await Assert.That(_index.Validate()).IsTrue();
        await Assert.That(_index.EntryCount).IsGreaterThan(0);

        // 验证所有剩余数据仍可访问
        var remainingKeys = keys.Except(keysToDelete);
        foreach (var key in remainingKeys)
        {
            var found = _index.FindExact(key);
            await Assert.That(found).IsNotNull();
        }
    }

    /// <summary>
    /// 测试根节点特殊处理 - 根节点为空时应该提升子节点
    /// </summary>
    [Test]
    public async Task RootNode_ShouldPromoteChildWhenEmpty()
    {
        // Arrange - 创建足够多的数据形成多层树结构
        var keys = Enumerable.Range(1, SmallMaxKeys * 3).Select(i => new IndexKey(new BsonInt32(i))).ToArray();
        var docIds = keys.Select(k => new BsonString($"doc_{((BsonInt32)k.Values.First()).Value}")).ToArray();

        foreach (var key in keys)
        {
            _index.Insert(key, docIds[keys.ToList().IndexOf(key)]);
        }

        var initialHeight = GetTreeHeight();
        await Assert.That(initialHeight).IsGreaterThan(1); // 确保有多层结构

        // Act - 删除大量数据，可能导致根节点变空
        var keysToDelete = keys.Skip(SmallMaxKeys).ToArray(); // 保留少量数据

        foreach (var key in keysToDelete)
        {
            _index.Delete(key, docIds[keys.ToList().IndexOf(key)]);
        }

        // Assert - 验证根节点处理
        await Assert.That(_index.Validate()).IsTrue();
        await Assert.That(_index.EntryCount).IsGreaterThan(0);

        var finalHeight = GetTreeHeight();
        // 高度可能减少，但索引应该仍然有效
        await Assert.That(finalHeight).IsGreaterThanOrEqualTo(1);

        // 验证剩余数据可访问
        var remainingKeys = keys.Except(keysToDelete);
        foreach (var key in remainingKeys)
        {
            var found = _index.FindExact(key);
            await Assert.That(found).IsNotNull();
        }
    }

    /// <summary>
    /// 测试并发操作的边界条件 - 同时进行插入和删除
    /// </summary>
    [Test]
    public async Task ConcurrentOperations_ShouldMaintainIndexIntegrity()
    {
        // Arrange
        var tasks = new List<Task>();
        var exceptions = new List<Exception>();
        const int operationsPerTask = 50;

        // Act - 并发执行插入和删除操作
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
                        _index.Insert(key, docId);

                        // 随机删除一些之前插入的数据
                        if (j > 10)
                        {
                            var deleteKey = new IndexKey(new BsonInt32(taskId * 1000 + j - 10));
                            _index.Delete(deleteKey, new BsonString($"doc_{taskId}_{j - 10}"));
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

        // Assert
        await Assert.That(exceptions).IsEmpty();
        await Assert.That(_index.Validate()).IsTrue();
        await Assert.That(_index.EntryCount).IsGreaterThan(0);
    }

    /// <summary>
    /// 测试边界值处理 - 极大和极小的键值
    /// </summary>
    [Test]
    public async Task BoundaryValues_ShouldHandleCorrectly()
    {
        // Arrange & Act - 测试极值键
        var extremeKeys = new[]
        {
            new IndexKey(new BsonInt32(int.MinValue)),
            new IndexKey(new BsonInt32(int.MaxValue)),
            new IndexKey(new BsonInt32(0)),
            new IndexKey(new BsonInt32(-1)),
            new IndexKey(new BsonInt32(1))
        };

        var docIds = extremeKeys.Select((k, i) => new BsonString($"doc_extreme_{i}")).ToArray();

        // 插入极值键
        for (int i = 0; i < extremeKeys.Length; i++)
        {
            var inserted = _index.Insert(extremeKeys[i], docIds[i]);
            await Assert.That(inserted).IsTrue();
        }

        // 验证极值键查找
        for (int i = 0; i < extremeKeys.Length; i++)
        {
            var found = _index.FindExact(extremeKeys[i]);
            await Assert.That(found).IsNotNull();
            await Assert.That(found).IsEqualTo(docIds[i]);
        }

        // 验证范围查询包含极值
        var allDocs = _index.GetAll().ToList();
        await Assert.That(allDocs).HasCount(extremeKeys.Length);

        // 测试范围查询的边界
        var minKey = new IndexKey(new BsonInt32(int.MinValue));
        var maxKey = new IndexKey(new BsonInt32(int.MaxValue));
        var rangeResults = _index.FindRange(minKey, maxKey).ToList();
        await Assert.That(rangeResults).HasCount(extremeKeys.Length);
    }

    /// <summary>
    /// 删除操作触发合并或借键时，父节点分隔键应即时刷新
    /// </summary>
    [Test]
    public async Task Delete_ShouldRefreshSeparators_AfterMergeAndBorrow()
    {
        var keys = Enumerable.Range(1, 8).Select(i => new IndexKey(new BsonInt32(i))).ToArray();
        var docIds = keys.Select((k, i) => new BsonString($"doc_{i + 1}")).ToArray();
        for (var i = 0; i < keys.Length; i++)
        {
            _index.Insert(keys[i], docIds[i]);
        }

        await AssertNodeKeys(GetRoot(_index), 3, 5);

        // 合并第一批叶子
        _index.Delete(keys[0], docIds[0]);
        var root = GetRoot(_index);
        await AssertNodeKeys(root, 5);
        await AssertNodeKeys(root.GetChild(0), 2, 3, 4);
        await AssertNodeKeys(root.GetChild(1), 5, 6, 7, 8);

        // 收缩右侧叶子并触发借键
        _index.Delete(keys[4], docIds[4]);
        await AssertNodeKeys(GetRoot(_index), 6);

        _index.Delete(keys[5], docIds[5]);
        await AssertNodeKeys(GetRoot(_index), 7);

        _index.Delete(keys[6], docIds[6]);
        root = GetRoot(_index);
        await AssertNodeKeys(root, 4);
        await AssertNodeKeys(root.GetChild(0), 2, 3);
        await AssertNodeKeys(root.GetChild(1), 4, 8);
    }

    /// <summary>
    /// 深层叶子首键变化时，所有祖先分隔键都应更新
    /// </summary>
    [Test]
    public async Task Delete_ShouldRefreshAllAncestorSeparators_ForDeepTrees()
    {
        _index.Clear();

        var keys = Enumerable.Range(1, 48).Select(i => new IndexKey(new BsonInt32(i))).ToArray();
        var docs = keys.Select((k, i) => new BsonString($"doc_{i + 1}")).ToArray();
        for (var i = 0; i < keys.Length; i++)
        {
            _index.Insert(keys[i], docs[i]);
        }

        _index.Delete(new IndexKey(new BsonInt32(25)), new BsonString("doc_25"));
        _index.Delete(new IndexKey(new BsonInt32(24)), new BsonString("doc_24"));

        var path = GetPathToLeaf(_index, new IndexKey(new BsonInt32(26)));
        var leafNode = path[^1].node;
        await Assert.That(leafNode.KeyCount).IsGreaterThan(0);
        var leafValues = GetNodeKeyValues(leafNode);
        await Assert.That(leafValues.First()).IsEqualTo(26);

        for (var i = path.Count - 2; i >= 0; i--)
        {
            var (parentNode, childIndex) = path[i];
            if (childIndex > 0)
            {
                var parentKeys = GetNodeKeyValues(parentNode);
                var childNode = parentNode.GetChild(childIndex);
                var expected = GetLeftmostKeyValue(childNode);
                if (expected.HasValue)
                {
                    await Assert.That(parentKeys[childIndex - 1]).IsEqualTo(expected.Value);
                }
            }
        }
    }

    private static BTreeNode GetRoot(BTreeIndex index)
    {
        var field = typeof(BTreeIndex).GetField("_root", BindingFlags.NonPublic | BindingFlags.Instance);
        return (BTreeNode)(field?.GetValue(index) ?? throw new InvalidOperationException("Root node not found"));
    }

    private static IReadOnlyList<int> GetNodeKeyValues(BTreeNode node)
    {
        return Enumerable.Range(0, node.KeyCount)
            .Select(i => node.GetKey(i))
            .Select(key => (BsonInt32)key.Values.First())
            .Select(value => value.Value)
            .ToArray();
    }

    private static int? GetLeftmostKeyValue(BTreeNode node)
    {
        var current = node;
        while (!current.IsLeaf)
        {
            if (current.ChildCount == 0)
                return null;
            current = current.GetChild(0);
        }

        return current.KeyCount > 0
            ? ((BsonInt32)current.GetKey(0).Values.First()).Value
            : null;
    }

    private static async Task AssertNodeKeys(BTreeNode node, params int[] expected)
    {
        var keys = GetNodeKeyValues(node);
        await Assert.That(keys.Count).IsEqualTo(expected.Length);
        for (var i = 0; i < expected.Length; i++)
        {
            await Assert.That(keys[i]).IsEqualTo(expected[i]);
        }
    }

    private static List<(BTreeNode node, int childIndex)> GetPathToLeaf(BTreeIndex index, IndexKey key)
    {
        var path = new List<(BTreeNode, int)>();
        var node = GetRoot(index);
        while (!node.IsLeaf)
        {
            var childIndex = node.FindKeyPosition(key);
            if (childIndex >= node.KeyCount)
            {
                childIndex = node.ChildCount - 1;
            }
            else if (childIndex < node.KeyCount && node.GetKey(childIndex).Equals(key))
            {
                childIndex++;
            }

            path.Add((node, childIndex));
            node = node.GetChild(childIndex);
        }

        path.Add((node, -1));
        return path;
    }

    private int GetTreeHeight()
    {
        var stats = _index.GetStatistics();
        return stats.TreeHeight;
    }

    /// <summary>
    /// 测试大量数据下的索引稳定性
    /// </summary>
    [Test]
    public async Task LargeDataset_ShouldMaintainStability()
    {
        // Arrange
        const int itemCount = 1000;
        var random = new Random(42); // 固定种子确保可重现
        var keys = new List<IndexKey>();
        var docIds = new List<BsonValue>();

        // Act - 插入大量随机数据
        for (int i = 0; i < itemCount; i++)
        {
            var keyValue = random.Next(1, 100000);
            var key = new IndexKey(new BsonInt32(keyValue));
            var docId = new BsonString($"doc_{keyValue}_{i}");

            // 确保键的唯一性
            if (!keys.Any(k => ((BsonInt32)k.Values.First()).Value == keyValue))
            {
                keys.Add(key);
                docIds.Add(docId);
                _index.Insert(key, docId);
            }
        }

        // Assert - 验证大数据集下的索引状态
        await Assert.That(_index.Validate()).IsTrue();
        await Assert.That(_index.EntryCount).IsEqualTo(keys.Count);

        // 随机验证一些数据点
        var testIndices = Enumerable.Range(0, Math.Min(10, keys.Count)).Select(i => random.Next(0, keys.Count));
        foreach (var index in testIndices)
        {
            var found = _index.FindExact(keys[index]);
            await Assert.That(found).IsNotNull();
            await Assert.That(found).IsEqualTo(docIds[index]);
        }

        // 测试范围查询性能
        var rangeStart = new IndexKey(new BsonInt32(10000));
        var rangeEnd = new IndexKey(new BsonInt32(50000));
        var rangeResults = _index.FindRange(rangeStart, rangeEnd).ToList();

        // 验证范围查询结果的有效性
        foreach (var docId in rangeResults)
        {
            await Assert.That(((BsonString)docId).Value).StartsWith("doc_");
        }
    }
}
