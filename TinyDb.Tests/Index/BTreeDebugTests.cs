using TinyDb.Bson;
using TinyDb.Index;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Index;

/// <summary>
/// B+树调试测试
/// </summary>
[NotInParallel]
public class BTreeDebugTests
{
    private BTreeIndex _index = null!;

    [Before(Test)]
    public void Setup()
    {
        _index = new BTreeIndex("debug", new[] { "test" }, false, 4);
    }

    [After(Test)]
    public void Cleanup()
    {
        _index?.Dispose();
    }

    /// <summary>
    /// 调试最基本的插入和查找
    /// </summary>
    [Test]
    public async Task BasicInsertAndFind_ShouldWork()
    {
        // 插入一个键
        var key = new IndexKey(new BsonInt32(1));
        var docId = new BsonString("doc_1");
        var inserted = _index.Insert(key, docId);

        await Assert.That(inserted).IsTrue();
        await Assert.That(_index.EntryCount).IsEqualTo(1);
        await Assert.That(_index.Validate()).IsTrue();

        // 查找刚插入的键
        var found = _index.FindExact(key);
        await Assert.That(found != null).IsTrue();
        await Assert.That(found).IsEqualTo(docId);
    }

    /// <summary>
    /// 调试插入多个键但不触发分裂的情况
    /// </summary>
    [Test]
    public async Task MultipleInsertWithoutSplit_ShouldWork()
    {
        // 插入3个键（不触发分裂，maxKeys=4）
        for (int i = 1; i <= 3; i++)
        {
            var key = new IndexKey(new BsonInt32(i));
            var docId = new BsonString($"doc_{i}");
            var inserted = _index.Insert(key, docId);
            await Assert.That(inserted).IsTrue();
        }

        await Assert.That(_index.EntryCount).IsEqualTo(3);
        await Assert.That(_index.Validate()).IsTrue();

        // 查找所有键
        for (int i = 1; i <= 3; i++)
        {
            var key = new IndexKey(new BsonInt32(i));
            var found = _index.FindExact(key);
            await Assert.That(found != null).IsTrue();
            await Assert.That(found).IsEqualTo(new BsonString($"doc_{i}"));
        }
    }

    /// <summary>
    /// 调试插入并触发一个分裂的情况
    /// </summary>
    [Test]
    public async Task InsertAndTriggerSplit_ShouldWork()
    {
        // 插入5个键（触发分裂，maxKeys=4）
        for (int i = 1; i <= 5; i++)
        {
            var key = new IndexKey(new BsonInt32(i));
            var docId = new BsonString($"doc_{i}");
            var inserted = _index.Insert(key, docId);
            Console.WriteLine($"插入 {i}: success={inserted}, nodes={_index.NodeCount}, entries={_index.EntryCount}, valid={_index.Validate()}");
            await Assert.That(inserted).IsTrue();
        }

        await Assert.That(_index.EntryCount).IsEqualTo(5);
        await Assert.That(_index.NodeCount).IsGreaterThan(1);
        await Assert.That(_index.Validate()).IsTrue();

        // 查找所有键
        for (int i = 1; i <= 5; i++)
        {
            var key = new IndexKey(new BsonInt32(i));
            var found = _index.FindExact(key);
            Console.WriteLine($"查找 {i}: found={found != null}");
            await Assert.That(found != null).IsTrue();
            await Assert.That(found).IsEqualTo(new BsonString($"doc_{i}"));
        }
    }
}