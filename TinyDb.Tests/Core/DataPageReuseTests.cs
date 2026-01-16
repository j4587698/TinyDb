using System;
using System.Linq;
using TinyDb.Core;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Core;

public class DataPageReuseTests : IDisposable
{
    private readonly string _dbPath;
    private readonly TinyDbEngine _engine;

    public DataPageReuseTests()
    {
        _dbPath = $"page_reuse_test_{Guid.NewGuid():N}.db";
        _engine = new TinyDbEngine(_dbPath);
    }

    public void Dispose()
    {
        _engine.Dispose();
        try { System.IO.File.Delete(_dbPath); } catch { }
    }

    [Test]
    public async Task Insert_DeleteAll_Insert_ShouldNotLoseData()
    {
        // 这个测试专门用于复现 "st.PageId 指向已释放页面" 的 Bug
        var collectionName = "reuse_test";
        
        // 1. 插入足够的数据以填满至少一个页面并设置 st.PageId
        // PageSize 默认 8192。插入 100 条数据足够填充。
        int count1 = 100;
        for (int i = 0; i < count1; i++)
        {
            _engine.InsertDocument(collectionName, new BsonDocument().Set("_id", i).Set("val", "data"));
        }
        
        await Assert.That(_engine.FindAll(collectionName).Count()).IsEqualTo(count1);

        // 2. 删除所有数据，触发页面释放 (FreePage)
        // 这会将页面放入 PageManager 的空闲列表，但 CollectionState.PageId 可能仍指向它
        for (int i = 0; i < count1; i++)
        {
            _engine.DeleteDocument(collectionName, new BsonInt32(i));
        }
        
        await Assert.That(_engine.FindAll(collectionName).Count()).IsEqualTo(0);

        // 3. 再次插入数据
        // 如果 Bug 存在：
        // - Insert 会直接写入旧的 st.PageId (现已是 Empty 页)
        // - 稍后 NewPage 分配时会从空闲列表再次拿出该页并重置
        // - 导致部分新数据丢失
        int count2 = 100;
        for (int i = 0; i < count2; i++)
        {
            // 使用新 ID 以避免主键冲突干扰逻辑
            _engine.InsertDocument(collectionName, new BsonDocument().Set("_id", i + 1000).Set("val", "new_data"));
        }

        // 4. 验证数据完整性
        var results = _engine.FindAll(collectionName).ToList();
        
        // 在修复前，这里会失败（数量 < 100）
        await Assert.That(results.Count).IsEqualTo(count2);
        
        // 验证内容
        foreach (var doc in results)
        {
            await Assert.That(doc["val"].ToString()).IsEqualTo("new_data");
        }
    }
}
