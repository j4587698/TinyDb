using TinyDb.Attributes;
using TinyDb.Core;
using TinyDb.Index;
using TinyDb.Storage;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Index;

/// <summary>
/// 索引结构局部损坏时，统计与压缩必须仍然可用。
/// </summary>
/// <remarks>
/// 回归背景：统计属性会递归遍历整棵索引树，遇到悬空子指针直接抛 InvalidDataException。
/// 而 CompactDatabase 复制索引时读取统计，于是"索引坏了 → 统计抛异常 → 压缩也抛异常"，
/// 唯一的修复手段被损坏本身挡住，一处局部损坏被放大成整库不可恢复。
/// </remarks>
public class DamagedIndexResilienceTests : IDisposable
{
    private readonly string _dbPath;

    public DamagedIndexResilienceTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"damaged_idx_{Guid.NewGuid()}.db");
    }

    public void Dispose()
    {
        foreach (var path in new[] { _dbPath, _dbPath + ".writer.lock", Path.ChangeExtension(_dbPath, null) + "-wal.db" })
        {
            try { if (File.Exists(path)) File.Delete(path); } catch { }
        }
    }

    [Test]
    public async Task CompactDatabase_WhenIndexSubtreeIsDangling_ShouldStillSucceed()
    {
        uint pageSize;

        // 1. 建库，插入足够多的文档让索引树长到多层（必须存在内部节点才有子指针可破坏）
        {
            using var engine = new TinyDbEngine(_dbPath);
            var collection = engine.GetCollection<DamagedIndexTestDoc>();
            for (var i = 1; i <= 2000; i++)
            {
                collection.Insert(new DamagedIndexTestDoc { Id = i, Name = $"name-{i:D5}" });
            }

            engine.EnsureIndex(nameof(DamagedIndexTestDoc), nameof(DamagedIndexTestDoc.Name), "idx_name");
            pageSize = engine.GetStatistics().PageSize;
        }

        // 2. 注入损坏：把某个被内部节点引用的子页改成数据页，制造悬空子指针
        var victimPageId = CorruptOneIndexChildPage(_dbPath, pageSize);
        await Assert.That(victimPageId).IsNotEqualTo(0u);

        // 3. 统计不再抛异常，而是如实报告损坏的子树数量
        {
            using var engine = new TinyDbEngine(_dbPath);
            var stats = engine.GetIndexManager(nameof(DamagedIndexTestDoc)).GetAllStatistics().ToList();
            await Assert.That(stats).IsNotEmpty();
            await Assert.That(stats.Sum(s => s.DamagedSubtreeCount)).IsGreaterThan(0);
        }

        // 4. 关键：压缩必须能跑通——它是把损坏索引重建回来的唯一手段
        {
            using var engine = new TinyDbEngine(_dbPath);
            engine.CompactDatabase();
        }

        // 5. 压缩后数据完好，且索引已重建为无损结构
        {
            using var engine = new TinyDbEngine(_dbPath);
            var collection = engine.GetCollection<DamagedIndexTestDoc>();
            await Assert.That(collection.Count()).IsEqualTo(2000);

            var stats = engine.GetIndexManager(nameof(DamagedIndexTestDoc)).GetAllStatistics().ToList();
            await Assert.That(stats.Sum(s => s.DamagedSubtreeCount)).IsEqualTo(0);
        }
    }

    /// <summary>
    /// 找到一个被内部节点引用的索引子页，把它从索引页“抽走”，返回被破坏的页号（0 表示没找到）。
    /// </summary>
    /// <remarks>
    /// 现实中的损坏形态是“索引页被回收后改作他用，父节点的子指针却还指着它”。
    /// 这里把页型改为 Empty 来模拟：对索引层而言子指针已悬空，而全表扫描不会去解析它，
    /// 避免测试里又叠加一个“把索引内容当文档读”的人造问题。
    /// </remarks>
    private static uint CorruptOneIndexChildPage(string dbPath, uint pageSize)
    {
        using var diskStream = new DiskStream(dbPath);
        using var pageManager = new PageManager(diskStream, pageSize);

        var childIds = new List<uint>();
        var totalPages = pageManager.TotalPages;
        for (uint id = 1; id <= totalPages; id++)
        {
            Page page;
            try { page = pageManager.GetPage(id); }
            catch { continue; }

            if (page.PageType != PageType.Index) continue;

            DiskBTreeNode node;
            try { node = new DiskBTreeNode(page, pageManager); }
            catch { continue; }

            if (!node.IsLeaf)
            {
                childIds.AddRange(node.ChildrenIds);
            }
        }

        var victim = childIds.FirstOrDefault();
        if (victim == 0) return 0;

        var victimPage = pageManager.GetPage(victim);
        victimPage.UpdatePageType(PageType.Empty);
        pageManager.SavePage(victimPage, forceFlush: true);
        return victim;
    }
}

[Entity]
public class DamagedIndexTestDoc
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
}
