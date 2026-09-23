using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using TinyDb.Attributes;
using TinyDb.Core;
using TinyDb.Query;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Query;

/// <summary>
/// 回归测试：查询层的 成员名 -> 存储字段名 解析。
/// 覆盖自定义 Id 属性名、嵌套 .Id 误判为根主键等历史缺陷。
/// </summary>
[NotInParallel]
public class QueryIdFieldResolutionTests
{
    private string _testFile = null!;
    private TinyDbEngine _engine = null!;

    [Before(Test)]
    public void Setup()
    {
        _testFile = Path.Combine(Path.GetTempPath(), $"id_resolution_{Guid.NewGuid():N}.db");
        _engine = new TinyDbEngine(_testFile);
    }

    [After(Test)]
    public void Cleanup()
    {
        _engine?.Dispose();
        if (File.Exists(_testFile))
        {
            File.Delete(_testFile);
        }
    }

    // ---------- [Id] 特性标注的自定义主键属性 ----------

    [Test]
    public async Task Find_WithIdAttributeCustomName_ShouldReturnMatchingDocument()
    {
        var col = _engine.GetCollection<IdAttrEntity>();
        col.Insert(new IdAttrEntity { Uid = 7, Text = "seven" });
        col.Insert(new IdAttrEntity { Uid = 8, Text = "eight" });

        var matches = col.Find(x => x.Uid == 7).ToList();

        await Assert.That(matches).Count().IsEqualTo(1);
        await Assert.That(matches[0].Text).IsEqualTo("seven");
    }

    [Test]
    public async Task FindOne_WithIdAttributeCustomName_ShouldReturnMatchingDocument()
    {
        var col = _engine.GetCollection<IdAttrEntity>();
        col.Insert(new IdAttrEntity { Uid = 7, Text = "seven" });
        col.Insert(new IdAttrEntity { Uid = 8, Text = "eight" });

        var match = col.FindOne(x => x.Uid == 8);

        await Assert.That(match).IsNotNull();
        await Assert.That(match!.Text).IsEqualTo("eight");
    }

    [Test]
    public async Task FindOneAsync_WithIdAttributeCustomName_ShouldReturnMatchingDocument()
    {
        var col = _engine.GetCollection<IdAttrEntity>();
        col.Insert(new IdAttrEntity { Uid = 7, Text = "seven" });

        var match = await col.FindOneAsync(x => x.Uid == 7);

        await Assert.That(match).IsNotNull();
        await Assert.That(match!.Text).IsEqualTo("seven");
    }

    [Test]
    public async Task Find_WithIdAttributeCustomName_RangePredicate_ShouldReturnMatchingDocuments()
    {
        var col = _engine.GetCollection<IdAttrEntity>();
        for (var i = 1; i <= 5; i++)
        {
            col.Insert(new IdAttrEntity { Uid = i, Text = $"t{i}" });
        }

        var matches = col.Find(x => x.Uid > 3).ToList();

        await Assert.That(matches).Count().IsEqualTo(2);
        await Assert.That(matches.All(x => x.Uid > 3)).IsTrue();
    }

    [Test]
    public async Task Find_WithIdAttributeCustomName_CombinedPredicate_ShouldReturnMatchingDocument()
    {
        var col = _engine.GetCollection<IdAttrEntity>();
        col.Insert(new IdAttrEntity { Uid = 1, Text = "keep" });
        col.Insert(new IdAttrEntity { Uid = 2, Text = "drop" });

        var matches = col.Find(x => x.Uid == 1 && x.Text == "keep").ToList();
        var noMatches = col.Find(x => x.Uid == 1 && x.Text == "drop").ToList();

        await Assert.That(matches).Count().IsEqualTo(1);
        await Assert.That(noMatches).Count().IsEqualTo(0);
    }

    // ---------- [Entity(IdProperty = ...)] 指定的自定义主键属性 ----------

    [Test]
    public async Task Find_WithEntityIdPropertyCustomName_ShouldReturnMatchingDocument()
    {
        var col = _engine.GetCollection<EntityIdPropEntity>();
        col.Insert(new EntityIdPropEntity { Key = 100, Label = "hundred" });
        col.Insert(new EntityIdPropEntity { Key = 200, Label = "two hundred" });

        var matches = col.Find(x => x.Key == 200).ToList();

        await Assert.That(matches).Count().IsEqualTo(1);
        await Assert.That(matches[0].Label).IsEqualTo("two hundred");
    }

    [Test]
    public async Task Query_OrderByCustomIdProperty_ShouldSortByStoredIdField()
    {
        var col = _engine.GetCollection<EntityIdPropEntity>();
        col.Insert(new EntityIdPropEntity { Key = 3, Label = "c" });
        col.Insert(new EntityIdPropEntity { Key = 1, Label = "a" });
        col.Insert(new EntityIdPropEntity { Key = 2, Label = "b" });

        var ordered = col.Query().OrderBy(x => x.Key).ToList();

        await Assert.That(ordered).Count().IsEqualTo(3);
        await Assert.That(ordered[0].Key).IsEqualTo(1);
        await Assert.That(ordered[1].Key).IsEqualTo(2);
        await Assert.That(ordered[2].Key).IsEqualTo(3);
    }

    // ---------- 嵌套成员访问不得被误判为根主键 ----------

    [Test]
    public async Task Find_NestedIdPredicate_ShouldNotBeTreatedAsRootPrimaryKey()
    {
        var col = _engine.GetCollection<OuterEntity>();
        col.Insert(new OuterEntity { Id = 100, Owner = new OwnerInfo { Id = 5, Name = "a" } });
        col.Insert(new OuterEntity { Id = 101, Owner = new OwnerInfo { Id = 6, Name = "b" } });

        var matches = col.Find(x => x.Owner.Id == 5).ToList();

        await Assert.That(matches).Count().IsEqualTo(1);
        await Assert.That(matches[0].Id).IsEqualTo(100);
    }

    [Test]
    public async Task Find_NestedNamePredicate_ShouldNotMatchRootField()
    {
        var col = _engine.GetCollection<OuterEntity>();
        col.Insert(new OuterEntity { Id = 100, Name = "b", Owner = new OwnerInfo { Id = 5, Name = "a" } });
        col.Insert(new OuterEntity { Id = 101, Name = "a", Owner = new OwnerInfo { Id = 6, Name = "b" } });

        var matches = col.Find(x => x.Owner.Name == "a").ToList();

        await Assert.That(matches).Count().IsEqualTo(1);
        await Assert.That(matches[0].Id).IsEqualTo(100);
    }

    [Test]
    public async Task Find_NestedIdPredicate_WithRootIdPredicate_ShouldApplyBoth()
    {
        var col = _engine.GetCollection<OuterEntity>();
        col.Insert(new OuterEntity { Id = 100, Owner = new OwnerInfo { Id = 5, Name = "a" } });
        col.Insert(new OuterEntity { Id = 101, Owner = new OwnerInfo { Id = 5, Name = "b" } });

        var matches = col.Find(x => x.Id == 101 && x.Owner.Id == 5).ToList();
        var noMatches = col.Find(x => x.Id == 101 && x.Owner.Id == 9).ToList();

        await Assert.That(matches).Count().IsEqualTo(1);
        await Assert.That(matches[0].Id).IsEqualTo(101);
        await Assert.That(noMatches).Count().IsEqualTo(0);
    }

    // ---------- 常规 Id 属性的对照组 ----------

    [Test]
    public async Task Find_ConventionalId_ShouldStillWork()
    {
        var col = _engine.GetCollection<OuterEntity>();
        col.Insert(new OuterEntity { Id = 100, Owner = new OwnerInfo { Id = 5, Name = "a" } });
        col.Insert(new OuterEntity { Id = 101, Owner = new OwnerInfo { Id = 6, Name = "b" } });

        var one = col.FindOne(x => x.Id == 101);
        var greater = col.Find(x => x.Id > 100).ToList();

        await Assert.That(one).IsNotNull();
        await Assert.That(one!.Id).IsEqualTo(101);
        await Assert.That(greater).Count().IsEqualTo(1);
    }

    [Test]
    public async Task Find_UppercaseIdProperty_ShouldResolveToStoredIdField()
    {
        var col = _engine.GetCollection<UpperCaseIdEntity>();
        col.Insert(new UpperCaseIdEntity { ID = 11, Tag = "x" });
        col.Insert(new UpperCaseIdEntity { ID = 22, Tag = "y" });

        var matches = col.Find(x => x.ID == 22).ToList();
        var ordered = col.Query().OrderByDescending(x => x.ID).ToList();

        await Assert.That(matches).Count().IsEqualTo(1);
        await Assert.That(matches[0].Tag).IsEqualTo("y");
        await Assert.That(ordered[0].ID).IsEqualTo(22);
    }
}

[Entity("id_attr_entities")]
public partial class IdAttrEntity
{
    [Id]
    public int Uid { get; set; }
    public string Text { get; set; } = "";
}

[Entity("entity_id_prop_entities", IdProperty = nameof(Key))]
public partial class EntityIdPropEntity
{
    public int Key { get; set; }
    public string Label { get; set; } = "";
}

[Entity("id_resolution_outer")]
public partial class OuterEntity
{
    public int Id { get; set; }
    public string Name { get; set; } = "";
    public OwnerInfo Owner { get; set; } = new();
}

public partial class OwnerInfo
{
    public int Id { get; set; }
    public string Name { get; set; } = "";
}

[Entity("upper_case_id_entities")]
public partial class UpperCaseIdEntity
{
    public int ID { get; set; }
    public string Tag { get; set; } = "";
}
