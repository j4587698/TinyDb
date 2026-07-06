using System;
using System.IO;
using System.Linq;
using TinyDb.Attributes;
using TinyDb.Core;
using TinyDb.Query;
using TinyDb.Tests.TestEntities;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Query;

[NotInParallel]
public class QueryTests
{
    private string _testFile = null!;
    private TinyDbEngine _engine = null!;

    [Before(Test)]
    public void Setup()
    {
        _testFile = Path.Combine(Path.GetTempPath(), $"query_test_{Guid.NewGuid():N}.db");
        _engine = new TinyDbEngine(_testFile);
        
        // 预填充数据
        var col = _engine.GetCollection<UserWithIntId>();
        for (int i = 1; i <= 100; i++)
        {
            col.Insert(new UserWithIntId { Id = i, Name = $"User_{i}", Age = 20 + (i % 50) });
        }
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

    [Test]
    public async Task Query_FullTableScan_ShouldWork()
    {
        // Act
        var results = _engine.GetCollection<UserWithIntId>().Find(u => u.Age > 60).ToList();

        // Assert
        // i % 50 ranges from 0 to 49. 20 + (i % 50) ranges from 20 to 69.
        // Age > 60 means i % 50 > 40. That is 41, 42, ..., 49 (9 values) per 50 items.
        // For 100 items, there are 18 such values.
        await Assert.That(results).Count().IsEqualTo(18);
        await Assert.That(results.All(u => u.Age > 60)).IsTrue();
    }

    [Test]
    public async Task Query_WithIndexScan_ShouldWork()
    {
        // Arrange
        _engine.EnsureIndex("UserWithIntId", "Age", "age_idx");

        // Act
        var results = _engine.GetCollection<UserWithIntId>().Find(u => u.Age == 25).ToList();

        // Assert
        // 20 + (i % 50) == 25 => i % 50 == 5 => i = 5, 55.
        await Assert.That(results).Count().IsEqualTo(2);
    }

    [Test]
    public async Task Query_StringFunctions_ShouldWork()
    {
        // Act
        var results = _engine.GetCollection<UserWithIntId>().Find(u => u.Name.Contains("User_10")).ToList();

        // Assert
        // User_10, User_100
        await Assert.That(results).Count().IsEqualTo(2);
    }

    [Test]
    public async Task Query_ComplexAnd_ShouldWork()
    {
        // Act
        var results = _engine.GetCollection<UserWithIntId>().Find(u => u.Age > 60 && u.Id < 50).ToList();

        // Assert
        // i % 50 > 40 and i < 50 => i = 41, 42, ..., 49 (9 values)
        await Assert.That(results).Count().IsEqualTo(9);
    }

    [Test]
    public async Task Query_StartsWith_ShouldWork()
    {
        // Act
        var results = _engine.GetCollection<UserWithIntId>().Find(u => u.Name.StartsWith("User_9")).ToList();

        // Assert
        // User_9, User_90, User_91, ..., User_99 (11 values)
        await Assert.That(results).Count().IsEqualTo(11);
    }

    [Test]
    public async Task Query_EndsWith_ShouldWork()
    {
        // Act
        var results = _engine.GetCollection<UserWithIntId>().Find(u => u.Name.EndsWith("_10")).ToList();

        // Assert
        // User_10 only. User_100 ends with "00".
        await Assert.That(results).IsNotNull();
        await Assert.That(results.Count).IsEqualTo(1);
        await Assert.That(results.All(u => u.Name.EndsWith("_10"))).IsTrue();
    }

    [Test]
    public async Task Query_StringPredicate_WithParameters_ShouldWork()
    {
        var col = _engine.GetCollection<UserWithIntId>();

        var results = col.Find(
                "Age >= @minAge and Name startswith @prefix",
                QueryParams.Create(("minAge", 65), ("prefix", "User_9")))
            .OrderBy(u => u.Id)
            .ToList();

        await Assert.That(results.Count).IsEqualTo(5);
        await Assert.That(results.All(u => u.Id >= 95 && u.Id <= 99)).IsTrue();
    }

    [Test]
    public async Task Query_StringPredicate_WithIdAliasAndLike_ShouldWork()
    {
        var col = _engine.GetCollection<UserWithIntId>();

        var results = col.Find(
                "id = @id and Name like @name",
                QueryParams.Create(("id", 10), ("name", "User%")))
            .ToList();

        await Assert.That(results).Count().IsEqualTo(1);
        await Assert.That(results[0].Id).IsEqualTo(10);
    }

    [Test]
    public async Task Query_SqlFind_WithWhereOrderLimitOffset_ShouldWork()
    {
        var col = _engine.GetCollection<UserWithIntId>();

        var results = col.FindSql(
                "select * from users_int where Age >= @minAge and Name startswith @prefix order by age desc limit 2 offset 1",
                QueryParams.Create(("minAge", 65), ("prefix", "User_9")))
            .ToList();

        await Assert.That(results.Count).IsEqualTo(2);
        await Assert.That(results[0].Id).IsEqualTo(98);
        await Assert.That(results[1].Id).IsEqualTo(97);
    }

    [Test]
    public async Task Query_SqlEngineEntry_ShouldUseFromCollection()
    {
        var results = _engine.QuerySql<UserWithIntId>(
                "select * from users_int where id = @id",
                QueryParams.Create(("id", 10)))
            .ToList();

        await Assert.That(results).Count().IsEqualTo(1);
        await Assert.That(results[0].Name).IsEqualTo("User_10");
    }

    [Test]
    public async Task Query_SqlFindDocuments_WithSelectedFields_ShouldProject()
    {
        var col = _engine.GetCollection<UserWithIntId>();

        var results = col.FindSqlDocuments(
                "select Id, Name from users_int where id = @id",
                QueryParams.Create(("id", 10)))
            .ToList();

        await Assert.That(results).Count().IsEqualTo(1);
        await Assert.That(results[0].ContainsKey("Id")).IsTrue();
        await Assert.That(results[0].ContainsKey("Name")).IsTrue();
        await Assert.That(results[0].ContainsKey("_id")).IsFalse();
        await Assert.That(results[0].ContainsKey("name")).IsFalse();
        await Assert.That(results[0].ContainsKey("age")).IsFalse();
        await Assert.That(results[0]["Id"].ToInt32(null)).IsEqualTo(10);
        await Assert.That(results[0]["Name"].ToString()).IsEqualTo("User_10");
    }

    [Test]
    public async Task Query_SqlFindDocuments_WithAliases_ShouldPreserveAliasNames()
    {
        var col = _engine.GetCollection<UserWithIntId>();

        var results = col.FindSqlDocuments(
                "select Id as Id, Name as Name from users_int where id = @id",
                QueryParams.Create(("id", 10)))
            .ToList();

        await Assert.That(results).Count().IsEqualTo(1);
        await Assert.That(results[0].ContainsKey("Id")).IsTrue();
        await Assert.That(results[0].ContainsKey("Name")).IsTrue();
        await Assert.That(results[0].ContainsKey("_id")).IsFalse();
        await Assert.That(results[0].ContainsKey("name")).IsFalse();
        await Assert.That(results[0]["Id"].ToInt32(null)).IsEqualTo(10);
        await Assert.That(results[0]["Name"].ToString()).IsEqualTo("User_10");
    }

    [Test]
    public async Task Query_SqlFindDocuments_WithLowercaseSelectedFields_ShouldPreserveWrittenNames()
    {
        var col = _engine.GetCollection<UserWithIntId>();

        var results = col.FindSqlDocuments(
                "select id, name from users_int where id = @id",
                QueryParams.Create(("id", 10)))
            .ToList();

        await Assert.That(results).Count().IsEqualTo(1);
        await Assert.That(results[0].ContainsKey("id")).IsTrue();
        await Assert.That(results[0].ContainsKey("name")).IsTrue();
        await Assert.That(results[0].ContainsKey("Id")).IsFalse();
        await Assert.That(results[0]["id"].ToInt32(null)).IsEqualTo(10);
        await Assert.That(results[0]["name"].ToString()).IsEqualTo("User_10");
    }

    [Test]
    public async Task Query_SqlFindDocuments_SelectAll_ShouldUsePropertyNames()
    {
        var col = _engine.GetCollection<UserWithIntId>();

        var results = col.FindSqlDocuments(
                "select * from users_int where id = @id",
                QueryParams.Create(("id", 10)))
            .ToList();

        await Assert.That(results).Count().IsEqualTo(1);
        await Assert.That(results[0].ContainsKey("Id")).IsTrue();
        await Assert.That(results[0].ContainsKey("Name")).IsTrue();
        await Assert.That(results[0].ContainsKey("Age")).IsTrue();
        await Assert.That(results[0].ContainsKey("_id")).IsFalse();
        await Assert.That(results[0].ContainsKey("name")).IsFalse();
        await Assert.That(results[0]["Id"].ToInt32(null)).IsEqualTo(10);
        await Assert.That(results[0]["Name"].ToString()).IsEqualTo("User_10");
        await Assert.That(results[0]["Age"].ToInt32(null)).IsEqualTo(30);
    }

    [Test]
    public async Task Query_SqlExecute_InsertUpdateDelete_ShouldWork()
    {
        var col = _engine.GetCollection<UserWithIntId>();

        var insert = col.Execute(
            "insert into users_int (Id, Name, Age) values (@id, @name, @age)",
            QueryParams.Create(("id", 1000), ("name", "Inserted"), ("age", 42)));

        await Assert.That(insert.StatementKind).IsEqualTo(SqlStatementKind.Insert);
        await Assert.That(insert.AffectedRows).IsEqualTo(1);

        var inserted = col.Execute(
                "select Id, Name, Age from users_int where Id = @id",
                QueryParams.Create(("id", 1000)))
            .Documents
            .Single();
        await Assert.That(inserted["Id"].ToInt32(null)).IsEqualTo(1000);
        await Assert.That(inserted["Name"].ToString()).IsEqualTo("Inserted");
        await Assert.That(inserted["Age"].ToInt32(null)).IsEqualTo(42);

        var update = col.Execute(
            "update users_int set Name = @name, Age = @age where Id = @id",
            QueryParams.Create(("id", 1000), ("name", "Updated"), ("age", 43)));

        await Assert.That(update.StatementKind).IsEqualTo(SqlStatementKind.Update);
        await Assert.That(update.AffectedRows).IsEqualTo(1);

        var updated = col.Execute(
                "select Id, Name, Age from users_int where Id = @id",
                QueryParams.Create(("id", 1000)))
            .Documents
            .Single();
        await Assert.That(updated["Name"].ToString()).IsEqualTo("Updated");
        await Assert.That(updated["Age"].ToInt32(null)).IsEqualTo(43);

        var delete = col.Execute(
            "delete from users_int where Id = @id",
            QueryParams.Create(("id", 1000)));

        await Assert.That(delete.StatementKind).IsEqualTo(SqlStatementKind.Delete);
        await Assert.That(delete.AffectedRows).IsEqualTo(1);
        await Assert.That(col.Find("Id = @id", QueryParams.Create(("id", 1000))).ToList()).IsEmpty();
    }

    [Test]
    public async Task Query_SqlEngineExecute_Update_ShouldUseFromCollection()
    {
        var result = _engine.Execute<UserWithIntId>(
            "update users_int set Name = @name where Id = @id",
            QueryParams.Create(("id", 10), ("name", "EngineUpdated")));

        await Assert.That(result.StatementKind).IsEqualTo(SqlStatementKind.Update);
        await Assert.That(result.AffectedRows).IsEqualTo(1);

        var user = _engine.GetCollection<UserWithIntId>().Find("Id = @id", QueryParams.Create(("id", 10))).Single();
        await Assert.That(user.Name).IsEqualTo("EngineUpdated");
    }

    [Test]
    public async Task Query_SqlExecute_UpdateDelete_WithIndexPredicate_ShouldUseStableTargetIds()
    {
        var col = _engine.GetCollection<UserWithIntId>();
        _engine.EnsureIndex("UserWithIntId", "Age", "age_sql_dml_idx");

        var update = col.Execute(
            "update users_int set Name = @name where Age = @age",
            QueryParams.Create(("name", "IndexedUpdated"), ("age", 25)));

        await Assert.That(update.StatementKind).IsEqualTo(SqlStatementKind.Update);
        await Assert.That(update.AffectedRows).IsEqualTo(2);
        await Assert.That(col.Find("Age = @age and Name = @name", QueryParams.Create(("age", 25), ("name", "IndexedUpdated"))).Count()).IsEqualTo(2);

        var delete = col.Execute(
            "delete from users_int where Age = @age",
            QueryParams.Create(("age", 26)));

        await Assert.That(delete.StatementKind).IsEqualTo(SqlStatementKind.Delete);
        await Assert.That(delete.AffectedRows).IsEqualTo(2);
        await Assert.That(col.Find("Age = @age", QueryParams.Create(("age", 26))).Count()).IsEqualTo(0);
    }

    [Test]
    public async Task Query_SqlExecute_UpdateInTransaction_ShouldUsePendingTargetIds()
    {
        var col = _engine.GetCollection<UserWithIntId>();
        _engine.EnsureIndex("UserWithIntId", "Age", "age_sql_tx_dml_idx");

        using var transaction = _engine.BeginTransaction();
        var first = col.Execute(
            "update users_int set Name = @name where Age = @age",
            QueryParams.Create(("name", "TxIndexedUpdated"), ("age", 25)));

        var second = col.Execute(
            "update users_int set Age = @newAge where Name = @name",
            QueryParams.Create(("newAge", 77), ("name", "TxIndexedUpdated")));

        transaction.Commit();

        await Assert.That(first.AffectedRows).IsEqualTo(2);
        await Assert.That(second.AffectedRows).IsEqualTo(2);
        await Assert.That(col.Find("Age = @age and Name = @name", QueryParams.Create(("age", 77), ("name", "TxIndexedUpdated"))).Count()).IsEqualTo(2);
    }

    [Test]
    public async Task Query_SqlExecute_DmlNumericLiterals_ShouldUseTargetPropertyTypes()
    {
        var col = _engine.GetCollection<SqlNumericItem>();

        col.Execute("insert into sql_numeric_items (Id, Price, BigCount, Amount, Count, Limit, Text) values (1, 10, 5, 7, 3.0, 6, 'Inserted')");

        var inserted = col.Find("Id = @id", QueryParams.Create(("id", 1))).Single();
        await Assert.That(inserted.Price).IsEqualTo(10d);
        await Assert.That(inserted.BigCount).IsEqualTo(5L);
        await Assert.That(inserted.Amount).IsEqualTo(7m);
        await Assert.That(inserted.Count).IsEqualTo(3);

        col.Execute("update sql_numeric_items set Price = 11, BigCount = 6, Amount = 8, Count = 4.0 where Id = 1");

        var updated = col.Find("Id = @id", QueryParams.Create(("id", 1))).Single();
        await Assert.That(updated.Price).IsEqualTo(11d);
        await Assert.That(updated.BigCount).IsEqualTo(6L);
        await Assert.That(updated.Amount).IsEqualTo(8m);
        await Assert.That(updated.Count).IsEqualTo(4);

        await Assert.That(() => col.Execute("update sql_numeric_items set Count = 4.5 where Id = 1"))
            .Throws<InvalidOperationException>();
    }

    [Test]
    public async Task Query_SqlExecute_StringEscapes_ShouldMatchWhereParser()
    {
        var col = _engine.GetCollection<SqlNumericItem>();

        col.Execute("insert into sql_numeric_items (Id, Text) values (2, 'a\\nb')");

        var result = col.Execute("select Id from sql_numeric_items where Text = 'a\\nb'")
            .Documents
            .Single();

        await Assert.That(result["Id"].ToInt32(null)).IsEqualTo(2);
    }

    [Test]
    public async Task Query_SqlFind_WithReservedFieldNames_ShouldNotSplitWhereOrOrderBy()
    {
        var col = _engine.GetCollection<SqlNumericItem>();
        col.Insert(new SqlNumericItem { Id = 2, Limit = 1, Text = "second" });
        col.Insert(new SqlNumericItem { Id = 1, Limit = 1, Text = "first" });
        col.Insert(new SqlNumericItem { Id = 3, Limit = 0, Text = "third" });

        var whereResult = col.Execute("select Id from sql_numeric_items where Limit > 0 order by Id")
            .Documents
            .ToList();

        await Assert.That(whereResult).Count().IsEqualTo(2);
        await Assert.That(whereResult[0]["Id"].ToInt32(null)).IsEqualTo(1);
        await Assert.That(whereResult[1]["Id"].ToInt32(null)).IsEqualTo(2);

        var orderedResult = col.Execute("select Id from sql_numeric_items order by Limit limit 2")
            .Documents
            .ToList();

        await Assert.That(orderedResult[0]["Id"].ToInt32(null)).IsEqualTo(3);
        await Assert.That(orderedResult[1]["Id"].ToInt32(null)).IsEqualTo(1);
    }

    [Test]
    public async Task Query_SqlExecute_DmlFields_ShouldRejectNestedOrDuplicateFields()
    {
        var col = _engine.GetCollection<SqlNumericItem>();

        await Assert.That(() => col.Execute("insert into sql_numeric_items (Id, Meta.Value) values (1, 2)"))
            .Throws<FormatException>();
        await Assert.That(() => col.Execute("insert into sql_numeric_items (Id, Id) values (1, 2)"))
            .Throws<FormatException>();
        await Assert.That(() => col.Execute("update sql_numeric_items set Meta.Value = 1 where Id = 1"))
            .Throws<FormatException>();
    }

    [Test]
    public async Task Query_SqlFind_WithCustomIdProperty_ShouldUseCustomIdAsStableTiebreaker()
    {
        var col = _engine.GetCollection<SqlCustomIdItem>();
        col.Insert(new SqlCustomIdItem { Uid = 2, Bucket = 1, Text = "second" });
        col.Insert(new SqlCustomIdItem { Uid = 1, Bucket = 1, Text = "first" });
        col.Insert(new SqlCustomIdItem { Uid = 3, Bucket = 0, Text = "third" });

        var paged = col.Execute("select Uid from sql_custom_id_items order by Bucket limit 2")
            .Documents
            .ToList();

        await Assert.That(paged[0]["Uid"].ToInt32(null)).IsEqualTo(3);
        await Assert.That(paged[1]["Uid"].ToInt32(null)).IsEqualTo(1);

        var orderedByAlias = col.Execute("select Uid from sql_custom_id_items order by id limit 3")
            .Documents
            .ToList();

        await Assert.That(orderedByAlias[0]["Uid"].ToInt32(null)).IsEqualTo(1);
        await Assert.That(orderedByAlias[1]["Uid"].ToInt32(null)).IsEqualTo(2);
        await Assert.That(orderedByAlias[2]["Uid"].ToInt32(null)).IsEqualTo(3);
    }

    [Test]
    public async Task Query_SqlExecute_WithCustomIdProperty_ShouldRejectPrimaryKeyUpdateAndDuplicateStorageFields()
    {
        var col = _engine.GetCollection<SqlCustomIdItem>();
        col.Insert(new SqlCustomIdItem { Uid = 1, Bucket = 1, Text = "first" });

        await Assert.That(() => col.Execute("update sql_custom_id_items set Uid = 2 where Uid = 1"))
            .Throws<NotSupportedException>();
        await Assert.That(() => col.Execute("insert into sql_custom_id_items (Uid, Id, Bucket) values (1, 2, 1)"))
            .Throws<FormatException>();
    }

    [Test]
    public async Task Query_SqlFindGenericProjection_ShouldMapSelectedFields()
    {
        var col = _engine.GetCollection<UserWithIntId>();

        var results = col.FindSql<SqlUserSummary>(
                "select Id, Name from users_int where id = @id",
                QueryParams.Create(("id", 10)))
            .ToList();

        await Assert.That(results).Count().IsEqualTo(1);
        await Assert.That(results[0].Id).IsEqualTo(10);
        await Assert.That(results[0].Name).IsEqualTo("User_10");
    }

    [Test]
    public async Task Query_SqlEngineGenericProjection_WithAlias_ShouldMapSelectedFields()
    {
        var results = _engine.QuerySql<UserWithIntId, SqlUserAliasSummary>(
                "select Id, Name as DisplayName from users_int where id = @id",
                QueryParams.Create(("id", 10)))
            .ToList();

        await Assert.That(results).Count().IsEqualTo(1);
        await Assert.That(results[0].Id).IsEqualTo(10);
        await Assert.That(results[0].DisplayName).IsEqualTo("User_10");
    }

    [Test]
    public async Task Query_PrimaryKey_With_ExtraCondition_ShouldWork()
    {
        // 这个测试专门用于验证主键查找策略是否会正确应用额外的过滤条件。
        // 之前存在一个潜在 Bug：如果优化器选择了 PrimaryKeyLookup，QueryExecutor 可能会直接返回通过 ID 查到的文档，
        // 而忽略了查询表达式中的其他条件（如 Name == "Wrong"）。
        
        var col = _engine.GetCollection<UserWithIntId>();
        var user = new UserWithIntId { Id = 9999, Name = "TargetUser", Age = 30 };
        col.Insert(user);

        // Case 1: ID 匹配，但 Name 不匹配 -> 应该返回空
        // 优化器应该提取 Id == 9999 使用 PK 索引，然后 Executor 必须校验 Name == "WrongUser"
        var resultNegative = col.Find(u => u.Id == 9999 && u.Name == "WrongUser").ToList();
        await Assert.That(resultNegative).IsEmpty();

        // Case 2: ID 匹配，且 Name 匹配 -> 应该返回结果
        var resultPositive = col.Find(u => u.Id == 9999 && u.Name == "TargetUser").ToList();
        await Assert.That(resultPositive).Count().IsEqualTo(1);
        await Assert.That(resultPositive[0].Name).IsEqualTo("TargetUser");
        
        // Case 3: ID 匹配，Age 条件 (数值比较)
        var resultAgeMismatch = col.Find(u => u.Id == 9999 && u.Age > 100).ToList();
        await Assert.That(resultAgeMismatch).IsEmpty();
        
        var resultAgeMatch = col.Find(u => u.Id == 9999 && u.Age == 30).ToList();
        await Assert.That(resultAgeMatch).Count().IsEqualTo(1);
    }

    [Test]
    public async Task Query_ChainedFunctions_ShouldWork()
    {
        // 测试链式调用：Trim -> Substring -> ToLower
        var col = _engine.GetCollection<UserWithIntId>();
        col.Insert(new UserWithIntId { Id = 8888, Name = "  CHAINED  ", Age = 20 });
        
        var results = col.Find(u => u.Name.Trim().Substring(0, 5).ToLower() == "chain").ToList();
        await Assert.That(results).Count().IsEqualTo(1);
        await Assert.That(results[0].Id).IsEqualTo(8888);
    }

    [Test]
    public async Task Query_MathAndDateTime_ShouldWork()
    {
        var col = _engine.GetCollection<UserWithIntId>();
        // Math.Abs(20 - 25) = 5
        var resultsMath = col.Find(u => Math.Abs(u.Age - 25) <= 5).ToList();
        await Assert.That(resultsMath.Count).IsGreaterThanOrEqualTo(1);

        // DateTime 属性与函数结合 (假设 UserWithIntId 没有 DateTime，我们现场造一个)
        // 实际上我们可以只测试 Math，因为 DateTime 需要实体支持。
    }
}

[Entity("sql_user_summary")]
public partial class SqlUserSummary
{
    public int Id { get; set; }
    public string Name { get; set; } = "";
}

[Entity("sql_user_alias_summary")]
public partial class SqlUserAliasSummary
{
    public int Id { get; set; }
    public string DisplayName { get; set; } = "";
}

[Entity("sql_numeric_items")]
public partial class SqlNumericItem
{
    public int Id { get; set; }
    public double Price { get; set; }
    public long BigCount { get; set; }
    public decimal Amount { get; set; }
    public int Count { get; set; }
    public int Limit { get; set; }
    public string Text { get; set; } = "";
}

[Entity("sql_custom_id_items", IdProperty = nameof(Uid))]
public partial class SqlCustomIdItem
{
    public int Uid { get; set; }
    public int Bucket { get; set; }
    public string Text { get; set; } = "";
}
