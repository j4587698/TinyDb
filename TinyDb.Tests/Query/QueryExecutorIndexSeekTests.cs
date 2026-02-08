using TinyDb.Bson;
using TinyDb.Collections;
using TinyDb.Core;
using TinyDb.Index;
using TinyDb.Query;
using TinyDb.Attributes;
using System.Linq.Expressions;

namespace TinyDb.Tests.Query;

/// <summary>
/// Tests for QueryExecutor IndexSeek functionality to improve coverage
/// IndexSeek is used for efficient lookups on unique indexes
/// </summary>
public class QueryExecutorIndexSeekTests : IDisposable
{
    private readonly string _dbPath;
    private readonly TinyDbEngine _engine;
    private readonly ITinyCollection<User> _users;

    public QueryExecutorIndexSeekTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"indexseek_test_{Guid.NewGuid()}.db");
        _engine = new TinyDbEngine(_dbPath);
        _users = _engine.GetCollection<User>();
        
        // Create a unique index on Email using EnsureIndex
        _engine.EnsureIndex(_users.CollectionName, "Email", "idx_email", unique: true);
        
        // Create a non-unique index on Category
        _engine.EnsureIndex(_users.CollectionName, "Category", "idx_category", unique: false);
        
        SeedData();
    }

    private void SeedData()
    {
        _users.Insert(new User { Id = 1, Name = "Alice", Email = "alice@test.com", Category = "Admin", Score = 95 });
        _users.Insert(new User { Id = 2, Name = "Bob", Email = "bob@test.com", Category = "User", Score = 85 });
        _users.Insert(new User { Id = 3, Name = "Charlie", Email = "charlie@test.com", Category = "User", Score = 75 });
        _users.Insert(new User { Id = 4, Name = "Diana", Email = "diana@test.com", Category = "Admin", Score = 90 });
        _users.Insert(new User { Id = 5, Name = "Eve", Email = "eve@test.com", Category = "Guest", Score = 60 });
    }

    public void Dispose()
    {
        _engine?.Dispose();
        if (File.Exists(_dbPath))
            try { File.Delete(_dbPath); } catch { }
    }

    #region Unique Index Seek Tests

    [Test]
    public async Task IndexSeek_UniqueIndex_ShouldFindExactMatch()
    {
        // Query by unique indexed field (Email)
        var result = _users.Query()
            .Where(u => u.Email == "alice@test.com")
            .ToList();

        await Assert.That(result.Count).IsEqualTo(1);
        await Assert.That(result[0].Name).IsEqualTo("Alice");
    }

    [Test]
    public async Task IndexSeek_UniqueIndex_NotFound_ShouldReturnEmpty()
    {
        var result = _users.Query()
            .Where(u => u.Email == "nonexistent@test.com")
            .ToList();

        await Assert.That(result.Count).IsEqualTo(0);
    }

    [Test]
    public async Task IndexSeek_UniqueIndex_WithAdditionalCondition_ShouldFilter()
    {
        // Query with unique index + additional condition that fails
        var result = _users.Query()
            .Where(u => u.Email == "alice@test.com" && u.Score < 90)
            .ToList();

        // Alice has Score 95, so should not match
        await Assert.That(result.Count).IsEqualTo(0);
    }

    [Test]
    public async Task IndexSeek_UniqueIndex_WithAdditionalCondition_ShouldPass()
    {
        // Query with unique index + additional condition that passes
        var result = _users.Query()
            .Where(u => u.Email == "alice@test.com" && u.Score > 90)
            .ToList();

        await Assert.That(result.Count).IsEqualTo(1);
        await Assert.That(result[0].Name).IsEqualTo("Alice");
    }

    #endregion

    #region Non-Unique Index Seek Tests

    [Test]
    public async Task IndexSeek_NonUniqueIndex_ShouldFindMultipleMatches()
    {
        // Query by non-unique indexed field (Category)
        var result = _users.Query()
            .Where(u => u.Category == "User")
            .ToList();

        await Assert.That(result.Count).IsEqualTo(2);
    }

    [Test]
    public async Task IndexSeek_NonUniqueIndex_SingleMatch()
    {
        var result = _users.Query()
            .Where(u => u.Category == "Guest")
            .ToList();

        await Assert.That(result.Count).IsEqualTo(1);
        await Assert.That(result[0].Name).IsEqualTo("Eve");
    }

    [Test]
    public async Task IndexSeek_NonUniqueIndex_NoMatch()
    {
        var result = _users.Query()
            .Where(u => u.Category == "NonExistent")
            .ToList();

        await Assert.That(result.Count).IsEqualTo(0);
    }

    [Test]
    public async Task IndexSeek_NonUniqueIndex_WithAdditionalCondition()
    {
        // Query with non-unique index + additional score condition
        var result = _users.Query()
            .Where(u => u.Category == "Admin" && u.Score >= 95)
            .ToList();

        await Assert.That(result.Count).IsEqualTo(1);
        await Assert.That(result[0].Name).IsEqualTo("Alice");
    }

    #endregion

    #region Index Scan Range Tests

    [Test]
    public async Task IndexScan_GreaterThan_ShouldFindRange()
    {
        // Alice: 95, Bob: 85, Diana: 90 - all > 80
        var result = _users.Query()
            .Where(u => u.Score > 80)
            .OrderBy(u => u.Score)
            .ToList();

        await Assert.That(result.Count).IsEqualTo(3);
    }

    [Test]
    public async Task IndexScan_LessThan_ShouldFindRange()
    {
        var result = _users.Query()
            .Where(u => u.Score < 80)
            .ToList();

        await Assert.That(result.Count).IsEqualTo(2);
    }

    [Test]
    public async Task IndexScan_GreaterThanOrEqual_ShouldIncludeBoundary()
    {
        var result = _users.Query()
            .Where(u => u.Score >= 85)
            .ToList();

        await Assert.That(result.Count).IsEqualTo(3);
    }

    [Test]
    public async Task IndexScan_LessThanOrEqual_ShouldIncludeBoundary()
    {
        var result = _users.Query()
            .Where(u => u.Score <= 75)
            .ToList();

        await Assert.That(result.Count).IsEqualTo(2);
    }

    [Test]
    public async Task IndexScan_RangeBetween_ShouldFindInRange()
    {
        var result = _users.Query()
            .Where(u => u.Score >= 75 && u.Score <= 90)
            .ToList();

        await Assert.That(result.Count).IsEqualTo(3);
    }

    #endregion

    #region Primary Key Lookup Tests

    [Test]
    public async Task PrimaryKeyLookup_ShouldFindById()
    {
        var result = _users.Query()
            .Where(u => u.Id == 3)
            .ToList();

        await Assert.That(result.Count).IsEqualTo(1);
        await Assert.That(result[0].Name).IsEqualTo("Charlie");
    }

    [Test]
    public async Task PrimaryKeyLookup_NotFound_ShouldReturnEmpty()
    {
        var result = _users.Query()
            .Where(u => u.Id == 999)
            .ToList();

        await Assert.That(result.Count).IsEqualTo(0);
    }

    [Test]
    public async Task PrimaryKeyLookup_WithAdditionalCondition_ShouldFilter()
    {
        // Find by ID but with failing additional condition
        var result = _users.Query()
            .Where(u => u.Id == 1 && u.Category == "User")
            .ToList();

        // Alice is Admin, not User
        await Assert.That(result.Count).IsEqualTo(0);
    }

    #endregion

    #region Fallback to Full Table Scan Tests

    [Test]
    public async Task Query_NoIndex_ShouldFallbackToFullScan()
    {
        // Query by non-indexed field
        var result = _users.Query()
            .Where(u => u.Name.StartsWith("A"))
            .ToList();

        await Assert.That(result.Count).IsEqualTo(1);
        await Assert.That(result[0].Name).IsEqualTo("Alice");
    }

    [Test]
    public async Task Query_ComplexCondition_ShouldWork()
    {
        var result = _users.Query()
            .Where(u => (u.Category == "Admin" || u.Category == "Guest") && u.Score >= 60)
            .ToList();

        await Assert.That(result.Count).IsEqualTo(3);
    }

    #endregion

    #region QueryExecutor Direct Tests

    [Test]
    public async Task QueryExecutor_NullCollectionName_ShouldThrow()
    {
        var executor = new QueryExecutor(_engine);

        await Assert.That(() => executor.Execute<User>(null!).ToList())
            .Throws<ArgumentException>();
    }

    [Test]
    public async Task QueryExecutor_EmptyCollectionName_ShouldThrow()
    {
        var executor = new QueryExecutor(_engine);

        await Assert.That(() => executor.Execute<User>("").ToList())
            .Throws<ArgumentException>();
    }

    [Test]
    public async Task QueryExecutor_WhitespaceCollectionName_ShouldThrow()
    {
        var executor = new QueryExecutor(_engine);

        await Assert.That(() => executor.Execute<User>("   ").ToList())
            .Throws<ArgumentException>();
    }

    #endregion

    #region ExecuteIndexSeek Branch Tests

    [Test]
    public async Task ExecuteIndexSeek_UseIndexNull_ShouldFallbackToFullScan()
    {
        Expression<Func<User, bool>> expr = u => u.Email == "alice@test.com";
        var plan = new QueryExecutionPlan
        {
            CollectionName = _users.CollectionName,
            Strategy = QueryExecutionStrategy.IndexSeek,
            OriginalExpression = expr
        };

        var results = ExecuteIndexSeekDirect(plan).ToList();

        await Assert.That(results.Count).IsEqualTo(1);
        await Assert.That(results[0].Name).IsEqualTo("Alice");
    }

    [Test]
    public async Task ExecuteIndexSeek_IndexNotFound_ShouldFallbackToFullScan()
    {
        Expression<Func<User, bool>> expr = u => u.Email == "bob@test.com";
        var plan = new QueryExecutionPlan
        {
            CollectionName = _users.CollectionName,
            Strategy = QueryExecutionStrategy.IndexSeek,
            OriginalExpression = expr,
            UseIndex = new IndexStatistics
            {
                Name = "missing_idx",
                Fields = new[] { "Email" },
                IsUnique = true
            },
            IndexScanKeys = new List<IndexScanKey>
            {
                new() { FieldName = "Email", Value = new BsonString("bob@test.com"), ComparisonType = ComparisonType.Equal }
            }
        };

        var results = ExecuteIndexSeekDirect(plan).ToList();

        await Assert.That(results.Count).IsEqualTo(1);
        await Assert.That(results[0].Name).IsEqualTo("Bob");
    }

    [Test]
    public async Task ExecuteIndexSeek_ExactKeyNull_ShouldFallbackToFullScan()
    {
        Expression<Func<User, bool>> expr = u => u.Email == "alice@test.com";
        var emailIndex = GetIndexStats("idx_email");

        var plan = new QueryExecutionPlan
        {
            CollectionName = _users.CollectionName,
            Strategy = QueryExecutionStrategy.IndexSeek,
            OriginalExpression = expr,
            UseIndex = emailIndex,
            IndexScanKeys = new List<IndexScanKey>
            {
                new() { FieldName = "Email", Value = new BsonString("alice@test.com"), ComparisonType = ComparisonType.GreaterThan }
            }
        };

        var results = ExecuteIndexSeekDirect(plan).ToList();

        await Assert.That(results.Count).IsEqualTo(1);
        await Assert.That(results[0].Name).IsEqualTo("Alice");
    }

    [Test]
    public async Task ExecuteIndexSeek_NonUniqueIndex_ShouldUseFind()
    {
        Expression<Func<User, bool>> expr = u => u.Category == "User";
        var categoryIndex = GetIndexStats("idx_category");

        var plan = new QueryExecutionPlan
        {
            CollectionName = _users.CollectionName,
            Strategy = QueryExecutionStrategy.IndexSeek,
            OriginalExpression = expr,
            UseIndex = categoryIndex,
            IndexScanKeys = new List<IndexScanKey>
            {
                new() { FieldName = "Category", Value = new BsonString("User"), ComparisonType = ComparisonType.Equal }
            }
        };

        var results = ExecuteIndexSeekDirect(plan).ToList();

        await Assert.That(results.Count).IsEqualTo(2);
    }

    private IEnumerable<User> ExecuteIndexSeekDirect(QueryExecutionPlan plan)
    {
        var executor = new QueryExecutor(_engine);
        return executor.ExecuteIndexSeekForTests<User>(plan);
    }

    private IndexStatistics GetIndexStats(string name)
    {
        var indexManager = _engine.GetIndexManager(_users.CollectionName);
        var stats = indexManager.GetAllStatistics().FirstOrDefault(s => s.Name == name);
        if (stats == null) throw new InvalidOperationException($"Index {name} not found.");
        return stats;
    }

    #endregion

    #region Test Entity

    [Entity]
    public class User
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public string Email { get; set; } = string.Empty;
        public string Category { get; set; } = string.Empty;
        public int Score { get; set; }
    }

    #endregion
}
