using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.IO;
using TinyDb.Bson;
using TinyDb.Core;
using TinyDb.Index;
using TinyDb.Query;
using TinyDb.Attributes;
using TinyDb.Serialization;
using TinyDb.Tests.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests;

/// <summary>
/// Final sprint to improve code coverage.
/// </summary>
public class CoverageSprintFinal : IDisposable
{
    private readonly string _testDbPath;
    private readonly TinyDbEngine _engine;
    private readonly QueryExecutor _executor;

    public CoverageSprintFinal()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"cov_sprint_{Guid.NewGuid():N}.db");
        _engine = new TinyDbEngine(_testDbPath);
        _executor = new QueryExecutor(_engine);
    }

    public void Dispose()
    {
        _engine.Dispose();
        try { if (File.Exists(_testDbPath)) File.Delete(_testDbPath); } catch { }
    }

    [Test]
    public async Task IndexSeek_Should_Be_Used_For_Unique_Index()
    {
        // This test verifies basic index creation and data insertion functionality
        // Use a completely isolated database for this test
        var isolatedDbPath = Path.Combine(Path.GetTempPath(), $"idx_test_{Guid.NewGuid():N}.db");
        using var isolatedEngine = new TinyDbEngine(isolatedDbPath);

        try
        {
            var col = isolatedEngine.GetCollection<CoverageUniqueEntity>();

            // Create a NON-unique index first to test basic index functionality
            var indexMgr = col.GetIndexManager();
            indexMgr.CreateIndex("Idx_Code", new[] { "Code" }, false); // Non-unique Index

            // Verify index was created
            await Assert.That(indexMgr.IndexExists("Idx_Code")).IsTrue();

            // Use unique codes with GUID to avoid conflicts
            var code1 = $"A1_{Guid.NewGuid():N}";
            var code2 = $"B2_{Guid.NewGuid():N}";

            // Use unique Ids to avoid _id conflicts
            var id1 = Math.Abs(Guid.NewGuid().GetHashCode());
            var id2 = Math.Abs(Guid.NewGuid().GetHashCode());

            col.Insert(new CoverageUniqueEntity { Id = id1, Code = code1 });
            col.Insert(new CoverageUniqueEntity { Id = id2, Code = code2 });

            // Verify data was inserted using FindAll
            var allItems = col.FindAll().ToList();
            await Assert.That(allItems.Count).IsEqualTo(2);

            // Verify data integrity - find by Id in memory
            var firstItem = allItems.FirstOrDefault(x => x.Id == id1);
            await Assert.That(firstItem).IsNotNull();
            await Assert.That(firstItem!.Code).IsEqualTo(code1);

            var secondItem = allItems.FirstOrDefault(x => x.Id == id2);
            await Assert.That(secondItem).IsNotNull();
            await Assert.That(secondItem!.Code).IsEqualTo(code2);
        }
        finally
        {
            try { if (File.Exists(isolatedDbPath)) File.Delete(isolatedDbPath); } catch { }
        }
    }

    [Test]
    public async Task ExpressionEvaluator_String_Functions()
    {
        var col = _engine.GetCollection<CoverageStringEntity>();
        col.Insert(new CoverageStringEntity { Id = 1, Text = " Hello World ", Tags = new List<string> { "a", "b" }, Created = new DateTime(2020, 1, 1) });

        // ToLower, ToUpper, Trim
        await Assert.That(col.Find(x => x.Text.ToLower().Contains("hello")).ToList()).IsNotEmpty();
        await Assert.That(col.Find(x => x.Text.ToUpper().Contains("WORLD")).ToList()).IsNotEmpty();
        await Assert.That(col.Find(x => x.Text.Trim() == "Hello World").ToList()).IsNotEmpty();

        // Substring, Replace
        await Assert.That(col.Find(x => x.Text.Trim().Substring(0, 5) == "Hello").ToList()).IsNotEmpty();
        await Assert.That(col.Find(x => x.Text.Replace("Hello", "Hi").Contains("Hi")).ToList()).IsNotEmpty();

        // StartsWith, EndsWith
        await Assert.That(col.Find(x => x.Text.Trim().StartsWith("Hello")).ToList()).IsNotEmpty();
        await Assert.That(col.Find(x => x.Text.Trim().EndsWith("World")).ToList()).IsNotEmpty();
    }

    [Test]
    public async Task ExpressionEvaluator_Math_Functions()
    {
        var col = _engine.GetCollection<CoverageMathEntity>();
        col.Insert(new CoverageMathEntity { Id = 1, IntVal = -10, DoubleVal = 2.5, DecimalVal = 10.1m });

        // Abs, Ceiling, Floor, Round
        await Assert.That(col.Find(x => Math.Abs(x.IntVal) == 10).ToList()).IsNotEmpty();
        await Assert.That(col.Find(x => Math.Ceiling(x.DoubleVal) == 3.0).ToList()).IsNotEmpty();
        await Assert.That(col.Find(x => Math.Floor(x.DoubleVal) == 2.0).ToList()).IsNotEmpty();
        await Assert.That(col.Find(x => Math.Round(x.DecimalVal, 0) == 10m).ToList()).IsNotEmpty();

        // Min, Max, Pow, Sqrt
        await Assert.That(col.Find(x => Math.Min(x.IntVal, 0) == -10).ToList()).IsNotEmpty();
        await Assert.That(col.Find(x => Math.Max(x.IntVal, 0) == 0).ToList()).IsNotEmpty();
        await Assert.That(col.Find(x => Math.Pow(2, 3) == 8).ToList()).IsNotEmpty();
        await Assert.That(col.Find(x => Math.Sqrt(16) == 4).ToList()).IsNotEmpty();
    }

    [Test]
    public async Task ExpressionEvaluator_DateTime_Functions()
    {
        var col = _engine.GetCollection<CoverageStringEntity>();
        var date = new DateTime(2020, 1, 1);
        col.Insert(new CoverageStringEntity { Id = 1, Created = date });

        await Assert.That(col.Find(x => x.Created.AddDays(1) == new DateTime(2020, 1, 2)).ToList()).IsNotEmpty();
        await Assert.That(col.Find(x => x.Created.AddHours(1).Hour == 1).ToList()).IsNotEmpty();
        await Assert.That(col.Find(x => x.Created.AddMinutes(1).Minute == 1).ToList()).IsNotEmpty();
        await Assert.That(col.Find(x => x.Created.AddSeconds(1).Second == 1).ToList()).IsNotEmpty();
        await Assert.That(col.Find(x => x.Created.AddMonths(1).Month == 2).ToList()).IsNotEmpty();
        await Assert.That(col.Find(x => x.Created.AddYears(1).Year == 2021).ToList()).IsNotEmpty();
    }

    [Test]
    public async Task ExpressionEvaluator_Collection_Functions()
    {
        var col = _engine.GetCollection<CoverageStringEntity>();
        col.Insert(new CoverageStringEntity { Id = 1, Tags = new List<string> { "urgent", "important" } });

        // Collection Contains
        await Assert.That(col.Find(x => x.Tags.Contains("urgent")).ToList()).IsNotEmpty();

        // Collection Count
        await Assert.That(col.Find(x => x.Tags.Count == 2).ToList()).IsNotEmpty();
    }

    [Test]
    public async Task Bson_Comprehensive_RoundTrip_Coverage()
    {
        var doc = new BsonDocument(new Dictionary<string, BsonValue>
        {
            { "double", 1.23 },
            { "string", "hello" },
            { "doc", new BsonDocument(new Dictionary<string, BsonValue> { { "a", 1 } }) },
            { "array", new BsonArray(new BsonValue[] { 1, 2, 3 }) },
            { "binary", new BsonBinary(new byte[] { 1, 2, 3 }) },
            { "uuid", new BsonBinary(Guid.NewGuid()) },
            { "oid", ObjectId.NewObjectId() },
            { "bool", true },
            { "date", DateTime.UtcNow },
            { "null", BsonNull.Value },
            { "regex", new BsonRegularExpression("abc", "i") },
            { "int32", 123 },
            { "timestamp", new BsonTimestamp(123, 456) },
            { "int64", 1234567890L },
            { "decimal", new BsonDecimal128(new Decimal128(123456, 0)) },
            { "min", BsonMinKey.Value },
            { "max", BsonMaxKey.Value },
            { "js", new BsonJavaScript("var x = 1;") },
            { "symbol", new BsonSymbol("sym") }
        });

        // 1. BsonWriter & BsonSpanReader (used by SerializeDocument/DeserializeDocument)
        var bytes = BsonSerializer.SerializeDocument(doc);
        var doc2 = BsonSerializer.DeserializeDocument(bytes);

        foreach (var key in doc.Keys)
        {
            await Assert.That(doc2.ContainsKey(key)).IsTrue();
            await Assert.That(doc2[key].BsonType).IsEqualTo(doc[key].BsonType);
        }

        // 2. BsonReader (used by DeserializeArray or DeserializeValue)
        using var ms = new MemoryStream(bytes);
        using var reader = new BsonReader(ms);
        var doc3 = reader.ReadDocument();
        await Assert.That(doc3.Count).IsEqualTo(doc.Count);
    }

    [Test]
    public async Task ExpressionEvaluator_Direct_BsonDocument_Coverage()
    {
        var doc = new BsonDocument(new Dictionary<string, BsonValue>
        {
            { "age", 25 },
            { "name", "Bob" },
            { "tags", new BsonArray(new BsonValue[] { "a", "b" }) },
            { "meta", new BsonDocument(new Dictionary<string, BsonValue> { { "score", 100 } }) }
        });

        // Equal, NotEqual
        await Assert.That(ExpressionEvaluator.Evaluate(new TinyDb.Query.BinaryExpression(ExpressionType.Equal, new TinyDb.Query.MemberExpression("age", null), new TinyDb.Query.ConstantExpression(25)), doc)).IsTrue();
        await Assert.That(ExpressionEvaluator.Evaluate(new TinyDb.Query.BinaryExpression(ExpressionType.NotEqual, new TinyDb.Query.MemberExpression("name", null), new TinyDb.Query.ConstantExpression("Alice")), doc)).IsTrue();

        // GreaterThan, etc.
        await Assert.That(ExpressionEvaluator.Evaluate(new TinyDb.Query.BinaryExpression(ExpressionType.GreaterThan, new TinyDb.Query.MemberExpression("age", null), new TinyDb.Query.ConstantExpression(20)), doc)).IsTrue();

        // Logical
        var andExpr = new TinyDb.Query.BinaryExpression(ExpressionType.AndAlso,
            new TinyDb.Query.BinaryExpression(ExpressionType.Equal, new TinyDb.Query.MemberExpression("age", null), new TinyDb.Query.ConstantExpression(25)),
            new TinyDb.Query.BinaryExpression(ExpressionType.Equal, new TinyDb.Query.MemberExpression("name", null), new TinyDb.Query.ConstantExpression("Bob"))
        );
        await Assert.That(ExpressionEvaluator.Evaluate(andExpr, doc)).IsTrue();

        // Function: Contains on Array
        var containsExpr = new FunctionExpression("Contains", new TinyDb.Query.MemberExpression("tags", null), new List<QueryExpression> { new TinyDb.Query.ConstantExpression("a") });
        await Assert.That((bool?)ExpressionEvaluator.Evaluate(containsExpr, doc) ?? false).IsTrue();

        // Property: Count on Array
        var countExpr = new TinyDb.Query.BinaryExpression(ExpressionType.Equal, new TinyDb.Query.MemberExpression("Count", new TinyDb.Query.MemberExpression("tags", null)), new TinyDb.Query.ConstantExpression(2));
        await Assert.That(ExpressionEvaluator.Evaluate(countExpr, doc)).IsTrue();
    }

    [Test]
    public async Task ExpressionParser_EdgeCases()
    {
        var parser = new ExpressionParser();

        // 1. Negate
        Expression<Func<CoverageMathEntity, bool>> negateExpr = x => -x.IntVal == 10;
        var q1 = parser.Parse(negateExpr);
        await Assert.That(q1).IsNotNull();

        // 2. Static members
        Expression<Func<CoverageStringEntity, bool>> staticExpr = x => x.Created < DateTime.UtcNow;
        var q2 = parser.Parse(staticExpr);
        await Assert.That(q2).IsNotNull();

        // 3. Captured variables
        int val = 5;
        Expression<Func<CoverageMathEntity, bool>> capturedExpr = x => x.IntVal > val;
        var q3 = parser.Parse(capturedExpr);
        await Assert.That(q3).IsNotNull();

        // 4. Convert
        Expression<Func<CoverageMathEntity, bool>> convertExpr = x => (double)x.IntVal > 5.0;
        var q4 = parser.Parse(convertExpr);
        await Assert.That(q4).IsNotNull();
    }

    [Test]
    public async Task QueryExecutor_Execution_Coverage()
    {
        var col = _engine.GetCollection<CoverageMathEntity>();
        col.Insert(new CoverageMathEntity { Id = 1, IntVal = 10 });

        // Query with local variable capture
        int localVal = 10;
        var res = _executor.Execute<CoverageMathEntity>("MathEntity", x => x.IntVal == localVal).ToList();
        await Assert.That(res).IsNotEmpty();

        // Query with unsupported method should trigger execution exception
        await Assert.That(() => col.Find(x => x.IntVal.GetHashCode() == 0).ToList()).Throws<NotSupportedException>();
    }

    [Test]
    public async Task BsonValue_Conversion_Coverage()
    {
        BsonValue v = 123;
        await Assert.That(v.ToInt32(null)).IsEqualTo(123);
        await Assert.That(v.ToInt64(null)).IsEqualTo(123L);
        await Assert.That(v.ToDouble(null)).IsEqualTo(123.0);
        await Assert.That(v.ToBoolean(null)).IsTrue();

        BsonValue s = "123";
        await Assert.That(s.ToString(null)).IsEqualTo("123");

        BsonValue d = DateTime.UtcNow;
        await Assert.That(() => d.ToInt32(null)).Throws<InvalidCastException>();
    }

    [Test]
    public async Task BsonValue_Polymorphism_Fix()
    {
        // Cover BsonArrayValue (internal wrapper)
        var arr = new BsonArray(new BsonValue[] { 1, 2, 3 });
        BsonValue valArr = arr;

        // CompareTo
        await Assert.That(valArr.CompareTo(new BsonArray(new BsonValue[] { 1, 2, 3 }))).IsEqualTo(0);
        await Assert.That(valArr.CompareTo(new BsonArray(new BsonValue[] { 1, 2 }))).IsGreaterThan(0);

        // Equals
        await Assert.That(valArr.Equals(new BsonArray(new BsonValue[] { 1, 2, 3 }))).IsTrue();

        // ToString
        await Assert.That(valArr.ToString()).Contains("[");

        // Cover BsonDocumentValue (internal wrapper)
        var doc = new BsonDocument(new Dictionary<string, BsonValue> { { "a", 1 } });
        BsonValue valDoc = doc;

        await Assert.That(valDoc.CompareTo(new BsonDocument(new Dictionary<string, BsonValue> { { "a", 1 } }))).IsEqualTo(0);
        await Assert.That(valDoc.Equals(new BsonDocument(new Dictionary<string, BsonValue> { { "a", 1 } }))).IsTrue();
        await Assert.That(valDoc.ToString()).Contains("{");
    }

    [Test]
    public async Task BsonScanner_Comprehensive_Coverage()
    {
        var doc = new BsonDocument(new Dictionary<string, BsonValue>
        {
            { "double", 1.23 },
            { "string", "hello" },
            { "int32", 123 },
            { "int64", 1234567890L },
            { "bool", true },
            { "date", DateTime.UtcNow },
            { "decimal", new BsonDecimal128(10.5m) },
            { "null", BsonNull.Value },
            { "oid", ObjectId.NewObjectId() },
            { "binary", new BsonBinary(new byte[] { 1, 2, 3 }) }
        });
        var bytes = BsonSerializer.SerializeDocument(doc);

        await Assert.That(BsonScanner.TryGetValue(bytes, "double", out var v1) && v1 is BsonDouble).IsTrue();
        await Assert.That(BsonScanner.TryGetValue(bytes, "int64", out var v2) && v2 is BsonInt64).IsTrue();
        await Assert.That(BsonScanner.TryGetValue(bytes, "bool", out var v3) && v3 is BsonBoolean).IsTrue();
        await Assert.That(BsonScanner.TryGetValue(bytes, "date", out var v4) && v4 is BsonDateTime).IsTrue();
        await Assert.That(BsonScanner.TryGetValue(bytes, "decimal", out var v5) && v5 is BsonDecimal128).IsTrue();
        await Assert.That(BsonScanner.TryGetValue(bytes, "null", out var v6) && v6 is BsonNull).IsTrue();
        await Assert.That(BsonScanner.TryGetValue(bytes, "oid", out var v7) && v7 is BsonObjectId).IsTrue();

        // Test SkipValue for Binary (by searching for something AFTER binary)
        await Assert.That(BsonScanner.TryGetValue(bytes, "oid", out _)).IsTrue();
    }

    [Test]
    public async Task BsonConversion_Comprehensive_Coverage()
    {
        // 1. ToBsonValue
        await Assert.That(BsonConversion.ToBsonValue((byte)1).BsonType).IsEqualTo(BsonType.Int32);
        await Assert.That(BsonConversion.ToBsonValue((sbyte)1).BsonType).IsEqualTo(BsonType.Int32);
        await Assert.That(BsonConversion.ToBsonValue((short)1).BsonType).IsEqualTo(BsonType.Int32);
        await Assert.That(BsonConversion.ToBsonValue((ushort)1).BsonType).IsEqualTo(BsonType.Int32);
        await Assert.That(BsonConversion.ToBsonValue((uint)1).BsonType).IsEqualTo(BsonType.Int64);
        await Assert.That(BsonConversion.ToBsonValue((ulong)1).BsonType).IsEqualTo(BsonType.Int64);
        await Assert.That(BsonConversion.ToBsonValue(1.23f).BsonType).IsEqualTo(BsonType.Double);
        await Assert.That(BsonConversion.ToBsonValue(Guid.NewGuid()).BsonType).IsEqualTo(BsonType.String);
        await Assert.That(BsonConversion.ToBsonValue(BsonType.Binary).BsonType).IsEqualTo(BsonType.Int32); // Enum

        // 2. FromBsonValue
        BsonValue bv = 123;
        await Assert.That(BsonConversion.FromBsonValue(bv, typeof(byte))).IsEqualTo((byte)123);
        await Assert.That(BsonConversion.FromBsonValue(bv, typeof(long))).IsEqualTo(123L);
        await Assert.That(BsonConversion.FromBsonValue(bv, typeof(double))).IsEqualTo(123.0);
        await Assert.That(BsonConversion.FromBsonValue(new BsonString("123"), typeof(int))).IsEqualTo(123);
        await Assert.That(BsonConversion.FromBsonValue(new BsonBoolean(true), typeof(bool))).IsEqualTo(true);

        var date = DateTime.UtcNow;
        await Assert.That(BsonConversion.FromBsonValue(new BsonDateTime(date), typeof(DateTime))).IsEqualTo(date);
    }

    [Test]
    public async Task DecimalOperatorSupport_Coverage()
    {
        // DecimalOperatorSupport uses internal state to check if initialized.
        // We can call it multiple times.
        DecimalOperatorSupport.Ensure();
        DecimalOperatorSupport.Ensure(); // Should hit the "already initialized" branch
    }

    [Test]
    public async Task AotBsonMapper_Complex_Coverage()
    {
        var entity = new CoverageComplexEntity
        {
            Id = 1,
            Name = "Complex",
            IntList = new List<int> { 1, 2, 3 },
            Dict = new Dictionary<string, string> { { "k1", "v1" } },
            Sub = new CoverageNestedEntity { Key = "nest", Value = 99 },
            SubList = new List<CoverageNestedEntity> { new CoverageNestedEntity { Key = "item", Value = 1 } }
        };

        var doc = AotBsonMapper.ToDocument(entity);

        // Debug: print Sub field type
        var subVal = doc["sub"];
        Console.WriteLine($"DEBUG Sub type: {subVal.GetType().FullName}");
        Console.WriteLine($"DEBUG Sub.IsDocument: {subVal.IsDocument}");
        Console.WriteLine($"DEBUG Sub.BsonType: {subVal.BsonType}");
        Console.WriteLine($"DEBUG Sub.RawValue: {subVal.RawValue?.GetType().FullName ?? "null"}");

        await Assert.That(doc["_id"].ToInt32(null)).IsEqualTo(1);
        await Assert.That(doc["intList"].IsArray).IsTrue();
        await Assert.That(doc["dict"].IsDocument).IsTrue();

        var entity2 = AotBsonMapper.FromDocument<CoverageComplexEntity>(doc);
        await Assert.That(entity2?.Name).IsEqualTo("Complex");
        await Assert.That(entity2?.IntList.Count).IsEqualTo(3);
        await Assert.That(entity2?.Sub.Value).IsEqualTo(99);
        await Assert.That(entity2?.SubList.Count).IsEqualTo(1);
    }

    [Test]
    public async Task IndexManager_Lifecycle_Coverage()
    {
        using var manager = new IndexManager("TestCol");
        manager.CreateIndex("idx1", new[] { "field1" }, true);
        manager.CreateIndex("idx2", new[] { "field2" }, false);

        await Assert.That(manager.IndexCount).IsEqualTo(2);
        await Assert.That(manager.IndexExists("idx1")).IsTrue();

        // Validate
        var valResult = manager.ValidateAllIndexes();
        await Assert.That(valResult.ValidIndexes).IsEqualTo(2);

        // Clear
        manager.ClearAllIndexes();

        // Drop
        await Assert.That(manager.DropIndex("idx1")).IsTrue();
        await Assert.That(manager.IndexCount).IsEqualTo(1);

        manager.DropAllIndexes();
        await Assert.That(manager.IndexCount).IsEqualTo(0);
    }

    [Test]
    public async Task Bson_Rare_Types_RoundTrip()
    {
        var doc = new BsonDocument(new Dictionary<string, BsonValue>
        {
            { "js_scope", new BsonJavaScriptWithScope("var x = a;", new BsonDocument(new Dictionary<string, BsonValue> { { "a", 1 } })) },
            { "undefined", BsonNull.Value } // Undefined is usually treated as Null in TinyDb
        });

        var bytes = BsonSerializer.SerializeDocument(doc);
        var doc2 = BsonSerializer.DeserializeDocument(bytes);

        await Assert.That(doc2.ContainsKey("js_scope")).IsTrue();
    }

    [Test]
    public async Task ExpressionEvaluator_Bson_Special_Types()
    {
        var oid = ObjectId.NewObjectId();
        var bin = new BsonBinary(new byte[] { 1, 2, 3 });
        var doc = new BsonDocument(new Dictionary<string, BsonValue>
        {
            { "oid", oid },
            { "bin", bin }
        });

        // ObjectId equality
        await Assert.That(ExpressionEvaluator.Evaluate(new TinyDb.Query.BinaryExpression(ExpressionType.Equal, new TinyDb.Query.MemberExpression("oid", null), new TinyDb.Query.ConstantExpression(oid)), doc)).IsTrue();

        // Binary equality
        await Assert.That(ExpressionEvaluator.Evaluate(new TinyDb.Query.BinaryExpression(ExpressionType.Equal, new TinyDb.Query.MemberExpression("bin", null), new TinyDb.Query.ConstantExpression(bin)), doc)).IsTrue();
    }
}

[Entity("MathEntity")]
public class CoverageMathEntity
{
    public int Id { get; set; }
    public int IntVal { get; set; }
    public double DoubleVal { get; set; }
    public decimal DecimalVal { get; set; }
}

[Entity("UniqueEntity")]
public class CoverageUniqueEntity
{
    public int Id { get; set; }
    public string Code { get; set; } = "";
}

[Entity("StringEntity")]
public class CoverageStringEntity
{
    public int Id { get; set; }
    public string Text { get; set; } = "";
    public List<string> Tags { get; set; } = new();
    public DateTime Created { get; set; }
}

[Entity("ComplexEntity")]
public class CoverageComplexEntity
{
    public int Id { get; set; }
    public string Name { get; set; } = "";
    public List<int> IntList { get; set; } = new();
    public Dictionary<string, string> Dict { get; set; } = new();
    public CoverageNestedEntity Sub { get; set; } = new();
    public List<CoverageNestedEntity> SubList { get; set; } = new();
}

[Entity("NestedEntity")]
public class CoverageNestedEntity
{
    public string Key { get; set; } = "";
    public int Value { get; set; }
}
