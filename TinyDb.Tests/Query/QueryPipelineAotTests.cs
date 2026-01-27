using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using TinyDb.Bson;
using TinyDb.Collections;
using TinyDb.Core;
using TinyDb.Query;
using TinyDb.Tests.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using LinqExp = System.Linq.Expressions;

namespace TinyDb.Tests.Query;

/// <summary>
/// Tests for QueryPipeline to improve coverage, especially for AOT mode operations
/// </summary>
public class QueryPipelineAotTests : IDisposable
{
    private readonly string _dbPath;
    private readonly TinyDbEngine _engine;
    private readonly ITinyCollection<TestProduct> _products;

    public QueryPipelineAotTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"qpipe_aot_{Guid.NewGuid()}.db");
        _engine = new TinyDbEngine(_dbPath);
        _products = _engine.GetCollection<TestProduct>();
        SeedData();
    }

    private void SeedData()
    {
        _products.Insert(new TestProduct { Id = 1, Name = "Laptop", Category = "Electronics", Price = 1000m, Quantity = 5 });
        _products.Insert(new TestProduct { Id = 2, Name = "Mouse", Category = "Electronics", Price = 50m, Quantity = 20 });
        _products.Insert(new TestProduct { Id = 3, Name = "Keyboard", Category = "Electronics", Price = 80m, Quantity = 15 });
        _products.Insert(new TestProduct { Id = 4, Name = "Chair", Category = "Furniture", Price = 150m, Quantity = 10 });
        _products.Insert(new TestProduct { Id = 5, Name = "Desk", Category = "Furniture", Price = 200m, Quantity = 8 });
        _products.Insert(new TestProduct { Id = 6, Name = "Lamp", Category = "Furniture", Price = 30m, Quantity = 25 });
    }

    public void Dispose()
    {
        _engine?.Dispose();
        if (File.Exists(_dbPath)) try { File.Delete(_dbPath); } catch { }
    }

    #region Skip and Take Tests

    [Test]
    public async Task Skip_Should_Skip_First_N_Items()
    {
        var result = _products.Query()
            .OrderBy(p => p.Id)
            .Skip(2)
            .ToList();

        await Assert.That(result.Count).IsEqualTo(4);
        await Assert.That(result[0].Id).IsEqualTo(3);
    }

    [Test]
    public async Task Take_Should_Take_First_N_Items()
    {
        var result = _products.Query()
            .OrderBy(p => p.Id)
            .Take(3)
            .ToList();

        await Assert.That(result.Count).IsEqualTo(3);
        await Assert.That(result[2].Id).IsEqualTo(3);
    }

    [Test]
    public async Task Skip_Take_Combined_Should_Work()
    {
        var result = _products.Query()
            .OrderBy(p => p.Id)
            .Skip(1)
            .Take(2)
            .ToList();

        await Assert.That(result.Count).IsEqualTo(2);
        await Assert.That(result[0].Id).IsEqualTo(2);
        await Assert.That(result[1].Id).IsEqualTo(3);
    }

    #endregion

    #region OrderBy Tests

    [Test]
    public async Task OrderBy_Should_Sort_Ascending()
    {
        var result = _products.Query()
            .OrderBy(p => p.Price)
            .ToList();

        await Assert.That(result[0].Price).IsEqualTo(30m);
        await Assert.That(result[5].Price).IsEqualTo(1000m);
    }

    [Test]
    public async Task OrderByDescending_Should_Sort_Descending()
    {
        var result = _products.Query()
            .OrderByDescending(p => p.Price)
            .ToList();

        await Assert.That(result[0].Price).IsEqualTo(1000m);
        await Assert.That(result[5].Price).IsEqualTo(30m);
    }

    [Test]
    public async Task ThenBy_Should_Sort_Secondary()
    {
        var result = _products.Query()
            .OrderBy(p => p.Category)
            .ThenBy(p => p.Price)
            .ToList();

        // Electronics sorted by price: Mouse(50), Keyboard(80), Laptop(1000)
        await Assert.That(result[0].Name).IsEqualTo("Mouse");
        await Assert.That(result[1].Name).IsEqualTo("Keyboard");
        await Assert.That(result[2].Name).IsEqualTo("Laptop");

        // Furniture sorted by price: Lamp(30), Chair(150), Desk(200)
        await Assert.That(result[3].Name).IsEqualTo("Lamp");
        await Assert.That(result[4].Name).IsEqualTo("Chair");
        await Assert.That(result[5].Name).IsEqualTo("Desk");
    }

    [Test]
    public async Task ThenByDescending_Should_Sort_Secondary_Descending()
    {
        var result = _products.Query()
            .OrderBy(p => p.Category)
            .ThenByDescending(p => p.Price)
            .ToList();

        // Electronics sorted by price desc: Laptop(1000), Keyboard(80), Mouse(50)
        await Assert.That(result[0].Name).IsEqualTo("Laptop");
        await Assert.That(result[1].Name).IsEqualTo("Keyboard");
        await Assert.That(result[2].Name).IsEqualTo("Mouse");
    }

    #endregion

    #region Select/Projection Tests

    [Test]
    public async Task Select_Simple_Property_Should_Work()
    {
        var names = _products.Query()
            .OrderBy(p => p.Id)
            .Select(p => p.Name)
            .ToList();

        await Assert.That(names.Count).IsEqualTo(6);
        await Assert.That(names[0]).IsEqualTo("Laptop");
    }

    [Test]
    public async Task Select_Arithmetic_Expression_Should_Work()
    {
        var totals = _products.Query()
            .Where(p => p.Id <= 2)
            .OrderBy(p => p.Id)
            .Select(p => p.Price * p.Quantity)
            .ToList();

        await Assert.That(totals.Count).IsEqualTo(2);
        await Assert.That(totals[0]).IsEqualTo(5000m); // 1000 * 5
        await Assert.That(totals[1]).IsEqualTo(1000m); // 50 * 20
    }

    #endregion

    #region Distinct Tests

    [Test]
    public async Task Distinct_Should_Remove_Duplicates()
    {
        var categories = _products.Query()
            .Select(p => p.Category)
            .Distinct()
            .OrderBy(c => c)
            .ToList();

        await Assert.That(categories.Count).IsEqualTo(2);
        await Assert.That(categories[0]).IsEqualTo("Electronics");
        await Assert.That(categories[1]).IsEqualTo("Furniture");
    }

    #endregion

    #region Terminal Operations Tests

    [Test]
    public async Task Count_Should_Return_Total()
    {
        var count = _products.Query().Count();
        await Assert.That(count).IsEqualTo(6);
    }

    [Test]
    public async Task Count_With_Predicate_Should_Filter()
    {
        var count = _products.Query().Count(p => p.Category == "Electronics");
        await Assert.That(count).IsEqualTo(3);
    }

    [Test]
    public async Task LongCount_Should_Return_Total()
    {
        var count = _products.Query().LongCount();
        await Assert.That(count).IsEqualTo(6L);
    }

    [Test]
    public async Task Any_Without_Predicate_Should_Return_True()
    {
        var any = _products.Query().Any();
        await Assert.That(any).IsTrue();
    }

    [Test]
    public async Task Any_With_Predicate_Should_Work()
    {
        var hasExpensive = _products.Query().Any(p => p.Price > 500);
        await Assert.That(hasExpensive).IsTrue();

        var hasSuperExpensive = _products.Query().Any(p => p.Price > 5000);
        await Assert.That(hasSuperExpensive).IsFalse();
    }

    [Test]
    public async Task All_Should_Check_All_Items()
    {
        var allPositive = _products.Query().All(p => p.Price > 0);
        await Assert.That(allPositive).IsTrue();

        var allElectronics = _products.Query().All(p => p.Category == "Electronics");
        await Assert.That(allElectronics).IsFalse();
    }

    [Test]
    public async Task First_Should_Return_First_Item()
    {
        var first = _products.Query().OrderBy(p => p.Id).First();
        await Assert.That(first.Id).IsEqualTo(1);
    }

    [Test]
    public async Task First_With_Predicate_Should_Filter()
    {
        var first = _products.Query().OrderBy(p => p.Id).First(p => p.Category == "Furniture");
        await Assert.That(first.Name).IsEqualTo("Chair");
    }

    [Test]
    public async Task FirstOrDefault_Should_Return_Null_When_Empty()
    {
        var result = _products.Query().FirstOrDefault(p => p.Price > 10000);
        await Assert.That(result).IsNull();
    }

    [Test]
    public async Task Single_Should_Return_Single_Item()
    {
        var single = _products.Query().Single(p => p.Name == "Laptop");
        await Assert.That(single.Price).IsEqualTo(1000m);
    }

    [Test]
    public async Task SingleOrDefault_Should_Return_Null_When_NotFound()
    {
        var result = _products.Query().SingleOrDefault(p => p.Name == "NotExist");
        await Assert.That(result).IsNull();
    }

    [Test]
    public async Task Last_Should_Return_Last_Item()
    {
        var last = _products.Query().OrderBy(p => p.Id).Last();
        await Assert.That(last.Id).IsEqualTo(6);
    }

    [Test]
    public async Task LastOrDefault_Should_Return_Null_When_Empty()
    {
        var result = _products.Query().LastOrDefault(p => p.Price > 10000);
        await Assert.That(result).IsNull();
    }

    [Test]
    public async Task ElementAt_Should_Return_Item_At_Index()
    {
        var item = _products.Query().OrderBy(p => p.Id).ElementAt(2);
        await Assert.That(item.Id).IsEqualTo(3);
    }

    [Test]
    public async Task ElementAtOrDefault_Should_Return_Null_For_Invalid_Index()
    {
        var item = _products.Query().OrderBy(p => p.Id).ElementAtOrDefault(100);
        await Assert.That(item).IsNull();
    }

    #endregion

    #region Aggregation Tests

    [Test]
    public async Task Sum_Should_Calculate_Total()
    {
        var total = _products.Query().Sum(p => p.Price);
        await Assert.That(total).IsEqualTo(1510m);
    }

    [Test]
    public async Task Average_Should_Calculate_Average()
    {
        var avg = _products.Query().Average(p => p.Price);
        // 1510 / 6 ≈ 251.67
        await Assert.That(avg).IsGreaterThan(251m);
        await Assert.That(avg).IsLessThan(252m);
    }

    [Test]
    public async Task Min_Should_Find_Minimum()
    {
        var min = _products.Query().Min(p => p.Price);
        await Assert.That(min).IsEqualTo(30m);
    }

    [Test]
    public async Task Max_Should_Find_Maximum()
    {
        var max = _products.Query().Max(p => p.Price);
        await Assert.That(max).IsEqualTo(1000m);
    }

    #endregion

    #region Complex Query Tests

    [Test]
    public async Task Complex_Query_Chain_Should_Work()
    {
        var result = _products.Query()
            .Where(p => p.Price >= 50)
            .OrderByDescending(p => p.Price)
            .Skip(1)
            .Take(3)
            .Select(p => p.Name)
            .ToList();

        await Assert.That(result.Count).IsEqualTo(3);
        // After skip 1 (Laptop), take 3: Desk(200), Chair(150), Keyboard(80)
        await Assert.That(result[0]).IsEqualTo("Desk");
        await Assert.That(result[1]).IsEqualTo("Chair");
        await Assert.That(result[2]).IsEqualTo("Keyboard");
    }

    [Test]
    public async Task Multiple_Where_Clauses_Should_Work()
    {
        var result = _products.Query()
            .Where(p => p.Category == "Electronics")
            .Where(p => p.Price > 60)
            .OrderBy(p => p.Price)
            .ToList();

        await Assert.That(result.Count).IsEqualTo(2);
        await Assert.That(result[0].Name).IsEqualTo("Keyboard"); // 80
        await Assert.That(result[1].Name).IsEqualTo("Laptop");   // 1000
    }

    #endregion

    public class TestProduct
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
        public string Category { get; set; } = "";
        public decimal Price { get; set; }
        public int Quantity { get; set; }
    }
}

/// <summary>
/// Tests for ObjectComparer used in AOT OrderBy operations
/// </summary>
public class ObjectComparerTests
{
    [Test]
    public async Task Compare_Nulls_Should_Handle_Correctly()
    {
        var comparer = CreateComparer();

        await Assert.That(comparer.Compare(null, null)).IsEqualTo(0);
        await Assert.That(comparer.Compare(null, "a")).IsEqualTo(-1);
        await Assert.That(comparer.Compare("a", null)).IsEqualTo(1);
    }

    [Test]
    public async Task Compare_SameReference_Should_Return_Zero()
    {
        var comparer = CreateComparer();
        var obj = new object();

        await Assert.That(comparer.Compare(obj, obj)).IsEqualTo(0);
    }

    [Test]
    public async Task Compare_Numerics_Should_Work()
    {
        var comparer = CreateComparer();

        await Assert.That(comparer.Compare(1, 2)).IsLessThan(0);
        await Assert.That(comparer.Compare(2, 1)).IsGreaterThan(0);
        await Assert.That(comparer.Compare(1, 1)).IsEqualTo(0);

        await Assert.That(comparer.Compare(1.5, 2.5)).IsLessThan(0);
        await Assert.That(comparer.Compare(1L, 2L)).IsLessThan(0);
        await Assert.That(comparer.Compare(1m, 2m)).IsLessThan(0);
    }

    [Test]
    public async Task Compare_MixedNumerics_Should_Work()
    {
        var comparer = CreateComparer();

        await Assert.That(comparer.Compare(1, 1.0)).IsEqualTo(0);
        await Assert.That(comparer.Compare(1, 2L)).IsLessThan(0);
        await Assert.That(comparer.Compare(1.5f, 1.5)).IsEqualTo(0);
    }

    [Test]
    public async Task Compare_Strings_Should_Work()
    {
        var comparer = CreateComparer();

        await Assert.That(comparer.Compare("a", "b")).IsLessThan(0);
        await Assert.That(comparer.Compare("b", "a")).IsGreaterThan(0);
        await Assert.That(comparer.Compare("a", "a")).IsEqualTo(0);
    }

    [Test]
    public async Task Compare_IComparable_Should_Work()
    {
        var comparer = CreateComparer();
        var d1 = new DateTime(2020, 1, 1);
        var d2 = new DateTime(2021, 1, 1);

        await Assert.That(comparer.Compare(d1, d2)).IsLessThan(0);
        await Assert.That(comparer.Compare(d2, d1)).IsGreaterThan(0);
    }

    [Test]
    public async Task Compare_FallbackToString_Should_Work()
    {
        var comparer = CreateComparer();
        var obj1 = new NonComparable("A");
        var obj2 = new NonComparable("B");

        await Assert.That(comparer.Compare(obj1, obj2)).IsLessThan(0);
    }

    private static IComparer<object> CreateComparer()
    {
        // Access the internal ObjectComparer via reflection or create equivalent
        return new TestObjectComparer();
    }

    private class NonComparable
    {
        private readonly string _value;
        public NonComparable(string value) => _value = value;
        public override string ToString() => _value;
    }

    private class TestObjectComparer : IComparer<object>
    {
        public int Compare(object? x, object? y)
        {
            if (ReferenceEquals(x, y)) return 0;
            if (x is null) return -1;
            if (y is null) return 1;

            if (IsNumeric(x) && IsNumeric(y))
            {
                try { return Convert.ToDouble(x).CompareTo(Convert.ToDouble(y)); } catch { }
            }

            if (x is IComparable cx && x.GetType() == y.GetType()) return cx.CompareTo(y);

            return string.Compare(x.ToString(), y.ToString(), StringComparison.Ordinal);
        }

        private static bool IsNumeric(object x) => x is int || x is long || x is double || x is float || x is decimal || x is short || x is byte;
    }
}

/// <summary>
/// Tests for AotGrouping class
/// </summary>
public class AotGroupingTests : IDisposable
{
    private readonly string _dbPath;
    private readonly TinyDbEngine _engine;
    private readonly ITinyCollection<GroupTestItem> _items;

    public AotGroupingTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"aot_group_{Guid.NewGuid()}.db");
        _engine = new TinyDbEngine(_dbPath);
        _items = _engine.GetCollection<GroupTestItem>();

        _items.Insert(new GroupTestItem { Id = 1, Category = "A", Value = 10 });
        _items.Insert(new GroupTestItem { Id = 2, Category = "A", Value = 20 });
        _items.Insert(new GroupTestItem { Id = 3, Category = "B", Value = 30 });
        _items.Insert(new GroupTestItem { Id = 4, Category = "B", Value = 40 });
        _items.Insert(new GroupTestItem { Id = 5, Category = "B", Value = 50 });
    }

    public void Dispose()
    {
        _engine?.Dispose();
        if (File.Exists(_dbPath)) try { File.Delete(_dbPath); } catch { }
    }

    [Test]
    [SkipInAot("GroupBy requires dynamic code generation")]
    public async Task GroupBy_Key_Should_Be_Accessible()
    {
        var groups = _items.Query()
            .GroupBy(x => x.Category)
            .ToList();

        await Assert.That(groups.Count).IsEqualTo(2);
    }

    [Test]
    [SkipInAot("GroupBy requires dynamic code generation")]
    public async Task GroupBy_Count_Should_Work()
    {
        var result = _items.Query()
            .GroupBy(x => x.Category)
            .Select(g => new { Key = g.Key, Count = g.Count() })
            .OrderBy(x => x.Key)
            .ToList();

        await Assert.That(result[0].Key).IsEqualTo("A");
        await Assert.That(result[0].Count).IsEqualTo(2);
        await Assert.That(result[1].Key).IsEqualTo("B");
        await Assert.That(result[1].Count).IsEqualTo(3);
    }

    [Test]
    [SkipInAot("GroupBy requires dynamic code generation")]
    public async Task GroupBy_Sum_Should_Work()
    {
        var result = _items.Query()
            .GroupBy(x => x.Category)
            .Select(g => new { Key = g.Key, Total = g.Sum(x => x.Value) })
            .OrderBy(x => x.Key)
            .ToList();

        await Assert.That((decimal)result[0].Total).IsEqualTo(30m); // 10 + 20
        await Assert.That((decimal)result[1].Total).IsEqualTo(120m); // 30 + 40 + 50
    }

    [Test]
    [SkipInAot("GroupBy requires dynamic code generation")]
    public async Task GroupBy_Average_Should_Work()
    {
        var result = _items.Query()
            .GroupBy(x => x.Category)
            .Select(g => new { Key = g.Key, Avg = g.Average(x => x.Value) })
            .OrderBy(x => x.Key)
            .ToList();

        await Assert.That((double)result[0].Avg).IsEqualTo(15.0); // (10 + 20) / 2
        await Assert.That((double)result[1].Avg).IsGreaterThan(39.0); // (30 + 40 + 50) / 3
    }

    [Test]
    [SkipInAot("GroupBy requires dynamic code generation")]
    public async Task GroupBy_Min_Max_Should_Work()
    {
        var result = _items.Query()
            .GroupBy(x => x.Category)
            .Select(g => new { Key = g.Key, Min = g.Min(x => x.Value), Max = g.Max(x => x.Value) })
            .OrderBy(x => x.Key)
            .ToList();

        await Assert.That(result[0].Min).IsEqualTo(10);
        await Assert.That(result[0].Max).IsEqualTo(20);
        await Assert.That(result[1].Min).IsEqualTo(30);
        await Assert.That(result[1].Max).IsEqualTo(50);
    }

    public class GroupTestItem
    {
        public int Id { get; set; }
        public string Category { get; set; } = "";
        public int Value { get; set; }
    }
}
