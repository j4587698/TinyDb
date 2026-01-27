using System.Linq.Expressions;
using TinyDb.Core;
using TinyDb.Query;
using TinyDb.Bson;
using TinyDb.Collections;
using TinyDb.Attributes;
using TinyDb.Tests.TestEntities;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Query;

/// <summary>
/// Tests for improving Queryable.cs coverage
/// </summary>
[NotInParallel]
public class QueryableCoverageTests
{
    private string _testDirectory = null!;
    private string _testDbPath = null!;
    private TinyDbEngine _engine = null!;
    private QueryExecutor _executor = null!;
    private int _testCounter;

    [Before(Test)]
    public void Setup()
    {
        _testDirectory = Path.Combine(Path.GetTempPath(), "TinyDbQueryableCoverageTests", Guid.NewGuid().ToString());
        Directory.CreateDirectory(_testDirectory);
        _testDbPath = Path.Combine(_testDirectory, "test.db");

        _engine = new TinyDbEngine(_testDbPath);
        _executor = new QueryExecutor(_engine);
        _testCounter = 0;
    }

    [After(Test)]
    public void Cleanup()
    {
        _engine?.Dispose();
        if (Directory.Exists(_testDirectory))
        {
            try { Directory.Delete(_testDirectory, true); } catch { }
        }
    }
    
    private string GetUniqueCollectionName(string baseName = "products")
    {
        return $"{baseName}_{++_testCounter}_{Guid.NewGuid():N}";
    }

    #region Queryable<TSource, TData> Tests

    [Test]
    public async Task Queryable_TwoTypeParams_Should_Initialize_Correctly()
    {
        // Arrange
        const string collectionName = "products";
        var expression = Expression.Constant(new object());

        // Act
        var queryable = new Queryable<TestProduct, TestProduct>(_executor, collectionName, expression);

        // Assert
        await Assert.That(queryable).IsNotNull();
        await Assert.That(queryable.ElementType).IsEqualTo(typeof(TestProduct));
        await Assert.That(queryable.Expression).IsNotNull();
        await Assert.That(queryable.Provider).IsNotNull();
    }

    [Test]
    public async Task Queryable_ToString_Should_Return_Correct_Format()
    {
        // Arrange
        const string collectionName = "products";
        var queryable = new Queryable<TestProduct>(_executor, collectionName);

        // Act
        var result = queryable.ToString();

        // Assert
        await Assert.That(result).IsEqualTo("Queryable<TestProduct->TestProduct>[products]");
    }

    [Test]
    public async Task Queryable_GetEnumerator_NonGeneric_Should_Work()
    {
        // Arrange
        var collectionName = GetUniqueCollectionName();
        var products = new List<TestProduct>
        {
            new() { Name = "Laptop", Price = 999.99m, Category = "Electronics", InStock = true, CreatedAt = DateTime.UtcNow },
            new() { Name = "Mouse", Price = 29.99m, Category = "Electronics", InStock = true, CreatedAt = DateTime.UtcNow }
        };

        var collection = _engine.GetCollection<TestProduct>(collectionName);
        foreach (var product in products) collection.Insert(product);

        var queryable = new Queryable<TestProduct>(_executor, collectionName);

        // Act - use non-generic IEnumerable interface
        var enumerable = (System.Collections.IEnumerable)queryable;
        var count = 0;
        foreach (var item in enumerable) count++;

        // Assert
        await Assert.That(count).IsEqualTo(2);
    }

    #endregion

    #region Select Tests

    [Test]
    public async Task Select_Should_Project_To_Anonymous_Type()
    {
        // Arrange
        var collectionName = GetUniqueCollectionName();
        var products = new List<TestProduct>
        {
            new() { Name = "Laptop", Price = 999.99m, Category = "Electronics", InStock = true, CreatedAt = DateTime.UtcNow },
            new() { Name = "Mouse", Price = 29.99m, Category = "Electronics", InStock = true, CreatedAt = DateTime.UtcNow }
        };

        var collection = _engine.GetCollection<TestProduct>(collectionName);
        foreach (var product in products) collection.Insert(product);

        var queryable = new Queryable<TestProduct>(_executor, collectionName);

        // Act
        var results = queryable.Select(p => new { p.Name, p.Price }).ToList();

        // Assert
        await Assert.That(results.Count).IsEqualTo(2);
        await Assert.That(results[0].Name).IsEqualTo("Laptop");
    }

    [Test]
    public async Task Select_Should_Project_To_Single_Property()
    {
        // Arrange
        var collectionName = GetUniqueCollectionName();
        var products = new List<TestProduct>
        {
            new() { Name = "Laptop", Price = 999.99m, Category = "Electronics", InStock = true, CreatedAt = DateTime.UtcNow },
            new() { Name = "Mouse", Price = 29.99m, Category = "Electronics", InStock = true, CreatedAt = DateTime.UtcNow }
        };

        var collection = _engine.GetCollection<TestProduct>(collectionName);
        foreach (var product in products) collection.Insert(product);

        var queryable = new Queryable<TestProduct>(_executor, collectionName);

        // Act
        var names = queryable.Select(p => p.Name).ToList();

        // Assert
        await Assert.That(names.Count).IsEqualTo(2);
        await Assert.That(names).Contains("Laptop");
        await Assert.That(names).Contains("Mouse");
    }

    #endregion

    #region Skip and Take Tests

    [Test]
    public async Task Skip_Should_Skip_Items()
    {
        // Arrange
        var collectionName = GetUniqueCollectionName();
        var products = Enumerable.Range(1, 10)
            .Select(i => new TestProduct { Name = $"Product{i}", Price = i * 10, Category = "Cat", InStock = true, CreatedAt = DateTime.UtcNow })
            .ToList();

        var collection = _engine.GetCollection<TestProduct>(collectionName);
        foreach (var product in products) collection.Insert(product);

        var queryable = new Queryable<TestProduct>(_executor, collectionName);

        // Act
        var results = queryable.Skip(5).ToList();

        // Assert
        await Assert.That(results.Count).IsEqualTo(5);
    }

    [Test]
    public async Task Take_Should_Take_Items()
    {
        // Arrange
        var collectionName = GetUniqueCollectionName();
        var products = Enumerable.Range(1, 10)
            .Select(i => new TestProduct { Name = $"Product{i}", Price = i * 10, Category = "Cat", InStock = true, CreatedAt = DateTime.UtcNow })
            .ToList();

        var collection = _engine.GetCollection<TestProduct>(collectionName);
        foreach (var product in products) collection.Insert(product);

        var queryable = new Queryable<TestProduct>(_executor, collectionName);

        // Act
        var results = queryable.Take(3).ToList();

        // Assert
        await Assert.That(results.Count).IsEqualTo(3);
    }

    [Test]
    public async Task Skip_And_Take_Should_Work_Together()
    {
        // Arrange
        var collectionName = GetUniqueCollectionName();
        var products = Enumerable.Range(1, 10)
            .Select(i => new TestProduct { Name = $"Product{i}", Price = i * 10, Category = "Cat", InStock = true, CreatedAt = DateTime.UtcNow })
            .ToList();

        var collection = _engine.GetCollection<TestProduct>(collectionName);
        foreach (var product in products) collection.Insert(product);

        var queryable = new Queryable<TestProduct>(_executor, collectionName);

        // Act - pagination: skip 2, take 3
        var results = queryable.Skip(2).Take(3).ToList();

        // Assert
        await Assert.That(results.Count).IsEqualTo(3);
    }

    #endregion

    #region OrderBy Tests

    [Test]
    public async Task OrderBy_Should_Sort_Ascending()
    {
        // Arrange
        var collectionName = GetUniqueCollectionName();
        var products = new List<TestProduct>
        {
            new() { Name = "Banana", Price = 30m, Category = "Food", InStock = true, CreatedAt = DateTime.UtcNow },
            new() { Name = "Apple", Price = 20m, Category = "Food", InStock = true, CreatedAt = DateTime.UtcNow },
            new() { Name = "Cherry", Price = 40m, Category = "Food", InStock = true, CreatedAt = DateTime.UtcNow }
        };

        var collection = _engine.GetCollection<TestProduct>(collectionName);
        foreach (var product in products) collection.Insert(product);

        var queryable = new Queryable<TestProduct>(_executor, collectionName);

        // Act
        var results = queryable.OrderBy(p => p.Name).ToList();

        // Assert
        await Assert.That(results.Count).IsEqualTo(3);
        await Assert.That(results[0].Name).IsEqualTo("Apple");
        await Assert.That(results[1].Name).IsEqualTo("Banana");
        await Assert.That(results[2].Name).IsEqualTo("Cherry");
    }

    [Test]
    public async Task OrderByDescending_Should_Sort_Descending()
    {
        // Arrange
        var collectionName = GetUniqueCollectionName();
        var products = new List<TestProduct>
        {
            new() { Name = "Banana", Price = 30m, Category = "Food", InStock = true, CreatedAt = DateTime.UtcNow },
            new() { Name = "Apple", Price = 20m, Category = "Food", InStock = true, CreatedAt = DateTime.UtcNow },
            new() { Name = "Cherry", Price = 40m, Category = "Food", InStock = true, CreatedAt = DateTime.UtcNow }
        };

        var collection = _engine.GetCollection<TestProduct>(collectionName);
        foreach (var product in products) collection.Insert(product);

        var queryable = new Queryable<TestProduct>(_executor, collectionName);

        // Act
        var results = queryable.OrderByDescending(p => p.Price).ToList();

        // Assert
        await Assert.That(results.Count).IsEqualTo(3);
        await Assert.That(results[0].Name).IsEqualTo("Cherry");
        await Assert.That(results[1].Name).IsEqualTo("Banana");
        await Assert.That(results[2].Name).IsEqualTo("Apple");
    }

    [Test]
    public async Task ThenBy_Should_Sort_Secondary()
    {
        // Arrange
        var collectionName = GetUniqueCollectionName();
        var products = new List<TestProduct>
        {
            new() { Name = "Banana", Price = 30m, Category = "Food", InStock = true, CreatedAt = DateTime.UtcNow },
            new() { Name = "Apple", Price = 20m, Category = "Food", InStock = true, CreatedAt = DateTime.UtcNow },
            new() { Name = "Laptop", Price = 1000m, Category = "Electronics", InStock = true, CreatedAt = DateTime.UtcNow },
            new() { Name = "Mouse", Price = 50m, Category = "Electronics", InStock = true, CreatedAt = DateTime.UtcNow }
        };

        var collection = _engine.GetCollection<TestProduct>(collectionName);
        foreach (var product in products) collection.Insert(product);

        var queryable = new Queryable<TestProduct>(_executor, collectionName);

        // Act - sort by category, then by name
        var results = queryable.OrderBy(p => p.Category).ThenBy(p => p.Name).ToList();

        // Assert
        await Assert.That(results.Count).IsEqualTo(4);
        // Electronics first (alphabetically), then Food
        await Assert.That(results[0].Category).IsEqualTo("Electronics");
        await Assert.That(results[0].Name).IsEqualTo("Laptop");
        await Assert.That(results[1].Category).IsEqualTo("Electronics");
        await Assert.That(results[1].Name).IsEqualTo("Mouse");
    }

    [Test]
    public async Task ThenByDescending_Should_Sort_Secondary_Descending()
    {
        // Arrange
        var collectionName = GetUniqueCollectionName();
        var products = new List<TestProduct>
        {
            new() { Name = "Banana", Price = 30m, Category = "Food", InStock = true, CreatedAt = DateTime.UtcNow },
            new() { Name = "Apple", Price = 20m, Category = "Food", InStock = true, CreatedAt = DateTime.UtcNow },
            new() { Name = "Laptop", Price = 1000m, Category = "Electronics", InStock = true, CreatedAt = DateTime.UtcNow },
            new() { Name = "Mouse", Price = 50m, Category = "Electronics", InStock = true, CreatedAt = DateTime.UtcNow }
        };

        var collection = _engine.GetCollection<TestProduct>(collectionName);
        foreach (var product in products) collection.Insert(product);

        var queryable = new Queryable<TestProduct>(_executor, collectionName);

        // Act - sort by category, then by price descending
        var results = queryable.OrderBy(p => p.Category).ThenByDescending(p => p.Price).ToList();

        // Assert
        await Assert.That(results.Count).IsEqualTo(4);
        await Assert.That(results[0].Name).IsEqualTo("Laptop"); // Electronics, highest price
        await Assert.That(results[1].Name).IsEqualTo("Mouse"); // Electronics, lower price
    }

    #endregion

    #region Terminal Operations Tests

    [Test]
    public async Task LongCount_Should_Return_Correct_Count()
    {
        // Arrange
        var collectionName = GetUniqueCollectionName();
        var products = Enumerable.Range(1, 5)
            .Select(i => new TestProduct { Name = $"Product{i}", Price = i * 10, Category = "Cat", InStock = true, CreatedAt = DateTime.UtcNow })
            .ToList();

        var collection = _engine.GetCollection<TestProduct>(collectionName);
        foreach (var product in products) collection.Insert(product);

        var queryable = new Queryable<TestProduct>(_executor, collectionName);

        // Act
        var count = queryable.LongCount();

        // Assert
        await Assert.That(count).IsEqualTo(5L);
    }

    [Test]
    public async Task Single_Should_Return_Single_Item()
    {
        // Arrange
        var collectionName = GetUniqueCollectionName();
        var products = new List<TestProduct>
        {
            new() { Name = "UniqueProduct", Price = 100m, Category = "Unique", InStock = true, CreatedAt = DateTime.UtcNow }
        };

        var collection = _engine.GetCollection<TestProduct>(collectionName);
        foreach (var product in products) collection.Insert(product);

        var queryable = new Queryable<TestProduct>(_executor, collectionName);

        // Act
        var result = queryable.Single();

        // Assert
        await Assert.That(result).IsNotNull();
        await Assert.That(result.Name).IsEqualTo("UniqueProduct");
    }

    [Test]
    public async Task SingleOrDefault_Should_Return_Default_When_Empty()
    {
        // Arrange
        var collectionName = GetUniqueCollectionName("empty_products");
        var queryable = new Queryable<TestProduct>(_executor, collectionName);

        // Act
        var result = queryable.SingleOrDefault();

        // Assert
        await Assert.That(result).IsNull();
    }

    [Test]
    public async Task Last_Should_Return_Last_Item()
    {
        // Arrange
        var collectionName = GetUniqueCollectionName();
        var products = new List<TestProduct>
        {
            new() { Name = "First", Price = 10m, Category = "Cat", InStock = true, CreatedAt = DateTime.UtcNow },
            new() { Name = "Second", Price = 20m, Category = "Cat", InStock = true, CreatedAt = DateTime.UtcNow },
            new() { Name = "Third", Price = 30m, Category = "Cat", InStock = true, CreatedAt = DateTime.UtcNow }
        };

        var collection = _engine.GetCollection<TestProduct>(collectionName);
        foreach (var product in products) collection.Insert(product);

        var queryable = new Queryable<TestProduct>(_executor, collectionName);

        // Act
        var result = queryable.Last();

        // Assert
        await Assert.That(result).IsNotNull();
    }

    [Test]
    public async Task LastOrDefault_Should_Return_Default_When_Empty()
    {
        // Arrange
        var collectionName = GetUniqueCollectionName("empty_products");
        var queryable = new Queryable<TestProduct>(_executor, collectionName);

        // Act
        var result = queryable.LastOrDefault();

        // Assert
        await Assert.That(result).IsNull();
    }

    [Test]
    public async Task ElementAt_Should_Return_Item_At_Index()
    {
        // Arrange
        var collectionName = GetUniqueCollectionName();
        var products = new List<TestProduct>
        {
            new() { Name = "First", Price = 10m, Category = "Cat", InStock = true, CreatedAt = DateTime.UtcNow },
            new() { Name = "Second", Price = 20m, Category = "Cat", InStock = true, CreatedAt = DateTime.UtcNow },
            new() { Name = "Third", Price = 30m, Category = "Cat", InStock = true, CreatedAt = DateTime.UtcNow }
        };

        var collection = _engine.GetCollection<TestProduct>(collectionName);
        foreach (var product in products) collection.Insert(product);

        var queryable = new Queryable<TestProduct>(_executor, collectionName);

        // Act
        var result = queryable.ElementAt(1);

        // Assert
        await Assert.That(result).IsNotNull();
    }

    [Test]
    public async Task ElementAtOrDefault_Should_Return_Default_When_OutOfRange()
    {
        // Arrange
        var collectionName = GetUniqueCollectionName();
        var products = new List<TestProduct>
        {
            new() { Name = "Only", Price = 10m, Category = "Cat", InStock = true, CreatedAt = DateTime.UtcNow }
        };

        var collection = _engine.GetCollection<TestProduct>(collectionName);
        foreach (var product in products) collection.Insert(product);

        var queryable = new Queryable<TestProduct>(_executor, collectionName);

        // Act
        var result = queryable.ElementAtOrDefault(100);

        // Assert
        await Assert.That(result).IsNull();
    }

    [Test]
    public async Task All_Should_Return_True_When_All_Match()
    {
        // Arrange
        var collectionName = GetUniqueCollectionName();
        var products = new List<TestProduct>
        {
            new() { Name = "P1", Price = 100m, Category = "Cat", InStock = true, CreatedAt = DateTime.UtcNow },
            new() { Name = "P2", Price = 200m, Category = "Cat", InStock = true, CreatedAt = DateTime.UtcNow },
            new() { Name = "P3", Price = 300m, Category = "Cat", InStock = true, CreatedAt = DateTime.UtcNow }
        };

        var collection = _engine.GetCollection<TestProduct>(collectionName);
        foreach (var product in products) collection.Insert(product);

        var queryable = new Queryable<TestProduct>(_executor, collectionName);

        // Act
        var result = queryable.All(p => p.Price > 50);

        // Assert
        await Assert.That(result).IsTrue();
    }

    [Test]
    public async Task All_Should_Return_False_When_Not_All_Match()
    {
        // Arrange
        var collectionName = GetUniqueCollectionName();
        var products = new List<TestProduct>
        {
            new() { Name = "P1", Price = 100m, Category = "Cat", InStock = true, CreatedAt = DateTime.UtcNow },
            new() { Name = "P2", Price = 20m, Category = "Cat", InStock = true, CreatedAt = DateTime.UtcNow },
            new() { Name = "P3", Price = 300m, Category = "Cat", InStock = true, CreatedAt = DateTime.UtcNow }
        };

        var collection = _engine.GetCollection<TestProduct>(collectionName);
        foreach (var product in products) collection.Insert(product);

        var queryable = new Queryable<TestProduct>(_executor, collectionName);

        // Act
        var result = queryable.All(p => p.Price > 50);

        // Assert
        await Assert.That(result).IsFalse();
    }

    #endregion

    #region Distinct Tests

    [Test]
    public async Task Distinct_Should_Remove_Duplicates()
    {
        // Arrange
        var collectionName = GetUniqueCollectionName();
        var products = new List<TestProduct>
        {
            new() { Name = "P1", Price = 100m, Category = "Electronics", InStock = true, CreatedAt = DateTime.UtcNow },
            new() { Name = "P2", Price = 200m, Category = "Electronics", InStock = true, CreatedAt = DateTime.UtcNow },
            new() { Name = "P3", Price = 300m, Category = "Food", InStock = true, CreatedAt = DateTime.UtcNow }
        };

        var collection = _engine.GetCollection<TestProduct>(collectionName);
        foreach (var product in products) collection.Insert(product);

        var queryable = new Queryable<TestProduct>(_executor, collectionName);

        // Act
        var categories = queryable.Select(p => p.Category).Distinct().ToList();

        // Assert
        await Assert.That(categories.Count).IsEqualTo(2);
        await Assert.That(categories).Contains("Electronics");
        await Assert.That(categories).Contains("Food");
    }

    #endregion

    #region Aggregation Tests

    [Test]
    public async Task Sum_Should_Calculate_Sum()
    {
        // Arrange
        var collectionName = GetUniqueCollectionName();
        var products = new List<TestProduct>
        {
            new() { Name = "P1", Price = 100m, Category = "Cat", InStock = true, CreatedAt = DateTime.UtcNow },
            new() { Name = "P2", Price = 200m, Category = "Cat", InStock = true, CreatedAt = DateTime.UtcNow },
            new() { Name = "P3", Price = 300m, Category = "Cat", InStock = true, CreatedAt = DateTime.UtcNow }
        };

        var collection = _engine.GetCollection<TestProduct>(collectionName);
        foreach (var product in products) collection.Insert(product);

        var queryable = new Queryable<TestProduct>(_executor, collectionName);

        // Act
        var sum = queryable.Sum(p => p.Price);

        // Assert
        await Assert.That(sum).IsEqualTo(600m);
    }

    [Test]
    public async Task Average_Should_Calculate_Average()
    {
        // Arrange
        var collectionName = GetUniqueCollectionName();
        var products = new List<TestProduct>
        {
            new() { Name = "P1", Price = 100m, Category = "Cat", InStock = true, CreatedAt = DateTime.UtcNow },
            new() { Name = "P2", Price = 200m, Category = "Cat", InStock = true, CreatedAt = DateTime.UtcNow },
            new() { Name = "P3", Price = 300m, Category = "Cat", InStock = true, CreatedAt = DateTime.UtcNow }
        };

        var collection = _engine.GetCollection<TestProduct>(collectionName);
        foreach (var product in products) collection.Insert(product);

        var queryable = new Queryable<TestProduct>(_executor, collectionName);

        // Act
        var avg = queryable.Average(p => p.Price);

        // Assert
        await Assert.That(avg).IsEqualTo(200m);
    }

    [Test]
    public async Task Min_Should_Find_Minimum()
    {
        // Arrange
        var collectionName = GetUniqueCollectionName();
        var products = new List<TestProduct>
        {
            new() { Name = "P1", Price = 100m, Category = "Cat", InStock = true, CreatedAt = DateTime.UtcNow },
            new() { Name = "P2", Price = 50m, Category = "Cat", InStock = true, CreatedAt = DateTime.UtcNow },
            new() { Name = "P3", Price = 300m, Category = "Cat", InStock = true, CreatedAt = DateTime.UtcNow }
        };

        var collection = _engine.GetCollection<TestProduct>(collectionName);
        foreach (var product in products) collection.Insert(product);

        var queryable = new Queryable<TestProduct>(_executor, collectionName);

        // Act
        var min = queryable.Min(p => p.Price);

        // Assert
        await Assert.That(min).IsEqualTo(50m);
    }

    [Test]
    public async Task Max_Should_Find_Maximum()
    {
        // Arrange
        var collectionName = GetUniqueCollectionName();
        var products = new List<TestProduct>
        {
            new() { Name = "P1", Price = 100m, Category = "Cat", InStock = true, CreatedAt = DateTime.UtcNow },
            new() { Name = "P2", Price = 50m, Category = "Cat", InStock = true, CreatedAt = DateTime.UtcNow },
            new() { Name = "P3", Price = 300m, Category = "Cat", InStock = true, CreatedAt = DateTime.UtcNow }
        };

        var collection = _engine.GetCollection<TestProduct>(collectionName);
        foreach (var product in products) collection.Insert(product);

        var queryable = new Queryable<TestProduct>(_executor, collectionName);

        // Act
        var max = queryable.Max(p => p.Price);

        // Assert
        await Assert.That(max).IsEqualTo(300m);
    }

    #endregion

    #region Complex Query Tests

    [Test]
    public async Task Complex_Query_With_Where_Select_OrderBy()
    {
        // Arrange
        var collectionName = GetUniqueCollectionName();
        var products = new List<TestProduct>
        {
            new() { Name = "Laptop", Price = 1000m, Category = "Electronics", InStock = true, CreatedAt = DateTime.UtcNow },
            new() { Name = "Mouse", Price = 50m, Category = "Electronics", InStock = true, CreatedAt = DateTime.UtcNow },
            new() { Name = "Keyboard", Price = 100m, Category = "Electronics", InStock = false, CreatedAt = DateTime.UtcNow },
            new() { Name = "Chair", Price = 200m, Category = "Furniture", InStock = true, CreatedAt = DateTime.UtcNow }
        };

        var collection = _engine.GetCollection<TestProduct>(collectionName);
        foreach (var product in products) collection.Insert(product);

        var queryable = new Queryable<TestProduct>(_executor, collectionName);

        // Act - Filter electronics in stock, project to names, order alphabetically
        var results = queryable
            .Where(p => p.Category == "Electronics" && p.InStock)
            .OrderBy(p => p.Name)
            .Select(p => p.Name)
            .ToList();

        // Assert
        await Assert.That(results.Count).IsEqualTo(2);
        await Assert.That(results[0]).IsEqualTo("Laptop");
        await Assert.That(results[1]).IsEqualTo("Mouse");
    }

    [Test]
    public async Task Empty_Result_Should_Return_Empty_List()
    {
        // Arrange
        var collectionName = GetUniqueCollectionName();
        var products = new List<TestProduct>
        {
            new() { Name = "P1", Price = 10m, Category = "Cat", InStock = true, CreatedAt = DateTime.UtcNow }
        };

        var collection = _engine.GetCollection<TestProduct>(collectionName);
        foreach (var product in products) collection.Insert(product);

        var queryable = new Queryable<TestProduct>(_executor, collectionName);

        // Act - Filter with condition that matches nothing
        var results = queryable.Where(p => p.Price > 1000000).ToList();

        // Assert
        await Assert.That(results.Count).IsEqualTo(0);
    }

    #endregion
}
