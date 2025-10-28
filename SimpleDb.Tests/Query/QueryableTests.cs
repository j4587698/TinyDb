using System.Linq.Expressions;
using SimpleDb.Core;
using SimpleDb.Query;
using SimpleDb.Bson;
using SimpleDb.Collections;
using SimpleDb.Attributes;
using SimpleDb.Tests.TestEntities;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace SimpleDb.Tests.Query;

public class QueryableTests : IDisposable
{
    private readonly string _testDirectory;
    private readonly string _testDbPath;
    private readonly SimpleDbEngine _engine;
    private readonly QueryExecutor _executor;

    public QueryableTests()
    {
        _testDirectory = Path.Combine(Path.GetTempPath(), "SimpleDbQueryableTests", Guid.NewGuid().ToString());
        Directory.CreateDirectory(_testDirectory);
        _testDbPath = Path.Combine(_testDirectory, "test.db");

        _engine = new SimpleDbEngine(_testDbPath);
        _executor = new QueryExecutor(_engine);
    }

    public void Dispose()
    {
        _engine?.Dispose();
        if (Directory.Exists(_testDirectory))
        {
            try
            {
                Directory.Delete(_testDirectory, true);
            }
            catch
            {
                // Ignore cleanup errors
            }
        }
    }

    [Test]
    public async Task Constructor_Should_Initialize_Correctly()
    {
        // Arrange
        const string collectionName = "Products";

        // Act
        var queryable = new Queryable<TestProduct>(_executor, collectionName);

        // Assert
        await Assert.That(queryable).IsNotNull();
        await Assert.That(queryable.ElementType).IsEqualTo(typeof(TestProduct));
        await Assert.That(queryable.Expression).IsNotNull();
        await Assert.That(queryable.Provider).IsNotNull();
    }

    [Test]
    public async Task Constructor_Should_Throw_For_Null_Executor()
    {
        // Arrange
        const string collectionName = "Products";

        // Act & Assert
        await Assert.That(() => new Queryable<TestProduct>(null!, collectionName)).Throws<ArgumentNullException>();
    }

    [Test]
    public async Task Constructor_Should_Throw_For_Null_CollectionName()
    {
        // Act & Assert
        await Assert.That(() => new Queryable<TestProduct>(_executor, null!)).Throws<ArgumentNullException>();
    }

    [Test]
    public async Task Constructor_Should_Throw_For_Empty_CollectionName()
    {
        // Act & Assert
        await Assert.That(() => new Queryable<TestProduct>(_executor, "")).Throws<ArgumentNullException>();
    }

    [Test]
    public async Task GetEnumerator_Should_Return_All_Items_When_No_Filter()
    {
        // Arrange
        const string collectionName = "Products";
        var products = new List<TestProduct>
        {
            new() { Name = "Laptop", Price = 999.99m, Category = "Electronics", InStock = true, CreatedAt = DateTime.UtcNow },
            new() { Name = "Mouse", Price = 29.99m, Category = "Electronics", InStock = true, CreatedAt = DateTime.UtcNow },
            new() { Name = "Book", Price = 19.99m, Category = "Books", InStock = false, CreatedAt = DateTime.UtcNow }
        };

        var collection = _engine.GetCollection<TestProduct>(collectionName);
        foreach (var product in products)
        {
            collection.Insert(product);
        }

        var queryable = new Queryable<TestProduct>(_executor, collectionName);

        // Act
        var results = queryable.ToList();

        // Assert
        await Assert.That(results.Count).IsEqualTo(3);
        await Assert.That(results.Any(p => p.Name == "Laptop")).IsTrue();
        await Assert.That(results.Any(p => p.Name == "Mouse")).IsTrue();
        await Assert.That(results.Any(p => p.Name == "Book")).IsTrue();
    }

    [Test]
    public async Task Where_Should_Filter_Correctly()
    {
        // Arrange
        const string collectionName = "Products";
        var products = new List<TestProduct>
        {
            new() { Name = "Laptop", Price = 999.99m, Category = "Electronics", InStock = true, CreatedAt = DateTime.UtcNow },
            new() { Name = "Mouse", Price = 29.99m, Category = "Electronics", InStock = true, CreatedAt = DateTime.UtcNow },
            new() { Name = "Book", Price = 19.99m, Category = "Books", InStock = false, CreatedAt = DateTime.UtcNow }
        };

        var collection = _engine.GetCollection<TestProduct>(collectionName);
        foreach (var product in products)
        {
            collection.Insert(product);
        }

        var queryable = new Queryable<TestProduct>(_executor, collectionName);

        // Act
        var results = queryable.Where(p => p.InStock).ToList();

        // Assert
        await Assert.That(results.Count).IsEqualTo(2);
        await Assert.That(results.All(p => p.InStock)).IsTrue();
    }

    [Test]
    public async Task Where_With_Price_Filter_Should_Work()
    {
        // Arrange
        const string collectionName = "Products";
        var products = new List<TestProduct>
        {
            new() { Name = "Laptop", Price = 999.99m, Category = "Electronics", InStock = true, CreatedAt = DateTime.UtcNow },
            new() { Name = "Mouse", Price = 29.99m, Category = "Electronics", InStock = true, CreatedAt = DateTime.UtcNow },
            new() { Name = "Monitor", Price = 299.99m, Category = "Electronics", InStock = true, CreatedAt = DateTime.UtcNow },
            new() { Name = "Book", Price = 19.99m, Category = "Books", InStock = false, CreatedAt = DateTime.UtcNow }
        };

        var collection = _engine.GetCollection<TestProduct>(collectionName);
        foreach (var product in products)
        {
            collection.Insert(product);
        }

        var queryable = new Queryable<TestProduct>(_executor, collectionName);

        // Act
        var results = queryable.Where(p => p.Price > 100).ToList();

        // Assert
        await Assert.That(results.Count).IsEqualTo(2);
        await Assert.That(results.All(p => p.Price > 100)).IsTrue();
        await Assert.That(results.Any(p => p.Name == "Laptop")).IsTrue();
        await Assert.That(results.Any(p => p.Name == "Monitor")).IsTrue();
    }

    [Test]
    public async Task Where_With_Category_Filter_Should_Work()
    {
        // Arrange
        const string collectionName = "Products";
        var products = new List<TestProduct>
        {
            new() { Name = "Laptop", Price = 999.99m, Category = "Electronics", InStock = true, CreatedAt = DateTime.UtcNow },
            new() { Name = "Mouse", Price = 29.99m, Category = "Electronics", InStock = true, CreatedAt = DateTime.UtcNow },
            new() { Name = "Book", Price = 19.99m, Category = "Books", InStock = false, CreatedAt = DateTime.UtcNow },
            new() { Name = "Pen", Price = 2.99m, Category = "Stationery", InStock = true, CreatedAt = DateTime.UtcNow }
        };

        var collection = _engine.GetCollection<TestProduct>(collectionName);
        foreach (var product in products)
        {
            collection.Insert(product);
        }

        var queryable = new Queryable<TestProduct>(_executor, collectionName);

        // Act
        var results = queryable.Where(p => p.Category == "Electronics").ToList();

        // Assert
        await Assert.That(results.Count).IsEqualTo(2);
        await Assert.That(results.All(p => p.Category == "Electronics")).IsTrue();
    }

    [Test]
    public async Task Where_With_Complex_Filter_Should_Work()
    {
        // Arrange
        const string collectionName = "Products";
        var products = new List<TestProduct>
        {
            new() { Name = "Laptop", Price = 999.99m, Category = "Electronics", InStock = true, CreatedAt = DateTime.UtcNow },
            new() { Name = "Mouse", Price = 29.99m, Category = "Electronics", InStock = true, CreatedAt = DateTime.UtcNow },
            new() { Name = "Monitor", Price = 299.99m, Category = "Electronics", InStock = false, CreatedAt = DateTime.UtcNow },
            new() { Name = "Book", Price = 19.99m, Category = "Books", InStock = true, CreatedAt = DateTime.UtcNow }
        };

        var collection = _engine.GetCollection<TestProduct>(collectionName);
        foreach (var product in products)
        {
            collection.Insert(product);
        }

        var queryable = new Queryable<TestProduct>(_executor, collectionName);

        // Act - Electronics products that are in stock and cost more than 50
        var results = queryable.Where(p => p.Category == "Electronics" && p.InStock && p.Price > 50).ToList();

        // Assert
        await Assert.That(results.Count).IsEqualTo(1);
        await Assert.That(results[0].Name).IsEqualTo("Laptop");
    }

    [Test]
    public async Task Count_Should_Return_Correct_Count()
    {
        // Arrange
        const string collectionName = "Products";
        var products = new List<TestProduct>
        {
            new() { Name = "Laptop", Price = 999.99m, Category = "Electronics", InStock = true, CreatedAt = DateTime.UtcNow },
            new() { Name = "Mouse", Price = 29.99m, Category = "Electronics", InStock = true, CreatedAt = DateTime.UtcNow },
            new() { Name = "Book", Price = 19.99m, Category = "Books", InStock = false, CreatedAt = DateTime.UtcNow }
        };

        var collection = _engine.GetCollection<TestProduct>(collectionName);
        foreach (var product in products)
        {
            collection.Insert(product);
        }

        var queryable = new Queryable<TestProduct>(_executor, collectionName);

        // Act
        var totalCount = queryable.Count();
        var inStockCount = queryable.Count(p => p.InStock);

        // Assert
        await Assert.That(totalCount).IsEqualTo(3);
        await Assert.That(inStockCount).IsEqualTo(2);
    }

    [Test]
    public async Task First_Should_Return_First_Matching_Item()
    {
        // Arrange
        const string collectionName = "Products";
        var products = new List<TestProduct>
        {
            new() { Name = "Laptop", Price = 999.99m, Category = "Electronics", InStock = true, CreatedAt = DateTime.UtcNow },
            new() { Name = "Mouse", Price = 29.99m, Category = "Electronics", InStock = true, CreatedAt = DateTime.UtcNow },
            new() { Name = "Book", Price = 19.99m, Category = "Books", InStock = false, CreatedAt = DateTime.UtcNow }
        };

        var collection = _engine.GetCollection<TestProduct>(collectionName);
        foreach (var product in products)
        {
            collection.Insert(product);
        }

        var queryable = new Queryable<TestProduct>(_executor, collectionName);

        // Act
        var firstProduct = queryable.First();
        var firstElectronics = queryable.First(p => p.Category == "Electronics");

        // Assert
        await Assert.That(firstProduct).IsNotNull();
        await Assert.That(firstElectronics).IsNotNull();
        await Assert.That(firstElectronics.Category).IsEqualTo("Electronics");
    }

    [Test]
    public async Task FirstOrDefault_Should_Return_Default_When_No_Match()
    {
        // Arrange
        const string collectionName = "Products";
        var queryable = new Queryable<TestProduct>(_executor, collectionName);

        // Act
        var result = queryable.FirstOrDefault(p => p.Price > 1000);

        // Assert
        await Assert.That(result).IsNull();
    }

    [Test]
    public async Task Any_Should_Return_Correct_Result()
    {
        // Arrange
        const string collectionName = "Products";
        var products = new List<TestProduct>
        {
            new() { Name = "Laptop", Price = 999.99m, Category = "Electronics", InStock = true, CreatedAt = DateTime.UtcNow },
            new() { Name = "Mouse", Price = 29.99m, Category = "Electronics", InStock = true, CreatedAt = DateTime.UtcNow },
            new() { Name = "Book", Price = 19.99m, Category = "Books", InStock = false, CreatedAt = DateTime.UtcNow }
        };

        var collection = _engine.GetCollection<TestProduct>(collectionName);
        foreach (var product in products)
        {
            collection.Insert(product);
        }

        var queryable = new Queryable<TestProduct>(_executor, collectionName);

        // Act
        var hasItems = queryable.Any();
        var hasExpensiveItems = queryable.Any(p => p.Price > 500);
        var hasToys = queryable.Any(p => p.Category == "Toys");

        // Assert
        await Assert.That(hasItems).IsTrue();
        await Assert.That(hasExpensiveItems).IsTrue();
        await Assert.That(hasToys).IsFalse();
    }

    [Test]
    public async Task Multiple_Where_Calls_Should_Work()
    {
        // Arrange
        const string collectionName = "Products";
        var products = new List<TestProduct>
        {
            new() { Name = "Laptop", Price = 999.99m, Category = "Electronics", InStock = true, CreatedAt = DateTime.UtcNow },
            new() { Name = "Mouse", Price = 29.99m, Category = "Electronics", InStock = true, CreatedAt = DateTime.UtcNow },
            new() { Name = "Monitor", Price = 299.99m, Category = "Electronics", InStock = false, CreatedAt = DateTime.UtcNow },
            new() { Name = "Book", Price = 19.99m, Category = "Books", InStock = true, CreatedAt = DateTime.UtcNow }
        };

        var collection = _engine.GetCollection<TestProduct>(collectionName);
        foreach (var product in products)
        {
            collection.Insert(product);
        }

        var queryable = new Queryable<TestProduct>(_executor, collectionName);

        // Act - Chain multiple Where calls
        var results = queryable
            .Where(p => p.Category == "Electronics")
            .Where(p => p.InStock)
            .Where(p => p.Price > 50)
            .ToList();

        // Assert
        await Assert.That(results.Count).IsEqualTo(1);
        await Assert.That(results[0].Name).IsEqualTo("Laptop");
    }

    [Test]
    public async Task Queryable_Should_Support_LINQ_Methods()
    {
        // Arrange
        const string collectionName = "Products";
        var products = new List<TestProduct>
        {
            new() { Name = "Laptop", Price = 999.99m, Category = "Electronics", InStock = true, CreatedAt = DateTime.UtcNow },
            new() { Name = "Mouse", Price = 29.99m, Category = "Electronics", InStock = true, CreatedAt = DateTime.UtcNow },
            new() { Name = "Monitor", Price = 299.99m, Category = "Electronics", InStock = false, CreatedAt = DateTime.UtcNow },
            new() { Name = "Book", Price = 19.99m, Category = "Books", InStock = true, CreatedAt = DateTime.UtcNow },
            new() { Name = "Pen", Price = 2.99m, Category = "Stationery", InStock = true, CreatedAt = DateTime.UtcNow }
        };

        var collection = _engine.GetCollection<TestProduct>(collectionName);
        foreach (var product in products)
        {
            collection.Insert(product);
        }

        var queryable = new Queryable<TestProduct>(_executor, collectionName);

        // Act & Assert - Test various LINQ methods
        await Assert.That(queryable.Where(p => p.Price > 100).Count()).IsEqualTo(2);
        await Assert.That(queryable.Where(p => p.InStock).Any()).IsTrue();
        await Assert.That(queryable.Where(p => p.Category == "Toys").Any()).IsFalse();
        await Assert.That(queryable.Where(p => p.Category == "Electronics").FirstOrDefault()).IsNotNull();
        await Assert.That(queryable.Where(p => p.Price > 1000).FirstOrDefault()).IsNull();
    }
}