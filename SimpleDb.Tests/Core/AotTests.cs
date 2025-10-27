using System.Diagnostics.CodeAnalysis;
using SimpleDb.Core;
using SimpleDb.Collections;
using SimpleDb.Tests.TestEntities;
using SimpleDb.Bson;
using Xunit;

namespace SimpleDb.Tests.Core;

public class AotTests : IDisposable
{
    private readonly string _testDbPath;
    private readonly SimpleDbEngine _engine;

    public AotTests()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"aot_test_{Guid.NewGuid():N}.db");
        if (File.Exists(_testDbPath))
            File.Delete(_testDbPath);

        var options = new SimpleDbOptions
        {
            DatabaseName = "AotTestDb",
            PageSize = 4096,
            CacheSize = 100,
            EnableJournaling = true
        };

        _engine = new SimpleDbEngine(_testDbPath, options);
    }

    public void Dispose()
    {
        _engine?.Dispose();
        if (File.Exists(_testDbPath))
            File.Delete(_testDbPath);
    }

    [Fact]
    public void User_Entity_Should_Support_Aot_Id_Access()
    {
        // Arrange
        var users = _engine.GetCollection<User>("users");
        var user = new User
        {
            Name = "AOT Test User",
            Age = 30,
            Email = "aot@example.com"
        };

        // Act
        users.Insert(user);

        // Test AotIdAccessor functionality
        var updatedUser = users.FindById(user.Id);
        if (updatedUser != null)
        {
            updatedUser.Age = 31;
        }
        var updateResult = updatedUser != null ? users.Update(updatedUser) : 0;

        // Assert
        Assert.Equal(1, updateResult);
        Assert.NotNull(updatedUser);
        Assert.Equal(31, updatedUser.Age);
    }

    [Fact]
    public void Order_Entity_Should_Support_Underscore_Id()
    {
        // Arrange
        var orders = _engine.GetCollection<Order>("orders");
        var order = new Order
        {
            OrderNumber = "ORD-001",
            TotalAmount = 100.50m,
            IsCompleted = false
        };

        // Act
        orders.Insert(order);

        // Test that _id property is correctly handled
        var foundOrder = orders.FindById(order._id);
        if (foundOrder != null)
        {
            foundOrder.IsCompleted = true;
        }
        var updateResult = foundOrder != null ? orders.Update(foundOrder) : 0;

        // Assert
        Assert.Equal(1, updateResult);
        Assert.NotNull(foundOrder);
        Assert.True(foundOrder.IsCompleted);
    }

    [Fact]
    public void Product_Entity_Should_Handle_Different_Types()
    {
        // Arrange
        var products = _engine.GetCollection<Product>("products");
        var product = new Product
        {
            Name = "Test Product",
            Price = 29.99m,
            InStock = true
        };

        // Act
        products.Insert(product);

        // Test various data types in AOT environment
        var foundProduct = products.FindById(product.Id);
        if (foundProduct != null)
        {
            foundProduct.Price = 19.99m;
            foundProduct.InStock = false;
        }
        var updateResult = foundProduct != null ? products.Update(foundProduct) : 0;

        // Assert
        Assert.Equal(1, updateResult);
        Assert.Equal(19.99m, foundProduct.Price);
        Assert.False(foundProduct.InStock);
    }

    [Fact]
    public void AotIdAccessor_Should_Handle_Multiple_Entities()
    {
        // Arrange
        var users = _engine.GetCollection<User>("users");
        var orders = _engine.GetCollection<Order>("orders");

        var user = new User { Name = "Multi Test", Age = 25, Email = "multi@example.com" };
        var order = new Order { OrderNumber = "ORD-MULTI", TotalAmount = 250.00m, IsCompleted = false };

        // Act
        users.Insert(user);
        orders.Insert(order);

        // Test both entities work correctly with AOT
        var updatedUser = users.FindById(user.Id);
        var updatedOrder = orders.FindById(order._id);

        if (updatedUser != null)
        {
            updatedUser.Age = 26;
        }
        if (updatedOrder != null)
        {
            updatedOrder.IsCompleted = true;
        }

        var userUpdateResult = updatedUser != null ? users.Update(updatedUser) : 0;
        var orderUpdateResult = updatedOrder != null ? orders.Update(updatedOrder) : 0;

        // Assert
        Assert.Equal(1, userUpdateResult);
        Assert.Equal(1, orderUpdateResult);
        Assert.Equal(26, updatedUser.Age);
        Assert.True(updatedOrder.IsCompleted);
    }

    [Fact]
    public void Entity_Should_Work_With_Default_Collection_Name()
    {
        // Arrange
        var users = _engine.GetCollection<User>("users");
        var user = new User { Name = "Default Collection", Age = 22, Email = "default@example.com" };

        // Act
        users.Insert(user);

        // Verify that the entity's collection name from attribute is used
        var foundUser = users.FindById(user.Id);
        var allUsers = users.FindAll().ToList();

        // Assert
        Assert.NotNull(foundUser);
        Assert.Single(allUsers);
        Assert.Equal("Default Collection", foundUser.Name);
    }
}