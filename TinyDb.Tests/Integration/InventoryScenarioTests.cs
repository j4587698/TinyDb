using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using TinyDb.Attributes;
using TinyDb.Bson;
using TinyDb.Core;
using TinyDb.Index;
using TinyDb.Tests.TestEntities;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Integration;

[Entity]
public class StockItem
{
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();
    public string ProductId { get; set; } = "";
    public int Quantity { get; set; }
    public string Location { get; set; } = "Warehouse A";
}

[NotInParallel]
public class InventoryScenarioTests
{
    private string _databasePath = null!;
    private TinyDbOptions _options = null!;

    [Before(Test)]
    public void Setup()
    {
        _databasePath = Path.Combine(Path.GetTempPath(), $"tinydb_inventory_{Guid.NewGuid():N}.db");
        if (File.Exists(_databasePath))
            File.Delete(_databasePath);

        _options = new TinyDbOptions
        {
            DatabaseName = "InventoryDb",
            PageSize = 4096,
            CacheSize = 512,
            EnableJournaling = true
        };
    }

    [After(Test)]
    public void Cleanup()
    {
        if (File.Exists(_databasePath))
            File.Delete(_databasePath);
    }

    [Test]
    public async Task InventoryManagement_EndToEnd_Scenario()
    {
        using (var engine = new TinyDbEngine(_databasePath, _options))
        {
            var products = engine.GetCollection<Product>();
            var stock = engine.GetCollection<StockItem>();
            var orders = engine.GetCollection<Order>();

            // 1. Setup Initial Data
            var prod1 = new Product { Name = "Gaming Laptop", Price = 1500m, InStock = true };
            var prod2 = new Product { Name = "Mechanical Keyboard", Price = 100m, InStock = true };
            
            products.Insert(prod1);
            products.Insert(prod2);

            // Ensure Indexes before insert to ensure they are populated
            engine.EnsureIndex("StockItem", "ProductId", "idx_stock_pid", unique: true);

            var stock1 = new StockItem { ProductId = prod1.Id.ToString(), Quantity = 5 };
            var stock2 = new StockItem { ProductId = prod2.Id.ToString(), Quantity = 2 };

            stock.Insert(stock1);
            stock.Insert(stock2);

            // 2. Successful Purchase Transaction
            using (var transaction = engine.BeginTransaction())
            {
                // Find stock
                var currentStock = stock.Find(s => s.ProductId == prod2.Id.ToString()).FirstOrDefault();
                await Assert.That(currentStock).IsNotNull();
                await Assert.That(currentStock!.Quantity).IsGreaterThan(0);

                // Decrement stock
                currentStock!.Quantity -= 1;
                stock.Update(currentStock);

                // Create Order
                var order = new Order 
                { 
                    OrderNumber = "ORD-001", 
                    TotalAmount = prod2.Price, 
                    IsCompleted = false,
                    OrderDate = DateTime.UtcNow 
                };
                orders.Insert(order);

                transaction.Commit();
            }

            // Verify Stock Decremented
            var updatedStock2 = stock.Find(s => s.ProductId == prod2.Id.ToString()).First();
            await Assert.That(updatedStock2.Quantity).IsEqualTo(1);
            
            // Verify Order Created
            var storedOrder = orders.Find(o => o.OrderNumber == "ORD-001").FirstOrDefault();
            await Assert.That(storedOrder).IsNotNull();

            // 3. Purchase Loop to Deplete Stock
            using (var transaction = engine.BeginTransaction())
            {
                 var currentStock = stock.Find(s => s.ProductId == prod2.Id.ToString()).First();
                 currentStock.Quantity -= 1;
                 stock.Update(currentStock);
                 
                 var order = new Order { OrderNumber = "ORD-002", TotalAmount = prod2.Price };
                 orders.Insert(order);
                 
                 transaction.Commit();
            }
            
            // Verify stock is 0
            updatedStock2 = stock.Find(s => s.ProductId == prod2.Id.ToString()).First();
            await Assert.That(updatedStock2.Quantity).IsEqualTo(0);

            // 4. Failed Transaction (Out of Stock) with Rollback
            using (var transaction = engine.BeginTransaction())
            {
                try
                {
                    var currentStock = stock.Find(s => s.ProductId == prod2.Id.ToString()).First();
                    if (currentStock.Quantity <= 0)
                    {
                        throw new InvalidOperationException("Out of stock");
                    }
                    
                    // Should not reach here
                    currentStock.Quantity -= 1;
                    stock.Update(currentStock);
                    
                    var order = new Order { OrderNumber = "ORD-003-FAILED" };
                    orders.Insert(order);
                    
                    transaction.Commit();
                }
                catch (InvalidOperationException)
                {
                    // Expected
                }
            }

            // Verify Stock still 0 (not -1)
            updatedStock2 = stock.Find(s => s.ProductId == prod2.Id.ToString()).First();
            await Assert.That(updatedStock2.Quantity).IsEqualTo(0);

            // Verify Failed Order not exists
            var failedOrder = orders.Find(o => o.OrderNumber == "ORD-003-FAILED").FirstOrDefault();
            await Assert.That(failedOrder).IsNull();

            // 5. Update Product Price (Update)
            prod1.Price = 1400m;
            products.Update(prod1);

            var updatedProd1 = products.FindById(prod1.Id);
            await Assert.That(updatedProd1!.Price).IsEqualTo(1400m);

            // 6. Delete Product and Stock (Delete)
            products.Delete(prod1.Id);
            stock.Delete(stock1.Id);

            var deletedProd = products.FindById(prod1.Id);
            await Assert.That(deletedProd).IsNull();
            
            var deletedStock = stock.FindById(stock1.Id);
            await Assert.That(deletedStock).IsNull();
        }
    }
}