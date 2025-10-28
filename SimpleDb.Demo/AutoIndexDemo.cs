using System;
using System.Linq;
using SimpleDb.Core;
using SimpleDb.Collections;
using SimpleDb.Attributes;
using SimpleDb.Bson;
using SimpleDb.Index;

namespace SimpleDb.Demo;

/// <summary>
/// 自动索引功能演示
/// </summary>
public static class AutoIndexDemo
{
    /// <summary>
    /// 运行自动索引演示
    /// </summary>
    public static async Task RunAsync()
    {
        Console.WriteLine("=== SimpleDb 自动索引功能演示 ===");
        Console.WriteLine();

        // 创建临时数据库
        var testDbFile = "autoindex_demo.db";
        if (System.IO.File.Exists(testDbFile))
        {
            System.IO.File.Delete(testDbFile);
        }

        var options = new SimpleDbOptions
        {
            DatabaseName = "AutoIndexDemoDb",
            PageSize = 8192,
            CacheSize = 1000
        };

        using var engine = new SimpleDbEngine(testDbFile, options);
        Console.WriteLine("✅ 数据库引擎创建成功！");

        // 演示基本的自动索引创建
        await BasicAutoIndexDemo(engine);

        // 演示唯一索引
        await UniqueIndexDemo(engine);

        // 演示复合索引
        await CompositeIndexDemo(engine);

        // 演示索引优先级
        await IndexPriorityDemo(engine);

        // 演示索引信息查询
        IndexInfoQueryDemo(engine);

        Console.WriteLine("\n=== 自动索引演示完成！ ===");
        Console.WriteLine($"数据库统计: {engine.GetStatistics()}");

        // 清理
        if (System.IO.File.Exists(testDbFile))
        {
            System.IO.File.Delete(testDbFile);
        }
    }

    /// <summary>
    /// 基本自动索引演示
    /// </summary>
    private static async Task BasicAutoIndexDemo(SimpleDbEngine engine)
    {
        Console.WriteLine("--- 基本自动索引演示 ---");

        var users = engine.GetCollection<Employee>("employees");

        // 插入一些数据，自动索引会在集合创建时自动生成
        var employees = new[]
        {
            new Employee
            {
                Name = "张三",
                Email = "zhangsan@company.com",
                Department = "研发部",
                Salary = 8000,
                HireDate = DateTime.Now.AddDays(-100)
            },
            new Employee
            {
                Name = "李四",
                Email = "lisi@company.com",
                Department = "销售部",
                Salary = 6000,
                HireDate = DateTime.Now.AddDays(-50)
            },
            new Employee
            {
                Name = "王五",
                Email = "wangwu@company.com",
                Department = "研发部",
                Salary = 7500,
                HireDate = DateTime.Now.AddDays(-25)
            }
        };

        foreach (var emp in employees)
        {
            users.Insert(emp);
        }

        Console.WriteLine($"✅ 插入了 {employees.Length} 个员工记录，自动索引已创建");

        // 显示所有自动创建的索引
        var indexManager = users.GetIndexManager();
        var statistics = indexManager.GetAllStatistics();

        Console.WriteLine("📊 自动创建的索引:");
        foreach (var stat in statistics)
        {
            Console.WriteLine($"   - {stat}");
        }

        // 测试索引查询
        Console.WriteLine("\n🔍 测试索引查询:");
        var devEmployees = users.Find(e => e.Department == "研发部").ToList();
        Console.WriteLine($"   研发部员工: {devEmployees.Count} 人");

        var highSalaryEmployees = users.Find(e => e.Salary >= 7000).ToList();
        Console.WriteLine($"   高薪员工 (>=7000): {highSalaryEmployees.Count} 人");

        var recentHires = users.Find(e => e.HireDate >= DateTime.Now.AddDays(-30)).ToList();
        Console.WriteLine($"   新入职员工 (30天内): {recentHires.Count} 人");

        // 清理数据
        foreach (var emp in employees)
        {
            users.Delete(emp.Id);
        }

        Console.WriteLine();
    }

    /// <summary>
    /// 唯一索引演示
    /// </summary>
    private static async Task UniqueIndexDemo(SimpleDbEngine engine)
    {
        Console.WriteLine("--- 唯一索引演示 ---");

        var users = engine.GetCollection<AutoUser>("auto_users");

        // 插入第一个用户
        var user1 = new AutoUser
        {
            Username = "zhangsan",
            Email = "zhangsan@example.com",
            Phone = "13800138000"
        };
        users.Insert(user1);
        Console.WriteLine($"✅ 插入用户: {user1.Username} ({user1.Email})");

        // 尝试插入重复邮箱的用户
        var user2 = new AutoUser
        {
            Username = "lisi",
            Email = "zhangsan@example.com", // 重复邮箱
            Phone = "13900139000"
        };

        try
        {
            users.Insert(user2);
            Console.WriteLine($"❌ 意外成功: 插入了重复邮箱的用户");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"✅ 正确阻止重复邮箱: {ex.Message}");
        }

        // 插入不同邮箱的用户
        user2.Email = "lisi@example.com";
        users.Insert(user2);
        Console.WriteLine($"✅ 插入用户: {user2.Username} ({user2.Email})");

        // 验证唯一索引查询
        var foundUser = users.FindOne(u => u.Email == "zhangsan@example.com");
        Console.WriteLine($"🔍 查询结果: {foundUser?.Username} ({foundUser?.Email})");

        // 清理数据
        users.Delete(user1.Id);
        users.Delete(user2.Id);

        Console.WriteLine();
    }

    /// <summary>
    /// 复合索引演示
    /// </summary>
    private static async Task CompositeIndexDemo(SimpleDbEngine engine)
    {
        Console.WriteLine("--- 复合索引演示 ---");

        var orders = engine.GetCollection<Order>("orders");

        // 插入订单数据
        var orderData = new[]
        {
            new Order
            {
                OrderNumber = "ORD-001",
                CustomerId = "CUST-001",
                Status = "pending",
                Amount = 1000.50m,
                OrderDate = DateTime.Now.AddDays(-1)
            },
            new Order
            {
                OrderNumber = "ORD-002",
                CustomerId = "CUST-001",
                Status = "completed",
                Amount = 2500.75m,
                OrderDate = DateTime.Now.AddDays(-2)
            },
            new Order
            {
                OrderNumber = "ORD-003",
                CustomerId = "CUST-002",
                Status = "pending",
                Amount = 1500.00m,
                OrderDate = DateTime.Now.AddDays(-3)
            }
        };

        foreach (var order in orderData)
        {
            orders.Insert(order);
        }

        Console.WriteLine($"✅ 插入了 {orderData.Length} 个订单记录");

        // 显示复合索引信息
        var indexManager = orders.GetIndexManager();
        var compositeIndex = indexManager.GetIndex("idx_customer_status");
        if (compositeIndex != null)
        {
            Console.WriteLine($"📊 复合索引信息: {compositeIndex.GetStatistics()}");
        }

        // 测试复合索引查询
        Console.WriteLine("\n🔍 测试复合索引查询:");
        var customerOrders = orders.Find(o => o.CustomerId == "CUST-001").ToList();
        Console.WriteLine($"   客户 CUST-001 的订单: {customerOrders.Count} 个");

        var pendingOrders = orders.Find(o => o.Status == "pending").ToList();
        Console.WriteLine($"   待处理订单: {pendingOrders.Count} 个");

        var customerPendingOrders = orders.Find(o => o.CustomerId == "CUST-001" && o.Status == "pending").ToList();
        Console.WriteLine($"   客户 CUST-001 的待处理订单: {customerPendingOrders.Count} 个");

        // 清理数据
        foreach (var order in orderData)
        {
            orders.Delete(order.Id);
        }

        Console.WriteLine();
    }

    /// <summary>
    /// 索引优先级演示
    /// </summary>
    private static async Task IndexPriorityDemo(SimpleDbEngine engine)
    {
        Console.WriteLine("--- 索引优先级演示 ---");

        var products = engine.GetCollection<Product>("products");

        // 插入产品数据
        var productsData = new[]
        {
            new Product { Name = "笔记本电脑", Category = "电子产品", Price = 5000, Stock = 50 },
            new Product { Name = "智能手机", Category = "电子产品", Price = 3000, Stock = 100 },
            new Product { Name = "办公椅", Category = "办公用品", Price = 800, Stock = 25 },
            new Product { Name = "台灯", Category = "办公用品", Price = 200, Stock = 75 }
        };

        foreach (var product in productsData)
        {
            products.Insert(product);
        }

        Console.WriteLine($"✅ 插入了 {productsData.Length} 个产品记录");

        // 显示所有索引和优先级
        var indexManager = products.GetIndexManager();
        var allIndexes = IndexScanner.GetEntityIndexes(typeof(Product));

        Console.WriteLine("📊 产品实体的索引定义 (按优先级排序):");
        foreach (var index in allIndexes)
        {
            Console.WriteLine($"   {index}");
        }

        // 测试不同优先级索引的查询
        Console.WriteLine("\n🔍 测试不同优先级的索引查询:");
        var electronics = products.Find(p => p.Category == "电子产品").ToList();
        Console.WriteLine($"   电子产品: {electronics.Count} 种 (高优先级索引)");

        var expensiveProducts = products.Find(p => p.Price >= 1000).ToList();
        Console.WriteLine($"   高价产品 (>=1000): {expensiveProducts.Count} 种 (中优先级索引)");

        var lowStockProducts = products.Find(p => p.Stock < 30).ToList();
        Console.WriteLine($"   低库存产品 (<30): {lowStockProducts.Count} 种 (低优先级索引)");

        // 清理数据
        foreach (var product in productsData)
        {
            products.Delete(product.Id);
        }

        Console.WriteLine();
    }

    /// <summary>
    /// 索引信息查询演示
    /// </summary>
    private static void IndexInfoQueryDemo(SimpleDbEngine engine)
    {
        Console.WriteLine("--- 索引信息查询演示 ---");

        // 查询不同集合的索引信息
        var collections = new[] { "employees", "users", "orders", "products" };

        foreach (var collectionName in collections)
        {
            try
            {
                // 临时获取集合以触发索引创建
                switch (collectionName)
                {
                    case "employees":
                        var employees = engine.GetCollection<Employee>(collectionName);
                        var employeeIndexManager = employees.GetIndexManager();
                        Console.WriteLine($"\n📋 {collectionName} 集合索引:");
                        foreach (var stat in employeeIndexManager.GetAllStatistics())
                        {
                            Console.WriteLine($"   {stat}");
                        }
                        break;

                    case "users":
                        var users = engine.GetCollection<User>(collectionName);
                        var userIndexManager = users.GetIndexManager();
                        Console.WriteLine($"\n📋 {collectionName} 集合索引:");
                        foreach (var stat in userIndexManager.GetAllStatistics())
                        {
                            Console.WriteLine($"   {stat}");
                        }
                        break;

                    case "orders":
                        var orders = engine.GetCollection<Order>(collectionName);
                        var orderIndexManager = orders.GetIndexManager();
                        Console.WriteLine($"\n📋 {collectionName} 集合索引:");
                        foreach (var stat in orderIndexManager.GetAllStatistics())
                        {
                            Console.WriteLine($"   {stat}");
                        }
                        break;

                    case "products":
                        var products = engine.GetCollection<Product>(collectionName);
                        var productIndexManager = products.GetIndexManager();
                        Console.WriteLine($"\n📋 {collectionName} 集合索引:");
                        foreach (var stat in productIndexManager.GetAllStatistics())
                        {
                            Console.WriteLine($"   {stat}");
                        }
                        break;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ 查询 {collectionName} 集合索引时出错: {ex.Message}");
            }
        }

        // 显示索引验证结果
        Console.WriteLine("\n🔍 索引验证结果:");
        foreach (var collectionName in collections)
        {
            try
            {
                switch (collectionName)
                {
                    case "employees":
                        var employees = engine.GetCollection<Employee>(collectionName);
                        var employeeIndexManager = employees.GetIndexManager();
                        var validation = employeeIndexManager.ValidateAllIndexes();
                        Console.WriteLine($"   {collectionName}: {validation}");
                        break;

                    case "users":
                        var users = engine.GetCollection<User>(collectionName);
                        var userIndexManager = users.GetIndexManager();
                        var userValidation = userIndexManager.ValidateAllIndexes();
                        Console.WriteLine($"   {collectionName}: {userValidation}");
                        break;

                    case "orders":
                        var orders = engine.GetCollection<Order>(collectionName);
                        var orderIndexManager = orders.GetIndexManager();
                        var orderValidation = orderIndexManager.ValidateAllIndexes();
                        Console.WriteLine($"   {collectionName}: {orderValidation}");
                        break;

                    case "products":
                        var products = engine.GetCollection<Product>(collectionName);
                        var productIndexManager = products.GetIndexManager();
                        var productValidation = productIndexManager.ValidateAllIndexes();
                        Console.WriteLine($"   {collectionName}: {productValidation}");
                        break;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"   {collectionName}: 验证失败 - {ex.Message}");
            }
        }
    }
}

/// <summary>
/// 员工实体 - 基本索引
/// </summary>
[Entity("employees")]
public class Employee
{
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();

    [Index]
    public string Name { get; set; } = "";

    [Index(Unique = true)]
    public string Email { get; set; } = "";

    [Index]
    public string Department { get; set; } = "";

    [Index]
    public decimal Salary { get; set; }

    [Index]
    public DateTime HireDate { get; set; }

    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
}

/// <summary>
/// 用户实体 - 唯一索引
/// </summary>
[Entity("auto_users")]
public class AutoUser
{
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();

    [Index]
    public string Username { get; set; } = "";

    [Index(Unique = true)]
    public string Email { get; set; } = "";

    [Index(Unique = true)]
    public string Phone { get; set; } = "";

    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
}

/// <summary>
/// 订单实体 - 复合索引
/// </summary>
[Entity("orders")]
[CompositeIndex("idx_customer_status", "CustomerId", "Status")]
[CompositeIndex("idx_order_date_status", "OrderDate", "Status")]
public class Order
{
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();

    [Index(Unique = true)]
    public string OrderNumber { get; set; } = "";

    [Index]
    public string CustomerId { get; set; } = "";

    [Index]
    public string Status { get; set; } = "";

    public decimal Amount { get; set; }

    [Index]
    public DateTime OrderDate { get; set; }

    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
}

/// <summary>
/// 产品实体 - 多个不同优先级的索引
/// </summary>
[Entity("products")]
public class Product
{
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();

    [Index(Priority = 1)]
    public string Name { get; set; } = "";

    [Index(Priority = 5)]
    public string Category { get; set; } = "";

    [Index(Priority = 10)]
    public decimal Price { get; set; }

    [Index(Priority = 15)]
    public int Stock { get; set; }

    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
}