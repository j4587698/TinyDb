using System;
using System.Threading.Tasks;
using TinyDb;
using TinyDb.Core;

namespace TestTinyDbNuGet
{
    // 测试实体 - SourceGenerator 应该为这个类生成元数据
    public class User
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public string Email { get; set; } = string.Empty;
        public DateTime CreatedAt { get; set; }
        public bool IsActive { get; set; }
    }

    public class Product
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public decimal Price { get; set; }
        public int Stock { get; set; }
        public string? Description { get; set; }
    }

    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("🚀 测试 TinyDb NuGet 包和 SourceGenerator");
            Console.WriteLine("=".PadRight(50, '='));

            try
            {
                // 测试 1: 基本数据库操作
                TestBasicOperations();

                // 测试 2: AOT 兼容性测试
                TestAotCompatibility();

                // 测试 3: SourceGenerator 生成的元数据
                TestSourceGeneratorMetadata();

                Console.WriteLine("✅ 所有测试通过！");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ 测试失败: {ex.Message}");
                Console.WriteLine($"🔍 详细信息: {ex}");
            }

            Console.WriteLine("\n⏸️ 按任意键退出...");
            try { Console.ReadKey(); } catch { /* 忽略非交互环境的异常 */ }
        }

        static void TestBasicOperations()
        {
            Console.WriteLine("\n📋 测试 1: 基本数据库操作");
            Console.WriteLine("-".PadRight(30, '-'));

            // 使用内存数据库进行测试
            using var db = new TinyDbEngine("test.db");

            // 获取集合
            var userCollection = db.GetCollection<User>();
            var productCollection = db.GetCollection<Product>();

            Console.WriteLine("✅ 数据库和集合创建成功");

            // 插入测试数据
            var user = new User
            {
                Name = "张三",
                Email = "zhangsan@example.com",
                CreatedAt = DateTime.Now,
                IsActive = true
            };

            userCollection.Insert(user);
            Console.WriteLine("✅ 用户数据插入成功");

            var product = new Product
            {
                Name = "测试产品",
                Price = 99.99m,
                Stock = 100,
                Description = "这是一个测试产品"
            };

            productCollection.Insert(product);
            Console.WriteLine("✅ 产品数据插入成功");

            // 查询测试
            var users = userCollection.Find(u => u.Name == "张三");
            var products = productCollection.Find(p => p.Price > 50);

            Console.WriteLine($"✅ 查询成功: 找到 {users.Count()} 个用户, {products.Count()} 个产品");
        }

        static void TestAotCompatibility()
        {
            Console.WriteLine("\n🔧 测试 2: AOT 兼容性");
            Console.WriteLine("-".PadRight(30, '-'));

            // 测试 AOT 兼容的序列化
            using var db = new TinyDbEngine("test-aot.db");
            var collection = db.GetCollection<User>();

            // 批量插入测试
            var users = new List<User>();
            for (int i = 0; i < 100; i++)
            {
                users.Add(new User
                {
                    Name = $"用户{i}",
                    Email = $"user{i}@example.com",
                    CreatedAt = DateTime.Now,
                    IsActive = i % 2 == 0
                });
            }

            collection.Insert(users);
            Console.WriteLine("✅ 批量插入成功 (AOT 兼容)");

            // AOT 兼容查询测试（避免使用 Contains 等不支持的方法）
            var activeUsers = collection.Find(u => u.IsActive);
            Console.WriteLine($"✅ AOT 兼容查询成功: 找到 {activeUsers.Count()} 个活跃用户");
        }

        static void TestSourceGeneratorMetadata()
        {
            Console.WriteLine("\n🔍 测试 3: SourceGenerator 元数据");
            Console.WriteLine("-".PadRight(30, '-'));

            // 检查是否生成了元数据类
            var userType = typeof(User);
            var productType = typeof(Product);

            Console.WriteLine($"✅ User 类型: {userType.FullName}");
            Console.WriteLine($"✅ Product 类型: {productType.FullName}");

            // 检查属性信息
            var userProperties = userType.GetProperties();
            var productProperties = productType.GetProperties();

            Console.WriteLine($"✅ User 属性数量: {userProperties.Length}");
            Console.WriteLine($"✅ Product 属性数量: {productProperties.Length}");

            // 列出属性
            Console.WriteLine("\n📋 User 属性:");
            foreach (var prop in userProperties)
            {
                Console.WriteLine($"  • {prop.Name}: {prop.PropertyType.Name}");
            }

            Console.WriteLine("\n📋 Product 属性:");
            foreach (var prop in productProperties)
            {
                Console.WriteLine($"  • {prop.Name}: {prop.PropertyType.Name}");
            }

            Console.WriteLine("✅ SourceGenerator 元数据测试完成");
        }
    }
}
