# SimpleDb 快速开始指南

## 概述

SimpleDb 是一个高性能的嵌入式文档数据库，提供类似 MongoDB 的使用体验。本指南将帮助您快速上手 SimpleDb，从安装到基本操作，让您在几分钟内开始使用 SimpleDb。

## 系统要求

- **.NET 9.0 或更高版本**
- **操作系统**：Windows 10+、macOS 10.15+、Linux（主流发行版）
- **内存**：最少 512MB，推荐 2GB+
- **磁盘空间**：最少 100MB 可用空间

## 安装

### 通过 NuGet 安装

```bash
dotnet add package SimpleDb
```

### 手动安装

1. 下载 SimpleDb 二进制包
2. 解压到项目目录
3. 在项目中引用 SimpleDb.dll

## 基本使用

### 1. 创建数据库实例

```csharp
using SimpleDb.Core;
using SimpleDb.Collections;
using SimpleDb.Attributes;
using SimpleDb.Bson;

// 基本配置
var options = new SimpleDbOptions
{
    DatabaseName = "MyApp",
    PageSize = 8192,
    CacheSize = 1000,
    EnableJournaling = true,
    WriteConcern = WriteConcern.Synced
};

// 创建数据库引擎
using var engine = new SimpleDbEngine("myapp.db", options);
```

### 2. 定义实体类

```csharp
[Entity("users")]
public class User
{
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();

    [Index(Unique = true)]
    public string Email { get; set; } = "";

    [Index(Priority = 1)]
    public string Name { get; set; } = "";

    public int Age { get; set; }
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;

    // 忽略此属性不存储
    [BsonIgnore]
    public string TempInfo { get; set; } = "";
}

[Entity("products")]
public class Product
{
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();

    public string Name { get; set; } = "";
    public decimal Price { get; set; }
    public string[] Tags { get; set; } = Array.Empty<string>();
    public bool IsActive { get; set; } = true;
}
```

### 3. 基本 CRUD 操作

#### 插入文档

```csharp
// 获取集合
var users = engine.GetCollection<User>("users");

// 插入单个文档
var user = new User
{
    Name = "张三",
    Email = "zhangsan@example.com",
    Age = 25
};

var id = users.Insert(user);
Console.WriteLine($"插入用户，ID: {id}");

// 批量插入
var newUsers = new List<User>
{
    new User { Name = "李四", Email = "lisi@example.com", Age = 30 },
    new User { Name = "王五", Email = "wangwu@example.com", Age = 28 }
};

users.Insert(newUsers);
Console.WriteLine($"批量插入 {newUsers.Count} 个用户");
```

#### 查询文档

```csharp
// 查询所有用户
var allUsers = users.FindAll().ToList();
Console.WriteLine($"总用户数: {allUsers.Count}");

// 按条件查询
var youngUsers = users.Find(u => u.Age < 30).ToList();
Console.WriteLine($"30岁以下用户数: {youngUsers.Count}");

// 按 ID 查询
var user = users.FindById(id);
if (user != null)
{
    Console.WriteLine($"找到用户: {user.Name}");
}

// 复杂查询
var queryUsers = users.Find(u =>
    u.Name.Contains("张") &&
    u.Age >= 20 &&
    u.Age <= 40
).OrderBy(u => u.Name).ToList();

// 使用 LINQ 查询
var linqQuery = from u in users.AsQueryable()
               where u.Age >= 25
               orderby u.CreatedAt descending
               select u;

var result = linqQuery.Take(10).ToList();
```

#### 更新文档

```csharp
// 更新单个文档
user.Age = 26;
users.Update(user);

// 批量更新
users.UpdateMany(u => u.Age < 25, u => new User
{
    Age = u.Age + 1,
    Name = u.Name
});

// 使用 Update 方法更新特定字段
users.UpdateMany(u => u.Email.Contains("@example.com"),
    Builders<User>.Set(u => u.Name, "已更新"));
```

#### 删除文档

```csharp
// 删除单个文档
users.Delete(user.Id);

// 按条件删除
var deletedCount = users.DeleteMany(u => u.Age > 65);
Console.WriteLine($"删除了 {deletedCount} 个用户");

// 清空集合
users.DeleteAll();
```

### 4. 索引操作

```csharp
// 创建单字段索引
users.EnsureIndex(u => u.Email, true); // 唯一索引

// 创建复合索引
users.EnsureIndex(u => new { u.Name, u.Age });

// 创建稀疏索引
users.EnsureIndex(u => u.Tags, sparse: true);

// 查看索引信息
var indexInfo = users.GetIndexes();
foreach (var index in indexInfo)
{
    Console.WriteLine($"索引: {index.Name}, 字段: {string.Join(", ", index.Fields)}");
}
```

### 5. 事务操作

```csharp
// 开始事务
using var transaction = engine.BeginTransaction();

try
{
    var users = engine.GetCollection<User>("users");
    var products = engine.GetCollection<Product>("products");

    // 在事务中执行操作
    var user = new User { Name = "事务用户", Email = "tx@example.com", Age = 35 };
    users.Insert(user);

    var product = new Product { Name = "事务商品", Price = 99.99m };
    products.Insert(product);

    // 创建保存点
    var savepoint = transaction.CreateSavepoint("before_update");

    // 更多操作
    user.Age = 36;
    users.Update(user);

    // 如果需要，可以回滚到保存点
    // transaction.RollbackToSavepoint(savepoint);

    // 提交事务
    transaction.Commit();
    Console.WriteLine("事务提交成功");
}
catch (Exception ex)
{
    // 回滚事务
    transaction.Rollback();
    Console.WriteLine($"事务回滚: {ex.Message}");
}
```

### 6. 聚合操作

```csharp
// 简单聚合
var userCount = users.Count(u => u.Age >= 18);
var avgAge = users.Average(u => u.Age);
var maxAge = users.Max(u => u.Age);

// 分组聚合
var ageGroups = users.Aggregate()
    .Group(u => u.Age / 10 * 10, g => new
    {
        AgeRange = $"{g.Key}-{g.Key + 9}",
        Count = g.Count(),
        AvgAge = g.Average(u => u.Age)
    })
    .OrderBy(g => g.AgeRange)
    .ToList();

foreach (var group in ageGroups)
{
    Console.WriteLine($"{group.AgeRange}岁: {group.Count}人, 平均年龄: {group.AvgAge:F1}岁");
}
```

## 高级功能

### 1. 性能优化配置

```csharp
var highPerformanceOptions = new SimpleDbOptions
{
    DatabaseName = "HighPerformanceDB",
    PageSize = 16384,           // 更大的页面大小
    CacheSize = 10000,          // 更大的缓存
    EnableJournaling = false,   // 禁用日志以获得更高性能
    WriteConcern = WriteConcern.None, // 异步写入
    BackgroundFlushInterval = TimeSpan.FromMilliseconds(50),
    JournalFlushDelay = TimeSpan.Zero
};
```

### 2. 数据库管理操作

```csharp
// 获取数据库统计信息
var stats = engine.GetStatistics();
Console.WriteLine($"数据库大小: {stats.FileSize} 字节");
Console.WriteLine($"使用页面: {stats.UsedPages}/{stats.TotalPages}");

// 列出所有集合
var collections = engine.GetCollectionNames();
foreach (var collection in collections)
{
    Console.WriteLine($"集合: {collection}");
}

// 删除集合
engine.DropCollection("old_collection");

// 压缩数据库
engine.Compact();

// 备份数据库
engine.Backup("backup.db");

// 刷写缓存到磁盘
engine.Flush();
```

### 3. 并发控制

```csharp
// 多线程安全使用
Parallel.For(0, 10, i =>
{
    var users = engine.GetCollection<User>("users");
    var user = new User
    {
        Name = $"用户{i}",
        Email = $"user{i}@example.com",
        Age = 20 + i
    };

    users.Insert(user);
});

// 等待所有操作完成
engine.Flush();
```

### 4. 数据导入导出

```csharp
// 导出为 JSON
var users = engine.GetCollection<User>("users");
var userData = users.FindAll().ToList();
var json = System.Text.Json.JsonSerializer.Serialize(userData, new System.Text.Json.JsonSerializerOptions
{
    WriteIndented = true
});
File.WriteAllText("users.json", json);

// 从 JSON 导入
var importData = System.Text.Json.JsonSerializer.Deserialize<List<User>>(json);
if (importData != null)
{
    users.Insert(importData);
}
```

## 最佳实践

### 1. 实体设计

```csharp
[Entity("orders")]
public class Order
{
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();

    [Index]
    public string OrderNumber { get; set; } = "";

    [Index]
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;

    public decimal TotalAmount { get; set; }

    // 嵌套文档
    public CustomerInfo Customer { get; set; } = new();

    // 数组字段
    public List<OrderItem> Items { get; set; } = new();

    // 计算属性（不存储）
    [BsonIgnore]
    public string Status => TotalAmount > 1000 ? "大额订单" : "普通订单";
}

public class CustomerInfo
{
    public string Name { get; set; } = "";
    public string Email { get; set; } = "";
    public string Phone { get; set; } = "";
}

public class OrderItem
{
    public string ProductId { get; set; } = "";
    public string ProductName { get; set; } = "";
    public int Quantity { get; set; }
    public decimal UnitPrice { get; set; }
}
```

### 2. 性能优化建议

1. **合理设置页面大小**：
   - 小文档：4KB - 8KB
   - 大文档：16KB - 32KB

2. **优化缓存配置**：
   ```csharp
   // 根据可用内存设置缓存
   var availableMemory = GC.GetTotalMemory(false) / 1024 / 1024; // MB
   var cacheSize = Math.Max(100, availableMemory / 10);
   options.CacheSize = cacheSize;
   ```

3. **批量操作优化**：
   ```csharp
   // 使用批量插入而非循环单条插入
   var documents = new List<User>();
   for (int i = 0; i < 1000; i++)
   {
       documents.Add(new User { /* ... */ });
   }
   users.Insert(documents); // 好
   // 而不是：
   // for (int i = 0; i < 1000; i++)
   // {
   //     users.Insert(new User { /* ... */ }); // 差
   // }
   ```

4. **索引策略**：
   - 为常用查询字段创建索引
   - 避免过多索引影响写入性能
   - 使用复合索引优化多字段查询

### 3. 错误处理

```csharp
try
{
    var users = engine.GetCollection<User>("users");
    var user = users.FindById(id);

    if (user == null)
    {
        Console.WriteLine("用户不存在");
        return;
    }

    // 执行操作...
}
catch (StorageException ex)
{
    Console.WriteLine($"存储错误: {ex.Message}");
    // 处理存储相关错误
}
catch (TransactionException ex)
{
    Console.WriteLine($"事务错误: {ex.Message}");
    // 处理事务相关错误
}
catch (Exception ex)
{
    Console.WriteLine($"未知错误: {ex.Message}");
    // 处理其他错误
}
```

### 4. 资源管理

```csharp
// 使用 using 语句确保资源释放
using var engine = new SimpleDbEngine("myapp.db", options);
{
    // 使用数据库...
    // 引擎会在 using 块结束时自动释放
}

// 或者手动释放
var engine = new SimpleDbEngine("myapp.db", options);
try
{
    // 使用数据库...
}
finally
{
    engine?.Dispose();
}
```

## 故障排除

### 常见问题

1. **数据库文件损坏**
   ```csharp
   try
   {
       var engine = new SimpleDbEngine("corrupted.db", options);
   }
   catch (StorageException ex)
   {
       Console.WriteLine("数据库文件可能损坏，尝试恢复...");
       // 从备份恢复或重建数据库
   }
   ```

2. **内存不足**
   ```csharp
   // 减少缓存大小
   options.CacheSize = 100;
   options.PageSize = 4096;
   ```

3. **性能问题**
   ```csharp
   // 检查统计信息
   var stats = engine.GetStatistics();
   Console.WriteLine($"缓存命中率: {stats.CacheHitRate:P1}");

   // 优化配置
   options.BackgroundFlushInterval = TimeSpan.FromMilliseconds(10);
   options.WriteConcern = WriteConcern.Journaled;
   ```

### 调试技巧

```csharp
// 启用详细日志
options.EnableLogging = true;
options.LogLevel = LogLevel.Debug;

// 监控性能
var stopwatch = Stopwatch.StartNew();
var result = users.Find(u => u.Age > 18).ToList();
stopwatch.Stop();
Console.WriteLine($"查询耗时: {stopwatch.ElapsedMilliseconds}ms，返回 {result.Count} 条记录");
```

## 示例项目

### 完整的博客系统示例

```csharp
// 定义实体
[Entity("posts")]
public class Post
{
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();

    [Index]
    public string Title { get; set; } = "";

    [Index]
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;

    public string Content { get; set; } = "";
    public string Author { get; set; } = "";
    public string[] Tags { get; set; } = Array.Empty<string>();
    public int ViewCount { get; set; }
    public bool IsPublished { get; set; }
}

[Entity("comments")]
public class Comment
{
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();

    [Index]
    public ObjectId PostId { get; set; }

    public string Author { get; set; } = "";
    public string Content { get; set; } = "";
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    public bool IsApproved { get; set; }
}

// 使用示例
public class BlogService
{
    private readonly SimpleDbEngine _engine;
    private readonly ILiteCollection<Post> _posts;
    private readonly ILiteCollection<Comment> _comments;

    public BlogService(string dbPath)
    {
        var options = new SimpleDbOptions
        {
            DatabaseName = "BlogDB",
            EnableJournaling = true,
            WriteConcern = WriteConcern.Synced
        };

        _engine = new SimpleDbEngine(dbPath, options);
        _posts = _engine.GetCollection<Post>("posts");
        _comments = _engine.GetCollection<Comment>("comments");

        // 创建索引
        _posts.EnsureIndex(p => p.Title);
        _posts.EnsureIndex(p => p.CreatedAt);
        _comments.EnsureIndex(c => c.PostId);
    }

    public Post CreatePost(string title, string content, string author, string[] tags)
    {
        var post = new Post
        {
            Title = title,
            Content = content,
            Author = author,
            Tags = tags,
            IsPublished = true
        };

        _posts.Insert(post);
        return post;
    }

    public IEnumerable<Post> GetPublishedPosts(int limit = 10, int skip = 0)
    {
        return _posts.Find(p => p.IsPublished)
                   .OrderByDescending(p => p.CreatedAt)
                   .Skip(skip)
                   .Take(limit)
                   .ToList();
    }

    public IEnumerable<Post> SearchPosts(string keyword)
    {
        return _posts.Find(p =>
                   p.IsPublished &&
                   (p.Title.Contains(keyword) || p.Content.Contains(keyword)))
               .OrderByDescending(p => p.CreatedAt)
               .ToList();
    }

    public Comment AddComment(ObjectId postId, string author, string content)
    {
        var comment = new Comment
        {
            PostId = postId,
            Author = author,
            Content = content,
            IsApproved = true
        };

        _comments.Insert(comment);

        // 更新文章评论数（这里简化处理）
        var post = _posts.FindById(postId);
        if (post != null)
        {
            // 实际实现中可能需要添加评论数字段
        }

        return comment;
    }

    public IEnumerable<Comment> GetComments(ObjectId postId)
    {
        return _comments.Find(c => c.PostId && c.IsApproved)
                       .OrderBy(c => c.CreatedAt)
                       .ToList();
    }

    public void Dispose()
    {
        _engine?.Dispose();
    }
}

// 使用服务
var blogService = new BlogService("blog.db");

// 创建文章
var post = blogService.CreatePost(
    "SimpleDb 入门指南",
    "SimpleDb 是一个高性能的嵌入式数据库...",
    "作者名",
    new[] { "数据库", "SimpleDb", "教程" }
);

// 添加评论
var comment = blogService.AddComment(
    post.Id,
    "读者",
    "很好的文章，学到了很多！"
);

// 查询文章
var recentPosts = blogService.GetPublishedPosts(5);
var searchResults = blogService.SearchPosts("SimpleDb");
```

## 下一步

现在您已经掌握了 SimpleDb 的基本用法，可以继续学习：

1. **[API 参考文档](api-reference.md)** - 详细的 API 文档
2. **[性能优化指南](performance.md)** - 深入了解性能优化技巧
3. **[AOT 部署指南](aot-deployment.md)** - 学习如何部署 AOT 应用
4. **[架构设计文档](architecture.md)** - 了解 SimpleDb 的内部架构

## 获取帮助

- **GitHub 仓库**：[SimpleDb GitHub](https://github.com/your-repo/SimpleDb)
- **文档网站**：[SimpleDb Docs](https://docs.simpledb.com)
- **社区论坛**：[SimpleDb Community](https://community.simpledb.com)
- **问题报告**：[Issues](https://github.com/your-repo/SimpleDb/issues)

---

恭喜！您已经完成了 SimpleDb 快速开始指南。现在可以开始构建您的应用程序了！