# SimpleDb Quick Start Guide

## Overview

SimpleDb is a high-performance embedded document database that provides a MongoDB-like experience. This guide will help you get started with SimpleDb quickly, from installation to basic operations, enabling you to start using SimpleDb within minutes.

## System Requirements

- **.NET 9.0 or higher**
- **Operating System**: Windows 10+, macOS 10.15+, Linux (major distributions)
- **Memory**: Minimum 512MB, recommended 2GB+
- **Disk Space**: Minimum 100MB available space

## Installation

### Install via NuGet

```bash
dotnet add package SimpleDb
```

### Manual Installation

1. Download SimpleDb binary package
2. Extract to project directory
3. Reference SimpleDb.dll in your project

## Basic Usage

### 1. Create Database Instance

```csharp
using SimpleDb.Core;
using SimpleDb.Collections;
using SimpleDb.Attributes;
using SimpleDb.Bson;

// Basic configuration
var options = new SimpleDbOptions
{
    DatabaseName = "MyApp",
    PageSize = 8192,
    CacheSize = 1000,
    EnableJournaling = true,
    WriteConcern = WriteConcern.Synced
};

// Create database engine
using var engine = new SimpleDbEngine("myapp.db", options);
```

### 2. Define Entity Classes

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

    // Ignore this property from storage
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

### 3. Basic CRUD Operations

#### Insert Documents

```csharp
// Get collection
var users = engine.GetCollection<User>("users");

// Insert single document
var user = new User
{
    Name = "John Doe",
    Email = "john@example.com",
    Age = 25
};

var id = users.Insert(user);
Console.WriteLine($"Inserted user with ID: {id}");

// Batch insert
var newUsers = new List<User>
{
    new User { Name = "Jane Smith", Email = "jane@example.com", Age = 30 },
    new User { Name = "Bob Johnson", Email = "bob@example.com", Age = 28 }
};

users.Insert(newUsers);
Console.WriteLine($"Batch inserted {newUsers.Count} users");
```

#### Query Documents

```csharp
// Query all users
var allUsers = users.FindAll().ToList();
Console.WriteLine($"Total users: {allUsers.Count}");

// Query by condition
var youngUsers = users.Find(u => u.Age < 30).ToList();
Console.WriteLine($"Users under 30: {youngUsers.Count}");

// Query by ID
var user = users.FindById(id);
if (user != null)
{
    Console.WriteLine($"Found user: {user.Name}");
}

// Complex query
var queryUsers = users.Find(u =>
    u.Name.Contains("John") &&
    u.Age >= 20 &&
    u.Age <= 40
).OrderBy(u => u.Name).ToList();

// Use LINQ query
var linqQuery = from u in users.AsQueryable()
               where u.Age >= 25
               orderby u.CreatedAt descending
               select u;

var result = linqQuery.Take(10).ToList();
```

#### Update Documents

```csharp
// Update single document
user.Age = 26;
users.Update(user);

// Batch update
users.UpdateMany(u => u.Age < 25, u => new User
{
    Age = u.Age + 1,
    Name = u.Name
});

// Use Update method to update specific fields
users.UpdateMany(u => u.Email.Contains("@example.com"),
    Builders<User>.Set(u => u.Name, "Updated"));
```

#### Delete Documents

```csharp
// Delete single document
users.Delete(user.Id);

// Delete by condition
var deletedCount = users.DeleteMany(u => u.Age > 65);
Console.WriteLine($"Deleted {deletedCount} users");

// Clear collection
users.DeleteAll();
```

### 4. Index Operations

```csharp
// Create single-field index
users.EnsureIndex(u => u.Email, true); // Unique index

// Create composite index
users.EnsureIndex(u => new { u.Name, u.Age });

// Create sparse index
users.EnsureIndex(u => u.Tags, sparse: true);

// View index information
var indexInfo = users.GetIndexes();
foreach (var index in indexInfo)
{
    Console.WriteLine($"Index: {index.Name}, Fields: {string.Join(", ", index.Fields)}");
}
```

### 5. Transaction Operations

```csharp
// Begin transaction
using var transaction = engine.BeginTransaction();

try
{
    var users = engine.GetCollection<User>("users");
    var products = engine.GetCollection<Product>("products");

    // Execute operations within transaction
    var user = new User { Name = "Transaction User", Email = "tx@example.com", Age = 35 };
    users.Insert(user);

    var product = new Product { Name = "Transaction Product", Price = 99.99m };
    products.Insert(product);

    // Create savepoint
    var savepoint = transaction.CreateSavepoint("before_update");

    // More operations
    user.Age = 36;
    users.Update(user);

    // If needed, rollback to savepoint
    // transaction.RollbackToSavepoint(savepoint);

    // Commit transaction
    transaction.Commit();
    Console.WriteLine("Transaction committed successfully");
}
catch (Exception ex)
{
    // Rollback transaction
    transaction.Rollback();
    Console.WriteLine($"Transaction rolled back: {ex.Message}");
}
```

### 6. Aggregation Operations

```csharp
// Simple aggregation
var userCount = users.Count(u => u.Age >= 18);
var avgAge = users.Average(u => u.Age);
var maxAge = users.Max(u => u.Age);

// Group aggregation
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
    Console.WriteLine($"{group.AgeRange} years: {group.Count} people, Avg Age: {group.AvgAge:F1}");
}
```

## Advanced Features

### 1. Performance Optimization Configuration

```csharp
var highPerformanceOptions = new SimpleDbOptions
{
    DatabaseName = "HighPerformanceDB",
    PageSize = 16384,           // Larger page size
    CacheSize = 10000,          // Larger cache
    EnableJournaling = false,   // Disable journaling for higher performance
    WriteConcern = WriteConcern.None, // Asynchronous writes
    BackgroundFlushInterval = TimeSpan.FromMilliseconds(50),
    JournalFlushDelay = TimeSpan.Zero
};
```

### 2. Database Management Operations

```csharp
// Get database statistics
var stats = engine.GetStatistics();
Console.WriteLine($"Database size: {stats.FileSize} bytes");
Console.WriteLine($"Used pages: {stats.UsedPages}/{stats.TotalPages}");

// List all collections
var collections = engine.GetCollectionNames();
foreach (var collection in collections)
{
    Console.WriteLine($"Collection: {collection}");
}

// Drop collection
engine.DropCollection("old_collection");

// Compact database
engine.Compact();

// Backup database
engine.Backup("backup.db");

// Flush cache to disk
engine.Flush();
```

### 3. Concurrency Control

```csharp
// Thread-safe usage
Parallel.For(0, 10, i =>
{
    var users = engine.GetCollection<User>("users");
    var user = new User
    {
        Name = $"User{i}",
        Email = $"user{i}@example.com",
        Age = 20 + i
    };

    users.Insert(user);
});

// Wait for all operations to complete
engine.Flush();
```

### 4. Data Import/Export

```csharp
// Export to JSON
var users = engine.GetCollection<User>("users");
var userData = users.FindAll().ToList();
var json = System.Text.Json.JsonSerializer.Serialize(userData, new System.Text.Json.JsonSerializerOptions
{
    WriteIndented = true
});
File.WriteAllText("users.json", json);

// Import from JSON
var importData = System.Text.Json.JsonSerializer.Deserialize<List<User>>(json);
if (importData != null)
{
    users.Insert(importData);
}
```

## Best Practices

### 1. Entity Design

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

    // Nested document
    public CustomerInfo Customer { get; set; } = new();

    // Array field
    public List<OrderItem> Items { get; set; } = new();

    // Computed property (not stored)
    [BsonIgnore]
    public string Status => TotalAmount > 1000 ? "Large Order" : "Regular Order";
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

### 2. Performance Optimization Tips

1. **Set appropriate page size**:
   - Small documents: 4KB - 8KB
   - Large documents: 16KB - 32KB

2. **Optimize cache configuration**:
   ```csharp
   // Set cache based on available memory
   var availableMemory = GC.GetTotalMemory(false) / 1024 / 1024; // MB
   var cacheSize = Math.Max(100, availableMemory / 10);
   options.CacheSize = cacheSize;
   ```

3. **Batch operation optimization**:
   ```csharp
   // Use batch insert instead of loop single insert
   var documents = new List<User>();
   for (int i = 0; i < 1000; i++)
   {
       documents.Add(new User { /* ... */ });
   }
   users.Insert(documents); // Good
   // Instead of:
   // for (int i = 0; i < 1000; i++)
   // {
   //     users.Insert(new User { /* ... */ }); // Bad
   // }
   ```

4. **Indexing strategy**:
   - Create indexes for frequently queried fields
   - Avoid too many indexes affecting write performance
   - Use composite indexes for multi-field queries

### 3. Error Handling

```csharp
try
{
    var users = engine.GetCollection<User>("users");
    var user = users.FindById(id);

    if (user == null)
    {
        Console.WriteLine("User not found");
        return;
    }

    // Perform operations...
}
catch (StorageException ex)
{
    Console.WriteLine($"Storage error: {ex.Message}");
    // Handle storage-related errors
}
catch (TransactionException ex)
{
    Console.WriteLine($"Transaction error: {ex.Message}");
    // Handle transaction-related errors
}
catch (Exception ex)
{
    Console.WriteLine($"Unknown error: {ex.Message}");
    // Handle other errors
}
```

### 4. Resource Management

```csharp
// Use using statement to ensure resource disposal
using var engine = new SimpleDbEngine("myapp.db", options);
{
    // Use database...
    // Engine will be automatically disposed when using block ends
}

// Or manual disposal
var engine = new SimpleDbEngine("myapp.db", options);
try
{
    // Use database...
}
finally
{
    engine?.Dispose();
}
```

## Troubleshooting

### Common Issues

1. **Database file corruption**
   ```csharp
   try
   {
       var engine = new SimpleDbEngine("corrupted.db", options);
   }
   catch (StorageException ex)
   {
       Console.WriteLine("Database file may be corrupted, attempting recovery...");
       // Recover from backup or rebuild database
   }
   ```

2. **Insufficient memory**
   ```csharp
   // Reduce cache size
   options.CacheSize = 100;
   options.PageSize = 4096;
   ```

3. **Performance issues**
   ```csharp
   // Check statistics
   var stats = engine.GetStatistics();
   Console.WriteLine($"Cache hit rate: {stats.CacheHitRate:P1}");

   // Optimize configuration
   options.BackgroundFlushInterval = TimeSpan.FromMilliseconds(10);
   options.WriteConcern = WriteConcern.Journaled;
   ```

### Debugging Tips

```csharp
// Enable verbose logging
options.EnableLogging = true;
options.LogLevel = LogLevel.Debug;

// Monitor performance
var stopwatch = Stopwatch.StartNew();
var result = users.Find(u => u.Age > 18).ToList();
stopwatch.Stop();
Console.WriteLine($"Query time: {stopwatch.ElapsedMilliseconds}ms, returned {result.Count} records");
```

## Example Project

### Complete Blog System Example

```csharp
// Define entities
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

// Usage example
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

        // Create indexes
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

        // Update post comment count (simplified here)
        var post = _posts.FindById(postId);
        if (post != null)
        {
            // In actual implementation, might need to add comment count field
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

// Use service
var blogService = new BlogService("blog.db");

// Create post
var post = blogService.CreatePost(
    "SimpleDb Getting Started Guide",
    "SimpleDb is a high-performance embedded database...",
    "Author Name",
    new[] { "database", "SimpleDb", "tutorial" }
);

// Add comment
var comment = blogService.AddComment(
    post.Id,
    "Reader",
    "Great article, learned a lot!"
);

// Query posts
var recentPosts = blogService.GetPublishedPosts(5);
var searchResults = blogService.SearchPosts("SimpleDb");
```

## Next Steps

Now that you've mastered the basics of SimpleDb, you can continue learning:

1. **[API Reference Documentation](api-reference.md)** - Detailed API documentation
2. **[Performance Optimization Guide](performance.md)** - Deep dive into performance optimization techniques
3. **[AOT Deployment Guide](aot-deployment.md)** - Learn how to deploy AOT applications
4. **[Architecture Design Document](architecture.md)** - Understand SimpleDb's internal architecture

## Getting Help

- **GitHub Repository**: [SimpleDb GitHub](https://github.com/your-repo/SimpleDb)
- **Documentation Website**: [SimpleDb Docs](https://docs.simpledb.com)
- **Community Forum**: [SimpleDb Community](https://community.simpledb.com)
- **Issue Reporting**: [Issues](https://github.com/your-repo/SimpleDb/issues)

---

Congratulations! You've completed the SimpleDb Quick Start Guide. Now you can start building your applications!