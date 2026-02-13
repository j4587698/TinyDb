<p align="center">
  <h1 align="center">TinyDb</h1>
  <p align="center">
    <strong>Lightweight AOT-Compatible Embedded NoSQL Database</strong>
  </p>
  <p align="center">
    <a href="https://opensource.org/licenses/MIT"><img src="https://img.shields.io/badge/License-MIT-yellow.svg" alt="License: MIT"></a>
    <a href="https://www.nuget.org/packages/TinyDb"><img src="https://img.shields.io/nuget/v/TinyDb.svg" alt="NuGet"></a>
    <a href="https://www.nuget.org/packages/TinyDb"><img src="https://img.shields.io/nuget/dt/TinyDb.svg" alt="NuGet Downloads"></a>
    <a href="https://app.codecov.io/gh/j4587698/TinyDb"><img src="https://codecov.io/gh/j4587698/TinyDb/graph/badge.svg" alt="codecov"></a>
    <img src="https://img.shields.io/badge/.NET-8.0%20|%209.0%20|%2010.0-blue.svg" alt=".NET Version">
    <img src="https://img.shields.io/badge/AOT-Compatible-green.svg" alt="AOT Compatible">
  </p>
  <p align="center">
    <a href="./README.md">中文</a> | <a href="./README_EN.md">English</a>
  </p>
</p>

---

## AOT-First Philosophy

TinyDb is an **AOT-first**, **LiteDB-inspired** single-file embedded NoSQL database:

- **AOT is the source of truth**: behavior after NativeAOT compilation is the reference; no fallback logic.
- **Dev/prod parity**: JIT (non-AOT) development should behave the same as the AOT-published binary.
- **If you don't need AOT**: consider using LiteDB (more mature ecosystem and features).

## Key Features

- **Single-File Database** - All data stored in one file, easy deployment
- **100% AOT Compatible** - Full Native AOT compilation support, no reflection
- **Source Generator** - Compile-time serialization code generation, zero runtime overhead
- **LINQ Support** - Complete LINQ support with type-safe queries
- **ACID Transactions** - Full transaction support for data consistency
- **Password Protection** - Built-in database-level encryption
- **High-Performance Indexing** - B+ tree indexes for fast data retrieval
- **Cross-Platform** - Windows, Linux, macOS support

## Quick Start

### Installation

```bash
dotnet add package TinyDb
```

### Define Entity

```csharp
using TinyDb.Attributes;
using TinyDb.Bson;

[Entity("users")]
public partial class User
{
    [Id]
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();

    [Index]
    public string Name { get; set; } = "";

    [Index(Unique = true)]
    public string Email { get; set; } = "";

    public int Age { get; set; }

    [BsonIgnore]  // This property won't be serialized
    public string? TempToken { get; set; }

    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
}
```

### Basic CRUD Operations

```csharp
using TinyDb.Core;

// Create/Open database
using var db = new TinyDbEngine("myapp.db");
var users = db.GetCollection<User>();

// Insert
var user = new User { Name = "John", Email = "john@example.com", Age = 25 };
users.Insert(user);

// Query
var found = users.Find(u => u.Age > 20).ToList();
var one = users.FindOne(u => u.Email == "john@example.com");

// Update
user.Age = 26;
users.Update(user);

// Delete
users.Delete(user.Id);
```

### LINQ Queries

```csharp
// Complex queries
var results = users.Query()
    .Where(u => u.Age >= 18 && u.Age <= 30)
    .Where(u => u.Name.StartsWith("J"))
    .OrderByDescending(u => u.CreatedAt)
    .Skip(10)
    .Take(20)
    .ToList();

// Aggregate queries
var count = users.Query().Where(u => u.Age > 20).Count();
var exists = users.Query().Any(u => u.Email.Contains("@gmail.com"));
```

### Password Protection

```csharp
// Create encrypted database
var options = new TinyDbOptions { Password = "MySecurePassword123!" };
using var secureDb = new TinyDbEngine("secure.db", options);

// Access encrypted database
using var db = new TinyDbEngine("secure.db", new TinyDbOptions { Password = "MySecurePassword123!" });
```

### Transaction Support

```csharp
using var db = new TinyDbEngine("myapp.db");
var users = db.GetCollection<User>();
var orders = db.GetCollection<Order>();

// Begin transaction
db.BeginTransaction();
try
{
    users.Insert(new User { Name = "New User" });
    orders.Insert(new Order { UserId = "...", Amount = 99.99m });
    db.Commit();  // Commit transaction
}
catch
{
    db.Rollback();  // Rollback transaction
    throw;
}
```

## Advanced Features

### Attributes

| Attribute | Description |
|-----------|-------------|
| `[Entity("name")]` | Mark entity class, specify collection name |
| `[Id]` | Mark primary key property |
| `[Index]` | Create index |
| `[Index(Unique = true)]` | Create unique index |
| `[BsonIgnore]` | Ignore property during serialization |
| `[BsonField("name")]` | Custom BSON field name |

### Supported Data Types

- **Primitives**: `int`, `long`, `double`, `decimal`, `bool`, `string`, `DateTime`, `Guid`
- **Nullable**: `int?`, `DateTime?`, etc.
- **Collections**: `List<T>`, `T[]`, `Dictionary<string, T>`
- **Nested Objects**: Complex object nesting supported
- **Special Types**: `ObjectId`, `BsonDocument`

### Configuration Options

```csharp
var options = new TinyDbOptions
{
    Password = "password",          // Database password (optional)
    PageSize = 8192,               // Page size (default 8KB)
    CacheSize = 1000,              // Cache pages count
    EnableJournaling = true,       // Enable WAL journaling
    Timeout = TimeSpan.FromMinutes(5)  // Operation timeout
};
```

## Performance

Based on actual results after core refactoring (Reactive Group Commit, Lock Stripping, Zero-Allocation Serialization):

| Operation | Performance | Allocation | Notes |
|-----------|-------------|------------|-------|
| **Single Insert** | ~270 ops/s | ~5 KB/op | Journaled mode, 3 active indexes |
| **Fast Insert** | ~7600 ops/s | ~4 KB/op | Journaled mode, no extra indexes |
| **Batch Insert** | ~4300 ops/s | ~4 KB/op | Optimized 1000-item batch write |
| **PK Lookup** | ~5000 ops/s | < 7 KB/op | B+ tree primary key lookup |
| **Index Query** | ~2500 ops/s | < 8 KB/op | Complex LINQ filtering via index |

> **Note:** Benchmarks performed on AMD EPYC 7763 2.44GHz. Memory allocation has been reduced by over **90%** compared to previous versions thanks to advanced pooling techniques.

## Version History

### v0.3.0 (Current)
- **Performance Leap**: Achieved zero/low allocation serialization via Span and Pooled Buffers, reducing memory allocation by 90%+
- **Core Refactoring**: Introduced high-performance Reactive Group Commit and Lock Stripping technologies
- **Metadata Refactoring**: Deeply refactored the metadata management system for better architecture and scalability
- **Query Optimization**: Supported Predicate Push-down and Sort/Paging Push-down for more efficient BSON scanning
- **Async Support**: Added true asynchronous read API interfaces

### v0.2.0
- Enhanced `[BsonIgnore]` attribute support
- Added AOT-compatible serialization tests
- Fixed source generator issues
- All 2610 tests passing

### v0.1.5
- Improved DbRef reference support
- Enhanced nested class Entity support
- Performance optimizations

## Project Structure

```
TinyDb/
├── TinyDb/                    # Core library
├── TinyDb.SourceGenerator/    # Source code generator
├── TinyDb.Tests/              # Test project
├── TinyDb.Demo/               # Demo project
└── TinyDb.UI/                 # Visual management tool
```

## Run Demo

```bash
dotnet run --project TinyDb.Demo
```

## Development

- .NET 8.0 / 9.0 / 10.0
- C# 12+
- Recommended IDE: Rider / Visual Studio 2022

## Contributing

Issues and Pull Requests are welcome!

```bash
# Run tests
dotnet test

# AOT compilation test
dotnet publish -c Release -r win-x64 --self-contained -p:PublishAot=true
```

## License

[MIT License](LICENSE) - Free for commercial use

---

<p align="center">
  <sub>If this project helps you, please give it a ⭐ Star!</sub>
</p>
