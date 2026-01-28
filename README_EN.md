<p align="center">
  <h1 align="center">TinyDb</h1>
  <p align="center">
    <strong>Lightweight AOT-Compatible Embedded NoSQL Database</strong>
  </p>
  <p align="center">
    <a href="https://opensource.org/licenses/MIT"><img src="https://img.shields.io/badge/License-MIT-yellow.svg" alt="License: MIT"></a>
    <a href="https://www.nuget.org/packages/TinyDb"><img src="https://img.shields.io/nuget/v/TinyDb.svg" alt="NuGet"></a>
    <a href="https://www.nuget.org/packages/TinyDb"><img src="https://img.shields.io/nuget/dt/TinyDb.svg" alt="NuGet Downloads"></a>
    <img src="https://img.shields.io/badge/.NET-8.0%20|%209.0%20|%2010.0-blue.svg" alt=".NET Version">
    <img src="https://img.shields.io/badge/AOT-Compatible-green.svg" alt="AOT Compatible">
  </p>
  <p align="center">
    <a href="./README.md">中文</a> | <a href="./README_EN.md">English</a>
  </p>
</p>

---

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

Based on actual test results with 2610 test cases:

| Operation | Performance | Notes |
|-----------|-------------|-------|
| Single Insert | ~80 ops/s | Synchronous write mode |
| Batch Insert | ~120 ops/s | Batch operation optimized |
| Primary Key Query | >1000 ops/s | B+ tree index lookup |
| Index Query | >500 ops/s | Index scan |

## Version History

### v0.2.0 (Current)
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
