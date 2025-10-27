# SimpleDb

SimpleDb is a lightweight, high-performance NoSQL embedded database for .NET applications. Inspired by LiteDB, it provides a simple yet powerful document database with LINQ query support, BSON serialization, and page-based storage architecture.

## Features

- 🗄️ **Single File Database** - Store all data in a single file
- 📄 **Page-Based Storage** - Efficient memory-mapped page management
- 🔄 **BSON Serialization** - Binary JSON format for fast serialization
- 🔍 **LINQ Query Support** - Full LINQ expression tree parsing and execution
- 🆔 **Auto-Generated IDs** - Automatic ObjectId generation and mapping
- 🏛️ **Collection Management** - Organize documents in collections
- ⚡ **AOT Compatible** - Ahead-of-Time compilation support
- 💾 **In-Memory Caching** - LRU cache for improved performance
- 🔒 **ACID Transactions** - Atomic operations with rollback support

## Quick Start

```csharp
using SimpleDb.Core;
using SimpleDb.Collections;

// Create or open a database
using var engine = new SimpleDbEngine("mydb.db");

// Get a collection
var users = engine.GetCollection<User>();

// Insert documents
var user = new User
{
    Name = "John Doe",
    Age = 30,
    Email = "john@example.com"
};
users.Insert(user);

// Query with LINQ
var adults = users.Find(u => u.Age >= 18).ToList();

// Update documents
user.Age = 31;
users.Update(user);

// Delete documents
users.Delete(user.Id);
```

## Architecture

### Core Components

- **SimpleDbEngine** - Main database engine and entry point
- **PageManager** - Page allocation, caching, and I/O management
- **BsonSerializer** - BSON document serialization/deserialization
- **QueryExecutor** - LINQ expression parsing and execution
- **DocumentCollection** - Collection management and CRUD operations

### Storage Architecture

SimpleDb uses a page-based storage system:

```
Database File
├── Header Page (Page 1)
├── System Pages (Collection, Index, Journal info)
└── Data Pages (Document storage)
```

Each page is 8KB by default and contains:
- Page header (32 bytes)
- Document data
- Free space for new documents

## BSON Document Format

SimpleDb uses BSON (Binary JSON) for document storage:

```csharp
public class User
{
    public ObjectId Id { get; set; }  // Mapped to "_id" in BSON
    public string Name { get; set; }
    public int Age { get; set; }
    public string Email { get; set; }
}
```

## LINQ Query Support

Full LINQ expression support with automatic translation:

```csharp
// Basic queries
var result = users.Find(u => u.Age > 25).ToList();

// Complex queries
var query = users.Where(u => u.Name.StartsWith("J"))
                 .OrderBy(u => u.Age)
                 .Take(10);

// Projection
var names = users.FindAll().Select(u => u.Name).ToList();
```

## AOT Compatibility

SimpleDb is designed to work with Native AOT compilation:

```csharp
// Publish as native executable
dotnet publish -c Release -r win-x64 --self-contained true /p:PublishAot=true
```

## Performance

- **Fast Serialization** - Custom BSON serializer optimized for .NET
- **Memory Efficiency** - Page-based storage with configurable cache size
- **Query Optimization** - Expression tree caching and optimization
- **Concurrent Access** - Thread-safe operations with proper locking

## Requirements

- .NET 8.0 or later
- Compatible with .NET Standard 2.0+

## Installation

```bash
dotnet add package SimpleDb
```

## License

MIT License - see LICENSE file for details.

## Contributing

Contributions are welcome! Please read the contributing guidelines and submit pull requests to the repository.

---

**SimpleDb** - Simple by design, powerful by nature.