# SimpleDb API Reference Documentation

## Overview

This document provides a complete API reference for SimpleDb, including all major classes, interfaces, methods, and properties. The API is organized by functional modules for easy lookup and usage.

## Core Namespaces

### SimpleDb.Core

Contains the core functionality of the database engine.

#### SimpleDbEngine

The main database engine class that provides database management and collection operations.

```csharp
public sealed class SimpleDbEngine : IDisposable
```

**Constructor**

```csharp
public SimpleDbEngine(string filePath, SimpleDbOptions options)
```

**Parameters**:
- `filePath`: Database file path
- `options`: Database configuration options

**Example**:
```csharp
var options = new SimpleDbOptions
{
    DatabaseName = "MyDatabase",
    PageSize = 8192,
    CacheSize = 1000
};

using var engine = new SimpleDbEngine("mydb.db", options);
```

**Properties**

| Property | Type | Description |
|----------|------|-------------|
| `DatabaseName` | `string` | Database name |
| `CollectionCount` | `int` | Current collection count |
| `IsDisposed` | `bool` | Whether disposed |

**Methods**

##### Collection Management

```csharp
public ILiteCollection<T> GetCollection<T>(string collectionName)
```

Gets a collection instance of the specified type.

**Parameters**:
- `collectionName`: Collection name

**Returns**: Collection instance

**Example**:
```csharp
var users = engine.GetCollection<User>("users");
```

```csharp
public bool CollectionExists(string collectionName)
```

Checks if a collection exists.

```csharp
public IEnumerable<string> GetCollectionNames()
```

Gets all collection names.

```csharp
public bool DropCollection(string collectionName)
```

Deletes the specified collection.

##### Database Operations

```csharp
public DatabaseStatistics GetStatistics()
```

Gets database statistics.

```csharp
public void Flush()
```

Forces cache flush to disk.

```csharp
public void Compact()
```

Compacts the database file.

```csharp
public void Backup(string backupPath)
```

Backs up the database to the specified path.

##### Transaction Management

```csharp
public ITransaction BeginTransaction()
```

Begins a new transaction.

#### SimpleDbOptions

Database configuration options class.

```csharp
public sealed class SimpleDbOptions
```

**Properties**

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `DatabaseName` | `string` | "SimpleDb" | Database name |
| `PageSize` | `uint` | 8192 | Page size (bytes) |
| `CacheSize` | `int` | 1000 | Page cache size |
| `EnableJournaling` | `bool` | true | Enable journaling |
| `WriteConcern` | `WriteConcern` | `Synced` | Write concern level |
| `BackgroundFlushInterval` | `TimeSpan` | 100ms | Background flush interval |
| `JournalFlushDelay` | `TimeSpan` | 10ms | Journal flush delay |
| `Timeout` | `TimeSpan` | 30s | Operation timeout |
| `ReadOnly` | `bool` | false | Read-only mode |
| `StrictMode` | `bool` | true | Strict mode |

**Methods**

```csharp
public void Validate()
```

Validates configuration options.

```csharp
public SimpleDbOptions Clone()
```

Clones configuration options.

#### WriteConcern

Write concern level enumeration.

```csharp
public enum WriteConcern
{
    /// <summary>
    /// Don't wait for write acknowledgment
    /// </summary>
    None,

    /// <summary>
    /// Wait for journal write acknowledgment
    /// </summary>
    Journaled,

    /// <summary>
    /// Wait for data and journal write acknowledgment
    /// </summary>
    Synced
}
```

### SimpleDb.Collections

Contains collection-related interfaces and classes.

#### ILiteCollection<T>

Collection interface providing document CRUD operations.

```csharp
public interface ILiteCollection<T> where T : class
```

**Properties**

| Property | Type | Description |
|----------|------|-------------|
| `CollectionName` | `string` | Collection name |

**Query Methods**

```csharp
public IQueryable<T> AsQueryable()
```

Returns a queryable LINQ provider.

```csharp
public IEnumerable<T> FindAll()
```

Queries all documents.

```csharp
public IEnumerable<T> Find(Expression<Func<T, bool>> predicate)
```

Queries documents by condition.

```csharp
public T FindById(ObjectId id)
```

Queries a document by ID.

```csharp
public T FindOne(Expression<Func<T, bool>> predicate)
```

Queries a single document.

```csharp
public int Count(Expression<Func<T, bool>>? predicate = null)
```

Counts documents.

**Insert Methods**

```csharp
public ObjectId Insert(T entity)
```

Inserts a single document, returns document ID.

```csharp
public int Insert(IEnumerable<T> entities)
```

Batch inserts documents, returns insert count.

**Update Methods**

```csharp
public bool Update(T entity)
```

Updates a single document.

```csharp
public int UpdateMany(Expression<Func<T, bool>> predicate, T update)
```

Batch updates documents.

```csharp
public int UpdateMany(Expression<Func<T, bool>> predicate, UpdateBuilder<T> update)
```

Batch updates using update builder.

**Delete Methods**

```csharp
public bool Delete(ObjectId id)
```

Deletes a document by ID.

```csharp
public int DeleteMany(Expression<Func<T, bool>> predicate)
```

Batch deletes by condition.

```csharp
public int DeleteAll()
```

Deletes all documents.

**Index Methods**

```csharp
public void EnsureIndex<TKey>(Expression<Func<T, TKey>> keySelector, bool unique = false)
```

Creates a single-field index.

```csharp
public void EnsureIndex(Expression<Func<T, object>> keySelector, bool unique = false)
```

Creates a composite index.

```csharp
public IEnumerable<IndexInfo> GetIndexes()
```

Gets all index information.

```csharp
public void DropIndex(string name)
```

Deletes the specified index.

**Aggregation Methods**

```csharp
public TResult Aggregate<TResult>(Expression<Func<IGrouping<T, TKey>, TResult>> resultSelector)
```

Executes aggregation operations.

```csharp
public TAverage Average<TAverage>(Expression<Func<T, TAverage>> selector)
```

Calculates average value.

```csharp
public TMax Max<TMax>(Expression<Func<T, TMax>> selector)
```

Calculates maximum value.

```csharp
public TMin Min<TMin>(Expression<Func<T, TMin>> selector)
```

Calculates minimum value.

```csharp
public TSum Sum<TSum>(Expression<Func<T, TSum>> selector)
```

Calculates sum value.

#### DocumentCollection<T>

Default implementation of document collection.

```csharp
public sealed class DocumentCollection<T> : ILiteCollection<T> where T : class
```

Inherits all methods from `ILiteCollection<T>` interface.

#### UpdateBuilder<T>

Update builder for constructing complex update operations.

```csharp
public sealed class UpdateBuilder<T>
```

**Static Methods**

```csharp
public static UpdateBuilder<T> Set<TField>(Expression<Func<T, TField>> field, TField value)
```

Sets field value.

```csharp
public static UpdateBuilder<T> Inc<TField>(Expression<Func<T, TField>> field, TField value)
```

Increments field value.

```csharp
public static UpdateBuilder<T> Push<TField>(Expression<Func<T, IEnumerable<TField>>> field, TField value)
```

Adds element to array field.

```csharp
public static UpdateBuilder<T> Pull<TField>(Expression<Func<T, IEnumerable<TField>>> field, TField value)
```

Removes element from array field.

**Example**:
```csharp
users.UpdateMany(u => u.Age < 25,
    UpdateBuilder<User>.Set(u => u.Status, "Young")
                   .Inc(u => u.LoginCount, 1)
                   .Push(u => u.Tags, "new"));
```

### SimpleDb.Bson

Contains BSON data types and related operations.

#### ObjectId

BSON ObjectId type for uniquely identifying documents.

```csharp
public sealed class ObjectId : IComparable<ObjectId>, IEquatable<ObjectId>
```

**Constructors**

```csharp
public ObjectId()
public ObjectId(string value)
public ObjectId(byte[] bytes)
```

**Static Methods**

```csharp
public static ObjectId NewObjectId()
public static ObjectId Empty { get; }
public static bool TryParse(string value, out ObjectId objectId)
```

**Properties**

| Property | Type | Description |
|----------|------|-------------|
| `Timestamp` | `int` | Timestamp |
| `Machine` | `int` | Machine identifier |
| `Pid` | `short` | Process ID |
| `Increment` | `int` | Increment counter |

**Methods**

```csharp
public string ToString()
public byte[] ToByteArray()
public int CompareTo(ObjectId other)
public bool Equals(ObjectId other)
```

#### BsonValue

Base class for BSON values.

```csharp
public abstract class BsonValue
```

**Properties**

| Property | Type | Description |
|----------|------|-------------|
| `BsonType` | `BsonType` | BSON type |
| `IsNull` | `bool` | Whether null |

**Concrete Subclasses**

- `BsonString`: String value
- `BsonInt32`: 32-bit integer
- `BsonInt64`: 64-bit integer
- `BsonDouble`: Double precision floating point
- `BsonBoolean`: Boolean value
- `BsonDateTime`: Date time
- `BsonObjectId`: ObjectId
- `BsonArray`: Array
- `BsonDocument`: Document
- `BsonNull`: Null value

#### BsonDocument

BSON document type.

```csharp
public sealed class BsonDocument : BsonValue, IEnumerable<KeyValuePair<string, BsonValue>>
```

**Constructors**

```csharp
public BsonDocument()
public BsonDocument(Dictionary<string, BsonValue> dictionary)
```

**Indexer**

```csharp
public BsonValue this[string key] { get; set; }
```

**Methods**

```csharp
public void Add(string key, BsonValue value)
public bool ContainsKey(string key)
public bool TryGetValue(string key, out BsonValue value)
public bool Remove(string key)
public void Clear()
```

### SimpleDb.Attributes

Contains entity and property marking attributes.

#### EntityAttribute

Marks entity classes.

```csharp
[AttributeUsage(AttributeTargets.Class)]
public sealed class EntityAttribute : Attribute
```

**Properties**

| Property | Type | Description |
|----------|------|-------------|
| `CollectionName` | `string?` | Collection name |
| `IdProperty` | `string?` | ID property name |

**Example**:
```csharp
[Entity("users")]
public class User
{
    public ObjectId Id { get; set; }
    public string Name { get; set; }
}
```

#### IndexAttribute

Marks indexed properties.

```csharp
[AttributeUsage(AttributeTargets.Property)]
public sealed class IndexAttribute : Attribute
```

**Properties**

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `Unique` | `bool` | false | Unique index |
| `Priority` | `int` | 0 | Index priority |
| `Sparse` | `bool` | false | Sparse index |

**Example**:
```csharp
[Index(Unique = true, Priority = 1)]
public string Email { get; set; }
```

#### IdAttribute

Marks ID properties.

```csharp
[AttributeUsage(AttributeTargets.Property)]
public sealed class IdAttribute : Attribute
```

#### BsonIgnoreAttribute

Marks properties to be ignored.

```csharp
[AttributeUsage(AttributeTargets.Property)]
public sealed class BsonIgnoreAttribute : Attribute
```

### SimpleDb.Core.Transaction

Contains transaction-related classes and interfaces.

#### ITransaction

Transaction interface.

```csharp
public interface ITransaction : IDisposable
```

**Properties**

| Property | Type | Description |
|----------|------|-------------|
| `TransactionId` | `Guid` | Transaction ID |
| `IsActive` | `bool` | Whether active |
| `State` | `TransactionState` | Transaction state |

**Methods**

```csharp
public void Commit()
public void Rollback()
public Guid CreateSavepoint(string name)
public void RollbackToSavepoint(Guid savepointId)
public void ReleaseSavepoint(Guid savepointId)
```

#### TransactionState

Transaction state enumeration.

```csharp
public enum TransactionState
{
    Active,
    Committing,
    Committed,
    RollingBack,
    RolledBack,
    Failed
}
```

### SimpleDb.Storage

Contains storage-related classes.

#### DatabaseHeader

Database header information structure.

```csharp
public struct DatabaseHeader
```

**Properties**

| Property | Type | Description |
|----------|------|-------------|
| `Magic` | `uint` | Magic number |
| `DatabaseVersion` | `uint` | Database version |
| `PageSize` | `uint` | Page size |
| `TotalPages` | `uint` | Total pages |
| `UsedPages` | `uint` | Used pages |
| `DatabaseName` | `string` | Database name |
| `CreatedAt` | `long` | Creation timestamp |
| `ModifiedAt` | `long` | Modification timestamp |

**Methods**

```csharp
public void Initialize(uint pageSize, string databaseName = "SimpleDb")
public bool IsValid()
public uint CalculateChecksum()
public bool VerifyChecksum()
public void UpdateModification()
public byte[] ToByteArray()
public static DatabaseHeader FromByteArray(byte[] data)
public DatabaseHeader Clone()
```

#### PageType

Page type enumeration.

```csharp
public enum PageType : byte
{
    Empty = 0x00,
    Header = 0x01,
    Collection = 0x02,
    Data = 0x03,
    Index = 0x04,
    Journal = 0x05,
    Extension = 0x06
}
```

### SimpleDb.Utils

Contains utility classes.

#### LRUCache<TKey, TValue>

Thread-safe LRU cache implementation.

```csharp
public sealed class LRUCache<TKey, TValue> where TKey : notnull
```

**Constructor**

```csharp
public LRUCache(int capacity)
```

**Properties**

| Property | Type | Description |
|----------|------|-------------|
| `Capacity` | `int` | Cache capacity |
| `Count` | `int` | Current item count |
| `Hits` | `long` | Hit count |
| `Misses` | `long` | Miss count |
| `HitRatio` | `double` | Hit ratio |

**Methods**

```csharp
public bool TryGetValue(TKey key, out TValue? value)
public TValue GetOrAdd(TKey key, Func<TKey, TValue> valueFactory)
public void Put(TKey key, TValue value)
public bool TryRemove(TKey key)
public void Remove(TKey key)
public void Touch(TKey key)
public void Clear()
public bool ContainsKey(TKey key)
public void Trim(int newCapacity)
```

## Exception Classes

### StorageException

Storage-related exception.

```csharp
public class StorageException : Exception
```

### TransactionException

Transaction-related exception.

```csharp
public class TransactionException : Exception
```

### ValidationException

Data validation exception.

```csharp
public class ValidationException : Exception
```

## Configuration and Statistics Classes

### DatabaseStatistics

Database statistics information.

```csharp
public sealed class DatabaseStatistics
```

**Properties**

| Property | Type | Description |
|----------|------|-------------|
| `FileSize` | `long` | File size (bytes) |
| `TotalPages` | `uint` | Total pages |
| `UsedPages` | `uint` | Used pages |
| `FreePages` | `uint` | Free pages |
| `CollectionCount` | `int` | Collection count |
| `CacheHitRatio` | `double` | Cache hit ratio |

### IndexInfo

Index information.

```csharp
public sealed class IndexInfo
```

**Properties**

| Property | Type | Description |
|----------|------|-------------|
| `Name` | `string` | Index name |
| `Fields` | `string[]` | Index fields |
| `IsUnique` | `bool` | Whether unique |
| `IsSparse` | `bool` | Whether sparse |

### LockManagerStatistics

Lock manager statistics information.

```csharp
public sealed class LockManagerStatistics
```

**Properties**

| Property | Type | Description |
|----------|------|-------------|
| `ActiveLockCount` | `int` | Active lock count |
| `PendingLockCount` | `int` | Pending lock count |
| `LockTypeCounts` | `Dictionary<LockType, int>` | Lock type statistics |
| `BucketCount` | `int` | Bucket count |
| `DefaultTimeout` | `TimeSpan` | Default timeout |

### TransactionManagerStatistics

Transaction manager statistics information.

```csharp
public sealed class TransactionManagerStatistics
```

**Properties**

| Property | Type | Description |
|----------|------|-------------|
| `ActiveTransactionCount` | `int` | Active transaction count |
| `MaxTransactions` | `int` | Maximum transaction count |
| `TransactionTimeout` | `TimeSpan` | Transaction timeout |
| `AverageOperationCount` | `double` | Average operation count |
| `TotalOperations` | `int` | Total operation count |
| `AverageTransactionAge` | `double` | Average transaction age |
| `States` | `Dictionary<TransactionState, int>` | Transaction state statistics |

## Extension Methods

### QueryExtensions

Query extension methods.

```csharp
public static class QueryExtensions
{
    public static IOrderedQueryable<T> OrderBy<T, TKey>(
        this IQueryable<T> source,
        Expression<Func<T, TKey>> keySelector);

    public static IOrderedQueryable<T> OrderByDescending<T, TKey>(
        this IQueryable<T> source,
        Expression<Func<T, TKey>> keySelector);

    public static IQueryable<T> Skip<T>(
        this IQueryable<T> source,
        int count);

    public static IQueryable<T> Take<T>(
        this IQueryable<T> source,
        int count);
}
```

### CollectionExtensions

Collection extension methods.

```csharp
public static class CollectionExtensions
{
    public static async Task<IEnumerable<T>> ToListAsync<T>(
        this IEnumerable<T> source);

    public static async Task<T> FirstOrDefaultAsync<T>(
        this IEnumerable<T> source);

    public static async Task<bool> AnyAsync<T>(
        this IEnumerable<T> source);
}
```

## Thread Safety

### Thread-Safe Components

- `SimpleDbEngine`: Thread-safe, supports multi-threaded concurrent access
- `ILiteCollection<T>`: Thread-safe, supports concurrent reads and writes
- `LRUCache<TKey, TValue>`: Thread-safe LRU cache
- `LockManager`: Thread-safe lock manager
- `TransactionManager`: Thread-safe transaction manager

### Considerations

1. **Transaction Isolation**: Each transaction has an independent context, operations within a transaction do not affect other transactions
2. **Locking Mechanism**: System automatically manages lock acquisition and release, avoiding deadlocks
3. **Cache Consistency**: Cache data remains consistent in multi-threaded environments
4. **Resource Management**: Use `using` statements to ensure proper resource disposal

## Performance Considerations

### Batch Operations

```csharp
// Recommended: Batch insert
var users = new List<User>();
for (int i = 0; i < 1000; i++)
{
    users.Add(new User { /* ... */ });
}
collection.Insert(users);

// Avoid: Loop single insert
for (int i = 0; i < 1000; i++)
{
    collection.Insert(new User { /* ... */ });
}
```

### Index Optimization

```csharp
// Create indexes for frequently queried fields
collection.EnsureIndex(u => u.Email, unique: true);
collection.EnsureIndex(u => new { u.Name, u.Age });
```

### Cache Configuration

```csharp
var options = new SimpleDbOptions
{
    CacheSize = Environment.ProcessorCount * 1000, // Based on processor count
    PageSize = 16384, // Page size suitable for large documents
    BackgroundFlushInterval = TimeSpan.FromMilliseconds(50)
};
```

## Example Code

### Complete CRUD Operations Example

```csharp
using SimpleDb.Core;
using SimpleDb.Collections;
using SimpleDb.Attributes;
using SimpleDb.Bson;

// Define entity
[Entity("products")]
public class Product
{
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();

    [Index(Unique = true)]
    public string Sku { get; set; } = "";

    [Index]
    public string Name { get; set; } = "";

    public decimal Price { get; set; }
    public string[] Tags { get; set; } = Array.Empty<string>();
    public bool IsActive { get; set; } = true;
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
}

// Use database
var options = new SimpleDbOptions
{
    DatabaseName = "ProductDB",
    EnableJournaling = true,
    WriteConcern = WriteConcern.Synced
};

using var engine = new SimpleDbEngine("products.db", options);
var products = engine.GetCollection<Product>("products");

// Create indexes
products.EnsureIndex(p => p.Sku, unique: true);
products.EnsureIndex(p => p.Name);
products.EnsureIndex(p => p.Tags, sparse: true);

// Insert product
var product = new Product
{
    Sku = "SKU001",
    Name = "Laptop",
    Price = 999.99m,
    Tags = new[] { "electronics", "computer" },
    IsActive = true
};

var id = products.Insert(product);
Console.WriteLine($"Inserted product ID: {id}");

// Query product
var laptop = products.FindById(id);
if (laptop != null)
{
    Console.WriteLine($"Found product: {laptop.Name}, Price: {laptop.Price}");
}

// Conditional query
var activeProducts = products.Find(p => p.IsActive && p.Price > 100)
                           .OrderBy(p => p.Name)
                           .Take(10)
                           .ToList();

// Update product
laptop.Price = 899.99m;
products.Update(laptop);

// Batch update
products.UpdateMany(
    p => p.Tags.Contains("electronics"),
    UpdateBuilder<Product>.Set(p => p.IsActive, true)
                       .Inc(p => p.ViewCount, 1));

// Delete product
products.Delete(laptop.Id);

// Aggregation queries
var avgPrice = products.Average(p => p.Price);
var electronicsCount = products.Count(p => p.Tags.Contains("electronics"));

// Statistics
var stats = engine.GetStatistics();
Console.WriteLine($"Database size: {stats.FileSize} bytes");
Console.WriteLine($"Cache hit ratio: {stats.CacheHitRatio:P1}");
```

This API reference documentation covers all major SimpleDb functionality, providing complete interface descriptions and usage examples for developers.