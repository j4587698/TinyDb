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
- **Dynamic Predicates and SQL Subset** - AOT-compatible string predicates, `Execute`, and `select/insert/update/delete`
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

### String Predicates and SQL Execute

TinyDb provides AOT-compatible dynamic query APIs. String predicates and SQL are parsed into an internal AST; TinyDb does not generate runtime code for them.

#### String Predicate Queries

```csharp
using TinyDb.Query;

var adults = users.Find(
    "Age >= @minAge and Name startswith @prefix",
    QueryParams.Create(("minAge", 18), ("prefix", "J")))
    .ToList();

var page = users.Find(
    "Age >= @minAge",
    QueryParams.Create(("minAge", 18)),
    skip: 20,
    limit: 10)
    .ToList();
```

String predicates support:

- Field paths: `Id`, `Name`, `Address.City`; `Id` / `id` / `_id` are treated as the primary key.
- Comparisons: `=`, `==`, `!=`, `<>`, `>`, `>=`, `<`, `<=`, plus `eq`, `ne`, `gt`, `gte`, `lt`, `lte`.
- Logic: `and`, `or`, `not`, and parentheses.
- Null checks: `is null`, `is not null`.
- String matching: `contains`, `startswith` / `starts_with`, `endswith` / `ends_with`.
- `like`: supports `abc%`, `%abc`, `%abc%`, and exact matches; `_` and multi-segment middle `%` wildcards are not supported.
- Functions: `contains(field, value)`, `startswith(field, value)`, `endswith(field, value)`, `lower(field)`, `upper(field)`, `trim(field)`.
- Parameters: `@name`, preferably passed with `QueryParams.Create(("name", value))`.

Not supported:

- `in` / `not in`, subqueries, regular expressions.
- Arbitrary .NET method calls; only the fixed functions listed above are supported.
- Anonymous-object projection; under AOT use `BsonDocument` or a `[Entity]` DTO.

#### SQL Query and DML

New code should prefer the unified `Execute` API:

```csharp
// SELECT -> BsonDocument; field names follow the SELECT list as written
var select = users.Execute(
    "select Id, Name from users where Age >= @age order by Age desc limit 10 offset 0",
    QueryParams.Create(("age", 18)));

foreach (var doc in select.Documents)
{
    var id = doc["Id"];
    var name = doc["Name"].ToString();
}

// SELECT * expands to entity property names
var all = users.Execute("select * from users where Id = @id", QueryParams.Create(("id", id)));
var userName = all.Documents.Single()["Name"].ToString();

// Generic DTO projection. The DTO must have [Entity] and source-generator support.
var summaries = users.Execute<UserSummary>(
    "select Id, Name from users where Age >= @age",
    QueryParams.Create(("age", 18))).Rows;

// INSERT / UPDATE / DELETE return affected row counts
var inserted = users.Execute(
    "insert into users (Id, Name, Email, Age) values (@id, @name, @email, @age)",
    QueryParams.Create(("id", ObjectId.NewObjectId()), ("name", "Alice"), ("email", "alice@example.com"), ("age", 22)));

var updated = users.Execute(
    "update users set Name = @name, Age = @age where Id = @id",
    QueryParams.Create(("id", id), ("name", "Bob"), ("age", 23)));

var deleted = users.Execute(
    "delete from users where Id = @id",
    QueryParams.Create(("id", id)));
```

SQL supports:

- `select * from collection`
- `select Id, Name from collection`
- `select Name as DisplayName from collection`
- `where`: reuses the string predicate syntax.
- `order by Field asc|desc`, including multiple fields.
- `limit` and `offset`, with literals or parameters.
- `insert into collection (Field1, Field2) values (@v1, @v2)`.
- `update collection set Field = @value where ...`.
- `delete from collection where ...`.
- Literals: strings, numbers, `true`, `false`, `null`.
- DML writes normalize numeric literals to the target entity property type, avoiding BSON type mismatches under AOT deserialization.
- `FindSql`, `FindSqlDocuments`, and `FindSql<TProjection>` remain for compatibility, but internally route through `Execute`.
- `TinyDbEngine.Execute<TSource>(...)` and `TinyDbEngine.Execute<TSource, TProjection>(...)` route from the engine by the collection name in the SQL.

Note: `where` is optional for `update` / `delete` by SQL semantics. Omitting it affects the whole collection; prefer an explicit `where` for batch writes and wrap with TinyDb transactions when needed.

SQL does not currently support:

- `join`; use `Include(...)` for entity reference loading.
- `group by`, `having`, aggregate functions, window functions.
- Subqueries, CTEs, `union`.
- `insert into ... select ...`.
- Assignment expressions such as `update set Age = Age + 1`; only literal or parameter assignment is supported.
- Updating primary key fields `Id` / `_id`.
- Nested-field writes in DML; `insert` / `update` currently support top-level fields only.
- Writing the same DML field more than once; duplicate `insert` fields and duplicate `update set` fields are rejected.
- SQL transaction syntax; wrap calls with `BeginTransaction()` / `Commit()` / `Rollback()` instead.

#### Include and SQL

`Include(...)` is not SQL `join`. It loads references after entity queries using DBRef / foreign-key metadata:

```csharp
var orders = db.GetCollection<Order>()
    .Include("Customer")
    .FindSql("select * from orders where Total >= @min", QueryParams.Create(("min", 100)))
    .ToList();
```

`Include` targets entity results and does not apply to `FindSqlDocuments` or `Execute(...).Documents` dynamic document projection.

### Password Protection and Page Encryption

`Password` can still be used for compatible database password protection. To encrypt data pages and WAL payloads, set `EnableEncryption = true` when creating a new database. Existing plaintext databases are not encrypted implicitly when `EnableEncryption = true`; TinyDb throws instead, so migrate explicitly or compact into a new encrypted database.

```csharp
// Create encrypted database
var options = new TinyDbOptions
{
    EnableEncryption = true,
    Password = "MySecurePassword123!"
};
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
    EnableEncryption = true,        // Encrypt data pages and WAL for new databases
    PageSize = 8192,               // Page size (default 8KB)
    CacheSize = 1000,              // Cache pages count
    EnableJournaling = true,       // Enable WAL journaling
    Timeout = TimeSpan.FromMinutes(5), // Operation timeout
    Logger = (level, message, ex) =>
    {
        Console.WriteLine($"[{level}] {message}");
        if (ex != null) Console.WriteLine(ex);
    }
};
```

`Logger` is an optional callback with signature `Action<TinyDbLogLevel, string, Exception?>`, with levels: `Debug`, `Information`, `Warning`, `Error`, `Critical`.

## Performance

Latest measured means from `BenchmarkDotNet` (`QuickIndexBenchmark`, 2026-07-10):

| Operation | `SynchronousWrites=true` | `SynchronousWrites=false` | Allocation (true / false) |
|-----------|--------------------------|---------------------------|------------|
| `Insert1000_Individual` | `8,976,726.1 μs` (~`111 ops/s`) | `412,939.3 μs` (~`2,422 ops/s`) | `12.60 MB / 10.80 MB` |
| `Insert1000_Batch` | `297,528.8 μs` (~`3,361 ops/s`) | `215,342.3 μs` (~`4,644 ops/s`) | `13.27 MB / 13.49 MB` |
| `QueryWithoutIndex` | `752.2 μs` | `1,827.3 μs` | `280.56 KB / 292.28 KB` |
| `QueryWithIndex` | `525.3 μs` | `737.6 μs` | `74.23 KB / 76.57 KB` |
| `QueryWithUniqueIndex` | `331.2 μs` | `419.0 μs` | `18.16 KB / 18.24 KB` |
| `FindById` | `267.3 μs` | `295.0 μs` | `6.59 KB / 6.67 KB` |

> **Note:** Environment: AMD EPYC 7763 2.44GHz, .NET 9.0.12. This benchmark set uses `EnableJournaling=false` to isolate core read/write path behavior.

## Version History

### v0.5.0 (Current)
- **Concurrency and write-path hardening**: refined collection write locks, page locks, and document lock boundaries to reduce contention in concurrent writes, transaction commits, and cache writeback paths.
- **WAL and durability hardening**: strengthened synced flush, batched commit, replay validation, and transaction recovery paths across crash-recovery and half-written-page scenarios.
- **Query and SQL execution improvements**: hardened dynamic SQL/DML, runtime expression binding, index planning, ordering/TopK, and transaction visibility behavior.
- **AOT and source generator stability**: split and hardened source generator type analysis, dependency analysis, field naming, and mapper generation paths to reduce AOT/trim edge cases.
- **Serialization and BSON compatibility**: improved round-tripping for `Decimal128`, `ObjectId`, `DateTime`, numeric conversions, complex collections, and nested objects.
- **Performance baseline refresh**: updated `QuickIndexBenchmark` data for write, indexed query, unique-index query, and primary-key lookup timing and allocation.
- **Regression coverage**: added and expanded concurrency, WAL, index, query, AOT, encryption, source generator, and serialization regression tests.

### v0.4.5
- **Dynamic query and SQL subset**: added AOT-compatible string predicate queries, a unified `Execute` SQL entry point, `BsonDocument` dynamic projection, generic DTO projection, and basic `select/insert/update/delete` parsing and execution.
- **SQL capability boundaries documented**: documented supported predicate syntax, projection rules, DML scope, and unsupported capabilities such as joins, aggregation, subqueries, and expression assignments.
- **WAL crash recovery hardening**: appends and flushes WAL before page writes, and replay now validates disk page headers, page IDs, and checksums so latest-LSN half-written pages are restored from WAL.
- **Page checksum upgrade**: page checksums now use CRC32 instead of the previous additive checksum, improving half-write and silent corruption detection.
- **Existing database compatibility**: checksum verification accepts both CRC32 and legacy additive checksums, so existing databases do not require migration and pages naturally upgrade to CRC32 when rewritten.
- **Regression coverage**: added WAL half-write recovery, CRC32 zeroed-range calculation, and legacy checksum compatibility tests.

### v0.4.4
- **Concurrent initialization fix**: serialized collection registration, collection-state construction, and index-manager creation to prevent `ConcurrentDictionary.GetOrAdd` factories from running side-effectful schema, metadata, and index-page initialization more than once under contention.
- **Schema write race fix**: `MetadataManager.EnsureSchema()` now uses synchronization and a second existence check to avoid duplicate `__sys_catalog` writes during first concurrent access to the same entity.
- **ASP.NET metadata compatibility**: `BsonConversion` now natively supports `JsonElement` / `JsonDocument`, recursively converting objects, arrays, numbers, booleans, and `null` values from JSON-backed `Dictionary<string, object>` payloads.
- **Regression coverage**: added reopen validation for concurrent first access to indexed collections and recursive BSON conversion coverage for `JsonElement`.

### v0.4.3
- **New database initialization fix**: immediately writes and flushes a valid header for newly created databases, preventing `Invalid database header` after application restarts.
- **WAL safety fix**: ignores stale WAL data when the main database file was deleted and a new database is being created, preventing old log records from contaminating the new database.
- **Index reliability improvements**: `EnsureIndex()` now backfills existing documents, persists index root pages, restores indexes after reopen, and leaves no invalid index definitions after failed backfill.
- **Transaction visibility fix**: `FindById()` now observes pending inserts/deletes in the current transaction.
- **AOT validation improvements**: cleaned AOT/trim warnings and added NativeAOT regression coverage.

### v0.4.2
- **AOT complex-collection fix**: restored and improved `List<complex-type>` deserialization in AOT mode, including complex object collections inside dependent types referenced by `Entity`.
- **Source Generator enhancement**: completed collection/dictionary metadata tracking and dedicated serialization branches for dependent complex types, avoiding `List element type ... is not supported in AOT mode`.
- **Regression coverage**: added round-trip serialization regression tests and validated with published AOT binary execution.

### v0.4.1
- **Paged-count enhancement**: added `Find(..., skip, limit, out totalCount)` to return page data and total count in one query call.
- **Query experience enhancement**: added `Query().Count(out totalCount)` extension to get total count and paged result together in `Skip/Take` pipelines.

### v0.4.0
- **Dependency reduction**: removed external dependencies `Microsoft.IO.RecyclableMemoryStream`, `System.IO.Hashing`, and `System.IO.Pipelines`, replaced with built-in minimal compatible implementations.
- **Performance optimization**: reduced intermediate allocations (for example `ToArray`) and optimized batch insert and non-index full-scan query paths.

### v0.3.2
- **Query API enhancement**: added `Find` overloads for more flexible query invocation.

### v0.3.1
- **Concurrency consistency fix**: added collection-level write serialization to eliminate index conflicts and commit races under concurrent writes.
- **Stronger error propagation**: critical data-safety paths (for example `Flush`, page switch, fallback-to-new-page failures) now throw instead of being swallowed.
- **Scan-path optimization**: raw scan moved from full-collection complete snapshot to compact per-page snapshot (`Snapshot(false)`), reducing extra scan overhead.
- **Observability**: added `TinyDbOptions.Logger` callback with `TinyDbLogLevel`-based structured levels.

### v0.3.0
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
