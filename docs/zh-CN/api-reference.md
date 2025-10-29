# SimpleDb API 参考文档

## 概述

本文档提供了 SimpleDb 的完整 API 参考，包括所有主要的类、接口、方法和属性。API 按功能模块组织，便于查找和使用。

## 核心命名空间

### SimpleDb.Core

包含数据库引擎的核心功能。

#### SimpleDbEngine

主要的数据库引擎类，提供数据库管理和集合操作功能。

```csharp
public sealed class SimpleDbEngine : IDisposable
```

**构造函数**

```csharp
public SimpleDbEngine(string filePath, SimpleDbOptions options)
```

**参数**：
- `filePath`：数据库文件路径
- `options`：数据库配置选项

**示例**：
```csharp
var options = new SimpleDbOptions
{
    DatabaseName = "MyDatabase",
    PageSize = 8192,
    CacheSize = 1000
};

using var engine = new SimpleDbEngine("mydb.db", options);
```

**属性**

| 属性 | 类型 | 描述 |
|------|------|------|
| `DatabaseName` | `string` | 数据库名称 |
| `CollectionCount` | `int` | 当前集合数量 |
| `IsDisposed` | `bool` | 是否已释放 |

**方法**

##### 集合管理

```csharp
public ILiteCollection<T> GetCollection<T>(string collectionName)
```

获取指定类型的集合实例。

**参数**：
- `collectionName`：集合名称

**返回值**：集合实例

**示例**：
```csharp
var users = engine.GetCollection<User>("users");
```

```csharp
public bool CollectionExists(string collectionName)
```

检查集合是否存在。

```csharp
public IEnumerable<string> GetCollectionNames()
```

获取所有集合名称。

```csharp
public bool DropCollection(string collectionName)
```

删除指定集合。

##### 数据库操作

```csharp
public DatabaseStatistics GetStatistics()
```

获取数据库统计信息。

```csharp
public void Flush()
```

强制刷新缓存到磁盘。

```csharp
public void Compact()
```

压缩数据库文件。

```csharp
public void Backup(string backupPath)
```

备份数据库到指定路径。

##### 事务管理

```csharp
public ITransaction BeginTransaction()
```

开始新事务。

#### SimpleDbOptions

数据库配置选项类。

```csharp
public sealed class SimpleDbOptions
```

**属性**

| 属性 | 类型 | 默认值 | 描述 |
|------|------|--------|------|
| `DatabaseName` | `string` | "SimpleDb" | 数据库名称 |
| `PageSize` | `uint` | 8192 | 页面大小（字节） |
| `CacheSize` | `int` | 1000 | 页面缓存大小 |
| `EnableJournaling` | `bool` | true | 是否启用日志 |
| `WriteConcern` | `WriteConcern` | `Synced` | 写入关注级别 |
| `BackgroundFlushInterval` | `TimeSpan` | 100ms | 后台刷盘间隔 |
| `JournalFlushDelay` | `TimeSpan` | 10ms | 日志刷新延迟 |
| `Timeout` | `TimeSpan` | 30s | 操作超时时间 |
| `ReadOnly` | `bool` | false | 是否只读模式 |
| `StrictMode` | `bool` | true | 是否启用严格模式 |

**方法**

```csharp
public void Validate()
```

验证配置选项的有效性。

```csharp
public SimpleDbOptions Clone()
```

克隆配置选项。

#### WriteConcern

写入关注级别枚举。

```csharp
public enum WriteConcern
{
    /// <summary>
    /// 不等待写入确认
    /// </summary>
    None,

    /// <summary>
    /// 等待日志写入确认
    /// </summary>
    Journaled,

    /// <summary>
    /// 等待数据和日志都写入确认
    /// </summary>
    Synced
}
```

### SimpleDb.Collections

包含集合相关的接口和类。

#### ILiteCollection<T>

集合接口，提供文档 CRUD 操作。

```csharp
public interface ILiteCollection<T> where T : class
```

**属性**

| 属性 | 类型 | 描述 |
|------|------|------|
| `CollectionName` | `string` | 集合名称 |

**查询方法**

```csharp
public IQueryable<T> AsQueryable()
```

返回可查询的 LINQ 提供程序。

```csharp
public IEnumerable<T> FindAll()
```

查询所有文档。

```csharp
public IEnumerable<T> Find(Expression<Func<T, bool>> predicate)
```

按条件查询文档。

```csharp
public T FindById(ObjectId id)
```

按 ID 查询文档。

```csharp
public T FindOne(Expression<Func<T, bool>> predicate)
```

查询单个文档。

```csharp
public int Count(Expression<Func<T, bool>>? predicate = null)
```

统计文档数量。

**插入方法**

```csharp
public ObjectId Insert(T entity)
```

插入单个文档，返回文档 ID。

```csharp
public int Insert(IEnumerable<T> entities)
```

批量插入文档，返回插入数量。

**更新方法**

```csharp
public bool Update(T entity)
```

更新单个文档。

```csharp
public int UpdateMany(Expression<Func<T, bool>> predicate, T update)
```

批量更新文档。

```csharp
public int UpdateMany(Expression<Func<T, bool>> predicate, UpdateBuilder<T> update)
```

使用更新构建器批量更新。

**删除方法**

```csharp
public bool Delete(ObjectId id)
```

按 ID 删除文档。

```csharp
public int DeleteMany(Expression<Func<T, bool>> predicate)
```

按条件批量删除。

```csharp
public int DeleteAll()
```

删除所有文档。

**索引方法**

```csharp
public void EnsureIndex<TKey>(Expression<Func<T, TKey>> keySelector, bool unique = false)
```

创建单字段索引。

```csharp
public void EnsureIndex(Expression<Func<T, object>> keySelector, bool unique = false)
```

创建复合索引。

```csharp
public IEnumerable<IndexInfo> GetIndexes()
```

获取所有索引信息。

```csharp
public void DropIndex(string name)
```

删除指定索引。

**聚合方法**

```csharp
public TResult Aggregate<TResult>(Expression<Func<IGrouping<T, TKey>, TResult>> resultSelector)
```

执行聚合操作。

```csharp
public TAverage Average<TAverage>(Expression<Func<T, TAverage>> selector)
```

计算平均值。

```csharp
public TMax Max<TMax>(Expression<Func<T, TMax>> selector)
```

计算最大值。

```csharp
public TMin Min<TMin>(Expression<Func<T, TMin>> selector)
```

计算最小值。

```csharp
public TSum Sum<TSum>(Expression<Func<T, TSum>> selector)
```

计算总和。

#### DocumentCollection<T>

文档集合的默认实现。

```csharp
public sealed class DocumentCollection<T> : ILiteCollection<T> where T : class
```

继承 `ILiteCollection<T>` 接口的所有方法。

#### UpdateBuilder<T>

更新构建器，用于构建复杂的更新操作。

```csharp
public sealed class UpdateBuilder<T>
```

**静态方法**

```csharp
public static UpdateBuilder<T> Set<TField>(Expression<Func<T, TField>> field, TField value)
```

设置字段值。

```csharp
public static UpdateBuilder<T> Inc<TField>(Expression<Func<T, TField>> field, TField value)
```

增加字段值。

```csharp
public static UpdateBuilder<T> Push<TField>(Expression<Func<T, IEnumerable<TField>>> field, TField value)
```

向数组字段添加元素。

```csharp
public static UpdateBuilder<T> Pull<TField>(Expression<Func<T, IEnumerable<TField>>> field, TField value)
```

从数组字段移除元素。

**示例**：
```csharp
users.UpdateMany(u => u.Age < 25,
    UpdateBuilder<User>.Set(u => u.Status, "Young")
                   .Inc(u => u.LoginCount, 1)
                   .Push(u => u.Tags, "new"));
```

### SimpleDb.Bson

包含 BSON 数据类型和相关操作。

#### ObjectId

BSON ObjectId 类型，用于唯一标识文档。

```csharp
public sealed class ObjectId : IComparable<ObjectId>, IEquatable<ObjectId>
```

**构造函数**

```csharp
public ObjectId()
public ObjectId(string value)
public ObjectId(byte[] bytes)
```

**静态方法**

```csharp
public static ObjectId NewObjectId()
public static ObjectId Empty { get; }
public static bool TryParse(string value, out ObjectId objectId)
```

**属性**

| 属性 | 类型 | 描述 |
|------|------|------|
| `Timestamp` | `int` | 时间戳 |
| `Machine` | `int` | 机器标识 |
| `Pid` | `short` | 进程 ID |
| `Increment` | `int` | 递增计数器 |

**方法**

```csharp
public string ToString()
public byte[] ToByteArray()
public int CompareTo(ObjectId other)
public bool Equals(ObjectId other)
```

#### BsonValue

BSON 值的基类。

```csharp
public abstract class BsonValue
```

**属性**

| 属性 | 类型 | 描述 |
|------|------|------|
| `BsonType` | `BsonType` | BSON 类型 |
| `IsNull` | `bool` | 是否为空值 |

**具体子类**

- `BsonString`：字符串值
- `BsonInt32`：32 位整数
- `BsonInt64`：64 位整数
- `BsonDouble`：双精度浮点数
- `BsonBoolean`：布尔值
- `BsonDateTime`：日期时间
- `BsonObjectId`：ObjectId
- `BsonArray`：数组
- `BsonDocument`：文档
- `BsonNull`：空值

#### BsonDocument

BSON 文档类型。

```csharp
public sealed class BsonDocument : BsonValue, IEnumerable<KeyValuePair<string, BsonValue>>
```

**构造函数**

```csharp
public BsonDocument()
public BsonDocument(Dictionary<string, BsonValue> dictionary)
```

**索引器**

```csharp
public BsonValue this[string key] { get; set; }
```

**方法**

```csharp
public void Add(string key, BsonValue value)
public bool ContainsKey(string key)
public bool TryGetValue(string key, out BsonValue value)
public bool Remove(string key)
public void Clear()
```

### SimpleDb.Attributes

包含实体和属性标记特性。

#### EntityAttribute

标记实体类。

```csharp
[AttributeUsage(AttributeTargets.Class)]
public sealed class EntityAttribute : Attribute
```

**属性**

| 属性 | 类型 | 描述 |
|------|------|------|
| `CollectionName` | `string?` | 集合名称 |
| `IdProperty` | `string?` | ID 属性名称 |

**示例**：
```csharp
[Entity("users")]
public class User
{
    public ObjectId Id { get; set; }
    public string Name { get; set; }
}
```

#### IndexAttribute

标记索引属性。

```csharp
[AttributeUsage(AttributeTargets.Property)]
public sealed class IndexAttribute : Attribute
```

**属性**

| 属性 | 类型 | 默认值 | 描述 |
|------|------|--------|------|
| `Unique` | `bool` | false | 是否唯一索引 |
| `Priority` | `int` | 0 | 索引优先级 |
| `Sparse` | `bool` | false | 是否稀疏索引 |

**示例**：
```csharp
[Index(Unique = true, Priority = 1)]
public string Email { get; set; }
```

#### IdAttribute

标记 ID 属性。

```csharp
[AttributeUsage(AttributeTargets.Property)]
public sealed class IdAttribute : Attribute
```

#### BsonIgnoreAttribute

标记应忽略的属性。

```csharp
[AttributeUsage(AttributeTargets.Property)]
public sealed class BsonIgnoreAttribute : Attribute
```

### SimpleDb.Core.Transaction

包含事务相关的类和接口。

#### ITransaction

事务接口。

```csharp
public interface ITransaction : IDisposable
```

**属性**

| 属性 | 类型 | 描述 |
|------|------|------|
| `TransactionId` | `Guid` | 事务 ID |
| `IsActive` | `bool` | 是否活跃 |
| `State` | `TransactionState` | 事务状态 |

**方法**

```csharp
public void Commit()
public void Rollback()
public Guid CreateSavepoint(string name)
public void RollbackToSavepoint(Guid savepointId)
public void ReleaseSavepoint(Guid savepointId)
```

#### TransactionState

事务状态枚举。

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

包含存储相关的类。

#### DatabaseHeader

数据库头部信息结构。

```csharp
public struct DatabaseHeader
```

**属性**

| 属性 | 类型 | 描述 |
|------|------|------|
| `Magic` | `uint` | 魔数 |
| `DatabaseVersion` | `uint` | 数据库版本 |
| `PageSize` | `uint` | 页面大小 |
| `TotalPages` | `uint` | 总页面数 |
| `UsedPages` | `uint` | 已使用页面数 |
| `DatabaseName` | `string` | 数据库名称 |
| `CreatedAt` | `long` | 创建时间戳 |
| `ModifiedAt` | `long` | 修改时间戳 |

**方法**

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

页面类型枚举。

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

包含工具类。

#### LRUCache<TKey, TValue>

线程安全的 LRU 缓存实现。

```csharp
public sealed class LRUCache<TKey, TValue> where TKey : notnull
```

**构造函数**

```csharp
public LRUCache(int capacity)
```

**属性**

| 属性 | 类型 | 描述 |
|------|------|------|
| `Capacity` | `int` | 缓存容量 |
| `Count` | `int` | 当前项数量 |
| `Hits` | `long` | 命中次数 |
| `Misses` | `long` | 未命中次数 |
| `HitRatio` | `double` | 命中率 |

**方法**

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

## 异常类

### StorageException

存储相关异常。

```csharp
public class StorageException : Exception
```

### TransactionException

事务相关异常。

```csharp
public class TransactionException : Exception
```

### ValidationException

数据验证异常。

```csharp
public class ValidationException : Exception
```

## 配置和统计类

### DatabaseStatistics

数据库统计信息。

```csharp
public sealed class DatabaseStatistics
```

**属性**

| 属性 | 类型 | 描述 |
|------|------|------|
| `FileSize` | `long` | 文件大小（字节） |
| `TotalPages` | `uint` | 总页面数 |
| `UsedPages` | `uint` | 已使用页面数 |
| `FreePages` | `uint` | 空闲页面数 |
| `CollectionCount` | `int` | 集合数量 |
| `CacheHitRatio` | `double` | 缓存命中率 |

### IndexInfo

索引信息。

```csharp
public sealed class IndexInfo
```

**属性**

| 属性 | 类型 | 描述 |
|------|------|------|
| `Name` | `string` | 索引名称 |
| `Fields` | `string[]` | 索引字段 |
| `IsUnique` | `bool` | 是否唯一 |
| `IsSparse` | `bool` | 是否稀疏 |

### LockManagerStatistics

锁管理器统计信息。

```csharp
public sealed class LockManagerStatistics
```

**属性**

| 属性 | 类型 | 描述 |
|------|------|------|
| `ActiveLockCount` | `int` | 活跃锁数量 |
| `PendingLockCount` | `int` | 等待锁数量 |
| `LockTypeCounts` | `Dictionary<LockType, int>` | 锁类型统计 |
| `BucketCount` | `int` | 锁桶数量 |
| `DefaultTimeout` | `TimeSpan` | 默认超时时间 |

### TransactionManagerStatistics

事务管理器统计信息。

```csharp
public sealed class TransactionManagerStatistics
```

**属性**

| 属性 | 类型 | 描述 |
|------|------|------|
| `ActiveTransactionCount` | `int` | 活跃事务数量 |
| `MaxTransactions` | `int` | 最大事务数量 |
| `TransactionTimeout` | `TimeSpan` | 事务超时时间 |
| `AverageOperationCount` | `double` | 平均操作数量 |
| `TotalOperations` | `int` | 总操作数量 |
| `AverageTransactionAge` | `double` | 平均事务年龄 |
| `States` | `Dictionary<TransactionState, int>` | 事务状态统计 |

## 扩展方法

### QueryExtensions

查询扩展方法。

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

集合扩展方法。

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

## 线程安全性

### 线程安全的组件

- `SimpleDbEngine`：线程安全，支持多线程并发访问
- `ILiteCollection<T>`：线程安全，支持并发读写
- `LRUCache<TKey, TValue>`：线程安全的 LRU 缓存
- `LockManager`：线程安全的锁管理器
- `TransactionManager`：线程安全的事务管理器

### 注意事项

1. **事务隔离**：每个事务都有独立的上下文，事务内的操作不会影响其他事务
2. **锁机制**：系统自动管理锁的获取和释放，避免死锁
3. **缓存一致性**：多线程环境下缓存数据保持一致性
4. **资源管理**：使用 `using` 语句确保资源正确释放

## 性能考虑

### 批量操作

```csharp
// 推荐：批量插入
var users = new List<User>();
for (int i = 0; i < 1000; i++)
{
    users.Add(new User { /* ... */ });
}
collection.Insert(users);

// 避免：循环单条插入
for (int i = 0; i < 1000; i++)
{
    collection.Insert(new User { /* ... */ });
}
```

### 索引优化

```csharp
// 为常用查询字段创建索引
collection.EnsureIndex(u => u.Email, unique: true);
collection.EnsureIndex(u => new { u.Name, u.Age });
```

### 缓存配置

```csharp
var options = new SimpleDbOptions
{
    CacheSize = Environment.ProcessorCount * 1000, // 基于处理器核心数
    PageSize = 16384, // 适合大文档的页面大小
    BackgroundFlushInterval = TimeSpan.FromMilliseconds(50)
};
```

## 示例代码

### 完整的 CRUD 操作示例

```csharp
using SimpleDb.Core;
using SimpleDb.Collections;
using SimpleDb.Attributes;
using SimpleDb.Bson;

// 定义实体
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

// 使用数据库
var options = new SimpleDbOptions
{
    DatabaseName = "ProductDB",
    EnableJournaling = true,
    WriteConcern = WriteConcern.Synced
};

using var engine = new SimpleDbEngine("products.db", options);
var products = engine.GetCollection<Product>("products");

// 创建索引
products.EnsureIndex(p => p.Sku, unique: true);
products.EnsureIndex(p => p.Name);
products.EnsureIndex(p => p.Tags, sparse: true);

// 插入产品
var product = new Product
{
    Sku = "SKU001",
    Name = "Laptop",
    Price = 999.99m,
    Tags = new[] { "electronics", "computer" },
    IsActive = true
};

var id = products.Insert(product);
Console.WriteLine($"插入产品 ID: {id}");

// 查询产品
var laptop = products.FindById(id);
if (laptop != null)
{
    Console.WriteLine($"找到产品: {laptop.Name}, 价格: {laptop.Price}");
}

// 条件查询
var activeProducts = products.Find(p => p.IsActive && p.Price > 100)
                           .OrderBy(p => p.Name)
                           .Take(10)
                           .ToList();

// 更新产品
laptop.Price = 899.99m;
products.Update(laptop);

// 批量更新
products.UpdateMany(
    p => p.Tags.Contains("electronics"),
    UpdateBuilder<Product>.Set(p => p.IsActive, true)
                       .Inc(p => p.ViewCount, 1));

// 删除产品
products.Delete(laptop.Id);

// 聚合查询
var avgPrice = products.Average(p => p.Price);
var electronicsCount = products.Count(p => p.Tags.Contains("electronics"));

// 统计信息
var stats = engine.GetStatistics();
Console.WriteLine($"数据库大小: {stats.FileSize} 字节");
Console.WriteLine($"缓存命中率: {stats.CacheHitRatio:P1}");
```

这个 API 参考文档涵盖了 SimpleDb 的所有主要功能，为开发者提供了完整的接口说明和使用示例。