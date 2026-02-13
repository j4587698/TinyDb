<p align="center">
  <h1 align="center">TinyDb</h1>
  <p align="center">
    <strong>轻量级 AOT 兼容的嵌入式 NoSQL 数据库</strong>
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

## AOT 优先声明

TinyDb 是一个 **AOT 优先**、受 **LiteDB** 启发的单文件嵌入式 NoSQL 数据库：

- **以 AOT 为准**：一切功能以 NativeAOT 编译后的行为为准，不提供任何回退逻辑。
- **开发一致性**：保障开发期（非 AOT/JIT）与 AOT 发布后的行为一致。
- **非 AOT 场景建议**：如果你的应用不需要 NativeAOT，推荐直接使用 LiteDB（生态更成熟、特性更完整）。

## 特性亮点

- **单文件数据库** - 所有数据存储在一个文件中，部署简单
- **100% AOT 兼容** - 完全支持 Native AOT 编译，无反射依赖
- **源代码生成器** - 编译时生成序列化代码，零运行时开销
- **LINQ 查询** - 完整的 LINQ 支持，类型安全的查询体验
- **ACID 事务** - 完整的事务支持，保证数据一致性
- **密码保护** - 内置数据库级别加密保护
- **高性能索引** - B+树索引，支持快速数据检索
- **跨平台** - 支持 Windows、Linux、macOS

## 快速开始

### 安装

```bash
dotnet add package TinyDb
```

### 定义实体

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

    [BsonIgnore]  // 此属性不会被序列化
    public string? TempToken { get; set; }

    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
}
```

### 基本 CRUD 操作

```csharp
using TinyDb.Core;

// 创建/打开数据库
using var db = new TinyDbEngine("myapp.db");
var users = db.GetCollection<User>();

// 插入
var user = new User { Name = "张三", Email = "zhangsan@example.com", Age = 25 };
users.Insert(user);

// 查询
var found = users.Find(u => u.Age > 20).ToList();
var one = users.FindOne(u => u.Email == "zhangsan@example.com");

// 更新
user.Age = 26;
users.Update(user);

// 删除
users.Delete(user.Id);
```

### LINQ 查询

```csharp
// 复杂查询
var results = users.Query()
    .Where(u => u.Age >= 18 && u.Age <= 30)
    .Where(u => u.Name.StartsWith("张"))
    .OrderByDescending(u => u.CreatedAt)
    .Skip(10)
    .Take(20)
    .ToList();

// 聚合查询
var count = users.Query().Where(u => u.Age > 20).Count();
var exists = users.Query().Any(u => u.Email.Contains("@gmail.com"));
```

### 密码保护

```csharp
// 创建加密数据库
var options = new TinyDbOptions { Password = "MySecurePassword123!" };
using var secureDb = new TinyDbEngine("secure.db", options);

// 访问加密数据库
using var db = new TinyDbEngine("secure.db", new TinyDbOptions { Password = "MySecurePassword123!" });
```

### 事务支持

```csharp
using var db = new TinyDbEngine("myapp.db");
var users = db.GetCollection<User>();
var orders = db.GetCollection<Order>();

// 开启事务
db.BeginTransaction();
try
{
    users.Insert(new User { Name = "新用户" });
    orders.Insert(new Order { UserId = "...", Amount = 99.99m });
    db.Commit();  // 提交事务
}
catch
{
    db.Rollback();  // 回滚事务
    throw;
}
```

## 高级特性

### 属性标注

| 属性 | 说明 |
|------|------|
| `[Entity("集合名")]` | 标记实体类，指定集合名称 |
| `[Id]` | 标记主键属性 |
| `[Index]` | 创建索引 |
| `[Index(Unique = true)]` | 创建唯一索引 |
| `[BsonIgnore]` | 序列化时忽略此属性 |
| `[BsonField("字段名")]` | 自定义 BSON 字段名 |

### 支持的数据类型

- **基本类型**: `int`, `long`, `double`, `decimal`, `bool`, `string`, `DateTime`, `Guid`
- **可空类型**: `int?`, `DateTime?` 等
- **集合类型**: `List<T>`, `T[]`, `Dictionary<string, T>`
- **嵌套对象**: 支持复杂对象嵌套
- **特殊类型**: `ObjectId`, `BsonDocument`

### 配置选项

```csharp
var options = new TinyDbOptions
{
    Password = "密码",           // 数据库密码（可选）
    PageSize = 8192,            // 页面大小（默认 8KB）
    CacheSize = 1000,           // 缓存页数
    EnableJournaling = true,    // 启用 WAL 日志
    Timeout = TimeSpan.FromMinutes(5)  // 操作超时时间
};
```

## 性能数据

基于深度重构（响应式组提交、锁瘦身、零分配序列化）后的实际运行结果：

| 操作 | 性能 | 内存分配 | 说明 |
|------|------|------|------|
| **单条插入** | ~270 ops/s | ~5 KB/条 | Journaled 模式，含 3 个活跃索引 |
| **极速插入** | ~7600 ops/s | ~4 KB/条 | Journaled 模式，无额外索引 |
| **批量插入** | ~4300 ops/s | ~4 KB/条 | 1000 条批量写入优化 |
| **主键查询** | ~5000 ops/s | < 7 KB/次 | B+树主键索引快速查找 |
| **索引查询** | ~2500 ops/s | < 8 KB/次 | 复杂 LINQ 条件索引过滤 |

> **注：** 以上数据在 AMD EPYC 7763 2.44GHz 环境下测得。通过 Pool 缓冲池技术，内存分配较旧版本降低了 **90%** 以上。

## 版本历史

### v0.3.0 (当前)
- **性能飞跃**：通过 Span 与池化缓冲区（Pooled Buffers）实现零/低分配序列化，内存分配降低 90%+
- **核心重构**：引入高性能响应式组提交（Reactive Group Commit）与锁剥离（Lock Stripping）技术
- **元数据重构**：深度重构元数据管理系统，提升架构清晰度与扩展性
- **查询优化**：支持谓词下推（Predicate Push-down）与排序/分页下推，提升 BSON 扫描效率
- **异步支持**：新增真正的异步读取 API 接口

### v0.2.0
- 完善 `[BsonIgnore]` 属性支持
- 新增 AOT 兼容的序列化测试
- 修复源生成器相关问题
- 2610 个测试全部通过

### v0.1.5
- 完善 DbRef 引用支持
- 增强嵌套类 Entity 支持
- 性能优化

## 项目结构

```
TinyDb/
├── TinyDb/                    # 核心库
├── TinyDb.SourceGenerator/    # 源代码生成器
├── TinyDb.Tests/              # 测试项目
├── TinyDb.Demo/               # 演示项目
└── TinyDb.UI/                 # 可视化管理工具
```

## 运行演示

```bash
dotnet run --project TinyDb.Demo
```

## 开发环境

- .NET 8.0 / 9.0 / 10.0
- C# 12+
- 推荐 IDE: Rider / Visual Studio 2022

## 贡献

欢迎提交 Issue 和 Pull Request！

```bash
# 运行测试
dotnet test

# AOT 编译测试
dotnet publish -c Release -r win-x64 --self-contained -p:PublishAot=true
```

## 许可证

[MIT License](LICENSE) - 可自由用于商业项目

---

<p align="center">
  <sub>如果这个项目对你有帮助，请给一个 ⭐ Star！</sub>
</p>
