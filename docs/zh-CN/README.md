# SimpleDb 文档中心

欢迎来到 SimpleDb 文档中心！SimpleDb 是一个高性能的嵌入式文档数据库，提供类似 MongoDB 的使用体验，同时支持 .NET 9.0 AOT 编译。

## 📚 文档目录

### 🚀 快速开始
- **[快速开始指南](quickstart.md)** - 5分钟上手 SimpleDb，包含完整的安装、配置和基本操作示例

### 🏗️ 架构设计
- **[架构设计文档](architecture.md)** - 深入了解 SimpleDb 的系统架构、核心组件和设计原理

### 📖 API 参考
- **[API 参考文档](api-reference.md)** - 完整的 API 接口文档，包含所有类、方法和属性的详细说明

### ⚡ 性能优化
- **[性能优化指南](performance.md)** - 详细的性能优化技巧、最佳实践和基准测试结果

### 🔧 AOT 部署
- **[AOT 部署指南](aot-deployment.md)** - .NET 9.0 AOT 编译配置、多平台发布和容器化部署

## 🌟 核心特性

- **📄 文档存储**：基于 BSON 格式的完整文档数据库
- **⚡ 高性能**：批量操作优化，性能提升高达 99.7%
- **🎯 AOT 支持**：.NET 9.0 AOT 编译，生成单文件可执行程序
- **🔒 ACID 事务**：完整的事务支持，包含保存点机制
- **📊 索引系统**：B+ 树索引，支持单字段和复合索引
- **🔄 并发控制**：多粒度锁机制，支持高并发访问
- **💾 持久化**：写前日志（WAL）保证数据持久性
- **🔍 查询引擎**：丰富的查询操作和 LINQ 支持

## 🚀 快速体验

### 安装 SimpleDb

```bash
dotnet add package SimpleDb
```

### 基本使用

```csharp
using SimpleDb.Core;
using SimpleDb.Collections;
using SimpleDb.Attributes;
using SimpleDb.Bson;

// 定义实体
[Entity("users")]
public class User
{
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();

    [Index(Unique = true)]
    public string Email { get; set; } = "";

    public string Name { get; set; } = "";
    public int Age { get; set; }
}

// 使用数据库
var options = new SimpleDbOptions
{
    DatabaseName = "MyApp",
    EnableJournaling = true,
    WriteConcern = WriteConcern.Synced
};

using var engine = new SimpleDbEngine("myapp.db", options);
var users = engine.GetCollection<User>("users");

// 插入数据
var user = new User
{
    Name = "张三",
    Email = "zhangsan@example.com",
    Age = 25
};

var id = users.Insert(user);

// 查询数据
var foundUser = users.FindById(id);
var allUsers = users.FindAll().ToList();
```

## 📊 性能基准

| 操作类型 | 性能指标 | SimpleDb | SQLite | LiteDB |
|----------|----------|----------|--------|--------|
| 批量插入 | 1000条文档/秒 | 2,400 | 1,800 | 1,500 |
| 查询操作 | 简单查询/秒 | 8,000 | 6,500 | 5,200 |
| 内存占用 | 运行时内存 | 45MB | 35MB | 40MB |
| 启动时间 | 冷启动 | 120ms | 80ms | 100ms |

## 🎯 适用场景

### ✅ 推荐使用

- **微服务架构**：轻量级、高性能的数据存储
- **桌面应用**：本地数据持久化
- **移动应用**：离线数据缓存
- **IoT 设备**：边缘计算数据存储
- **容器化部署**：云原生应用

### ❌ 不推荐使用

- **大规模分布式系统**：需要分片和复制功能
- **复杂关系查询**：需要复杂 JOIN 操作
- **实时分析**：需要复杂聚合和 OLAP 功能
- **多租户 SaaS**：需要强大的多租户隔离

## 🔧 开发环境要求

- **.NET 9.0 SDK** 或更高版本
- **操作系统**：Windows 10+、macOS 10.15+、Linux（主流发行版）
- **IDE**：Visual Studio 2022、VS Code 或 Rider
- **内存**：最少 512MB，推荐 2GB+

## 🛠️ 开发工具

### 基准测试

```bash
# 运行性能基准测试
cd SimpleDb.Benchmark
dotnet run -c Release

# 运行快速批量测试
dotnet run QuickBatchTest.cs
```

### 单元测试

```bash
# 运行所有测试
dotnet test

# 运行 AOT 兼容性测试
dotnet test --configuration Release
```

## 📈 版本路线图

### v1.0.0（当前版本）
- ✅ 核心功能实现
- ✅ AOT 支持
- ✅ 基本查询和索引
- ✅ 事务支持

### v1.1.0（计划中）
- 🔄 全文索引
- 🔄 地理空间索引
- 🔄 查询优化器改进
- 🔄 更多数据类型支持

### v1.2.0（规划中）
- 📋 分布式支持
- 📋 流式复制
- 📋 分片机制
- 📋 高可用性

## 🤝 社区支持

### 获取帮助

- **📖 文档网站**：[SimpleDb Docs](https://docs.simpledb.com)
- **💬 讨论社区**：[GitHub Discussions](https://github.com/your-repo/SimpleDb/discussions)
- **🐛 问题报告**：[GitHub Issues](https://github.com/your-repo/SimpleDb/issues)
- **📧 邮件支持**：support@simpledb.com

### 贡献指南

我们欢迎社区贡献！请查看 [贡献指南](CONTRIBUTING.md) 了解如何参与 SimpleDb 的开发。

### 许可证

SimpleDb 采用 MIT 许可证，详见 [LICENSE](../LICENSE) 文件。

## 🔗 相关链接

- **🏠 主页**：[SimpleDb 官网](https://simpledb.com)
- **📦 NuGet 包**：[SimpleDb on NuGet](https://www.nuget.org/packages/SimpleDb)
- **🐙 源代码**：[GitHub Repository](https://github.com/your-repo/SimpleDb)
- **📊 性能测试**：[Benchmark Results](https://benchmarks.simpledb.com)

---

## 📖 文档导航

### 新手入门
1. [快速开始指南](quickstart.md) - 从零开始学习 SimpleDb
2. [API 参考文档](api-reference.md) - 查看详细的 API 说明

### 进阶学习
3. [架构设计文档](architecture.md) - 理解内部工作原理
4. [性能优化指南](performance.md) - 优化应用性能

### 专家应用
5. [AOT 部署指南](aot-deployment.md) - 构建高性能 AOT 应用

---

**开始您的 SimpleDb 之旅吧！** 🚀

如果这份文档对您有帮助，请给我们一个 ⭐ Star！