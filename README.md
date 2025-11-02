# TinyDb

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

⚠️ **早期测试版本警告**
这是一个早期测试版本，不建议在生产环境中使用。如果要在生产环境使用，请进行充分的测试。

## 什么是 TinyDb？

TinyDb 是一个轻量级的、AOT兼容的单文件NoSQL数据库，专为.NET应用程序设计。提供完整的事务支持、安全保护和元数据管理功能。

### 版本状态：v0.1.0
- ✅ **基本CRUD操作**：创建、读取、更新、删除
- ✅ **AOT兼容性**：100%测试通过率（470/470测试）
- ✅ **LINQ查询支持**：完整的查询功能
- ✅ **事务支持**：ACID事务和回滚机制
- ✅ **安全保护**：数据库级密码保护系统
- ✅ **元数据管理**：动态UI生成支持
- ✅ **索引系统**：高性能数据检索
- ⚠️ **功能限制**：某些高级功能可能尚未完善

## 核心特性

### 🔐 数据库安全
- **密码保护**: 使用PBKDF2+SHA256加密算法
- **访问控制**: 自动验证数据库访问权限
- **Option配置**: 简化的密码配置API
- **密码管理**: 支持密码设置、更改、移除

### 📊 元数据系统
- **实体元数据**: 自动分析实体结构
- **UI生成**: 支持动态界面生成
- **字段属性**: 丰富的属性配置选项
- **密码字段**: 专门的密码字段元数据

### 🚀 高性能存储
- **单文件设计**: 简化部署和管理
- **页面管理**: 高效的存储空间利用
- **索引优化**: 快速数据检索
- **大文档支持**: 处理大型数据对象

## 快速开始

### 1. 安装

```bash
dotnet add package TinyDb --version 0.1.0
```

### 2. 定义实体

```csharp
using TinyDb.Attributes;
using TinyDb.Bson;

[Entity("products")]
public class Product
{
    [Id]
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();

    public string Name { get; set; } = string.Empty;
    public decimal Price { get; set; }
    public string Category { get; set; } = string.Empty;
    public int Stock { get; set; }
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
}
```

### 3. 基本使用

```csharp
using TinyDb.Core;
using TinyDb.Collections;

// 创建数据库
using var engine = new TinyDbEngine("mydb.db");
var products = engine.GetCollection<Product>("products");

// 插入数据
var product = new Product
{
    Name = "超薄笔记本",
    Price = 6999.99m,
    Category = "电子产品",
    Stock = 50
};

var id = products.Insert(product);
Console.WriteLine($"插入产品ID: {id}");

// 查询数据
var allProducts = products.FindAll().ToList();
var expensiveProducts = products.Find(p => p.Price > 1000).ToList();

// 更新数据
var updateProduct = products.Find(p => p.Name == "超薄笔记本").FirstOrDefault();
if (updateProduct != null)
{
    updateProduct.Stock = 45;
    products.Update(updateProduct);
}

// 删除数据
var deleteProduct = products.Find(p => p.Name == "无线鼠标").FirstOrDefault();
if (deleteProduct != null)
{
    products.Delete(deleteProduct.Id);
}
```

### 4. 安全数据库使用

```csharp
using TinyDb.Core;

// 创建受密码保护的数据库
var options = new TinyDbOptions
{
    Password = "SecurePassword123!",
    DatabaseName = "SecureDB"
};

using var secureEngine = new TinyDbEngine("secure.db", options);
var users = secureEngine.GetCollection<User>("users");

// 后续访问需要提供正确密码
using var existingEngine = new TinyDbEngine("secure.db",
    new TinyDbOptions { Password = "SecurePassword123!" });
```

### 5. 高级配置

```csharp
var advancedOptions = new TinyDbOptions
{
    Password = "AdvancedPass123!",
    DatabaseName = "AdvancedDB",
    PageSize = 8192,
    CacheSize = 2000,
    EnableJournaling = true,
    Timeout = TimeSpan.FromMinutes(5)
};

using var advancedEngine = new TinyDbEngine("advanced.db", advancedOptions);
```

## 运行演示

项目包含完整的演示程序：

```bash
dotnet run --project TinyDb.Demo
```

演示程序将展示：
- 基本CRUD操作
- 元数据系统功能
- 数据库安全保护
- 真实的性能数据
- 数据库统计信息

## 真实测试数据

基于实际运行结果（470个测试全部通过）：

### 基本操作性能
```
✅ 自动创建主键索引: pk__id on _id (Unique=True)
1. 创建产品记录:
   ✅ 插入产品: 超薄笔记本 (ID: d7c60169c13367a4f6d38271)
   ✅ 插入产品: 无线鼠标 (ID: d7c60169c13367a4f6d48271)

2. 查询产品记录:
   📊 总产品数: 2
   🔌 电子产品数: 2
   💰 高价产品(>1000元): 1

3. 更新产品记录:
   更新前: 超薄笔记本 - 库存: 50, 价格: 6999.99
   更新后: 超薄笔记本 - 库存: 45, 价格: 6499.99

4. 删除产品记录:
   🗑️ 删除产品: 无线鼠标
   ✅ 删除成功
   📊 剩余产品数: 1

数据库统计: Database[TinyDb]: 2/1 pages, 1 collections, 32,768 bytes
```

### 安全功能演示
```
=== TinyDb Option方式密码保护演示 ===

1. 使用Option创建受密码保护的数据库
✅ 成功创建受密码保护的数据库
🔑 密码: MySecurePassword123!
📊 数据库名: SecureOptionDB

2. 验证密码保护功能
✅ 正确密码访问成功 - 用户数: 1
✅ 错误密码被正确拒绝
✅ 未提供密码被正确拒绝
```

## 测试覆盖率

- **总测试数**: 470个测试
- **通过率**: 100%（470/470）
- **AOT兼容性**: 完全兼容
- **测试框架**: TUnit
- **安全测试**: 包含完整的安全功能验证
- **性能测试**: 包含大数据集性能测试

## 架构特性

### 🏗️ 存储引擎
- **页面管理**: 8KB页面大小，高效空间利用
- **WAL日志**: 预写日志保证数据一致性
- **事务支持**: ACID事务完整实现
- **并发控制**: 线程安全的并发访问

### 🔍 查询系统
- **LINQ支持**: 完整的LINQ查询语法
- **索引系统**: B树索引，高性能检索
- **查询优化**: 自动查询计划优化
- **表达式解析**: 强大的表达式树处理

### 🛡️ 安全机制
- **加密存储**: PBKDF2密钥派生
- **访问控制**: 数据库级权限管理
- **时序防护**: 防止时序攻击
- **密码策略**: 可配置的密码强度要求

## 已知限制

### v0.1.0版本限制：
1. **复杂类型序列化**: 某些嵌套复杂对象可能存在序列化问题
2. **LINQ高级功能**: 部分复杂的LINQ操作尚未完全实现
3. **并发限制**: 高并发写入场景需要进一步优化
4. **大数据集**: 超大数据集（>10GB）的性能需要验证

### 不推荐的功能：
- 复杂的深度嵌套对象
- 大量并发写入操作
- 生产环境关键业务（当前版本）
- 超大数据集（>10GB）

## 开发状态

这是一个**早期测试版本**，主要用于：
- 验证核心功能可行性
- 收集用户反馈
- 测试AOT兼容性
- 演示基本使用方法
- 展示安全功能

## 贡献指南

欢迎提交Issue和Pull Request！

### 开发环境要求：
- .NET 9.0
- 支持C#最新语法
- 推荐使用Rider或Visual Studio 2022

### 测试：
```bash
dotnet test                    # 运行所有测试
dotnet test -c Release        # Release模式测试
dotnet publish -c Release -r linux-x64 --self-contained true -p:PublishAot=true  # AOT编译测试
```

### 代码规范：
- 遵循C#编码规范
- 添加适当的XML文档注释
- 确保AOT兼容性
- 编写相应的单元测试

## 许可证

本项目采用 [MIT 许可证](LICENSE)。

© 2025 TinyDb Contributors

### MIT 许可证摘要

✅ **商业使用** - 您可以将此软件用于商业目的
✅ **修改** - 您可以修改此软件
✅ **分发** - 您可以分发此软件
✅ **私人使用** - 您可以私人使用此软件
✅ **Sublicensing** - 您可以对您的修改进行子许可

⚠️ **责任** - 软件按"原样"提供，不提供任何担保
⚠️ **版权** - 必须包含版权声明和许可证文本

## 联系方式

如有问题或建议，请提交GitHub Issue：
- **项目地址**: https://github.com/j4587698/TinyDb
- **问题反馈**: [GitHub Issues](https://github.com/j4587698/TinyDb/issues)

---

**重要提醒**: 本版本为早期测试版本，请在充分测试后再考虑生产使用。