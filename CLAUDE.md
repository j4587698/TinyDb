# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目定位

TinyDb 是 **AOT 优先**、受 LiteDB 启发的单文件嵌入式 NoSQL 数据库。**一切功能以 NativeAOT 编译后的行为为准，不提供反射回退**。这一约束渗透到所有设计决策：序列化由源生成器在编译期完成，运行时不依赖反射。修改序列化、实体或元数据相关代码时，必须假定运行环境为 AOT（无 JIT、无运行时反射可用）。

## 常用命令

```bash
# 构建整个解决方案（多目标：net8.0/net9.0/net10.0）
dotnet build TinyDb.sln -c Release

# 运行全部测试（TUnit + Microsoft.Testing.Platform，自动覆盖所有目标框架）
dotnet test --project TinyDb.Tests/TinyDb.Tests.csproj -c Release

# 运行单个测试：用 TUnit 树状过滤器（按类/方法名）
dotnet test --project TinyDb.Tests/TinyDb.Tests.csproj --treenode-filter "/*/*/BsonDocumentTests/*"
dotnet test --project TinyDb.Tests/TinyDb.Tests.csproj --treenode-filter "/*/*/*/BsonDocument_Should_Add_And_Get_Values"

# AOT 发布验证（关键：行为以此为准；ILCompiler bug 使 AOT 仅在 net10.0 启用）
dotnet publish TinyDb.Tests/TinyDb.Tests.csproj -c Release -f net10.0 -r linux-x64 --self-contained true
./TinyDb.Tests/bin/Release/net10.0/linux-x64/publish/TinyDb.Tests   # 直接跑 AOT 二进制做回归

# 运行演示
dotnet run --project TinyDb.Demo

# 打 NuGet 包（含源生成器 analyzer + AOT 配置）
./build-nuget.sh
```

测试框架是 **TUnit**（非 xUnit/NUnit）：测试方法为 `async Task`，断言用 `await Assert.That(x).IsEqualTo(y)`。`dotnet test` 通过 `global.json` 指定的 Microsoft.Testing.Platform 运行。

## 发布机制

`main` 分支的 push 触发 `.github/workflows/release.yml`：脚本对比 `TinyDb/TinyDb.csproj` 中 `<Version>` 与上一个 git tag，**版本号变化才会构建、跑测试+AOT 测试、打 tag 并发布到 NuGet/GitHub Packages**。改版本号即等于触发发布——非发布意图不要动 `<Version>`。

## 架构分层

引擎核心 `TinyDbEngine`（`TinyDb/Core/`）协调三层，自上而下：

1. **操作层**：`TransactionManager` / `IndexManager` / `QueryExecutor`
2. **逻辑层**：`CollectionMetaStore` → `CollectionState` → `DocumentCollection`（`ITinyCollection`）
3. **存储层**：`IDiskStream` → `PageManager` → `WriteAheadLog` (WAL)

崩溃恢复靠 WAL：页写入前先追加并刷新 WAL，重放时校验磁盘页头/页号/CRC32 页校验和。页 checksum 同时兼容 CRC32 与旧累加和，旧库无需迁移即可重开。

### 源生成器是核心契约

`TinyDb.SourceGenerator`（`netstandard2.0`，`TinyDbSourceGenerator.cs`，~3500 行）扫描 `[Entity]` / `[EntityMetadata]` 标注的类，编译期生成 AOT 兼容的序列化适配器，注册到 `AotHelperRegistry`。运行时 `AotBsonMapper` / `AotIdAccessor` / `AotEntityAdapter`（`TinyDb/Serialization/`）只调用生成代码，**不走反射**。诊断码：`TINYDB001`（BsonRef 类型缺 `[Entity]`，Error）、`TINYDB002/003`（循环引用警告）。

源生成器以 analyzer 形式被三个项目引用（`OutputItemType="Analyzer"`，`ReferenceOutputAssembly="false"`），并通过 `GlobalPropertiesToRemove="PublishAot"` 避免 AOT 发布时移除。改源生成器后，消费项目须重新构建才能拿到新生成代码。

### 模块边界（`TinyDb/`）

- `Bson/` — BSON 类型系统（不可变 `BsonDocument`，`Set` 返回新实例，索引器 setter 抛异常）
- `Storage/` — 页、页编解码、WAL、磁盘流、大文档存储、刷盘调度
- `Index/` — B+树磁盘索引（`DiskBTree`）、索引扫描与范围
- `Query/` — 表达式解析/求值、查询优化、谓词与排序分页下推
- `Serialization/` — AOT 映射器 + 兼容门面（`BsonMapper` 委托给 `AotBsonMapper`）
- `Metadata/` — 集合 schema、`__sys_catalog`、C# 实体反向生成
- `Security/` — 数据页与 WAL payload 加密、密码保护、`SecureTinyDbEngine`

## 关键约束

- `TinyDb` 多目标 `net8.0;net9.0;net10.0`，启用 `unsafe`、`IsAotCompatible`；测试改动须保持三框架与 AOT 路径全绿。
- `[InternalsVisibleTo("TinyDb.Tests")]`：测试可访问 internal 成员。
- 加密为创建期决策：新库设 `EnableEncryption=true` 才加密；对已有明文库设置会抛异常，必须显式导出/compact 到新加密库，不做隐式迁移。
- `TinyDb.UI` 是 Avalonia 可视化工具（net9.0，非 AOT），与核心库独立。
