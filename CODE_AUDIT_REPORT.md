# 代码审查报告

此文件包含对 `TinyDb` 库进行全面代码审查的发现结果。
重点关注领域：“简易”实现、“尚未实现”的功能以及临时措施。

## 审查发现

### Core (核心引擎)
- **TinyDbEngine.cs**:
    - `InsertDocuments` 方法中的 `catch` 块忽略了批量插入中的单条失败，且未进行日志记录或报告。
    - `InitializeDatabase` 如果头部无效抛出通用的 `InvalidOperationException`，描述不够具体。
    - `DeleteDocument` 包含关于 `st.PageState.PageId` 的“Bug Fix reinforcement”注释，暗示逻辑依赖较为脆弱。
- **DataPageAccess.cs**:
    - 扫描文档时的长度校验逻辑较为基础。
    - `RewritePageWithDocuments` 在重写过程中暂存了 prev/next 指针，但在异常情况下可能导致链表断裂。
- **TransactionManager.cs**:
    - `ValidateOperations` 标记为简化实现，尚未实现真正的引用完整性检查。
    - `CheckAndCleanupExpiredTransactions` 静默吞掉清理过程中的所有异常。
- **LockManager.cs**:
    - `DetectAndResolveDeadlocks` 仅通过清除锁请求来“解决”死锁，未主动通知事务层抛出异常，严重依赖超时机制。

### Collections (集合)
- **DocumentCollection.cs**:
    - `CreateAutoIndexes` 静默失败，索引创建异常被吞掉。
    - 批量插入由于 BsonDocument 的不可变性，每次 Set 操作都创建新实例，存在性能优化空间（建议使用 Mutable Builder）。

### Index (索引)
- **IndexManager.cs**:
    - 索引操作使用集合级的全局锁 `_indexLock`，在高并发下是瓶颈。
    - `ExtractIndexKey` 目前硬编码只支持单字段索引，复合索引支持标为 TODO。
- **DiskBTreeNode.cs**:
    - `IsFull` 方法始终返回 `false`，导致节点分裂逻辑实际上依赖于 `_maxKeys` 计数而非页面实际剩余字节数。
- **DiskBTree.cs**:
    - `CanMerge` 始终返回 `true`，未进行容量校验。
    - `Validate` 方法是空桩，返回 `true`。

### Storage (存储)
- **DiskStream.cs**:
    - `FlushAsync` 在异步方法中持有同步锁 (`lock`)，属于反模式。
- **PageManager.cs**:
    - 初始化扫描时忽略所有损坏页面的异常且未记录日志。
- **WriteAheadLog.cs**:
    - 重放逻辑对文件截断和 CRC 校验失败的处理较为简单，直接停止重放可能导致后续有效记录丢失。

### Bson (基础类型)
- **ObjectId.cs**:
    - 时间戳和计数器的解析直接使用 `BitConverter`，未处理系统端序（Endianness）兼容性。

### Query (查询)
- **QueryExecutor.cs**:
    - `ExecutePrimaryKeyLookup` 缺乏对复杂过滤条件的二次验证。
    - `BuildIndexScanRange` 仅支持基于第一个条件的简单范围构建，不支持多条件合并优化。
- **QueryOptimizer.cs**:
    - `EvaluateMemberExpression` 无法正确求值局部变量和闭包，导致许多包含外部引用的查询无法使用索引。
- **ExpressionEvaluator.cs**:
    - `EvaluateFunction` 仅支持极少数字符串方法。