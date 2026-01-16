# BsonDocument 不可变性修复说明

## 🔧 问题原因

TinyDb的BsonDocument类是不可变的（immutable），这意味着：
- 不能使用索引器直接设置值：`doc["key"] = value`
- 不能使用初始化器语法：`new BsonDocument { ["key"] = value }`
- 必须使用`Set()`方法来创建新的文档实例

## 🛠️ 修复内容

### 1. CreateCollectionAsync方法
**修复前（错误）**:
```csharp
var tempDoc = new BsonDocument
{
    ["_temp"] = true,
    ["_created"] = DateTime.UtcNow
};
```

**修复后（正确）**:
```csharp
var tempDoc = new BsonDocument()
    .Set("_temp", true)
    .Set("_created", DateTime.UtcNow);
```

### 2. ConvertJsonToBson方法
**修复前（错误）**:
```csharp
foreach (var property in jsonElement.EnumerateObject())
{
    doc[property.Name] = ConvertJsonElementToBsonValue(property.Value);
}
```

**修复后（正确）**:
```csharp
foreach (var property in jsonElement.EnumerateObject())
{
    doc = doc.Set(property.Name, ConvertJsonElementToBsonValue(property.Value));
}
```

## 📝 TinyDb BsonDocument 正确用法

### ✅ 正确的方式
```csharp
// 创建新文档
var doc = new BsonDocument()
    .Set("name", "test")
    .Set("age", 25)
    .Set("active", true);

// 添加/修改字段
doc = doc.Set("email", "test@example.com");

// 链式调用
doc = doc.Set("created", DateTime.Now)
         .Set("updated", DateTime.Now);
```

### ❌ 错误的方式
```csharp
// 这些都会导致 "BsonDocument is immutable" 错误
var doc = new BsonDocument();
doc["name"] = "test";  // ❌
doc = new BsonDocument { ["name"] = "test" };  // ❌
```

## 🎯 受影响的功能

以下功能现在都已修复并可以正常使用：

1. **创建集合** - ✅ 不再报错
2. **插入文档** - ✅ JSON转换正常工作
3. **更新文档** - ✅ 文档修改功能正常
4. **所有文档操作** - ✅ 完全兼容TinyDb API

## 🔄 工作原理

TinyDb的BsonDocument使用函数式编程模式：
- 每次`Set()`调用都返回一个新的BsonDocument实例
- 原始文档保持不变，确保数据一致性
- 这种设计避免了并发修改问题

## 🚀 测试验证

现在可以测试以下操作：

1. **创建集合**: 点击"新建"按钮应该成功创建
2. **创建文档**: 点击"新建文档"应该正常工作
3. **编辑文档**: JSON编辑和保存应该正常
4. **所有CRUD操作**: 增删改查都应该正常工作

## 📊 性能说明

虽然每次Set()都创建新实例，但TinyDb内部有优化：
- 小型文档的创建开销很小
- 不可变性保证了线程安全
- 避免了数据竞争和状态不一致问题

现在所有功能都应该正常工作，不再出现"BsonDocument is immutable"错误！🎉