using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using TinyDb.Bson;
using TinyDb.Core;
using TinyDb.UI.Core.Schema;
using TinyDb.UI.Models;

namespace TinyDb.UI.Services;

/// <summary>
/// 系统目录服务 - 管理所有表的元数据 (模拟未来的引擎核心功能)
/// </summary>
public class SystemCatalogService
{
    private readonly TinyDbEngine _engine;
    private const string CATALOG_COLLECTION = "__sys_catalog";
    
    // 二级缓存：内存 LRU 缓存 (这里简化为 ConcurrentDictionary)
    private readonly ConcurrentDictionary<string, TableSchema> _schemaCache = new();

    public SystemCatalogService(TinyDbEngine engine)
    {
        _engine = engine;
        // 启动时可以预热缓存，或者懒加载
    }

    /// <summary>
    /// 注册/更新表结构
    /// </summary>
    public void RegisterSchema(TableStructure uiStructure)
    {
        // 1. 转换为高效的内部 Schema 模型
        var schema = new TableSchema
        {
            TableName = uiStructure.TableName,
            CreatedAt = DateTime.Now.Ticks,
            Columns = uiStructure.Fields.Select(f => new ColumnDef
            {
                Name = f.FieldName,
                TypeCode = (int)f.FieldType,
                IsPrimaryKey = f.IsPrimaryKey,
                IsRequired = f.IsRequired
            }).ToList()
        };

        // 2. 存入系统目录集合
        var catalogCol = _engine.GetCollection<BsonDocument>(CATALOG_COLLECTION);
        
        // 检查是否存在
        var existing = catalogCol.FindById(schema.TableName);
        if (existing != null)
        {
            catalogCol.Update(schema.ToBson());
        }
        else
        {
            catalogCol.Insert(schema.ToBson());
        }

        // 3. 更新缓存
        _schemaCache[schema.TableName] = schema;
        
        // 4. 确保物理数据表存在 (通过创建一个空集合)
        _engine.GetCollection<BsonDocument>(schema.TableName);
    }

    /// <summary>
    /// 获取表结构 (优先查缓存，再查磁盘)
    /// </summary>
    public TableSchema? GetSchema(string tableName)
    {
        // 1. 查缓存
        if (_schemaCache.TryGetValue(tableName, out var cachedSchema))
        {
            return cachedSchema;
        }

        // 2. 查磁盘 (System Catalog)
        try
        {
            var catalogCol = _engine.GetCollection<BsonDocument>(CATALOG_COLLECTION);
            var doc = catalogCol.FindById(tableName);
            
            if (doc != null)
            {
                var schema = TableSchema.FromBson(doc);
                _schemaCache[tableName] = schema; // 写入缓存
                return schema;
            }
        }
        catch (Exception)
        {
            // 忽略读取错误，可能是表不存在
        }

        return null;
    }

    /// <summary>
    /// 删除表结构
    /// </summary>
    public void DropSchema(string tableName)
    {
        // 1. 从磁盘删除
        var catalogCol = _engine.GetCollection<BsonDocument>(CATALOG_COLLECTION);
        catalogCol.Delete(tableName);

        // 2. 从缓存移除
        _schemaCache.TryRemove(tableName, out _);
    }
}
