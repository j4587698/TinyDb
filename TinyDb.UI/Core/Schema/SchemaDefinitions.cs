using System;
using System.Collections.Generic;
using System.Linq;
using TinyDb.Bson;
using TinyDb.UI.Models;

namespace TinyDb.UI.Core.Schema;

/// <summary>
/// 列定义 (对应底层的二进制存储结构)
/// </summary>
public class ColumnDef
{
    public string Name { get; set; } = string.Empty;
    public int TypeCode { get; set; } // 对应 TableFieldType 的整数值
    public bool IsPrimaryKey { get; set; }
    public bool IsRequired { get; set; }

    // AOT 友好的序列化：转为 BsonDocument
    public BsonDocument ToBson()
    {
        return new BsonDocument()
            .Set("n", Name) // 使用短键名节省空间
            .Set("t", TypeCode)
            .Set("pk", IsPrimaryKey)
            .Set("r", IsRequired);
    }

    // AOT 友好的反序列化：从 BsonDocument 读取
    public static ColumnDef FromBson(BsonDocument doc)
    {
        return new ColumnDef
        {
            Name = doc["n"].ToString(),
            TypeCode = doc["t"].ToInt32(),
            IsPrimaryKey = doc.ContainsKey("pk") && doc["pk"].ToBoolean(),
            IsRequired = doc.ContainsKey("r") && doc["r"].ToBoolean()
        };
    }
}

/// <summary>
/// 表模式定义 (Schema)
/// </summary>
public class TableSchema
{
    public string TableName { get; set; } = string.Empty;
    public long CreatedAt { get; set; }
    public List<ColumnDef> Columns { get; set; } = new();

    // AOT 友好的序列化
    public BsonDocument ToBson()
    {
        var cols = new BsonArray();
        foreach (var col in Columns)
        {
            cols = cols.AddValue(col.ToBson());
        }

        return new BsonDocument()
            .Set("_id", TableName) // 表名作为主键，保证唯一且查询快
            .Set("c", CreatedAt)
            .Set("cols", cols);
    }

    // AOT 友好的反序列化
    public static TableSchema FromBson(BsonDocument doc)
    {
        var schema = new TableSchema
        {
            TableName = doc["_id"].ToString(),
            CreatedAt = doc.ContainsKey("c") ? doc["c"].ToInt64() : DateTime.Now.Ticks
        };

        if (doc.ContainsKey("cols") && doc["cols"] is BsonArray arr)
        {
            foreach (var item in arr)
            {
                if (item is BsonDocument colDoc)
                {
                    schema.Columns.Add(ColumnDef.FromBson(colDoc));
                }
            }
        }

        return schema;
    }
    
    // 转换为 UI 模型
    public List<TableField> ToTableFields()
    {
        return Columns.Select(c => new TableField
        {
            FieldName = c.Name,
            FieldType = (TableFieldType)c.TypeCode,
            IsPrimaryKey = c.IsPrimaryKey,
            IsRequired = c.IsRequired
        }).ToList();
    }
}
