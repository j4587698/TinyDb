using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using TinyDb.Core;
using TinyDb.Bson;
using TinyDb.Metadata;
using TinyDb.UI.Models;

namespace TinyDb.UI.Services;

public class TableStructureService
{
    private readonly DatabaseService _databaseService;
    private TinyDbEngine? _engine;

    public TableStructureService(DatabaseService databaseService) => _databaseService = databaseService;
    public void SetEngine(TinyDbEngine engine) => _engine = engine;

    public async Task<List<TableStructure>> GetAllTablesAsync()
    {
        if (_engine == null) return new List<TableStructure>();
        var names = _engine.GetCollectionNames().Where(n => !n.StartsWith("__"));
        var list = new List<TableStructure>();
        foreach(var n in names) {
            var s = await GetTableStructureAsync(n);
            if (s != null) list.Add(s);
        }
        return list;
    }

    public async Task<bool> CreateTableAsync(TableStructure table)
    {
        if (_engine == null) return false;
        try {
            await Task.Run(() => {
                // 1. 获取引擎内的元数据管理器
                var mgr = _engine.MetadataManager;

                // 2. 构造原生元数据文档
                var doc = new MetadataDocument {
                    TableName = table.TableName,
                    DisplayName = table.DisplayName ?? table.TableName,
                    Columns = new BsonArray()
                };

                foreach(var f in table.Fields) {
                    doc.Columns = doc.Columns.AddValue(new BsonDocument()
                        .Set("n", f.FieldName)
                        .Set("t", MapToClrTypeName(f.FieldType))
                        .Set("o", f.Order)
                        .Set("pk", f.IsPrimaryKey)
                        .Set("r", f.IsRequired));
                }

                // 3. 保存并触发物理创建
                mgr.SaveMetadata(doc);
                _engine.GetCollection<BsonDocument>(table.TableName);
            });
            return true;
        } catch { return false; }
    }

    public async Task<TableStructure?> GetTableStructureAsync(string tableName)
    {
        if (_engine == null) return null;
        var fields = await _databaseService.GetFieldsForCollectionAsync(tableName);
        return new TableStructure {
            TableName = tableName,
            Fields = new System.Collections.ObjectModel.ObservableCollection<TableField>(fields)
        };
    }

    public async Task<bool> DropTableAsync(string tableName)
    {
        if (_engine == null) return false;
        return await Task.Run(() => {
            _engine.MetadataManager.DeleteMetadata(tableName);
            return _engine.DropCollection(tableName);
        });
    }

    private static string MapToClrTypeName(TableFieldType type)
    {
        return type switch
        {
            TableFieldType.String => "System.String",
            TableFieldType.Integer => "System.Int32",
            TableFieldType.Long => "System.Int64",
            TableFieldType.Double => "System.Double",
            TableFieldType.Decimal => "System.Decimal",
            TableFieldType.Boolean => "System.Boolean",
            TableFieldType.DateTime => "System.DateTime",
            TableFieldType.DateTimeOffset => "System.DateTimeOffset",
            TableFieldType.Guid => "System.Guid",
            TableFieldType.Binary => "System.Byte[]",
            _ => "System.Object"
        };
    }
}
