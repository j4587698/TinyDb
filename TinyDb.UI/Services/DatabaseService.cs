using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using TinyDb.Core;
using TinyDb.Collections;
using TinyDb.Bson;
using TinyDb.Serialization;
using TinyDb.UI.Models;
using TinyDb.Metadata;
using System.Text.Json;

namespace TinyDb.UI.Services;

public class DatabaseService : IDisposable
{
    private TinyDbEngine? _engine;
    private bool _isConnected;

    public bool IsConnected => _isConnected && _engine != null;
    public TinyDbEngine? Engine => _engine;

    public async Task<bool> ConnectAsync(string filePath, string? password = null)
    {
        try {
            var options = new TinyDbOptions { Password = password, EnableJournaling = false };
            _engine = new TinyDbEngine(filePath, options);
            _isConnected = true;
            return true;
        } catch (Exception ex) { throw new InvalidOperationException(ex.Message, ex); }
    }

    public void Disconnect() { _engine?.Dispose(); _engine = null; _isConnected = false; }

    public List<CollectionInfo> GetCollections()
    {
        if (!IsConnected || _engine == null) return new List<CollectionInfo>();
        // 获取所有集合，排除以 __ 开头的系统表
        return _engine.GetCollectionNames()
            .Where(name => !name.StartsWith("__"))
            .Select(name => new CollectionInfo { Name = name }).ToList();
    }

    public async Task<List<DocumentItem>> GetDocumentsAsync(string collectionName)
    {
        if (!IsConnected || _engine == null) return new List<DocumentItem>();
        return await Task.Run(() => {
            var col = _engine.GetCollection<BsonDocument>(collectionName);
            return col.FindAll().Select(doc => ConvertToDocumentItem(doc, collectionName)).ToList();
        });
    }

    // 核心改进：直接调用底层引擎的元数据管理器
    public async Task<List<TableField>> GetFieldsForCollectionAsync(string collectionName)
    {
        if (_engine == null) return new List<TableField>();
        
        // 我们通过引擎中新集成的 MetadataManager 获取
        var schema = _engine.MetadataManager.GetMetadata(collectionName);
        if (schema != null)
        {
            return schema.Columns.Select(c =>
            {
                if (c is not BsonDocument doc) return new TableField { FieldName = "Id", IsPrimaryKey = true };

                var typeName = doc.ContainsKey("t") ? doc["t"].ToString() : string.Empty;

                return new TableField
                {
                    FieldName = doc["n"].ToString(),
                    FieldType = MapToTableFieldType(typeName),
                    IsPrimaryKey = (doc.ContainsKey("pk") && doc["pk"].ToBoolean()) || doc["n"].ToString() == "_id"
                };
            }).ToList();
        }
        
        return new List<TableField> { new TableField { FieldName = "Id", IsPrimaryKey = true } };
    }

    private static TableFieldType MapToTableFieldType(string typeName)
    {
        if (string.IsNullOrWhiteSpace(typeName)) return TableFieldType.Object;

        if (typeName.EndsWith("System.String", StringComparison.Ordinal) || typeName.Equals("string", StringComparison.OrdinalIgnoreCase))
            return TableFieldType.String;
        if (typeName.EndsWith("System.Int32", StringComparison.Ordinal) || typeName.Equals("int", StringComparison.OrdinalIgnoreCase))
            return TableFieldType.Integer;
        if (typeName.EndsWith("System.Int64", StringComparison.Ordinal) || typeName.Equals("long", StringComparison.OrdinalIgnoreCase))
            return TableFieldType.Long;
        if (typeName.EndsWith("System.Double", StringComparison.Ordinal) || typeName.Equals("double", StringComparison.OrdinalIgnoreCase))
            return TableFieldType.Double;
        if (typeName.EndsWith("System.Decimal", StringComparison.Ordinal) || typeName.Equals("decimal", StringComparison.OrdinalIgnoreCase))
            return TableFieldType.Decimal;
        if (typeName.EndsWith("System.Boolean", StringComparison.Ordinal) || typeName.Equals("bool", StringComparison.OrdinalIgnoreCase))
            return TableFieldType.Boolean;
        if (typeName.EndsWith("System.DateTime", StringComparison.Ordinal))
            return TableFieldType.DateTime;
        if (typeName.EndsWith("System.Guid", StringComparison.Ordinal))
            return TableFieldType.Guid;
        if (typeName.EndsWith("System.Byte[]", StringComparison.Ordinal) || typeName.Equals("byte[]", StringComparison.OrdinalIgnoreCase))
            return TableFieldType.Binary;

        return TableFieldType.Object;
    }

    public async Task<string> InsertDocumentAsync(string col, string json)
    {
        if (!IsConnected || _engine == null) throw new InvalidOperationException();
        var collection = _engine.GetCollection<BsonDocument>(col);
        using var jd = JsonDocument.Parse(json);
        var doc = ConvertJsonToBson(jd.RootElement);
        var id = await Task.Run(() => collection.Insert(doc));
        return id.ToString();
    }

    public async Task<bool> UpdateDocumentAsync(string col, string id, string json)
    {
        if (!IsConnected || _engine == null) return false;
        var collection = _engine.GetCollection<BsonDocument>(col);
        using var jd = JsonDocument.Parse(json);
        var doc = ConvertJsonToBson(jd.RootElement).Set("_id", (BsonValue)id);
        return await Task.Run(() => collection.Update(doc) > 0);
    }

    public async Task<bool> DeleteDocumentAsync(string col, string id)
    {
        if (!IsConnected || _engine == null) return false;
        var collection = _engine.GetCollection<BsonDocument>(col);
        return await Task.Run(() => collection.Delete((BsonValue)id) > 0);
    }

    public async Task<bool> DropCollectionAsync(string col)
    {
        if (!IsConnected || _engine == null) return false;
        return await Task.Run(() => _engine.DropCollection(col));
    }

    private static DocumentItem ConvertToDocumentItem(BsonDocument doc, string col)
    {
        var id = doc.ContainsKey("_id") ? doc["_id"].ToString() : "";
        var dict = new Dictionary<string, object?>();
        foreach(var k in doc.Keys) dict[k] = doc[k].ToString();
        var json = JsonSerializer.Serialize(dict);
        return new DocumentItem { Id = id, Content = json, CollectionName = col, Size = json.Length };
    }

    private static BsonDocument ConvertJsonToBson(JsonElement el)
    {
        var doc = new BsonDocument();
        if (el.ValueKind == JsonValueKind.Object) {
            foreach (var p in el.EnumerateObject())
                doc = doc.Set(p.Name, ConvertJsonElementToBsonValue(p.Value));
        }
        return doc;
    }

    private static BsonValue ConvertJsonElementToBsonValue(JsonElement el)
    {
        return el.ValueKind switch {
            JsonValueKind.String => new BsonString(el.GetString()!),
            JsonValueKind.Number => el.TryGetInt64(out var l) ? new BsonInt64(l) : new BsonDouble(el.GetDouble()),
            JsonValueKind.True => new BsonBoolean(true),
            JsonValueKind.False => new BsonBoolean(false),
            _ => new BsonString(el.GetRawText())
        };
    }

    public void Dispose() => Disconnect();
}
