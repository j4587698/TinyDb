using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using TinyDb.Core;
using TinyDb.Collections;
using TinyDb.Bson;
using TinyDb.Serialization;
using TinyDb.UI.Models;
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
            var options = new TinyDbOptions { Password = password, EnableJournaling = false, ReadOnly = false };
            var directory = System.IO.Path.GetDirectoryName(filePath);
            if (!string.IsNullOrEmpty(directory) && !System.IO.Directory.Exists(directory))
                System.IO.Directory.CreateDirectory(directory);

            _engine = new TinyDbEngine(filePath, options);
            _isConnected = true;
            return true;
        } catch (Exception ex) { throw new InvalidOperationException(ex.Message, ex); }
    }

    public void Disconnect() { _engine?.Dispose(); _engine = null; _isConnected = false; }

    public DatabaseInfo GetDatabaseInfo()
    {
        if (!IsConnected || _engine == null) throw new InvalidOperationException("未连接");
        var stats = _engine.GetStatistics();
        return new DatabaseInfo { FilePath = stats.FilePath, DatabaseName = stats.DatabaseName, FileSize = stats.FileSize, CollectionCount = stats.CollectionCount };
    }

    public List<CollectionInfo> GetCollections()
    {
        if (!IsConnected || _engine == null) return new List<CollectionInfo>();
        // 过滤掉所有以 __ 开头的系统/元数据集合
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

    public async Task<List<TableField>> GetFieldsForCollectionAsync(string collectionName)
    {
        if (_engine == null) return new List<TableField>();
        try {
            var metaCol = _engine.GetCollection<TinyDb.Metadata.MetadataDocument>("__metadata_" + collectionName);
            var metaDoc = metaCol.FindAll().FirstOrDefault();
            
            if (metaDoc != null) {
                var entityMeta = metaDoc.ToEntityMetadata();
                return entityMeta.Properties.Select(p => new TableField {
                    FieldName = p.PropertyName,
                    FieldType = ConvertToTableFieldType(p.PropertyType),
                    IsPrimaryKey = p.PropertyName.Equals("Id", StringComparison.OrdinalIgnoreCase)
                }).ToList();
            }
        } catch { }
        return new List<TableField> { new TableField { FieldName = "Id", IsPrimaryKey = true } };
    }

    private static TableFieldType ConvertToTableFieldType(string type)
    {
        if (type.Contains("Int32")) return TableFieldType.Integer;
        if (type.Contains("Double")) return TableFieldType.Double;
        if (type.Contains("Boolean")) return TableFieldType.Boolean;
        if (type.Contains("DateTime")) return TableFieldType.DateTime;
        return TableFieldType.String;
    }

    public async Task<string> InsertDocumentAsync(string collectionName, string jsonContent)
    {
        if (!IsConnected || _engine == null) throw new InvalidOperationException("未连接");
        var col = _engine.GetCollection<BsonDocument>(collectionName);
        using var jsonDoc = JsonDocument.Parse(jsonContent);
        var bsonDoc = ConvertJsonToBson(jsonDoc.RootElement);
        var id = await Task.Run(() => col.Insert(bsonDoc));
        return id.ToString();
    }

    public async Task<bool> UpdateDocumentAsync(string collectionName, string id, string jsonContent)
    {
        if (!IsConnected || _engine == null) throw new InvalidOperationException("未连接");
        var col = _engine.GetCollection<BsonDocument>(collectionName);
        using var jsonDoc = JsonDocument.Parse(jsonContent);
        var bsonDoc = ConvertJsonToBson(jsonDoc.RootElement).Set("_id", (BsonValue)id);
        var count = await Task.Run(() => col.Update(bsonDoc));
        return count > 0;
    }

    public async Task<bool> DeleteDocumentAsync(string collectionName, string id)
    {
        if (!IsConnected || _engine == null) throw new InvalidOperationException("未连接");
        var col = _engine.GetCollection<BsonDocument>(collectionName);
        return await Task.Run(() => col.Delete((BsonValue)id) > 0);
    }

    public async Task<bool> DropCollectionAsync(string collectionName)
    {
        if (!IsConnected || _engine == null) return false;
        await Task.Run(() => _engine.DropCollection("__metadata_" + collectionName));
        return await Task.Run(() => _engine.DropCollection(collectionName));
    }

    private static DocumentItem ConvertToDocumentItem(BsonDocument doc, string collectionName)
    {
        var id = doc.ContainsKey("_id") ? doc["_id"].ToString() : string.Empty;
        var dict = new Dictionary<string, object?>();
        foreach(var key in doc.Keys) dict[key] = doc[key].ToString();
        var json = JsonSerializer.Serialize(dict);
        return new DocumentItem { Id = id, Content = json, CollectionName = collectionName, Size = json.Length };
    }

    private static BsonDocument ConvertJsonToBson(JsonElement jsonElement)
    {
        var doc = new BsonDocument();
        if (jsonElement.ValueKind == JsonValueKind.Object) {
            foreach (var property in jsonElement.EnumerateObject())
                doc = doc.Set(property.Name, ConvertJsonElementToBsonValue(property.Value));
        }
        return doc;
    }

    private static BsonValue ConvertJsonElementToBsonValue(JsonElement element)
    {
        return element.ValueKind switch {
            JsonValueKind.String => new BsonString(element.GetString()!),
            JsonValueKind.Number => element.TryGetInt64(out var l) ? new BsonInt64(l) : new BsonDouble(element.GetDouble()),
            JsonValueKind.True => new BsonBoolean(true),
            JsonValueKind.False => new BsonBoolean(false),
            _ => new BsonString(element.GetRawText())
        };
    }

    public void Dispose() => Disconnect();
}