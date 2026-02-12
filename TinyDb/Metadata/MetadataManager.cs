using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using TinyDb.Bson;
using TinyDb.Core;

namespace TinyDb.Metadata;

/// <summary>
/// 系统级元数据管理器 - 负责全局模式定义
/// </summary>
public class MetadataManager
{
    private readonly TinyDbEngine _engine;
    private const string CATALOG_COLLECTION = "__sys_catalog";
    
    // 内存 LRU 模式缓存 (AOT 环境下使用 ConcurrentDictionary 极其稳定)
    private readonly ConcurrentDictionary<string, MetadataDocument> _cache = new();
    private readonly ConcurrentDictionary<string, SchemaValidationProfile> _validationProfiles = new();

    private enum ExpectedBsonKind
    {
        String = 0,
        Boolean = 1,
        Numeric = 2,
        DateTime = 3,
        ObjectId = 4,
        Binary = 5,
        Array = 6,
        Document = 7
    }

    private readonly struct RequiredField
    {
        public RequiredField(string name, string camelName)
        {
            Name = name;
            CamelName = camelName;
        }

        public string Name { get; }
        public string CamelName { get; }
    }

    private sealed class SchemaValidationProfile
    {
        public required IReadOnlyList<RequiredField> RequiredFields { get; init; }
        public required HashSet<string> AllowedFields { get; init; }
        public required Dictionary<string, ExpectedBsonKind> ExpectedKindsByField { get; init; }

        public static SchemaValidationProfile Build(MetadataDocument schema)
        {
            if (schema == null) throw new ArgumentNullException(nameof(schema));

            var requiredFields = new List<RequiredField>();
            var allowed = new HashSet<string>(StringComparer.Ordinal);
            var expectedKinds = new Dictionary<string, ExpectedBsonKind>(StringComparer.Ordinal);

            foreach (var col in schema.Columns.OfType<BsonDocument>())
            {
                if (!TryGetFieldName(col, out var fieldName)) continue;

                allowed.Add(fieldName);
                var camel = ToCamelCase(fieldName);
                if (!string.Equals(camel, fieldName, StringComparison.Ordinal))
                {
                    allowed.Add(camel);
                }

                if (GetBool(col, "r") && !IsIdFieldName(fieldName))
                {
                    requiredFields.Add(new RequiredField(fieldName, camel));
                }

                if (TryGetExpectedKind(GetOptionalString(col, "t"), out var kind))
                {
                    expectedKinds[fieldName] = kind;
                    if (!string.Equals(camel, fieldName, StringComparison.Ordinal))
                    {
                        expectedKinds[camel] = kind;
                    }
                }
            }

            return new SchemaValidationProfile
            {
                RequiredFields = requiredFields,
                AllowedFields = allowed,
                ExpectedKindsByField = expectedKinds
            };
        }
    }

    public MetadataManager(TinyDbEngine engine)
    {
        _engine = engine ?? throw new ArgumentNullException(nameof(engine));
    }

    internal void EnsureSchema(string tableName, [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] Type documentType)
    {
        if (string.IsNullOrWhiteSpace(tableName)) throw new ArgumentException("tableName is required.", nameof(tableName));
        if (documentType == null) throw new ArgumentNullException(nameof(documentType));

        if (tableName.StartsWith("__", StringComparison.Ordinal)) return;

        if (GetMetadata(tableName) != null) return;

        MetadataDocument doc;
        if (_engine.Options.ReadOnly)
        {
            throw new InvalidOperationException($"Schema not found for table '{tableName}' (read-only mode).");
        }

        if (documentType == typeof(BsonDocument))
        {
            throw new InvalidOperationException(
                $"Schema is required for table '{tableName}'. Create schema via MetadataManager.SaveMetadata before accessing BsonDocument collections.");
        }

        var entityMeta = MetadataExtractor.ExtractEntityMetadata(documentType);
        if (entityMeta.Properties.Count == 0)
        {
            doc = CreateMinimalSchema(tableName);
        }
        else
        {
            entityMeta.CollectionName = tableName;
            doc = MetadataDocument.FromEntityMetadata(entityMeta);
            doc.TableName = tableName;
            if (string.IsNullOrWhiteSpace(doc.DisplayName)) doc.DisplayName = tableName;
        }

        SaveMetadata(doc);
    }

    /// <summary>
    /// 保存或更新表模式
    /// </summary>
    public void SaveMetadata(MetadataDocument doc)
    {
        if (doc == null) throw new ArgumentNullException(nameof(doc));
        if (string.IsNullOrWhiteSpace(doc.TableName)) throw new ArgumentException("TableName is required.", nameof(doc));

        var col = _engine.GetCollection<MetadataDocument>(CATALOG_COLLECTION);
        
        // 物理持久化
        var existing = col.FindById(doc.TableName);
        if (existing != null)
        {
            doc.CreatedAt = existing.CreatedAt;
            doc.UpdatedAt = DateTime.Now;
            col.Update(doc);
        }
        else
        {
            doc.CreatedAt = DateTime.Now;
            doc.UpdatedAt = DateTime.Now;
            col.Insert(doc);
        }

        // 更新缓存
        _cache[doc.TableName] = doc;
        _validationProfiles[doc.TableName] = SchemaValidationProfile.Build(doc);
    }

    /// <summary>
    /// 获取表模式 (原生高效：缓存优先)
    /// </summary>
    private static MetadataDocument CreateMinimalSchema(string tableName)
    {
        return new MetadataDocument
        {
            TableName = tableName,
            DisplayName = tableName,
            Columns = new BsonArray()
                .AddValue(new BsonDocument()
                    .Set("n", "_id")
                    .Set("pn", "Id")
                    .Set("t", typeof(ObjectId).FullName ?? "TinyDb.Bson.ObjectId")
                    .Set("pk", true)
                    .Set("r", true)
                    .Set("o", 0))
        };
    }

    public MetadataDocument? GetMetadata(string tableName)
    {
        if (_cache.TryGetValue(tableName, out var cached)) return cached;

        try {
            var col = _engine.GetCollection<MetadataDocument>(CATALOG_COLLECTION);
            var doc = col.FindById(tableName);
            if (doc != null) {
                _cache[tableName] = doc;
                _validationProfiles[tableName] = SchemaValidationProfile.Build(doc);
                return doc;
            }
        } catch { }
        
        return null;
    }

    /// <summary>
    /// 删除表模式
    /// </summary>
    public void DeleteMetadata(string tableName)
    {
        var col = _engine.GetCollection<MetadataDocument>(CATALOG_COLLECTION);
        col.Delete(tableName);
        _cache.TryRemove(tableName, out _);
        _validationProfiles.TryRemove(tableName, out _);
    }

    /// <summary>
    /// 获取所有已注册的表名
    /// </summary>
    public List<string> GetAllTableNames()
    {
        var col = _engine.GetCollection<MetadataDocument>(CATALOG_COLLECTION);
        return col.FindAll().Select(d => d.TableName).ToList();
    }

    public void SaveEntityMetadata([DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] Type entityType)
    {
        if (entityType == null) throw new ArgumentNullException(nameof(entityType));
        var metadata = MetadataExtractor.ExtractEntityMetadata(entityType);
        SaveMetadata(MetadataDocument.FromEntityMetadata(metadata));
    }

    public EntityMetadata? GetEntityMetadata([DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] Type entityType)
    {
        if (entityType == null) throw new ArgumentNullException(nameof(entityType));

        var runtimeMeta = MetadataExtractor.ExtractEntityMetadata(entityType);
        var stored = GetMetadata(runtimeMeta.CollectionName);
        return stored?.ToEntityMetadata();
    }

    public List<string> GetRegisteredEntityTypes()
    {
        var col = _engine.GetCollection<MetadataDocument>(CATALOG_COLLECTION);
        return col.FindAll()
            .Select(d => d.TypeName)
            .Where(n => !string.IsNullOrWhiteSpace(n))
            .Distinct(StringComparer.Ordinal)
            .ToList();
    }

    public bool HasMetadata([DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] Type entityType)
    {
        if (entityType == null) throw new ArgumentNullException(nameof(entityType));
        var runtimeMeta = MetadataExtractor.ExtractEntityMetadata(entityType);
        return GetMetadata(runtimeMeta.CollectionName) != null;
    }

    public void ValidateDocumentForWrite(string tableName, BsonDocument document, SchemaValidationMode mode)
    {
        if (mode == SchemaValidationMode.None) return;
        if (string.IsNullOrWhiteSpace(tableName)) throw new ArgumentException("tableName is required.", nameof(tableName));
        if (document == null) throw new ArgumentNullException(nameof(document));
        if (tableName.StartsWith("__", StringComparison.Ordinal)) return;

        var schema = GetMetadata(tableName);
        if (schema == null)
        {
            throw new InvalidOperationException($"Schema not found for table '{tableName}'.");
        }

        if (!_validationProfiles.TryGetValue(tableName, out var profile))
        {
            profile = SchemaValidationProfile.Build(schema);
            _validationProfiles[tableName] = profile;
        }

        foreach (var requiredField in profile.RequiredFields)
        {
            if (!TryGetValueWithCompat(document, requiredField, out var value) || value == null || value.IsNull)
            {
                throw new InvalidOperationException($"Schema validation failed for table '{tableName}': required field '{requiredField.Name}' is missing or null.");
            }
        }

        if (mode != SchemaValidationMode.Strict) return;

        foreach (var key in document.Keys)
        {
            if (IsSystemFieldName(key)) continue;
            if (!profile.AllowedFields.Contains(key))
            {
                throw new InvalidOperationException($"Schema validation failed for table '{tableName}': field '{key}' is not defined in schema.");
            }
        }

        foreach (var (fieldName, expectedKind) in profile.ExpectedKindsByField)
        {
            if (IsSystemFieldName(fieldName)) continue;
            if (!document.TryGetValue(fieldName, out var value) || value == null || value.IsNull) continue;

            if (!IsCompatible(value, expectedKind))
            {
                throw new InvalidOperationException(
                    $"Schema validation failed for table '{tableName}': field '{fieldName}' has BSON type '{value.BsonType}' which is incompatible with schema type.");
            }
        }
    }

    public BsonDocument ApplySchemaDefaults(string tableName, BsonDocument document)
    {
        if (string.IsNullOrWhiteSpace(tableName)) throw new ArgumentException("tableName is required.", nameof(tableName));
        if (document == null) throw new ArgumentNullException(nameof(document));

        var schema = GetMetadata(tableName);
        if (schema == null || schema.Columns.Count == 0) return document;

        var patched = document;

        foreach (var col in schema.Columns)
        {
            if (col is not BsonDocument colDoc) continue;
            if (!TryGetFieldName(colDoc, out var fieldName)) continue;
            if (IsPrimaryKey(colDoc, fieldName)) continue;

            if (patched.ContainsKey(fieldName)) continue;

            // ååŽå…¼å®¹ï¼šæ—§ Schema å¯èƒ½ç”¨ PascalCase å­˜ï¼Œæ–‡æ¡£ç”¨ camelCase å­˜
            var camelCase = ToCamelCase(fieldName);
            if (camelCase != fieldName && patched.ContainsKey(camelCase)) continue;

            patched = patched.Set(fieldName, DetermineDefaultValue(colDoc));
        }

        return patched;
    }

    private static bool TryGetFieldName(BsonDocument columnDoc, out string fieldName)
    {
        fieldName = string.Empty;
        if (!columnDoc.TryGetValue("n", out var nameValue) || nameValue == null || nameValue.IsNull) return false;

        fieldName = nameValue.ToString();
        return !string.IsNullOrWhiteSpace(fieldName);
    }

    private static bool IsPrimaryKey(BsonDocument columnDoc, string fieldName)
    {
        if (IsIdFieldName(fieldName)) return true;
        return columnDoc.TryGetValue("pk", out var pkValue) && pkValue != null && !pkValue.IsNull && pkValue.ToBoolean();
    }

    private static bool IsIdFieldName(string fieldName)
        => string.Equals(fieldName, "_id", StringComparison.Ordinal);

    private static bool IsSystemFieldName(string fieldName)
    {
        return fieldName switch
        {
            "_id" => true,
            "_collection" => true,
            "_isLargeDocument" => true,
            "_largeDocumentIndex" => true,
            "_largeDocumentSize" => true,
            _ => false
        };
    }

    private static bool TryGetValueWithCompat(BsonDocument document, RequiredField requiredField, out BsonValue? value)
    {
        if (document.TryGetValue(requiredField.Name, out value))
        {
            return true;
        }

        if (!string.Equals(requiredField.CamelName, requiredField.Name, StringComparison.Ordinal) &&
            document.TryGetValue(requiredField.CamelName, out value))
        {
            return true;
        }

        value = null;
        return false;
    }

    private static bool TryGetExpectedKind(string? typeName, out ExpectedBsonKind kind)
    {
        kind = default;
        if (string.IsNullOrWhiteSpace(typeName)) return false;

        var normalized = ClrTypeName.NormalizeForComparison(typeName);
        if (string.IsNullOrWhiteSpace(normalized)) return false;

        if (normalized.EndsWith("[]", StringComparison.Ordinal) ||
            normalized.Contains("System.Collections.Generic.List", StringComparison.Ordinal) ||
            normalized.Contains("System.Collections.Generic.IEnumerable", StringComparison.Ordinal))
        {
            kind = ExpectedBsonKind.Array;
            return true;
        }

        if (normalized.Contains("System.Collections.Generic.Dictionary", StringComparison.Ordinal) ||
            normalized.Contains("System.Collections.Generic.IDictionary", StringComparison.Ordinal))
        {
            kind = ExpectedBsonKind.Document;
            return true;
        }

        if (normalized.Equals("string", StringComparison.OrdinalIgnoreCase) ||
            normalized.EndsWith("System.String", StringComparison.Ordinal))
        {
            kind = ExpectedBsonKind.String;
            return true;
        }

        if (normalized.Equals("bool", StringComparison.OrdinalIgnoreCase) ||
            normalized.Equals("boolean", StringComparison.OrdinalIgnoreCase) ||
            normalized.EndsWith("System.Boolean", StringComparison.Ordinal))
        {
            kind = ExpectedBsonKind.Boolean;
            return true;
        }

        if (normalized.Equals("DateTime", StringComparison.OrdinalIgnoreCase) ||
            normalized.EndsWith("System.DateTime", StringComparison.Ordinal))
        {
            kind = ExpectedBsonKind.DateTime;
            return true;
        }

        if (normalized.Equals("Guid", StringComparison.OrdinalIgnoreCase) ||
            normalized.EndsWith("System.Guid", StringComparison.Ordinal))
        {
            kind = ExpectedBsonKind.String;
            return true;
        }

        if (normalized.Equals("byte[]", StringComparison.OrdinalIgnoreCase) ||
            normalized.Equals("System.Byte[]", StringComparison.OrdinalIgnoreCase) ||
            normalized.EndsWith("System.Byte[]", StringComparison.Ordinal))
        {
            kind = ExpectedBsonKind.Binary;
            return true;
        }

        if (normalized.EndsWith("ObjectId", StringComparison.Ordinal))
        {
            kind = ExpectedBsonKind.ObjectId;
            return true;
        }

        if (normalized.EndsWith("BsonDocument", StringComparison.Ordinal))
        {
            kind = ExpectedBsonKind.Document;
            return true;
        }

        if (normalized.EndsWith("BsonArray", StringComparison.Ordinal))
        {
            kind = ExpectedBsonKind.Array;
            return true;
        }

        if (normalized.Equals("int", StringComparison.OrdinalIgnoreCase) ||
            normalized.Equals("int16", StringComparison.OrdinalIgnoreCase) ||
            normalized.Equals("int32", StringComparison.OrdinalIgnoreCase) ||
            normalized.Equals("int64", StringComparison.OrdinalIgnoreCase) ||
            normalized.Equals("long", StringComparison.OrdinalIgnoreCase) ||
            normalized.Equals("short", StringComparison.OrdinalIgnoreCase) ||
            normalized.Equals("byte", StringComparison.OrdinalIgnoreCase) ||
            normalized.Equals("sbyte", StringComparison.OrdinalIgnoreCase) ||
            normalized.Equals("uint16", StringComparison.OrdinalIgnoreCase) ||
            normalized.Equals("uint32", StringComparison.OrdinalIgnoreCase) ||
            normalized.Equals("uint64", StringComparison.OrdinalIgnoreCase) ||
            normalized.Equals("ushort", StringComparison.OrdinalIgnoreCase) ||
            normalized.Equals("uint", StringComparison.OrdinalIgnoreCase) ||
            normalized.Equals("ulong", StringComparison.OrdinalIgnoreCase) ||
            normalized.Equals("double", StringComparison.OrdinalIgnoreCase) ||
            normalized.Equals("float", StringComparison.OrdinalIgnoreCase) ||
            normalized.Equals("single", StringComparison.OrdinalIgnoreCase) ||
            normalized.Equals("decimal", StringComparison.OrdinalIgnoreCase) ||
            normalized.EndsWith("System.Int16", StringComparison.Ordinal) ||
            normalized.EndsWith("System.Int32", StringComparison.Ordinal) ||
            normalized.EndsWith("System.Int64", StringComparison.Ordinal) ||
            normalized.EndsWith("System.UInt16", StringComparison.Ordinal) ||
            normalized.EndsWith("System.UInt32", StringComparison.Ordinal) ||
            normalized.EndsWith("System.UInt64", StringComparison.Ordinal) ||
            normalized.EndsWith("System.Byte", StringComparison.Ordinal) ||
            normalized.EndsWith("System.SByte", StringComparison.Ordinal) ||
            normalized.EndsWith("System.Double", StringComparison.Ordinal) ||
            normalized.EndsWith("System.Single", StringComparison.Ordinal) ||
            normalized.EndsWith("System.Decimal", StringComparison.Ordinal))
        {
            kind = ExpectedBsonKind.Numeric;
            return true;
        }

        return false;
    }

    private static bool IsCompatible(BsonValue value, ExpectedBsonKind expectedKind)
    {
        return expectedKind switch
        {
            ExpectedBsonKind.String => value.IsString,
            ExpectedBsonKind.Boolean => value.IsBoolean,
            ExpectedBsonKind.Numeric => value.IsNumeric,
            ExpectedBsonKind.DateTime => value.IsDateTime,
            ExpectedBsonKind.ObjectId => value.IsObjectId,
            ExpectedBsonKind.Binary => value.BsonType == BsonType.Binary,
            ExpectedBsonKind.Array => value.IsArray,
            ExpectedBsonKind.Document => value.IsDocument,
            _ => true
        };
    }

    private static bool GetBool(BsonDocument doc, string key)
    {
        return doc.TryGetValue(key, out var value) && value != null && !value.IsNull && value.ToBoolean();
    }

    private static string? GetOptionalString(BsonDocument doc, string key)
    {
        if (!doc.TryGetValue(key, out var value) || value == null || value.IsNull) return null;
        var s = value.ToString();
        return string.IsNullOrWhiteSpace(s) ? null : s;
    }

    private static BsonValue DetermineDefaultValue(BsonDocument columnDoc)
    {
        if (columnDoc.TryGetValue("dv", out var explicitDefault) && explicitDefault != null && !explicitDefault.IsNull)
        {
            return explicitDefault;
        }

        var isRequired = columnDoc.TryGetValue("r", out var requiredValue) &&
                         requiredValue != null &&
                         !requiredValue.IsNull &&
                         requiredValue.ToBoolean();

        if (!isRequired) return BsonNull.Value;

        var typeName = columnDoc.TryGetValue("t", out var typeValue) && typeValue != null && !typeValue.IsNull
            ? typeValue.ToString()
            : string.Empty;

        return GetTypeDefaultValue(typeName);
    }

    private static BsonValue GetTypeDefaultValue(string typeName)
    {
        if (string.IsNullOrWhiteSpace(typeName)) return BsonNull.Value;

        var normalized = ClrTypeName.NormalizeForComparison(typeName);
        if (string.IsNullOrWhiteSpace(normalized)) return BsonNull.Value;

        if (normalized.Equals("string", StringComparison.OrdinalIgnoreCase) ||
            normalized.EndsWith("System.String", StringComparison.Ordinal))
            return new BsonString(string.Empty);

        if (normalized.Equals("int", StringComparison.OrdinalIgnoreCase) ||
            normalized.Equals("int32", StringComparison.OrdinalIgnoreCase) ||
            normalized.EndsWith("System.Int32", StringComparison.Ordinal))
            return BsonInt32.FromValue(0);

        if (normalized.Equals("long", StringComparison.OrdinalIgnoreCase) ||
            normalized.Equals("int64", StringComparison.OrdinalIgnoreCase) ||
            normalized.EndsWith("System.Int64", StringComparison.Ordinal))
            return new BsonInt64(0L);

        if (normalized.Equals("double", StringComparison.OrdinalIgnoreCase) ||
            normalized.EndsWith("System.Double", StringComparison.Ordinal))
            return new BsonDouble(0d);

        if (normalized.Equals("float", StringComparison.OrdinalIgnoreCase) ||
            normalized.Equals("single", StringComparison.OrdinalIgnoreCase) ||
            normalized.EndsWith("System.Single", StringComparison.Ordinal))
            return new BsonDouble(0d);

        if (normalized.Equals("decimal", StringComparison.OrdinalIgnoreCase) ||
            normalized.EndsWith("System.Decimal", StringComparison.Ordinal))
            return new BsonDecimal128(0m);

        if (normalized.Equals("bool", StringComparison.OrdinalIgnoreCase) ||
            normalized.Equals("boolean", StringComparison.OrdinalIgnoreCase) ||
            normalized.EndsWith("System.Boolean", StringComparison.Ordinal))
            return BsonBoolean.FromValue(false);

        if (normalized.Equals("DateTime", StringComparison.OrdinalIgnoreCase) ||
            normalized.EndsWith("System.DateTime", StringComparison.Ordinal))
            return new BsonDateTime(default(DateTime));

        if (normalized.Equals("Guid", StringComparison.OrdinalIgnoreCase) ||
            normalized.EndsWith("System.Guid", StringComparison.Ordinal))
            return new BsonString(Guid.Empty.ToString());

        if (normalized.Equals("byte[]", StringComparison.OrdinalIgnoreCase) ||
            normalized.Equals("System.Byte[]", StringComparison.OrdinalIgnoreCase) ||
            normalized.EndsWith("System.Byte[]", StringComparison.Ordinal))
            return new BsonBinary(Array.Empty<byte>());

        if (normalized.EndsWith("ObjectId", StringComparison.Ordinal))
            return new BsonObjectId(ObjectId.Empty);

        if (normalized.Contains("System.Collections.Generic.List", StringComparison.Ordinal) ||
            normalized.Contains("System.Collections.Generic.IEnumerable", StringComparison.Ordinal) ||
            normalized.EndsWith("[]", StringComparison.Ordinal))
            return new BsonArray();

        if (normalized.Contains("System.Collections.Generic.Dictionary", StringComparison.Ordinal) ||
            normalized.Contains("System.Collections.Generic.IDictionary", StringComparison.Ordinal))
            return new BsonDocument();

        return BsonNull.Value;
    }

    private static string ToCamelCase(string name)
    {
        if (string.IsNullOrEmpty(name)) return name;
        if (name.Length == 1) return name.ToLowerInvariant();
        return char.ToLowerInvariant(name[0]) + name.Substring(1);
    }
}
