using System;
using System.Collections.Generic;
using System.Linq;
using TinyDb.Attributes;
using TinyDb.Bson;
using TinyDb.Serialization;

namespace TinyDb.Metadata;

/// <summary>
/// 原生元数据文档 - 100% BSON 存储，无 JSON，支持 AOT
/// </summary>
[Entity("__sys_catalog")]
public class MetadataDocument
{
    [Id]
    public string TableName { get; set; } = string.Empty;
    public string TypeName { get; set; } = string.Empty;
    public string DisplayName { get; set; } = string.Empty;
    public string? Description { get; set; }
    
    // 核心：直接存储 BsonArray 格式的字段定义
    public BsonArray Columns { get; set; } = new BsonArray();

    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    public DateTime UpdatedAt { get; set; } = DateTime.UtcNow;

    // 静态工厂：从 EntityMetadata 转换 (用于 Code-First)
    public static MetadataDocument FromEntityMetadata(EntityMetadata metadata)
    {
        if (metadata == null) throw new ArgumentNullException(nameof(metadata));

        var doc = new MetadataDocument
        {
            TableName = metadata.CollectionName,
            TypeName = metadata.TypeName,
            DisplayName = metadata.DisplayName,
            Description = metadata.Description,
            CreatedAt = DateTime.UtcNow,
            UpdatedAt = DateTime.UtcNow
        };

        var cols = new List<BsonValue>(metadata.Properties.Count);
        foreach (var p in metadata.Properties)
        {
            var fieldName = p.IsPrimaryKey ? "_id" : ToCamelCase(p.PropertyName);
            var col = new BsonDocument()
                .Set("n", fieldName)
                .Set("pn", p.PropertyName)
                .Set("t", p.PropertyType)
                .Set("o", p.Order)
                .Set("r", p.Required)
                .Set("pk", p.IsPrimaryKey);

            if (!string.IsNullOrEmpty(p.DisplayName) && p.DisplayName != p.PropertyName)
                col = col.Set("dn", p.DisplayName);

            if (!string.IsNullOrEmpty(p.Description))
                col = col.Set("desc", p.Description);

            if (!string.IsNullOrEmpty(p.ForeignKeyCollection))
                col = col.Set("fk", p.ForeignKeyCollection);

            if (p.Password != null)
                col = col.Set("pwd", PasswordToDocument(p.Password));

            cols.Add(col);
        }
        doc.Columns = new BsonArray(cols);
        return doc;
    }

    public EntityMetadata ToEntityMetadata()
    {
        var properties = new List<PropertyMetadata>();

        foreach (var col in Columns)
        {
            if (col is not BsonDocument colDoc) continue;

            var propertyName = colDoc.ContainsKey("pn") ? colDoc["pn"].ToString() : colDoc["n"].ToString();
            if (string.IsNullOrWhiteSpace(propertyName)) continue;

            var propertyType = colDoc.ContainsKey("t") ? colDoc["t"].ToString() : string.Empty;
            var displayName = colDoc.ContainsKey("dn") ? colDoc["dn"].ToString() : propertyName;
            var description = colDoc.ContainsKey("desc") ? colDoc["desc"].ToString() : null;
            var order = colDoc.ContainsKey("o") ? colDoc["o"].ToInt32() : 0;
            var required = colDoc.ContainsKey("r") && colDoc["r"].ToBoolean();
            var isPrimaryKey = colDoc.ContainsKey("pk") && colDoc["pk"].ToBoolean();
            var foreignKeyCollection = colDoc.ContainsKey("fk") ? colDoc["fk"].ToString() : null;
            var password = colDoc.ContainsKey("pwd") && colDoc["pwd"] is BsonDocument passwordDoc
                ? PasswordFromDocument(passwordDoc)
                : null;

            if (string.IsNullOrWhiteSpace(description)) description = null;
            if (string.IsNullOrWhiteSpace(foreignKeyCollection)) foreignKeyCollection = null;

            properties.Add(new PropertyMetadata
            {
                PropertyName = propertyName,
                PropertyType = propertyType,
                DisplayName = displayName,
                Description = description,
                Order = order,
                Required = required,
                IsPrimaryKey = isPrimaryKey,
                Password = password,
                ForeignKeyCollection = foreignKeyCollection
            });
        }

        return new EntityMetadata
        {
            TypeName = TypeName,
            CollectionName = TableName,
            DisplayName = DisplayName,
            Description = string.IsNullOrWhiteSpace(Description) ? null : Description,
            Properties = properties.OrderBy(p => p.Order).ToList()
        };
    }

    private static BsonDocument PasswordToDocument(PasswordMetadata password)
    {
        return new BsonDocument()
            .Set("is", password.IsPassword)
            .Set("strength", (int)password.RequiredStrength)
            .Set("min", password.MinLength)
            .Set("max", password.MaxLength)
            .Set("special", password.RequireSpecialChar)
            .Set("number", password.RequireNumber)
            .Set("upper", password.RequireUppercase)
            .Set("lower", password.RequireLowercase)
            .Set("hint", string.IsNullOrWhiteSpace(password.Hint) ? (BsonValue)BsonNull.Value : new BsonString(password.Hint))
            .Set("indicator", password.ShowStrengthIndicator)
            .Set("toggle", password.AllowToggle);
    }

    private static PasswordMetadata PasswordFromDocument(BsonDocument document)
    {
        return new PasswordMetadata
        {
            IsPassword = document.ContainsKey("is") && document["is"].ToBoolean(),
            RequiredStrength = document.ContainsKey("strength")
                ? (PasswordStrength)document["strength"].ToInt32()
                : PasswordStrength.Medium,
            MinLength = document.ContainsKey("min") ? document["min"].ToInt32() : 8,
            MaxLength = document.ContainsKey("max") ? document["max"].ToInt32() : 128,
            RequireSpecialChar = !document.ContainsKey("special") || document["special"].ToBoolean(),
            RequireNumber = !document.ContainsKey("number") || document["number"].ToBoolean(),
            RequireUppercase = !document.ContainsKey("upper") || document["upper"].ToBoolean(),
            RequireLowercase = !document.ContainsKey("lower") || document["lower"].ToBoolean(),
            Hint = document.ContainsKey("hint") && !document["hint"].IsNull ? document["hint"].ToString() : null,
            ShowStrengthIndicator = !document.ContainsKey("indicator") || document["indicator"].ToBoolean(),
            AllowToggle = !document.ContainsKey("toggle") || document["toggle"].ToBoolean()
        };
    }

    private static string ToCamelCase(string name)
    {
        return BsonFieldName.ToCamelCase(name);
    }
}
