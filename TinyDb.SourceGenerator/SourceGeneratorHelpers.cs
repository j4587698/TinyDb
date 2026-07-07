using System;
using System.Text;

namespace TinyDb.SourceGenerator;

public static partial class SourceGeneratorHelpers
{
    /// <summary>
    /// 检查属性是否为ID属性
    /// </summary>
    private static bool IsIdProperty(PropertyInfo prop)
    {
        return prop.IsId;
    }

    /// <summary>
    /// 生成属性序列化代码
    /// </summary>
    public static string GeneratePropertySerialization(PropertyInfo prop)
    {
        var propertyName = prop.Name;
        var propertyAccess = prop.AccessName;
        var propertyType = prop.Type;

        // 检查是否是ID属性，如果是则映射到_id字段，否则使用camelCase
        var bsonFieldName = IsIdProperty(prop) ? "_id" : SourceGeneratorFieldName.ToCamelCase(propertyName);

        // 处理 [BsonRef] 属性 - 序列化为 DbRef 格式
        if (prop.IsDbRef)
        {
            return GenerateDbRefSerialization(prop, bsonFieldName);
        }

        // 处理复杂类型
        if (prop.IsComplexType)
        {
            return GenerateComplexTypeSerialization(prop, bsonFieldName);
        }

        // 处理集合中包含复杂类型的情况
        if (prop.IsCollection && prop.IsElementComplexType)
        {
            return GenerateCollectionWithComplexElementSerialization(prop, bsonFieldName);
        }

        // 处理字典中包含复杂类型值的情况
        if (prop.IsDictionary && prop.IsDictionaryValueComplexType)
        {
            return GenerateDictionaryWithComplexValueSerialization(prop, bsonFieldName);
        }

        return propertyType switch
        {
            "string" => $"document = document.Set(\"{bsonFieldName}\", string.IsNullOrEmpty(entity.{propertyAccess}) ? BsonNull.Value : new BsonString(entity.{propertyAccess}));",
            "int" or "Int32" => $"document = document.Set(\"{bsonFieldName}\", new BsonInt32(entity.{propertyAccess}));",
            "long" or "Int64" => $"document = document.Set(\"{bsonFieldName}\", new BsonInt64(entity.{propertyAccess}));",
            "double" or "Double" => $"document = document.Set(\"{bsonFieldName}\", new BsonDouble(entity.{propertyAccess}));",
            "float" or "Single" => $"document = document.Set(\"{bsonFieldName}\", new BsonDouble(entity.{propertyAccess}));",
            "decimal" or "Decimal" => $"document = document.Set(\"{bsonFieldName}\", new BsonDecimal128(entity.{propertyAccess}));",
            "bool" or "Boolean" => $"document = document.Set(\"{bsonFieldName}\", new BsonBoolean(entity.{propertyAccess}));",
            "DateTime" => $"document = document.Set(\"{bsonFieldName}\", new BsonDateTime(entity.{propertyAccess}));",
            "Guid" => $"document = document.Set(\"{bsonFieldName}\", new BsonBinary(entity.{propertyAccess}));",
            "ObjectId" => $"document = document.Set(\"{bsonFieldName}\", new BsonObjectId(entity.{propertyAccess}));",
            _ when propertyType.EndsWith("?") => GenerateNullablePropertySerialization(prop, bsonFieldName),
            _ => $"document = document.Set(\"{bsonFieldName}\", ConvertToBsonValue(entity.{propertyAccess}));"
        };
    }

    /// <summary>
    /// 生成 DbRef 序列化代码
    /// </summary>
    private static string GenerateDbRefSerialization(PropertyInfo prop, string bsonFieldName)
    {
        var propertyName = prop.Name;
        var propertyAccess = prop.AccessName;
        var collectionName = prop.BsonRefCollectionName!;
        var isNullable = prop.IsNullableReferenceType || prop.Type.EndsWith("?");

        var sb = new StringBuilder();

        // 处理集合类型（List<T>, T[] 等）
        if (prop.IsCollection)
        {
            if (isNullable)
            {
                sb.AppendLine($@"if (entity.{propertyAccess} == null)
                document = document.Set(""{bsonFieldName}"", BsonNull.Value);
            else
            {{");
            }

            sb.AppendLine($@"var dbRefArray_{propertyName} = new BsonArray();
            foreach (var item in entity.{propertyAccess})
            {{
                if (item == null)
                    dbRefArray_{propertyName} = dbRefArray_{propertyName}.AddValue(BsonNull.Value);
                else
                {{
                    var itemId = global::TinyDb.References.DbRefSerializer.GetEntityId(item);
                    var itemRef = new BsonDocument()
                        .Set(""$id"", itemId)
                        .Set(""$ref"", {TinyDbSourceGenerator.ToCSharpStringLiteral(collectionName)});
                    dbRefArray_{propertyName} = dbRefArray_{propertyName}.AddValue(itemRef);
                }}
            }}
            document = document.Set(""{bsonFieldName}"", dbRefArray_{propertyName});");

            if (isNullable)
            {
                sb.AppendLine("}");
            }
        }
        else
        {
            // 单个对象引用
            if (isNullable)
            {
                sb.AppendLine($@"if (entity.{propertyAccess} == null)
                document = document.Set(""{bsonFieldName}"", BsonNull.Value);
            else
            {{
                var refId_{propertyName} = global::TinyDb.References.DbRefSerializer.GetEntityId(entity.{propertyAccess});
                var refDoc_{propertyName} = new BsonDocument()
                    .Set(""$id"", refId_{propertyName})
                    .Set(""$ref"", {TinyDbSourceGenerator.ToCSharpStringLiteral(collectionName)});
                document = document.Set(""{bsonFieldName}"", refDoc_{propertyName});
            }}");
            }
            else
            {
                sb.AppendLine($@"{{
                var refId_{propertyName} = global::TinyDb.References.DbRefSerializer.GetEntityId(entity.{propertyAccess});
                var refDoc_{propertyName} = new BsonDocument()
                    .Set(""$id"", refId_{propertyName})
                    .Set(""$ref"", {TinyDbSourceGenerator.ToCSharpStringLiteral(collectionName)});
                document = document.Set(""{bsonFieldName}"", refDoc_{propertyName});
            }}");
            }
        }

        return sb.ToString();
    }

    /// <summary>
    /// 生成可空属性序列化代码
    /// </summary>
    private static string GenerateNullablePropertySerialization(PropertyInfo prop, string bsonFieldName)
    {
        var propertyAccess = prop.AccessName;

        // 如果底层类型是复杂类型
        return $"document = document.Set(\"{bsonFieldName}\", entity.{propertyAccess} == null ? BsonNull.Value : ConvertToBsonValue(entity.{propertyAccess}));";
    }

    /// <summary>
    /// 生成复杂类型属性的序列化代码
    /// </summary>
    private static string GenerateComplexTypeSerialization(PropertyInfo prop, string bsonFieldName)
    {
        var propertyName = prop.Name;
        var propertyAccess = prop.AccessName;
        var isNullable = prop.IsNullableReferenceType || prop.Type.EndsWith("?");

        if (isNullable)
        {
            return $@"if (entity.{propertyAccess} == null)
                document = document.Set(""{bsonFieldName}"", BsonNull.Value);
            else
                document = document.Set(""{bsonFieldName}"", SerializeComplexObject(entity.{propertyAccess}));";
        }

        return $"document = document.Set(\"{bsonFieldName}\", SerializeComplexObject(entity.{propertyAccess}));";
    }

    /// <summary>
    /// 生成包含复杂类型元素的集合的序列化代码
    /// </summary>
    private static string GenerateCollectionWithComplexElementSerialization(PropertyInfo prop, string bsonFieldName)
    {
        var propertyName = prop.Name;
        var propertyAccess = prop.AccessName;
        var isNullable = prop.IsNullableReferenceType || prop.Type.EndsWith("?");
        var isElementValueType = prop.IsElementValueType;

        var sb = new StringBuilder();

        if (isNullable)
        {
            sb.AppendLine($@"if (entity.{propertyAccess} == null)
                document = document.Set(""{bsonFieldName}"", BsonNull.Value);
            else
            {{");
        }

        sb.AppendLine($@"var array_{propertyName} = new BsonArray();
            foreach (var item in entity.{propertyAccess})
            {{");

        // 值类型不能与 null 比较
        if (isElementValueType)
        {
            sb.AppendLine($@"                array_{propertyName} = array_{propertyName}.AddValue(SerializeComplexObject(item));");
        }
        else
        {
            sb.AppendLine($@"                if (item == null)
                    array_{propertyName} = array_{propertyName}.AddValue(BsonNull.Value);
                else
                    array_{propertyName} = array_{propertyName}.AddValue(SerializeComplexObject(item));");
        }

        sb.AppendLine($@"            }}
            document = document.Set(""{bsonFieldName}"", array_{propertyName});");

        if (isNullable)
        {
            sb.AppendLine("}");
        }

        return sb.ToString();
    }

    /// <summary>
    /// 生成包含复杂类型值的字典的序列化代码
    /// </summary>
    private static string GenerateDictionaryWithComplexValueSerialization(PropertyInfo prop, string bsonFieldName)
    {
        var propertyName = prop.Name;
        var propertyAccess = prop.AccessName;
        var isNullable = prop.IsNullableReferenceType || prop.Type.EndsWith("?");
        var isValueValueType = prop.IsDictionaryValueValueType;

        var sb = new StringBuilder();

        if (isNullable)
        {
            sb.AppendLine($@"if (entity.{propertyAccess} == null)
                document = document.Set(""{bsonFieldName}"", BsonNull.Value);
            else
            {{");
        }

        sb.AppendLine($@"var dict_{propertyName} = new BsonDocument();
            foreach (var kvp in entity.{propertyAccess})
            {{");

        // 值类型不能与 null 比较
        if (isValueValueType)
        {
            sb.AppendLine($@"                dict_{propertyName} = dict_{propertyName}.Set(kvp.Key.ToString(), SerializeComplexObject(kvp.Value));");
        }
        else
        {
            sb.AppendLine($@"                if (kvp.Value == null)
                    dict_{propertyName} = dict_{propertyName}.Set(kvp.Key.ToString(), BsonNull.Value);
                else
                    dict_{propertyName} = dict_{propertyName}.Set(kvp.Key.ToString(), SerializeComplexObject(kvp.Value));");
        }

        sb.AppendLine($@"            }}
            document = document.Set(""{bsonFieldName}"", dict_{propertyName});");

        if (isNullable)
        {
            sb.AppendLine("}");
        }

        return sb.ToString();
    }

    /// <summary>
    /// 生成属性反序列化代码
    /// </summary>
    public static string GeneratePropertyDeserialization(PropertyInfo prop)
    {
        var propertyName = prop.Name;
        var propertyAccess = prop.AccessName;
        var propertyType = prop.Type;

        // 检查是否是ID属性，如果是则从_id字段读取，否则使用camelCase
        var bsonFieldName = IsIdProperty(prop) ? "_id" : SourceGeneratorFieldName.ToCamelCase(propertyName);

        // 处理 [BsonRef] 属性 - 反序列化时只读取 DbRef 信息，不自动加载
        // 实际加载由 Include() 在查询时处理
        if (prop.IsDbRef)
        {
            return GenerateDbRefDeserialization(prop, bsonFieldName);
        }

        // 处理复杂类型
        if (prop.IsComplexType)
        {
            return GenerateComplexTypeDeserialization(prop, bsonFieldName);
        }

        // 处理集合类型（List, Array等）
        if (prop.IsCollection)
        {
            return GenerateCollectionDeserialization(prop, bsonFieldName);
        }

        // 处理字典类型
        if (prop.IsDictionary)
        {
            return GenerateDictionaryDeserialization(prop, bsonFieldName);
        }

        if (prop.IsEnum && !prop.IsNullableValueType && !prop.Type.EndsWith("?", StringComparison.Ordinal))
        {
            return $"if (document.TryGetValue(\"{bsonFieldName}\", out var bson{propertyName})) entity.{propertyAccess} = global::TinyDb.Serialization.BsonConversion.FromBsonValueEnum<{prop.FullyQualifiedNonNullableType}>(bson{propertyName});";
        }

        return propertyType switch
        {
            "string" => $"if (document.TryGetValue(\"{bsonFieldName}\", out var bson{propertyName}) && bson{propertyName} is BsonString str{propertyName}) entity.{propertyAccess} = str{propertyName}.Value;",
            "int" or "Int32" => $"if (document.TryGetValue(\"{bsonFieldName}\", out var bson{propertyName}) && bson{propertyName} is BsonInt32 int{propertyName}) entity.{propertyAccess} = int{propertyName}.Value;",
            "long" or "Int64" => $"if (document.TryGetValue(\"{bsonFieldName}\", out var bson{propertyName}) && bson{propertyName} is BsonInt64 long{propertyName}) entity.{propertyAccess} = long{propertyName}.Value;",
            "double" or "Double" => $"if (document.TryGetValue(\"{bsonFieldName}\", out var bson{propertyName}) && bson{propertyName} is BsonDouble dbl{propertyName}) entity.{propertyAccess} = dbl{propertyName}.Value;",
            "float" or "Single" => $"if (document.TryGetValue(\"{bsonFieldName}\", out var bson{propertyName}) && bson{propertyName} is BsonDouble dbl{propertyName}) entity.{propertyAccess} = (float)dbl{propertyName}.Value;",
            "decimal" or "Decimal" => $"if (document.TryGetValue(\"{bsonFieldName}\", out var bson{propertyName}) && bson{propertyName} is BsonDecimal128 dec{propertyName}) entity.{propertyAccess} = dec{propertyName}.Value;",
            "bool" or "Boolean" => $"if (document.TryGetValue(\"{bsonFieldName}\", out var bson{propertyName}) && bson{propertyName} is BsonBoolean bool{propertyName}) entity.{propertyAccess} = bool{propertyName}.Value;",
            "DateTime" => $"if (document.TryGetValue(\"{bsonFieldName}\", out var bson{propertyName}) && bson{propertyName} is BsonDateTime dt{propertyName}) entity.{propertyAccess} = dt{propertyName}.Value;",
            "Guid" => $"if (document.TryGetValue(\"{bsonFieldName}\", out var bson{propertyName})) {{ if (bson{propertyName} is BsonBinary guid{propertyName}) entity.{propertyAccess} = new Guid(guid{propertyName}.Bytes); else if (bson{propertyName} is BsonString guidString{propertyName} && Guid.TryParse(guidString{propertyName}.Value, out var parsedGuid{propertyName})) entity.{propertyAccess} = parsedGuid{propertyName}; }}",
            "ObjectId" => $"if (document.TryGetValue(\"{bsonFieldName}\", out var bson{propertyName}) && bson{propertyName} is BsonObjectId oid{propertyName}) entity.{propertyAccess} = oid{propertyName}.Value;",
            _ when propertyType.EndsWith("?") => GenerateNullablePropertyDeserialization(prop, bsonFieldName),
            _ => $"if (document.TryGetValue(\"{bsonFieldName}\", out var bson{propertyName})) entity.{propertyAccess} = ConvertFromBsonValue<{prop.FullyQualifiedNonNullableType}>(bson{propertyName});"
        };
    }

    /// <summary>
    /// 生成 DbRef 反序列化代码 - 仅存储 DbRef 信息，实际加载由 Include() 处理
    /// </summary>
    private static string GenerateDbRefDeserialization(PropertyInfo prop, string bsonFieldName)
    {
        var propertyName = prop.Name;

        // DbRef 属性在反序列化时保持为 null/default
        // 实际的对象加载由 DbRefResolver 在 Include() 查询时处理
        // 这里生成的代码只是一个占位符，表示该属性是 DbRef 类型

        var sb = new StringBuilder();
        sb.AppendLine($@"// DbRef 属性 {propertyName} 的反序列化");
        sb.AppendLine($@"// 注意: DbRef 属性在基础反序列化时不会自动加载引用对象");
        sb.AppendLine($@"// 使用 Include(x => x.{propertyName}) 方法来加载引用的实体");
        sb.AppendLine($@"// 这里只存储原始的 DbRef 文档以便后续 Include 处理");
        sb.AppendLine($@"if (document.TryGetValue(""{bsonFieldName}"", out var dbRef_{propertyName}))");
        sb.AppendLine($@"{{");
        sb.AppendLine($@"    // DbRef 数据已在文档中，将由 DbRefResolver.Resolve() 在 Include() 时加载");
        sb.AppendLine($@"    // entity.{propertyName} 在此保持为 default，直到显式调用 Include()");
        sb.AppendLine($@"}}");

        return sb.ToString();
    }

    /// <summary>
    /// 生成可空属性反序列化代码
    /// </summary>
    private static string GenerateNullablePropertyDeserialization(PropertyInfo prop, string bsonFieldName)
    {
        var propertyName = prop.Name;
        var propertyAccess = prop.AccessName;
        var underlyingType = prop.NonNullableType;

        // 如果底层类型是复杂类型
        if (prop.IsEnum)
        {
            return $"if (document.TryGetValue(\"{bsonFieldName}\", out var bson{propertyName}) && !bson{propertyName}.IsNull) entity.{propertyAccess} = global::TinyDb.Serialization.BsonConversion.FromBsonValueEnum<{prop.FullyQualifiedNonNullableType}>(bson{propertyName}); else entity.{propertyAccess} = null;";
        }

        return underlyingType switch
        {
            "int" or "Int32" => $"if (document.TryGetValue(\"{bsonFieldName}\", out var bson{propertyName}) && bson{propertyName} is BsonInt32 int{propertyName}) entity.{propertyAccess} = int{propertyName}.Value; else entity.{propertyAccess} = null;",
            "long" or "Int64" => $"if (document.TryGetValue(\"{bsonFieldName}\", out var bson{propertyName}) && bson{propertyName} is BsonInt64 long{propertyName}) entity.{propertyAccess} = long{propertyName}.Value; else entity.{propertyAccess} = null;",
            "double" or "Double" => $"if (document.TryGetValue(\"{bsonFieldName}\", out var bson{propertyName}) && bson{propertyName} is BsonDouble dbl{propertyName}) entity.{propertyAccess} = dbl{propertyName}.Value; else entity.{propertyAccess} = null;",
            "bool" or "Boolean" => $"if (document.TryGetValue(\"{bsonFieldName}\", out var bson{propertyName}) && bson{propertyName} is BsonBoolean bool{propertyName}) entity.{propertyAccess} = bool{propertyName}.Value; else entity.{propertyAccess} = null;",
            "DateTime" => $"if (document.TryGetValue(\"{bsonFieldName}\", out var bson{propertyName}) && bson{propertyName} is BsonDateTime dt{propertyName}) entity.{propertyAccess} = dt{propertyName}.Value; else entity.{propertyAccess} = null;",
            "Guid" => $"if (document.TryGetValue(\"{bsonFieldName}\", out var bson{propertyName})) {{ if (bson{propertyName} is BsonBinary guid{propertyName}) entity.{propertyAccess} = new Guid(guid{propertyName}.Bytes); else if (bson{propertyName} is BsonString guidString{propertyName} && Guid.TryParse(guidString{propertyName}.Value, out var parsedGuid{propertyName})) entity.{propertyAccess} = parsedGuid{propertyName}; else entity.{propertyAccess} = null; }} else entity.{propertyAccess} = null;",
            "ObjectId" => $"if (document.TryGetValue(\"{bsonFieldName}\", out var bson{propertyName}) && bson{propertyName} is BsonObjectId oid{propertyName}) entity.{propertyAccess} = oid{propertyName}.Value; else entity.{propertyAccess} = null;",
            _ => $"if (document.TryGetValue(\"{bsonFieldName}\", out var bson{propertyName}) && !bson{propertyName}.IsNull) entity.{propertyAccess} = ConvertFromBsonValue<{prop.FullyQualifiedNonNullableType}>(bson{propertyName}); else entity.{propertyAccess} = null;"
        };
    }

    /// <summary>
    /// 生成复杂类型属性的反序列化代码
    /// </summary>
    private static string GenerateComplexTypeDeserialization(PropertyInfo prop, string bsonFieldName)
    {
        var propertyName = prop.Name;
        var propertyAccess = prop.AccessName;
        var propertyType = prop.FullyQualifiedNonNullableType;
        var isNullable = prop.IsNullableReferenceType || prop.Type.EndsWith("?");

        var sb = new StringBuilder();
        sb.AppendLine($@"if (document.TryGetValue(""{bsonFieldName}"", out var bson{propertyName}))
            {{
                if (bson{propertyName}.IsNull)
                {{");

        if (isNullable)
        {
            sb.AppendLine($"                    entity.{propertyAccess} = null;");
        }

        sb.AppendLine($@"                }}
                else if (bson{propertyName} is BsonDocument nested{propertyName})
                {{
                    entity.{propertyAccess} = DeserializeComplexObject<{propertyType}>(nested{propertyName});
                }}
            }}");

        return sb.ToString();
    }

    /// <summary>
    /// 生成包含元素的集合的反序列化代码
    /// </summary>
    private static string GenerateCollectionDeserialization(PropertyInfo prop, string bsonFieldName)
    {
        var propertyName = prop.Name;
        var propertyAccess = prop.AccessName;
        var elementType = prop.ElementType ?? "object";
        var isArray = prop.IsArray;
        var isNullable = prop.IsNullableReferenceType || prop.Type.EndsWith("?");
        var isElementComplex = prop.IsElementComplexType;

        var sb = new StringBuilder();
        sb.AppendLine($@"if (document.TryGetValue(""{bsonFieldName}"", out var bson{propertyName}))
            {{
                if (bson{propertyName}.IsNull)
                {{");

        if (isNullable)
        {
            sb.AppendLine($"                    entity.{propertyAccess} = null;");
        }

        sb.AppendLine($@"                }}
                else if (bson{propertyName} is BsonArray array{propertyName})
                {{
                    var list_{propertyName} = new System.Collections.Generic.List<{elementType}>();
                    foreach (var item in array{propertyName})
                    {{
                        if (item.IsNull)
                            list_{propertyName}.Add(default!);");

        if (isElementComplex)
        {
            sb.AppendLine($@"                        else if (item is BsonDocument itemDoc)
                            list_{propertyName}.Add(DeserializeComplexObject<{elementType}>(itemDoc));
                        else
                            list_{propertyName}.Add(ConvertFromBsonValue<{elementType}>(item));");
        }
        else
        {
            sb.AppendLine($@"                        else
                            list_{propertyName}.Add(ConvertFromBsonValue<{elementType}>(item));");
        }

        sb.AppendLine(@"                    }");

        if (isArray)
        {
            sb.AppendLine($"                    entity.{propertyAccess} = list_{propertyName}.ToArray();");
        }
        else
        {
            sb.AppendLine($"                    entity.{propertyAccess} = list_{propertyName};");
        }

        sb.AppendLine(@"                }
            }");

        return sb.ToString();
    }

    /// <summary>
    /// 生成包含值的字典的反序列化代码
    /// </summary>
    private static string GenerateDictionaryDeserialization(PropertyInfo prop, string bsonFieldName)
    {
        var propertyName = prop.Name;
        var propertyAccess = prop.AccessName;
        var keyType = prop.DictionaryKeyType ?? "string";
        var valueType = prop.DictionaryValueType ?? "object";
        var isNullable = prop.IsNullableReferenceType || prop.Type.EndsWith("?");
        var isValueComplex = prop.IsDictionaryValueComplexType;

        var sb = new StringBuilder();
        sb.AppendLine($@"if (document.TryGetValue(""{bsonFieldName}"", out var bson{propertyName}))
            {{
                if (bson{propertyName}.IsNull)
                {{");

        if (isNullable)
        {
            sb.AppendLine($"                    entity.{propertyAccess} = null;");
        }

        sb.AppendLine($@"                }}
                else if (bson{propertyName} is BsonDocument dict{propertyName})
                {{
                    var result_{propertyName} = new System.Collections.Generic.Dictionary<{keyType}, {valueType}>();
                    foreach (var kvp in dict{propertyName})
                    {{
                        if (kvp.Value.IsNull)
                            result_{propertyName}[kvp.Key] = default!;");

        if (isValueComplex)
        {
            sb.AppendLine($@"                        else if (kvp.Value is BsonDocument valueDoc)
                            result_{propertyName}[kvp.Key] = DeserializeComplexObject<{valueType}>(valueDoc);
                        else
                            result_{propertyName}[kvp.Key] = ConvertFromBsonValue<{valueType}>(kvp.Value);");
        }
        else
        {
            sb.AppendLine($@"                        else
                            result_{propertyName}[kvp.Key] = ConvertFromBsonValue<{valueType}>(kvp.Value);");
        }

        sb.AppendLine($@"                    }}
                    entity.{propertyAccess} = result_{propertyName};
                }}
            }}");

        return sb.ToString();
    }

}
