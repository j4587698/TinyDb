using System;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Text;
using System.Collections.Immutable;
using System.Text;

namespace TinyDb.SourceGenerator;

public partial class TinyDbSourceGenerator
{

    /// <summary>
    /// 为依赖类型生成内联序列化方法
    /// </summary>
    private static void GenerateInlineSerializerForDependentType(StringBuilder sb, DependentComplexType depType)
    {
        sb.AppendLine($"        /// <summary>");
        sb.AppendLine($"        /// {depType.ShortName} 的内联序列化方法（AOT兼容）");
        sb.AppendLine($"        /// </summary>");
        sb.AppendLine($"        private static BsonDocument Serialize_{depType.SafeMethodName}({depType.FullyQualifiedName} obj)");
        sb.AppendLine("        {");
        sb.AppendLine("            var documentBuilder = new BsonDocumentBuilder();");
        sb.AppendLine();

        // 为每个属性生成序列化代码
        foreach (var prop in depType.Properties)
        {
            var bsonFieldName = SourceGeneratorFieldName.ToCamelCase(prop.Name);
            var propertyAccess = prop.AccessName;

            if (prop.IsComplexType && !string.IsNullOrEmpty(prop.ComplexTypeFullName))
            {
                // 检测是否是循环引用属性
                if (prop.IsCircularReference)
                {
                    // 循环引用属性：跳过递归序列化，设置为 null 避免栈溢出
                    sb.AppendLine($"            // 注意：属性 {prop.Name} 涉及循环引用，跳过递归序列化以避免栈溢出");
                    sb.AppendLine($"            documentBuilder.Set(\"{bsonFieldName}\", BsonNull.Value);");
                }
                else
                {
                    // 复杂类型使用递归调用
                    if (prop.IsNullable || !prop.IsValueType)
                    {
                        sb.AppendLine($"            if (obj.{propertyAccess} == null)");
                        sb.AppendLine($"                documentBuilder.Set(\"{bsonFieldName}\", BsonNull.Value);");
                        sb.AppendLine($"            else");
                        sb.AppendLine($"                documentBuilder.Set(\"{bsonFieldName}\", SerializeComplexObject(obj.{propertyAccess}));");
                    }
                    else
                    {
                        sb.AppendLine($"            documentBuilder.Set(\"{bsonFieldName}\", SerializeComplexObject(obj.{propertyAccess}));");
                    }
                }
            }
            else if (prop.IsCollection && prop.IsElementComplexType)
            {
                if (prop.IsNullable)
                {
                    sb.AppendLine($"            if (obj.{propertyAccess} == null)");
                    sb.AppendLine($"                documentBuilder.Set(\"{bsonFieldName}\", BsonNull.Value);");
                    sb.AppendLine($"            else");
                    sb.AppendLine($"            {{");
                }

                sb.AppendLine($"            var array_{prop.Name} = new BsonArray();");
                sb.AppendLine($"            foreach (var item in obj.{propertyAccess})");
                sb.AppendLine($"            {{");

                if (prop.IsElementValueType)
                {
                    sb.AppendLine($"                array_{prop.Name} = array_{prop.Name}.AddValue(SerializeComplexObject(item));");
                }
                else
                {
                    sb.AppendLine($"                if (item == null)");
                    sb.AppendLine($"                    array_{prop.Name} = array_{prop.Name}.AddValue(BsonNull.Value);");
                    sb.AppendLine($"                else");
                    sb.AppendLine($"                    array_{prop.Name} = array_{prop.Name}.AddValue(SerializeComplexObject(item));");
                }

                sb.AppendLine($"            }}");
                sb.AppendLine($"            documentBuilder.Set(\"{bsonFieldName}\", array_{prop.Name});");

                if (prop.IsNullable)
                {
                    sb.AppendLine($"            }}");
                }
            }
            else if (prop.IsDictionary)
            {
                var valueExpression = prop.IsDictionaryValueComplexType
                    ? "SerializeComplexObject(kvp.Value)"
                    : "ConvertToBsonValue(kvp.Value)";

                if (prop.IsNullable)
                {
                    sb.AppendLine($"            if (obj.{propertyAccess} == null)");
                    sb.AppendLine($"                documentBuilder.Set(\"{bsonFieldName}\", BsonNull.Value);");
                    sb.AppendLine($"            else");
                    sb.AppendLine($"            {{");
                }

                sb.AppendLine($"            var dict_{prop.Name} = new BsonDocument();");
                sb.AppendLine($"            foreach (var kvp in obj.{propertyAccess})");
                sb.AppendLine($"            {{");
                sb.AppendLine($"                var key_{prop.Name} = {SourceGeneratorHelpers.CreateDictionaryFieldNameExpression("kvp.Key")};");

                if (prop.IsDictionaryValueValueType)
                {
                    sb.AppendLine($"                dict_{prop.Name} = dict_{prop.Name}.Set(key_{prop.Name}, {valueExpression});");
                }
                else
                {
                    sb.AppendLine($"                if (kvp.Value == null)");
                    sb.AppendLine($"                    dict_{prop.Name} = dict_{prop.Name}.Set(key_{prop.Name}, BsonNull.Value);");
                    sb.AppendLine($"                else");
                    sb.AppendLine($"                    dict_{prop.Name} = dict_{prop.Name}.Set(key_{prop.Name}, {valueExpression});");
                }

                sb.AppendLine($"            }}");
                sb.AppendLine($"            documentBuilder.Set(\"{bsonFieldName}\", dict_{prop.Name});");

                if (prop.IsNullable)
                {
                    sb.AppendLine($"            }}");
                }
            }
            else
            {
                // 简单类型使用 ConvertToBsonValue
                sb.AppendLine($"            documentBuilder.Set(\"{bsonFieldName}\", ConvertToBsonValue(obj.{propertyAccess}));");
            }
        }

        sb.AppendLine("            return documentBuilder.Build();");
        sb.AppendLine("        }");
        sb.AppendLine();
    }


    /// <summary>
    /// 为依赖类型生成内联反序列化方法
    /// </summary>
    private static void GenerateInlineDeserializerForDependentType(StringBuilder sb, DependentComplexType depType)
    {
        sb.AppendLine($"        /// <summary>");
        sb.AppendLine($"        /// {depType.ShortName} 的内联反序列化方法（AOT兼容）");
        sb.AppendLine($"        /// </summary>");
        sb.AppendLine($"        private static {depType.FullyQualifiedName} Deserialize_{depType.SafeMethodName}(BsonDocument document)");
        sb.AppendLine("        {");

        // 对于 struct，需要使用 default 初始化
        if (depType.IsValueType)
        {
            sb.AppendLine($"            var result = default({depType.FullyQualifiedName});");
        }
        else if (depType.HasAccessibleParameterlessConstructor)
        {
            sb.AppendLine($"            var result = new {depType.FullyQualifiedName}();");
        }
        else
        {
            sb.AppendLine($"            throw new global::System.NotSupportedException(\"Dependent type '{depType.FullyQualifiedName}' must have an accessible parameterless constructor for inline TinyDb deserialization.\");");
            sb.AppendLine("        }");
            sb.AppendLine();
            return;
        }
        sb.AppendLine();

        // 为每个属性生成反序列化代码
        foreach (var prop in depType.Properties)
        {
            var bsonFieldName = SourceGeneratorFieldName.ToCamelCase(prop.Name);

            if (prop.IsComplexType && !string.IsNullOrEmpty(prop.ComplexTypeFullName))
            {
                // 检测是否是循环引用属性
                if (prop.IsCircularReference)
                {
                    // 循环引用属性：跳过递归反序列化，保持默认值
                    sb.AppendLine($"            // 注意：属性 {prop.Name} 涉及循环引用，跳过递归反序列化以避免栈溢出");
                    sb.AppendLine($"            // result.{prop.Name} 保持默认值");
                }
                else
                {
                    // 复杂类型使用递归调用
                    sb.AppendLine($"            if (document.TryGetValue(\"{bsonFieldName}\", out var bson_{prop.Name}))");
                    sb.AppendLine("            {");
                    sb.AppendLine($"                if (bson_{prop.Name}.IsNull)");
                    sb.AppendLine("                {");
                    // 仅在目标属性本身可空时才写入 null/default；
                    // 对非可空引用类型保持构造时默认值，避免生成 CS8625。
                    if (prop.IsNullable)
                    {
                        sb.AppendLine($"                    result.{prop.AccessName} = default;");
                    }
                    sb.AppendLine("                }");
                    sb.AppendLine($"                else if (bson_{prop.Name} is BsonDocument nested_{prop.Name})");
                    sb.AppendLine("                {");
                    sb.AppendLine($"                    result.{prop.AccessName} = DeserializeComplexObject<{prop.ComplexTypeFullName}>(nested_{prop.Name});");
                    sb.AppendLine("                }");
                    sb.AppendLine("            }");
                }
            }
            else if (prop.IsCollection)
            {
                var elementType = prop.ElementType ?? "object";
                var collectionInstance = SourceGeneratorHelpers.CreateCollectionInstanceExpression(
                    prop.FullyQualifiedTypeName,
                    elementType);

                sb.AppendLine($"            if (document.TryGetValue(\"{bsonFieldName}\", out var bson_{prop.Name}))");
                sb.AppendLine("            {");
                sb.AppendLine($"                if (bson_{prop.Name}.IsNull)");
                sb.AppendLine("                {");
                if (prop.IsNullable)
                {
                    sb.AppendLine($"                    result.{prop.AccessName} = default!;");
                }
                sb.AppendLine("                }");
                sb.AppendLine($"                else if (bson_{prop.Name} is BsonArray array_{prop.Name})");
                sb.AppendLine("                {");
                sb.AppendLine($"                    var list_{prop.Name} = {collectionInstance};");
                sb.AppendLine($"                    foreach (var item in array_{prop.Name})");
                sb.AppendLine("                    {");
                sb.AppendLine($"                        if (item.IsNull)");
                sb.AppendLine($"                            list_{prop.Name}.Add(default!);");

                if (prop.IsElementComplexType)
                {
                    sb.AppendLine($"                        else if (item is BsonDocument itemDoc)");
                    sb.AppendLine($"                            list_{prop.Name}.Add(DeserializeComplexObject<{elementType}>(itemDoc));");
                    sb.AppendLine("                        else");
                    sb.AppendLine($"                            list_{prop.Name}.Add(ConvertFromBsonValue<{elementType}>(item));");
                }
                else
                {
                    sb.AppendLine("                        else");
                    sb.AppendLine($"                            list_{prop.Name}.Add(ConvertFromBsonValue<{elementType}>(item));");
                }

                sb.AppendLine("                    }");
                if (prop.IsArray)
                {
                    sb.AppendLine($"                    result.{prop.AccessName} = list_{prop.Name}.ToArray();");
                }
                else
                {
                    sb.AppendLine($"                    result.{prop.AccessName} = list_{prop.Name};");
                }
                sb.AppendLine("                }");
                sb.AppendLine("            }");
            }
            else if (prop.IsDictionary)
            {
                var keyType = prop.DictionaryKeyType ?? "string";
                var valueType = prop.DictionaryValueType ?? "object";

                sb.AppendLine($"            if (document.TryGetValue(\"{bsonFieldName}\", out var bson_{prop.Name}))");
                sb.AppendLine("            {");
                sb.AppendLine($"                if (bson_{prop.Name}.IsNull)");
                sb.AppendLine("                {");
                if (prop.IsNullable)
                {
                    sb.AppendLine($"                    result.{prop.AccessName} = default!;");
                }
                sb.AppendLine("                }");
                sb.AppendLine($"                else if (bson_{prop.Name} is BsonDocument dict_{prop.Name})");
                sb.AppendLine("                {");
                sb.AppendLine($"                    var result_{prop.Name} = new System.Collections.Generic.Dictionary<{keyType}, {valueType}>();");
                sb.AppendLine($"                    foreach (var kvp in dict_{prop.Name})");
                sb.AppendLine("                    {");
                sb.AppendLine($"                        var key_{prop.Name} = {SourceGeneratorHelpers.CreateDictionaryKeyExpression(keyType, "kvp.Key")};");
                sb.AppendLine("                        if (kvp.Value.IsNull)");
                sb.AppendLine($"                            result_{prop.Name}[key_{prop.Name}] = default!;");

                if (prop.IsDictionaryValueComplexType)
                {
                    sb.AppendLine("                        else if (kvp.Value is BsonDocument valueDoc)");
                    sb.AppendLine($"                            result_{prop.Name}[key_{prop.Name}] = DeserializeComplexObject<{valueType}>(valueDoc);");
                    sb.AppendLine("                        else");
                    sb.AppendLine($"                            result_{prop.Name}[key_{prop.Name}] = ConvertFromBsonValue<{valueType}>(kvp.Value);");
                }
                else
                {
                    sb.AppendLine("                        else");
                    sb.AppendLine($"                            result_{prop.Name}[key_{prop.Name}] = ConvertFromBsonValue<{valueType}>(kvp.Value);");
                }

                sb.AppendLine("                    }");
                sb.AppendLine($"                    result.{prop.AccessName} = result_{prop.Name};");
                sb.AppendLine("                }");
                sb.AppendLine("            }");
            }
            else
            {
                // 简单类型使用 ConvertFromBsonValue
                sb.AppendLine($"            if (document.TryGetValue(\"{bsonFieldName}\", out var bson_{prop.Name}) && !bson_{prop.Name}.IsNull)");
                sb.AppendLine("            {");
                sb.AppendLine($"                result.{prop.AccessName} = ConvertFromBsonValue<{prop.FullyQualifiedTypeName}>(bson_{prop.Name});");
                sb.AppendLine("            }");
            }
        }

        sb.AppendLine("            return result;");
        sb.AppendLine("        }");
        sb.AppendLine();
    }

}
