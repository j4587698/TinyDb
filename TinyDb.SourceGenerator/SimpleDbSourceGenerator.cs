using System;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Text;
using System.Collections.Immutable;
using System.Text;

namespace TinyDb.SourceGenerator;

/// <summary>
/// SimpleDb AOT 源代码生成器
/// </summary>
[Generator(LanguageNames.CSharp)]
public class TinyDbSourceGenerator : IIncrementalGenerator
{
    /// <summary>
    /// 初始化生成器
    /// </summary>
    public void Initialize(IncrementalGeneratorInitializationContext context)
    {
        // 查找所有类声明
        var classDeclarations = context.SyntaxProvider
            .CreateSyntaxProvider(
                predicate: static (s, _) => s is ClassDeclarationSyntax,
                transform: static (ctx, _) => GetClassInfo(ctx))
            .Where(static m => m is not null)
            .Collect();

        // 注册源代码生成
        context.RegisterSourceOutput(classDeclarations, static (spc, classes) =>
        {
            if (classes.IsDefaultOrEmpty) return;

            var selectedClasses = classes
                .Where(classInfo => classInfo != null && ShouldGenerateMapper(classInfo))
                .Select(classInfo => classInfo!)
                .ToList();

            if (selectedClasses.Count == 0) return;

            foreach (var classInfo in selectedClasses)
            {
                var partialClassCode = GeneratePartialClass(classInfo);
                var partialFileName = $"{classInfo.Name}_AotHelper.g.cs";
                spc.AddSource(partialFileName, SourceText.From(partialClassCode, Encoding.UTF8));
            }

            var registrySource = GenerateRegistrySource(selectedClasses);
            spc.AddSource("AotHelperRegistry.g.cs", SourceText.From(registrySource, Encoding.UTF8));
        });
    }

    /// <summary>
    /// 获取类信息
    /// </summary>
    private static ClassInfo? GetClassInfo(GeneratorSyntaxContext context)
    {
        var classDeclaration = (ClassDeclarationSyntax)context.Node;
        var semanticModel = context.SemanticModel;

        // 获取类的完整名称
        var namespaceDeclaration = classDeclaration.FirstAncestorOrSelf<NamespaceDeclarationSyntax>();
        var namespaceName = namespaceDeclaration?.Name.ToString() ?? string.Empty;
        var className = classDeclaration.Identifier.Text;

        // 获取类符号信息
        var classSymbol = semanticModel.GetDeclaredSymbol(classDeclaration);

        // 如果没有找到命名空间声明，使用符号信息获取命名空间
        if (string.IsNullOrEmpty(namespaceName))
        {
            namespaceName = classSymbol?.ContainingNamespace?.ToString() ?? string.Empty;
        }

        // 检查是否有Entity属性
        var hasEntityAttribute = classSymbol?.GetAttributes()
            .Any(attr => attr.AttributeClass?.Name == "EntityAttribute") ?? false;

        if (!hasEntityAttribute) return null;

        // 获取Entity属性信息
        var entityAttribute = classSymbol?.GetAttributes()
            .FirstOrDefault(attr => attr.AttributeClass?.Name == "EntityAttribute");

        var collectionName = entityAttribute?.ConstructorArguments.FirstOrDefault().Value?.ToString();

        // 获取Entity属性中指定的IdProperty名称
        var specifiedIdProperty = entityAttribute?.NamedArguments
            .FirstOrDefault(arg => arg.Key == "IdProperty").Value.Value?.ToString();

        // 获取属性信息
        var properties = new List<PropertyInfo>();
        PropertyInfo? idProperty = null;

        foreach (var member in classDeclaration.Members)
        {
            if (member is PropertyDeclarationSyntax property)
            {
                var propertyName = property.Identifier.Text;
                var propertyType = property.Type.ToString();

                // 检查是否有BsonIgnore属性
                var propertySymbol = semanticModel.GetDeclaredSymbol(property) as IPropertySymbol;
                var hasIgnoreAttribute = propertySymbol?.GetAttributes()
                    .Any(attr => attr.AttributeClass?.Name == "BsonIgnoreAttribute") ?? false;

                var typeSymbol = propertySymbol?.Type;
                var isValueType = typeSymbol?.IsValueType ?? false;
                var isNullableValueType = typeSymbol is INamedTypeSymbol { IsGenericType: true } namedType &&
                                           namedType.OriginalDefinition.SpecialType == SpecialType.System_Nullable_T;
                var isNullableReferenceType = typeSymbol is { IsReferenceType: true } &&
                                              propertySymbol?.NullableAnnotation == NullableAnnotation.Annotated;

                var fullyQualifiedType = typeSymbol?.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat) ?? propertyType;
                var nonNullableType = propertyType;
                var fullyQualifiedNonNullableType = fullyQualifiedType;

                if (isNullableValueType && typeSymbol is INamedTypeSymbol nullableType && nullableType.TypeArguments.Length == 1)
                {
                    var underlyingType = nullableType.TypeArguments[0];
                    nonNullableType = underlyingType.ToDisplayString(SymbolDisplayFormat.MinimallyQualifiedFormat);
                    fullyQualifiedNonNullableType = underlyingType.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
                }
                else if (!isValueType && propertyType.EndsWith("?", StringComparison.Ordinal))
                {
                    nonNullableType = propertyType.TrimEnd('?').Trim();
                    fullyQualifiedNonNullableType = fullyQualifiedType.TrimEnd('?').Trim();
                }

                var propInfo = new PropertyInfo(
                    propertyName,
                    propertyType,
                    isId: false,
                    isIgnored: hasIgnoreAttribute,
                    hasIgnoreAttribute: hasIgnoreAttribute,
                    isValueType: isValueType,
                    isNullableValueType: isNullableValueType,
                    isNullableReferenceType: isNullableReferenceType,
                    nonNullableType: nonNullableType,
                    fullyQualifiedType: fullyQualifiedType,
                    fullyQualifiedNonNullableType: fullyQualifiedNonNullableType);
                properties.Add(propInfo);
            }
        }

        // 智能ID识别逻辑
        if (!string.IsNullOrEmpty(specifiedIdProperty))
        {
            // 1. 优先使用Entity属性中指定的ID属性
            idProperty = properties.FirstOrDefault(p => p.Name == specifiedIdProperty);
            if (idProperty != null)
            {
                idProperty.IsId = true;
            }
        }
        else
        {
            // 2. 自动查找标准ID属性名称
            var standardIdNames = new[] { "Id", "_id", "ID", "Id" };
            foreach (var idName in standardIdNames)
            {
                var foundIdProperty = properties.FirstOrDefault(p => p.Name == idName);
                if (foundIdProperty != null)
                {
                    foundIdProperty.IsId = true;
                    idProperty = foundIdProperty;
                    break;
                }
            }
        }

        // 3. 如果还是没有找到，检查是否有[Id]属性标记
        if (idProperty == null)
        {
            foreach (var member in classDeclaration.Members)
            {
                if (member is PropertyDeclarationSyntax property)
                {
                    var propertySymbol = semanticModel.GetDeclaredSymbol(property);
                    var hasIdAttribute = propertySymbol?.GetAttributes()
                        .Any(attr => attr.AttributeClass?.Name == "IdAttribute") ?? false;

                    if (hasIdAttribute)
                    {
                        var propertyName = property.Identifier.Text;
                        idProperty = properties.FirstOrDefault(p => p.Name == propertyName);
                        if (idProperty != null)
                        {
                            idProperty.IsId = true;
                            break;
                        }
                    }
                }
            }
        }

        return new ClassInfo(namespaceName, className, properties, idProperty, collectionName);
    }

    /// <summary>
    /// 判断是否应该为类生成映射器
    /// </summary>
    private static bool ShouldGenerateMapper(ClassInfo classInfo)
    {
        // 排除SimpleDb内部命名空间，但允许Demo项目
        if (!string.IsNullOrEmpty(classInfo.Namespace))
        {
            // 明确排除System命名空间
            if (classInfo.Namespace.Contains("System"))
            {
                return false;
            }

            // 排除SimpleDb核心命名空间（除了Demo和SourceGenerator）
            if (classInfo.Namespace == "TinyDb" ||
                (classInfo.Namespace.StartsWith("TinyDb.") &&
                 classInfo.Namespace != "TinyDb.Demo" &&
                 !classInfo.Namespace.Contains("SourceGenerator")))
            {
                return false;
            }
        }

        // 排除所有内部类型的类名
        var excludePatterns = new[]
        {
            "ObjectSerializer", "Serializer", "Mapper", "Query", "Bson", "Cache", "Page",
            "Index", "Lock", "Transaction", "Engine", "Collection", "Stream",
            "BinaryExpression", "ConstantExpression", "MemberExpression",
            "ParameterExpression", "PropertyAccessor", "BTreeNode", "DatabaseStatistics",
            "TinyDbOptions", "InsertResult", "BTreeNodeStatistics",
            "SimpleDbEntityAttribute", "SimpleDbIdAttribute", "SimpleDbQueryBuilderAttribute"
        };

        if (excludePatterns.Any(pattern => classInfo.Name.Contains(pattern)))
        {
            return false;
        }

        // 排除编译生成的类
        if (classInfo.Name.StartsWith("<") || classInfo.Name.Contains("Anonymous"))
        {
            return false;
        }

        // 只处理有属性的实体类（ID将通过智能识别获得）
        return classInfo.Properties.Count > 0;
    }

    /// <summary>
    /// 生成AOT静态帮助器类
    /// </summary>
    private static string GeneratePartialClass(ClassInfo classInfo)
    {
        var sb = new StringBuilder();

        // 添加文件头
        sb.AppendLine("// <auto-generated/>");
        sb.AppendLine("#nullable enable");
        sb.AppendLine("using System;");
        sb.AppendLine("using TinyDb.Bson;");

        // 添加实体类的命名空间引用 - 调试输出
        sb.AppendLine($"// DEBUG: Namespace = '{classInfo.Namespace}'");
        if (!string.IsNullOrEmpty(classInfo.Namespace))
        {
            sb.AppendLine($"using {classInfo.Namespace};");
        }
        sb.AppendLine();

        // 添加命名空间
        if (!string.IsNullOrEmpty(classInfo.Namespace))
        {
            sb.AppendLine($"namespace {classInfo.Namespace}");
            sb.AppendLine("{");
        }

        // 添加静态帮助器类
        sb.AppendLine($"    /// <summary>");
        sb.AppendLine($"    /// {classInfo.Name} 的AOT支持帮助器（源代码生成器生成）");
        sb.AppendLine($"    /// </summary>");
        sb.AppendLine($"    public static class {classInfo.Name}AotHelper");
        sb.AppendLine("    {");

        // 生成AOT兼容的ID访问方法
        if (classInfo.IdProperty != null)
        {
            var idProp = classInfo.IdProperty;
            var normalizedIdType = NormalizeTypeName(idProp.NonNullableType);
            var isObjectId = normalizedIdType.EndsWith("ObjectId", StringComparison.Ordinal);

            sb.AppendLine();
            sb.AppendLine("        /// <summary>");
            sb.AppendLine("        /// 获取实体的ID值（AOT兼容）");
            sb.AppendLine("        /// </summary>");
            sb.AppendLine("        /// <param name=\"entity\">实体实例</param>");
            sb.AppendLine("        /// <returns>ID值</returns>");
            sb.AppendLine($"        public static BsonValue GetId({classInfo.Name} entity)");
            sb.AppendLine("        {");
            sb.AppendLine($"            if (entity == null) return BsonNull.Value;");
            sb.AppendLine($"            // 硬编码ID属性访问 - 避免AOT反射问题");

            if (isObjectId)
            {
                sb.AppendLine($"            return new BsonObjectId(entity.{idProp.Name});");
            }
            else if (string.Equals(normalizedIdType, "string", StringComparison.OrdinalIgnoreCase))
            {
                sb.AppendLine($"            return new BsonString(entity.{idProp.Name} ?? \"\");");
            }
            else if (string.Equals(normalizedIdType, "int", StringComparison.OrdinalIgnoreCase))
            {
                sb.AppendLine($"            return new BsonInt32(entity.{idProp.Name});");
            }
            else if (string.Equals(normalizedIdType, "long", StringComparison.OrdinalIgnoreCase))
            {
                sb.AppendLine($"            return new BsonInt64(entity.{idProp.Name});");
            }
            else
            {
                sb.AppendLine($"            return TinyDb.Serialization.BsonConversion.ConvertToBsonValue(entity.{idProp.Name});");
            }

            sb.AppendLine("        }");
            sb.AppendLine();

            sb.AppendLine("        /// <summary>");
            sb.AppendLine("        /// 设置实体的ID值（AOT兼容）");
            sb.AppendLine("        /// </summary>");
            sb.AppendLine("        /// <param name=\"entity\">实体实例</param>");
            sb.AppendLine("        /// <param name=\"id\">ID值</param>");
            sb.AppendLine($"        public static void SetId({classInfo.Name} entity, BsonValue id)");
            sb.AppendLine("        {");
            sb.AppendLine($"            if (entity == null || id?.IsNull != false) return;");

            if (isObjectId)
            {
                sb.AppendLine("            if (id is BsonObjectId bsonObjectId)");
                sb.AppendLine($"                entity.{idProp.Name} = bsonObjectId.Value;");
            }
            else if (string.Equals(normalizedIdType, "string", StringComparison.OrdinalIgnoreCase))
            {
                sb.AppendLine("            if (id is BsonString bsonString)");
                sb.AppendLine($"                entity.{idProp.Name} = bsonString.Value ?? \"\";");
            }
            else if (string.Equals(normalizedIdType, "int", StringComparison.OrdinalIgnoreCase))
            {
                sb.AppendLine("            if (id is BsonInt32 bsonInt32)");
                sb.AppendLine($"                entity.{idProp.Name} = bsonInt32.Value;");
            }
            else if (string.Equals(normalizedIdType, "long", StringComparison.OrdinalIgnoreCase))
            {
                sb.AppendLine("            if (id is BsonInt64 bsonInt64)");
                sb.AppendLine($"                entity.{idProp.Name} = bsonInt64.Value;");
            }

            sb.AppendLine("        }");
            sb.AppendLine();

            sb.AppendLine("        /// <summary>");
            sb.AppendLine("        /// 检查实体是否有有效的ID（AOT兼容）");
            sb.AppendLine("        /// </summary>");
            sb.AppendLine("        /// <param name=\"entity\">实体实例</param>");
            sb.AppendLine("        /// <returns>是否有有效ID</returns>");
            sb.AppendLine($"        public static bool HasValidId({classInfo.Name} entity)");
            sb.AppendLine("        {");
            sb.AppendLine($"            if (entity == null) return false;");
            sb.AppendLine($"            // 硬编码ID属性验证 - 避免AOT反射问题");

            if (isObjectId)
            {
                sb.AppendLine($"            return entity.{idProp.Name} != ObjectId.Empty;");
            }
            else
            {
                if (string.Equals(normalizedIdType, "string", StringComparison.OrdinalIgnoreCase))
                {
                    sb.AppendLine($"            return !string.IsNullOrWhiteSpace(entity.{idProp.Name});");
                }
                else if (string.Equals(normalizedIdType, "Guid", StringComparison.Ordinal))
                {
                    if (idProp.IsNullableValueType)
                    {
                        sb.AppendLine($"            return entity.{idProp.Name}.HasValue && entity.{idProp.Name}.Value != Guid.Empty;");
                    }
                    else
                    {
                        sb.AppendLine($"            return entity.{idProp.Name} != Guid.Empty;");
                    }
                }
                else if (idProp.IsNullableValueType)
                {
                    sb.AppendLine($"            return entity.{idProp.Name}.HasValue && !System.Collections.Generic.EqualityComparer<{idProp.FullyQualifiedNonNullableType}>.Default.Equals(entity.{idProp.Name}.Value, default);");
                }
                else if (idProp.IsValueType)
                {
                    sb.AppendLine($"            return !System.Collections.Generic.EqualityComparer<{idProp.FullyQualifiedNonNullableType}>.Default.Equals(entity.{idProp.Name}, default);");
                }
                else
                {
                    sb.AppendLine($"            return entity.{idProp.Name} != null;");
                }
            }

            sb.AppendLine("        }");
        }
        else
        {
            sb.AppendLine();
            sb.AppendLine("        /// <summary>");
            sb.AppendLine("        /// 获取实体的ID值（AOT兼容）");
            sb.AppendLine("        /// </summary>");
            sb.AppendLine("        /// <param name=\"entity\">实体实例</param>");
            sb.AppendLine("        /// <returns>ID值</returns>");
            sb.AppendLine($"        public static BsonValue GetId({classInfo.Name} entity)");
            sb.AppendLine("        {");
            sb.AppendLine("            // 没有找到ID属性");
            sb.AppendLine("            return BsonNull.Value;");
            sb.AppendLine("        }");
            sb.AppendLine();

            sb.AppendLine("        /// <summary>");
            sb.AppendLine("        /// 设置实体的ID值（AOT兼容）");
            sb.AppendLine("        /// </summary>");
            sb.AppendLine("        /// <param name=\"entity\">实体实例</param>");
            sb.AppendLine("        /// <param name=\"id\">ID值</param>");
            sb.AppendLine($"        public static void SetId({classInfo.Name} entity, BsonValue id)");
            sb.AppendLine("        {");
            sb.AppendLine("            // 没有找到ID属性，忽略设置");
            sb.AppendLine("        }");
            sb.AppendLine();

            sb.AppendLine("        /// <summary>");
            sb.AppendLine("        /// 检查实体是否有有效的ID（AOT兼容）");
            sb.AppendLine("        /// </summary>");
            sb.AppendLine("        /// <param name=\"entity\">实体实例</param>");
            sb.AppendLine("        /// <returns>是否有有效ID</returns>");
            sb.AppendLine($"        public static bool HasValidId({classInfo.Name} entity)");
            sb.AppendLine("        {");
            sb.AppendLine("            // 没有找到ID属性");
            sb.AppendLine("            return false;");
            sb.AppendLine("        }");
        }

        // 生成完整的序列化方法
        sb.AppendLine();
        sb.AppendLine("        /// <summary>");
        sb.AppendLine("        /// 将实体序列化为BSON文档（AOT兼容）");
        sb.AppendLine("        /// </summary>");
        sb.AppendLine("        /// <param name=\"entity\">实体实例</param>");
        sb.AppendLine("        /// <returns>BSON文档</returns>");
        sb.AppendLine($"        public static BsonDocument ToDocument({classInfo.Name} entity)");
        sb.AppendLine("        {");
        sb.AppendLine("            if (entity == null) throw new ArgumentNullException(nameof(entity));");
        sb.AppendLine("            var document = new BsonDocument();");
        sb.AppendLine();

        // 为每个属性生成序列化代码
        foreach (var prop in classInfo.Properties.Where(p => !p.HasIgnoreAttribute))
        {
            var bsonCode = SourceGeneratorHelpers.GeneratePropertySerialization(prop);
            sb.AppendLine($"            // 序列化属性: {prop.Name}");
            sb.AppendLine(bsonCode);
            sb.AppendLine();
        }

        // 确保包含集合名称字段
        sb.AppendLine("            // 确保包含集合名称字段");
        sb.AppendLine("            if (!document.ContainsKey(\"_collection\"))");
        sb.AppendLine("            {");
        sb.AppendLine($"                document = document.Set(\"_collection\", \"{classInfo.CollectionName ?? classInfo.Name}\");");
        sb.AppendLine("            }");

        sb.AppendLine("            return document;");
        sb.AppendLine("        }");
        sb.AppendLine();

        // 生成反序列化方法
        sb.AppendLine("        /// <summary>");
        sb.AppendLine("        /// 从BSON文档反序列化实体（AOT兼容）");
        sb.AppendLine("        /// </summary>");
        sb.AppendLine("        /// <param name=\"document\">BSON文档</param>");
        sb.AppendLine("        /// <returns>实体实例</returns>");
        sb.AppendLine($"        public static {classInfo.Name} FromDocument(BsonDocument document)");
        sb.AppendLine("        {");
        sb.AppendLine("            if (document == null) throw new ArgumentNullException(nameof(document));");
        sb.AppendLine($"            var entity = new {classInfo.Name}();");
        sb.AppendLine();

        // 为每个属性生成反序列化代码
        foreach (var prop in classInfo.Properties.Where(p => !p.HasIgnoreAttribute))
        {
            var bsonCode = SourceGeneratorHelpers.GeneratePropertyDeserialization(prop);
            sb.AppendLine($"            // 反序列化属性: {prop.Name}");
            sb.AppendLine(bsonCode);
            sb.AppendLine();
        }

        sb.AppendLine("            return entity;");
        sb.AppendLine("        }");
        sb.AppendLine();

        // 生成属性访问方法
        sb.AppendLine("        /// <summary>");
        sb.AppendLine("        /// 获取属性值（AOT兼容）");
        sb.AppendLine("        /// </summary>");
        sb.AppendLine("        /// <param name=\"entity\">实体实例</param>");
        sb.AppendLine("        /// <param name=\"propertyName\">属性名称</param>");
        sb.AppendLine("        /// <returns>属性值</returns>");
        sb.AppendLine($"        public static object? GetPropertyValue({classInfo.Name} entity, string propertyName)");
        sb.AppendLine("        {");
        sb.AppendLine("            if (entity == null) throw new ArgumentNullException(nameof(entity));");
        sb.AppendLine("            return propertyName switch");
        sb.AppendLine("            {");

        foreach (var prop in classInfo.Properties.Where(p => !p.HasIgnoreAttribute))
        {
            sb.AppendLine($"                \"{prop.Name}\" => entity.{prop.Name},");
        }

        sb.AppendLine("                _ => throw new ArgumentException($\"Unknown property: {propertyName}\")");
        sb.AppendLine("            };");
        sb.AppendLine("        }");

        sb.AppendLine("    }");

        // 关闭命名空间
        if (!string.IsNullOrEmpty(classInfo.Namespace))
        {
            sb.AppendLine("}");
        }

        return sb.ToString();
    }

    /// <summary>
    /// 生成注册所有AOT辅助适配器的聚合类
    /// </summary>
    private static string GenerateRegistrySource(IReadOnlyCollection<ClassInfo> classes)
    {
        var sb = new StringBuilder();

        sb.AppendLine("// <auto-generated/>");
        sb.AppendLine("#nullable enable");
        sb.AppendLine("using System;");
        sb.AppendLine("using System.Collections.Generic;");
        sb.AppendLine("using System.Runtime.CompilerServices;");
        sb.AppendLine();
        sb.AppendLine("namespace TinyDb.Serialization;");
        sb.AppendLine();
        sb.AppendLine("internal static class AotHelperRegistryModule");
        sb.AppendLine("{");
        sb.AppendLine("    [ModuleInitializer]");
        sb.AppendLine("    internal static void Initialize()");
        sb.AppendLine("    {");

        foreach (var classInfo in classes)
        {
            sb.AppendLine($"        AotHelperRegistry.Register(new AotEntityAdapter<{classInfo.FullName}>(");
            sb.AppendLine($"            entity => {classInfo.FullName}AotHelper.ToDocument(entity),");
            sb.AppendLine($"            document => {classInfo.FullName}AotHelper.FromDocument(document),");
            sb.AppendLine($"            entity => {classInfo.FullName}AotHelper.GetId(entity),");
            sb.AppendLine($"            (entity, id) => {classInfo.FullName}AotHelper.SetId(entity, id),");
            sb.AppendLine($"            entity => {classInfo.FullName}AotHelper.HasValidId(entity),");
            sb.AppendLine($"            (entity, propertyName) => {classInfo.FullName}AotHelper.GetPropertyValue(entity, propertyName)));");
        }

        sb.AppendLine("    }");
        sb.AppendLine("}");

        return sb.ToString();
    }

    /// <summary>
    /// 生成映射器类
    /// </summary>
    private static string GenerateMapperClass(ClassInfo classInfo)
    {
        var sb = new StringBuilder();

        // 添加文件头
        sb.AppendLine("// <auto-generated/>");
        sb.AppendLine("using TinyDb.Bson;");
        sb.AppendLine("using System;");
        sb.AppendLine();

        // 添加命名空间
        if (!string.IsNullOrEmpty(classInfo.Namespace))
        {
            sb.AppendLine($"namespace {classInfo.Namespace}");
            sb.AppendLine("{");
        }

        // 添加类
        sb.AppendLine($"    /// <summary>");
        sb.AppendLine($"    /// {classInfo.Name} 的 BSON 映射器");
        sb.AppendLine($"    /// </summary>");
        sb.AppendLine($"    public static partial class {classInfo.Name}Mapper");
        sb.AppendLine("    {");

        // 生成 ToDocument 方法
        GenerateToDocumentMethod(sb, classInfo);

        // 生成 FromDocument 方法
        GenerateFromDocumentMethod(sb, classInfo);

        // 生成AOT兼容的ID访问器
        GenerateAotIdAccessor(sb, classInfo);

        sb.AppendLine("    }");

        // 关闭命名空间
        if (!string.IsNullOrEmpty(classInfo.Namespace))
        {
            sb.AppendLine("}");
        }

        return sb.ToString();
    }

    /// <summary>
    /// 生成 ToDocument 方法
    /// </summary>
    private static void GenerateToDocumentMethod(StringBuilder sb, ClassInfo classInfo)
    {
        sb.AppendLine("        /// <summary>");
        sb.AppendLine($"        /// 将 {classInfo.Name} 转换为 BSON 文档");
        sb.AppendLine("        /// </summary>");
        sb.AppendLine($"        /// <param name=\"entity\">{classInfo.Name} 实体</param>");
        sb.AppendLine("        /// <returns>BSON 文档</returns>");
        sb.AppendLine($"        public static BsonDocument ToDocument(this {classInfo.FullName} entity)");
        sb.AppendLine("        {");
        sb.AppendLine("            if (entity == null) return new BsonDocument();");
        sb.AppendLine();
        sb.AppendLine("            var doc = new BsonDocument();");

        foreach (var property in classInfo.Properties)
        {
            sb.AppendLine($"            doc[\"{property.Name}\"] = TinyDb.Serialization.BsonConversion.ConvertToBsonValue(entity.{property.Name});");
        }

        sb.AppendLine("            return doc;");
        sb.AppendLine("        }");
        sb.AppendLine();
    }

    /// <summary>
    /// 生成 FromDocument 方法
    /// </summary>
    private static void GenerateFromDocumentMethod(StringBuilder sb, ClassInfo classInfo)
    {
        sb.AppendLine("        /// <summary>");
        sb.AppendLine($"        /// 从 BSON 文档创建 {classInfo.Name}");
        sb.AppendLine("        /// </summary>");
        sb.AppendLine("        /// <param name=\"doc\">BSON 文档</param>");
        sb.AppendLine($"        /// <returns>{classInfo.Name} 实体</returns>");
        sb.AppendLine($"        public static {classInfo.FullName}? FromDocument(this BsonDocument doc)");
        sb.AppendLine("        {");
        sb.AppendLine("            if (doc == null) return null;");
        sb.AppendLine();
        sb.AppendLine($"            var entity = new {classInfo.FullName}();");

        foreach (var property in classInfo.Properties)
        {
            sb.AppendLine($"            if (doc.ContainsKey(\"{property.Name}\"))");
            sb.AppendLine($"            {{");
            sb.AppendLine($"                entity.{property.Name} = ConvertFromBsonValue<{property.Type}>(doc[\"{property.Name}\"]);");
            sb.AppendLine($"            }}");
        }

        sb.AppendLine("            return entity;");
        sb.AppendLine("        }");
        sb.AppendLine();
        sb.AppendLine("        /// <summary>");
        sb.AppendLine("        /// 转换为 BSON 值");
        sb.AppendLine("        /// </summary>");
        sb.AppendLine("        /// <param name=\"value\">值</param>");
        sb.AppendLine("        /// <returns>BSON 值</returns>");
        sb.AppendLine("        private static BsonValue ConvertToBsonValue(object? value)");
        sb.AppendLine("        {");
        sb.AppendLine("            return value switch");
        sb.AppendLine("            {");
        sb.AppendLine("                null => BsonNull.Value,");
        sb.AppendLine("                string s => s,");
        sb.AppendLine("                int i => i,");
        sb.AppendLine("                long l => l,");
        sb.AppendLine("                double d => d,");
        sb.AppendLine("                bool b => b,");
        sb.AppendLine("                DateTime dt => dt,");
        sb.AppendLine("                ObjectId id => id,");
        sb.AppendLine("                _ => BsonNull.Value");
        sb.AppendLine("            };");
        sb.AppendLine("        }");
        sb.AppendLine();
        sb.AppendLine("        /// <summary>");
        sb.AppendLine("        /// 从 BSON 值转换");
        sb.AppendLine("        /// </summary>");
        sb.AppendLine("        /// <typeparam name=\"T\">目标类型</typeparam>");
        sb.AppendLine("        /// <param name=\"value\">BSON 值</param>");
        sb.AppendLine("        /// <returns>转换后的值</returns>");
        sb.AppendLine("        private static T ConvertFromBsonValue<T>(BsonValue value)");
        sb.AppendLine("        {");
        sb.AppendLine("            if (value == null || value.IsNull) return default(T)!;");
        sb.AppendLine();
        sb.AppendLine("            return typeof(T).Name switch");
        sb.AppendLine("            {");
        sb.AppendLine("                \"String\" => (T)(object)value.AsString,");
        sb.AppendLine("                \"Int32\" => (T)(object)value.AsInt32,");
        sb.AppendLine("                \"Int64\" => (T)(object)value.AsInt64,");
        sb.AppendLine("                \"Double\" => (T)(object)value.AsDouble,");
        sb.AppendLine("                \"Boolean\" => (T)(object)value.AsBoolean,");
        sb.AppendLine("                \"DateTime\" => (T)(object)value.AsDateTime,");
        sb.AppendLine("                \"ObjectId\" => (T)(object)value.AsObjectId,");
        sb.AppendLine("                _ => default(T)!");
        sb.AppendLine("            };");
        sb.AppendLine("        }");
    }

    /// <summary>
    /// 生成AOT兼容的ID访问器
    /// </summary>
    private static void GenerateAotIdAccessor(StringBuilder sb, ClassInfo classInfo)
    {
        if (classInfo.IdProperty == null) return;

        var idProperty = classInfo.IdProperty;
        var normalizedIdType = NormalizeTypeName(idProperty.NonNullableType);
        var isObjectId = normalizedIdType.EndsWith("ObjectId", StringComparison.Ordinal);

        sb.AppendLine();
        sb.AppendLine("        /// <summary>");
        sb.AppendLine("        /// AOT兼容的ID属性访问器");
        sb.AppendLine("        /// </summary>");
        sb.AppendLine("        /// <param name=\"entity\">实体实例</param>");
        sb.AppendLine("        /// <returns>ID值</returns>");
        sb.AppendLine($"        public static BsonValue GetId({classInfo.FullName} entity)");
        sb.AppendLine("        {");
        sb.AppendLine("            if (entity == null) return BsonNull.Value;");

        if (isObjectId)
        {
            sb.AppendLine($"            return new BsonObjectId(entity.{idProperty.Name});");
        }
        else
        {
            sb.AppendLine($"            return ConvertToBsonValue(entity.{idProperty.Name});");
        }

        sb.AppendLine("        }");
        sb.AppendLine();

        sb.AppendLine("        /// <summary>");
        sb.AppendLine("        /// 设置实体的ID值");
        sb.AppendLine("        /// </summary>");
        sb.AppendLine("        /// <param name=\"entity\">实体实例</param>");
        sb.AppendLine("        /// <param name=\"id\">ID值</param>");
        sb.AppendLine($"        public static void SetId({classInfo.FullName} entity, BsonValue id)");
        sb.AppendLine("        {");
        sb.AppendLine("            if (entity == null || id?.IsNull != false) return;");

        if (isObjectId)
        {
            sb.AppendLine("            if (id is BsonObjectId bsonObjectId)");
            sb.AppendLine($"                entity.{idProperty.Name} = bsonObjectId.Value;");
        }
        else
        {
            sb.AppendLine($"            entity.{idProperty.Name} = ConvertFromBsonValue<{idProperty.Type}>(id);");
        }

        sb.AppendLine("        }");
        sb.AppendLine();

        sb.AppendLine("        /// <summary>");
        sb.AppendLine("        /// 检查实体是否有有效的ID");
        sb.AppendLine("        /// </summary>");
        sb.AppendLine("        /// <param name=\"entity\">实体实例</param>");
        sb.AppendLine("        /// <returns>是否有有效ID</returns>");
        sb.AppendLine($"        public static bool HasValidId({classInfo.FullName} entity)");
        sb.AppendLine("        {");
        sb.AppendLine("            if (entity == null) return false;");

        if (isObjectId)
        {
            sb.AppendLine($"            return entity.{idProperty.Name} != ObjectId.Empty;");
        }
        else
        {
            if (string.Equals(normalizedIdType, "string", StringComparison.OrdinalIgnoreCase))
            {
                sb.AppendLine($"            return !string.IsNullOrWhiteSpace(entity.{idProperty.Name});");
            }
            else if (string.Equals(normalizedIdType, "Guid", StringComparison.Ordinal))
            {
                if (idProperty.IsNullableValueType)
                {
                    sb.AppendLine($"            return entity.{idProperty.Name}.HasValue && entity.{idProperty.Name}.Value != Guid.Empty;");
                }
                else
                {
                    sb.AppendLine($"            return entity.{idProperty.Name} != Guid.Empty;");
                }
            }
            else if (idProperty.IsNullableValueType)
            {
                sb.AppendLine($"            return entity.{idProperty.Name}.HasValue && !System.Collections.Generic.EqualityComparer<{idProperty.FullyQualifiedNonNullableType}>.Default.Equals(entity.{idProperty.Name}.Value, default);");
            }
            else if (idProperty.IsValueType)
            {
                sb.AppendLine($"            return !System.Collections.Generic.EqualityComparer<{idProperty.FullyQualifiedNonNullableType}>.Default.Equals(entity.{idProperty.Name}, default);");
            }
            else
            {
                sb.AppendLine($"            return entity.{idProperty.Name} != null;");
            }
        }

        sb.AppendLine("        }");
    }

    private static string NormalizeTypeName(string type)
    {
        if (string.IsNullOrWhiteSpace(type)) return type;

        var normalized = type.Trim();
        if (normalized.StartsWith("global::", StringComparison.Ordinal))
        {
            normalized = normalized.Substring("global::".Length);
        }

        return normalized switch
        {
            "System.String" => "string",
            "System.Guid" => "Guid",
            "System.Int32" => "int",
            "System.Int64" => "long",
            "System.Int16" => "short",
            "System.UInt32" => "uint",
            "System.UInt64" => "ulong",
            "System.UInt16" => "ushort",
            "System.Byte" => "byte",
            "System.SByte" => "sbyte",
            "System.Boolean" => "bool",
            "System.Decimal" => "decimal",
            "System.Double" => "double",
            "System.Single" => "float",
            "System.DateTime" => "DateTime",
            "System.DateTimeOffset" => "DateTimeOffset",
            "System.TimeSpan" => "TimeSpan",
            _ => normalized
        };
    }

}

/// <summary>
/// 类信息
/// </summary>
public class ClassInfo
{
    public string Namespace { get; }
    public string Name { get; }
    public string FullName => string.IsNullOrEmpty(Namespace) ? Name : $"{Namespace}.{Name}";
    public List<PropertyInfo> Properties { get; }
    public PropertyInfo? IdProperty { get; }
    public string? CollectionName { get; }

    public ClassInfo(string @namespace, string name, List<PropertyInfo> properties, PropertyInfo? idProperty, string? collectionName = null)
    {
        Namespace = @namespace;
        Name = name;
        Properties = properties;
        IdProperty = idProperty;
        CollectionName = collectionName;
    }
}

/// <summary>
/// 属性信息
/// </summary>
public class PropertyInfo
{
    public string Name { get; }
    public string Type { get; }
    public bool IsId { get; set; }
    public bool IsIgnored { get; }
    public bool HasIgnoreAttribute { get; set; }
    public bool IsValueType { get; }
    public bool IsNullableValueType { get; }
    public bool IsNullableReferenceType { get; }
    public string NonNullableType { get; }
    public string FullyQualifiedType { get; }
    public string FullyQualifiedNonNullableType { get; }

    public PropertyInfo(
        string name,
        string type,
        bool isId = false,
        bool isIgnored = false,
        bool hasIgnoreAttribute = false,
        bool isValueType = false,
        bool isNullableValueType = false,
        bool isNullableReferenceType = false,
        string? nonNullableType = null,
        string? fullyQualifiedType = null,
        string? fullyQualifiedNonNullableType = null)
    {
        Name = name;
        Type = type;
        IsId = isId;
        IsIgnored = isIgnored;
        HasIgnoreAttribute = hasIgnoreAttribute;
        IsValueType = isValueType;
        IsNullableValueType = isNullableValueType;
        IsNullableReferenceType = isNullableReferenceType;
        NonNullableType = nonNullableType ?? type.TrimEnd('?');
        FullyQualifiedType = fullyQualifiedType ?? type;
        FullyQualifiedNonNullableType = fullyQualifiedNonNullableType ?? NonNullableType;
    }
}

/// <summary>
/// 生成属性序列化代码的辅助方法
/// </summary>
public static partial class SourceGeneratorHelpers
{
    /// <summary>
    /// 检查属性是否为ID属性
    /// </summary>
    private static bool IsIdProperty(PropertyInfo prop)
    {
        return prop.IsId || prop.Name.Equals("Id", StringComparison.OrdinalIgnoreCase) ||
               prop.Name.Equals("_id", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// 生成属性序列化代码
    /// </summary>
    public static string GeneratePropertySerialization(PropertyInfo prop)
    {
        var propertyName = prop.Name;
        var propertyType = prop.Type;

        // 检查是否是ID属性，如果是则映射到_id字段
        var bsonFieldName = IsIdProperty(prop) ? "_id" : propertyName;

        return propertyType switch
        {
            "string" => $"document = document.Set(\"{bsonFieldName}\", string.IsNullOrEmpty(entity.{propertyName}) ? BsonNull.Value : new BsonString(entity.{propertyName}));",
            "int" or "Int32" => $"document = document.Set(\"{bsonFieldName}\", new BsonInt32(entity.{propertyName}));",
            "long" or "Int64" => $"document = document.Set(\"{bsonFieldName}\", new BsonInt64(entity.{propertyName}));",
            "double" or "Double" => $"document = document.Set(\"{bsonFieldName}\", new BsonDouble(entity.{propertyName}));",
            "float" or "Single" => $"document = document.Set(\"{bsonFieldName}\", new BsonDouble(entity.{propertyName}));",
            "decimal" or "Decimal" => $"document = document.Set(\"{bsonFieldName}\", new BsonDecimal128(entity.{propertyName}));",
            "bool" or "Boolean" => $"document = document.Set(\"{bsonFieldName}\", new BsonBoolean(entity.{propertyName}));",
            "DateTime" => $"document = document.Set(\"{bsonFieldName}\", new BsonDateTime(entity.{propertyName}));",
            "Guid" => $"document = document.Set(\"{bsonFieldName}\", new BsonBinary(entity.{propertyName}));",
            "ObjectId" => $"document = document.Set(\"{bsonFieldName}\", new BsonObjectId(entity.{propertyName}));",
            _ when propertyType.EndsWith("?") => $"document = document.Set(\"{bsonFieldName}\", entity.{propertyName} == null ? BsonNull.Value : ConvertToBsonValue(entity.{propertyName}));",
            _ => $"document = document.Set(\"{bsonFieldName}\", ConvertToBsonValue(entity.{propertyName}));"
        };
    }

    /// <summary>
    /// 生成属性反序列化代码
    /// </summary>
    public static string GeneratePropertyDeserialization(PropertyInfo prop)
    {
        var propertyName = prop.Name;
        var propertyType = prop.Type;

        // 检查是否是ID属性，如果是则从_id字段读取
        var bsonFieldName = IsIdProperty(prop) ? "_id" : propertyName;

        return propertyType switch
        {
            "string" => $"if (document.TryGetValue(\"{bsonFieldName}\", out var bson{propertyName}) && bson{propertyName} is BsonString str{propertyName}) entity.{propertyName} = str{propertyName}.Value;",
            "int" or "Int32" => $"if (document.TryGetValue(\"{bsonFieldName}\", out var bson{propertyName}) && bson{propertyName} is BsonInt32 int{propertyName}) entity.{propertyName} = int{propertyName}.Value;",
            "long" or "Int64" => $"if (document.TryGetValue(\"{bsonFieldName}\", out var bson{propertyName}) && bson{propertyName} is BsonInt64 long{propertyName}) entity.{propertyName} = long{propertyName}.Value;",
            "double" or "Double" => $"if (document.TryGetValue(\"{bsonFieldName}\", out var bson{propertyName}) && bson{propertyName} is BsonDouble dbl{propertyName}) entity.{propertyName} = dbl{propertyName}.Value;",
            "float" or "Single" => $"if (document.TryGetValue(\"{bsonFieldName}\", out var bson{propertyName}) && bson{propertyName} is BsonDouble dbl{propertyName}) entity.{propertyName} = (float)dbl{propertyName}.Value;",
            "decimal" or "Decimal" => $"if (document.TryGetValue(\"{bsonFieldName}\", out var bson{propertyName}) && bson{propertyName} is BsonDecimal128 dec{propertyName}) entity.{propertyName} = dec{propertyName}.Value;",
            "bool" or "Boolean" => $"if (document.TryGetValue(\"{bsonFieldName}\", out var bson{propertyName}) && bson{propertyName} is BsonBoolean bool{propertyName}) entity.{propertyName} = bool{propertyName}.Value;",
            "DateTime" => $"if (document.TryGetValue(\"{bsonFieldName}\", out var bson{propertyName}) && bson{propertyName} is BsonDateTime dt{propertyName}) entity.{propertyName} = dt{propertyName}.Value;",
            "Guid" => $"if (document.TryGetValue(\"{bsonFieldName}\", out var bson{propertyName}) && bson{propertyName} is BsonBinary guid{propertyName}) entity.{propertyName} = new Guid(guid{propertyName}.Bytes);",
            "ObjectId" => $"if (document.TryGetValue(\"{bsonFieldName}\", out var bson{propertyName}) && bson{propertyName} is BsonObjectId oid{propertyName}) entity.{propertyName} = oid{propertyName}.Value;",
            _ when propertyType.EndsWith("?") => GenerateNullablePropertyDeserialization(prop),
            _ => $"entity.{propertyName} = ConvertFromBsonValue<{propertyType}>(document[\"{bsonFieldName}\"]);"
        };
    }

    /// <summary>
    /// 生成可空属性反序列化代码
    /// </summary>
    private static string GenerateNullablePropertyDeserialization(PropertyInfo prop)
    {
        var propertyName = prop.Name;
        var underlyingType = prop.Type.Replace("?", "");

        // 检查是否是ID属性，如果是则从_id字段读取
        var bsonFieldName = IsIdProperty(prop) ? "_id" : propertyName;

        return underlyingType switch
        {
            "int" or "Int32" => $"if (document.TryGetValue(\"{bsonFieldName}\", out var bson{propertyName}) && bson{propertyName} is BsonInt32 int{propertyName}) entity.{propertyName} = int{propertyName}.Value; else entity.{propertyName} = null;",
            "long" or "Int64" => $"if (document.TryGetValue(\"{bsonFieldName}\", out var bson{propertyName}) && bson{propertyName} is BsonInt64 long{propertyName}) entity.{propertyName} = long{propertyName}.Value; else entity.{propertyName} = null;",
            "double" or "Double" => $"if (document.TryGetValue(\"{bsonFieldName}\", out var bson{propertyName}) && bson{propertyName} is BsonDouble dbl{propertyName}) entity.{propertyName} = dbl{propertyName}.Value; else entity.{propertyName} = null;",
            "bool" or "Boolean" => $"if (document.TryGetValue(\"{bsonFieldName}\", out var bson{propertyName}) && bson{propertyName} is BsonBoolean bool{propertyName}) entity.{propertyName} = bool{propertyName}.Value; else entity.{propertyName} = null;",
            "DateTime" => $"if (document.TryGetValue(\"{bsonFieldName}\", out var bson{propertyName}) && bson{propertyName} is BsonDateTime dt{propertyName}) entity.{propertyName} = dt{propertyName}.Value; else entity.{propertyName} = null;",
            "Guid" => $"if (document.TryGetValue(\"{bsonFieldName}\", out var bson{propertyName}) && bson{propertyName} is BsonBinary guid{propertyName}) entity.{propertyName} = new Guid(guid{propertyName}.Bytes); else entity.{propertyName} = null;",
            "ObjectId" => $"if (document.TryGetValue(\"{bsonFieldName}\", out var bson{propertyName}) && bson{propertyName} is BsonObjectId oid{propertyName}) entity.{propertyName} = oid{propertyName}.Value; else entity.{propertyName} = null;",
            _ => $"entity.{propertyName} = document[\"{bsonFieldName}\"].IsNull ? null : ConvertFromBsonValue<{underlyingType}>(document[\"{bsonFieldName}\"]);"
        };
    }
}
