using System;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Text;
using System.Collections.Immutable;
using System.Text;

namespace TinyDb.SourceGenerator;

/// <summary>
/// TinyDb AOT 源代码生成器
/// </summary>
[Generator(LanguageNames.CSharp)]
public class TinyDbSourceGenerator : IIncrementalGenerator
{
    /// <summary>
    /// 嵌套类不支持的诊断描述符
    /// </summary>
    private static readonly DiagnosticDescriptor NestedClassNotSupportedDescriptor = new(
        id: "TINYDB001",
        title: "Nested class does not support [Entity] attribute",
        messageFormat: "Type '{0}' is a nested class. TinyDb source generator does not support nested classes. Please move the class to a top-level namespace.",
        category: "TinyDb.SourceGenerator",
        DiagnosticSeverity.Error,
        isEnabledByDefault: true,
        description: "TinyDb source generator does not support generating AOT adapter code for nested classes. Please define classes with [Entity] attribute at the top-level namespace instead of nesting them inside other classes.");

    /// <summary>
    /// 循环引用警告的诊断描述符
    /// </summary>
    private static readonly DiagnosticDescriptor CircularReferenceWarningDescriptor = new(
        id: "TINYDB002",
        title: "Circular reference detected in non-Entity types",
        messageFormat: "Circular reference detected in type '{0}': {1}. The circular reference will be handled by breaking the cycle, but this may cause incomplete serialization at runtime.",
        category: "TinyDb.SourceGenerator",
        DiagnosticSeverity.Warning,
        isEnabledByDefault: true,
        description: "A circular reference was detected among non-Entity complex types. The source generator will break the cycle to prevent infinite recursion, but properties involved in the cycle may not be fully serialized. Consider marking one of the types with [Entity] attribute or restructuring the types to avoid circular dependencies.");

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
        context.RegisterSourceOutput(classDeclarations, (spc, classes) =>
        {
            if (classes.IsDefaultOrEmpty) return;

            var validClasses = new List<ClassInfo>();

            foreach (var classInfo in classes)
            {
                if (classInfo == null) continue;

                // 检查是否是嵌套类
                if (!string.IsNullOrEmpty(classInfo.ContainingTypePath))
                {
                    // 报告诊断错误
                    var diagnostic = Diagnostic.Create(
                        NestedClassNotSupportedDescriptor,
                        classInfo.Location,
                        classInfo.FullName);
                    spc.ReportDiagnostic(diagnostic);
                    continue;
                }

                if (ShouldGenerateMapper(classInfo))
                {
                    validClasses.Add(classInfo);
                }
            }

            if (validClasses.Count == 0) return;

            foreach (var classInfo in validClasses)
            {
                // 报告循环引用警告
                foreach (var circularRef in classInfo.CircularReferences)
                {
                    var diagnostic = Diagnostic.Create(
                        CircularReferenceWarningDescriptor,
                        classInfo.Location,
                        circularRef.ContainingTypeName,
                        circularRef.CycleChain);
                    spc.ReportDiagnostic(diagnostic);
                }
                
                var partialClassCode = GeneratePartialClass(classInfo);
                // 使用UniqueFileName来保证文件名唯一
                var partialFileName = $"{classInfo.UniqueFileName}_AotHelper.g.cs";
                spc.AddSource(partialFileName, SourceText.From(partialClassCode, Encoding.UTF8));
            }

            var registrySource = GenerateRegistrySource(validClasses);
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

        // 获取包含类的全名（用于生成唯一的文件名）
        var containingTypeNames = new List<string>();
        var containingType = classSymbol?.ContainingType;
        while (containingType != null)
        {
            containingTypeNames.Insert(0, containingType.Name);
            containingType = containingType.ContainingType;
        }
        var containingTypePath = string.Join("_", containingTypeNames);

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
        // 收集所有属性的类型符号，用于后续分析依赖类型
        var typeSymbolMap = new Dictionary<string, ITypeSymbol>();

        foreach (var member in classDeclaration.Members)
        {
            if (member is PropertyDeclarationSyntax property)
            {
                var propertyName = property.Identifier.Text;
                var propertyType = property.Type.ToString();

                // 检查是否有BsonIgnore属性
                var propertySymbol = semanticModel.GetDeclaredSymbol(property) as IPropertySymbol;

                // 跳过私有属性
                if (propertySymbol?.DeclaredAccessibility != Accessibility.Public)
                {
                    continue;
                }

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
                    
                    // 记录底层类型符号
                    if (!typeSymbolMap.ContainsKey(fullyQualifiedNonNullableType))
                    {
                        typeSymbolMap[fullyQualifiedNonNullableType] = underlyingType;
                    }
                }
                else if (!isValueType && propertyType.EndsWith("?", StringComparison.Ordinal))
                {
                    nonNullableType = propertyType.TrimEnd('?').Trim();
                    fullyQualifiedNonNullableType = fullyQualifiedType.TrimEnd('?').Trim();
                }

                // 分析类型的复杂性
                var typeAnalysis = AnalyzePropertyType(typeSymbol);
                
                // 收集类型符号用于依赖分析
                if (typeSymbol != null && !typeSymbolMap.ContainsKey(fullyQualifiedNonNullableType))
                {
                    // 对于可空值类型，取底层类型
                    var actualTypeSymbol = typeSymbol;
                    if (isNullableValueType && typeSymbol is INamedTypeSymbol nt && nt.TypeArguments.Length == 1)
                    {
                        actualTypeSymbol = nt.TypeArguments[0];
                    }
                    typeSymbolMap[fullyQualifiedNonNullableType] = actualTypeSymbol;
                }
                
                // 收集集合元素类型
                if (typeAnalysis.IsCollection && !string.IsNullOrEmpty(typeAnalysis.ElementType))
                {
                    var elementTypeSymbol = GetElementTypeSymbol(typeSymbol);
                    if (elementTypeSymbol != null && !typeSymbolMap.ContainsKey(typeAnalysis.ElementType!))
                    {
                        typeSymbolMap[typeAnalysis.ElementType!] = elementTypeSymbol;
                    }
                }
                
                // 收集字典值类型
                if (typeAnalysis.IsDictionary && !string.IsNullOrEmpty(typeAnalysis.DictionaryValueType))
                {
                    var valueTypeSymbol = GetDictionaryValueTypeSymbol(typeSymbol);
                    if (valueTypeSymbol != null && !typeSymbolMap.ContainsKey(typeAnalysis.DictionaryValueType!))
                    {
                        typeSymbolMap[typeAnalysis.DictionaryValueType!] = valueTypeSymbol;
                    }
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
                    fullyQualifiedNonNullableType: fullyQualifiedNonNullableType,
                    isComplexType: typeAnalysis.IsComplexType,
                    isCollection: typeAnalysis.IsCollection,
                    isDictionary: typeAnalysis.IsDictionary,
                    isArray: typeAnalysis.IsArray,
                    elementType: typeAnalysis.ElementType,
                    isElementComplexType: typeAnalysis.IsElementComplexType,
                    isElementValueType: typeAnalysis.IsElementValueType,
                    dictionaryKeyType: typeAnalysis.DictionaryKeyType,
                    dictionaryValueType: typeAnalysis.DictionaryValueType,
                    isDictionaryValueComplexType: typeAnalysis.IsDictionaryValueComplexType,
                    isDictionaryValueValueType: typeAnalysis.IsDictionaryValueValueType);
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

        // 收集依赖的非Entity复杂类型
        var (dependentComplexTypes, circularReferences) = CollectDependentComplexTypes(properties, typeSymbolMap);

        return new ClassInfo(namespaceName, className, properties, idProperty, collectionName, containingTypePath, classDeclaration.GetLocation(), dependentComplexTypes, circularReferences);
    }

    /// <summary>
    /// 判断是否应该为类生成映射器
    /// </summary>
    private static bool ShouldGenerateMapper(ClassInfo classInfo)
    {
        // 注意：嵌套类的检查已移至 Initialize 方法中，会生成编译错误诊断

        // 排除TinyDb内部命名空间，但允许Demo项目和Tests项目
        if (!string.IsNullOrEmpty(classInfo.Namespace))
        {
            // 明确排除System命名空间
            if (classInfo.Namespace.Contains("System"))
            {
                return false;
            }

            // 排除TinyDb核心命名空间（除了Demo、Tests和SourceGenerator）
            if (classInfo.Namespace == "TinyDb" ||
                (classInfo.Namespace.StartsWith("TinyDb.") &&
                 classInfo.Namespace != "TinyDb.Demo" &&
                 !classInfo.Namespace.StartsWith("TinyDb.Tests") &&
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
            "TinyDbEntityAttribute", "TinyDbIdAttribute", "TinyDbQueryBuilderAttribute"
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
        sb.AppendLine("using System.Diagnostics.CodeAnalysis;");

        // 添加实体类的命名空间引用
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
        sb.AppendLine($"    /// {classInfo.TypeReference} 的AOT支持帮助器（源代码生成器生成）");
        sb.AppendLine($"    /// </summary>");
        sb.AppendLine($"    public static class {classInfo.HelperClassName}");
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
            sb.AppendLine($"        public static BsonValue GetId({classInfo.TypeReference} entity)");
            sb.AppendLine("        {");
            sb.AppendLine($"            if (entity == null) return BsonNull.Value;");
            sb.AppendLine($"            // 硬编码ID属性访问 - 避免AOT反射问题");

            if (isObjectId)
            {
                if (idProp.IsNullableValueType)
                {
                    sb.AppendLine($"            return entity.{idProp.Name}.HasValue ? new BsonObjectId(entity.{idProp.Name}.Value) : BsonNull.Value;");
                }
                else
                {
                    sb.AppendLine($"            return new BsonObjectId(entity.{idProp.Name});");
                }
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
                // 对于其他类型，使用BsonConversion辅助方法
                sb.AppendLine($"            return TinyDb.Serialization.BsonConversion.ConvertToBsonValue(entity.{idProp.Name});");
            }

            sb.AppendLine("        }");
            sb.AppendLine();

            sb.AppendLine("        /// <summary>");
            sb.AppendLine("        /// 设置实体的ID值（AOT兼容）");
            sb.AppendLine("        /// </summary>");
            sb.AppendLine("        /// <param name=\"entity\">实体实例</param>");
            sb.AppendLine("        /// <param name=\"id\">ID值</param>");
            sb.AppendLine($"        public static void SetId({classInfo.TypeReference} entity, BsonValue id)");
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
            sb.AppendLine($"        public static bool HasValidId({classInfo.TypeReference} entity)");
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
            sb.AppendLine($"        public static BsonValue GetId({classInfo.TypeReference} entity)");
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
            sb.AppendLine($"        public static void SetId({classInfo.TypeReference} entity, BsonValue id)");
            sb.AppendLine("        {");
            sb.AppendLine("            // 没有找到ID属性，忽略设置");
            sb.AppendLine("        }");
            sb.AppendLine();

            sb.AppendLine("        /// <summary>");
            sb.AppendLine("        /// 检查实体是否有有效的ID（AOT兼容）");
            sb.AppendLine("        /// </summary>");
            sb.AppendLine("        /// <param name=\"entity\">实体实例</param>");
            sb.AppendLine("        /// <returns>是否有有效ID</returns>");
            sb.AppendLine($"        public static bool HasValidId({classInfo.TypeReference} entity)");
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
        sb.AppendLine($"        public static BsonDocument ToDocument({classInfo.TypeReference} entity)");
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
        sb.AppendLine($"        public static {classInfo.TypeReference} FromDocument(BsonDocument document)");
        sb.AppendLine("        {");
        sb.AppendLine("            if (document == null) throw new ArgumentNullException(nameof(document));");
        sb.AppendLine($"            var entity = new {classInfo.TypeReference}();");
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
        AppendBsonConversionHelpers(sb, classInfo);

        // 生成属性访问方法
        sb.AppendLine("        /// <summary>");
        sb.AppendLine("        /// 获取属性值（AOT兼容）");
        sb.AppendLine("        /// </summary>");
        sb.AppendLine("        /// <param name=\"entity\">实体实例</param>");
        sb.AppendLine("        /// <param name=\"propertyName\">属性名称</param>");
        sb.AppendLine("        /// <returns>属性值</returns>");
        sb.AppendLine($"        public static object? GetPropertyValue({classInfo.TypeReference} entity, string propertyName)");
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
            // 使用完整命名空间加帮助器类名
            var helperFullName = string.IsNullOrEmpty(classInfo.Namespace)
                ? classInfo.HelperClassName
                : $"{classInfo.Namespace}.{classInfo.HelperClassName}";

            sb.AppendLine($"        AotHelperRegistry.Register(new AotEntityAdapter<{classInfo.FullName}>(");
            sb.AppendLine($"            entity => {helperFullName}.ToDocument(entity),");
            sb.AppendLine($"            document => {helperFullName}.FromDocument(document),");
            sb.AppendLine($"            entity => {helperFullName}.GetId(entity),");
            sb.AppendLine($"            (entity, id) => {helperFullName}.SetId(entity, id),");
            sb.AppendLine($"            entity => {helperFullName}.HasValidId(entity),");
            sb.AppendLine($"            (entity, propertyName) => {helperFullName}.GetPropertyValue(entity, propertyName)));");
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
        sb.AppendLine("using System.Diagnostics.CodeAnalysis;");
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

        // 生成BSON转换辅助方法
        AppendBsonConversionHelpers(sb, classInfo);

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
            sb.AppendLine($"            doc[\"{property.Name}\"] = ConvertToBsonValue(entity.{property.Name});");
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
    }

    /// <summary>
    /// 生成 BSON 值转换的辅助方法
    /// </summary>
    private static void AppendBsonConversionHelpers(StringBuilder sb, ClassInfo classInfo)
    {
        sb.AppendLine("        /// <summary>");
        sb.AppendLine("        /// 转换为 BSON 值");
        sb.AppendLine("        /// </summary>");
        sb.AppendLine("        /// <param name=\"value\">值</param>");
        sb.AppendLine("        /// <returns>BSON 值</returns>");
        sb.AppendLine("        private static BsonValue ConvertToBsonValue(object? value)");
        sb.AppendLine("        {");
        sb.AppendLine("            return value == null");
        sb.AppendLine("                ? BsonNull.Value");
        sb.AppendLine("                : global::TinyDb.Serialization.BsonConversion.ConvertToBsonValue(value);");
        sb.AppendLine("        }");
        sb.AppendLine();
        sb.AppendLine("        /// <summary>");
        sb.AppendLine("        /// 从 BSON 值转换");
        sb.AppendLine("        /// </summary>");
        sb.AppendLine("        /// <typeparam name=\"T\">目标类型</typeparam>");
        sb.AppendLine("        /// <param name=\"value\">BSON 值</param>");
        sb.AppendLine("        /// <returns>转换后的值</returns>");
        sb.AppendLine("        private static T ConvertFromBsonValue<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicMethods | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields)] T>(BsonValue value)");
        sb.AppendLine("        {");
        sb.AppendLine("            if (value == null || value.IsNull) return default!;");
        sb.AppendLine();
        sb.AppendLine("            var converted = global::TinyDb.Serialization.BsonConversion.FromBsonValue(value, typeof(T));");
        sb.AppendLine("            if (converted is T typed)");
        sb.AppendLine("            {");
        sb.AppendLine("                return typed;");
        sb.AppendLine("            }");
        sb.AppendLine();
        sb.AppendLine("            return converted is null ? default! : (T)converted;");
        sb.AppendLine("        }");
        sb.AppendLine();
        
        // 如果有依赖的复杂类型，生成专用的内联序列化方法
        if (classInfo.DependentComplexTypes.Count > 0)
        {
            // 生成带类型检查的 SerializeComplexObject 方法
            GenerateSerializeComplexObjectWithInline(sb, classInfo);
            
            // 生成带类型检查的 DeserializeComplexObject 方法
            GenerateDeserializeComplexObjectWithInline(sb, classInfo);
            
            // 为每个依赖类型生成专用的序列化/反序列化方法
            foreach (var depType in classInfo.DependentComplexTypes)
            {
                GenerateInlineSerializerForDependentType(sb, depType);
                GenerateInlineDeserializerForDependentType(sb, depType);
            }
        }
        else
        {
            // 没有依赖类型时，使用原来的通用方法
            GenerateGenericSerializeComplexObject(sb);
            GenerateGenericDeserializeComplexObject(sb);
        }
    }
    
    /// <summary>
    /// 生成带内联方法调用的 SerializeComplexObject
    /// </summary>
    private static void GenerateSerializeComplexObjectWithInline(StringBuilder sb, ClassInfo classInfo)
    {
        sb.AppendLine("        /// <summary>");
        sb.AppendLine("        /// 序列化复杂对象为 BSON 文档（AOT兼容，使用内联序列化器）");
        sb.AppendLine("        /// </summary>");
        sb.AppendLine("        /// <typeparam name=\"T\">对象类型</typeparam>");
        sb.AppendLine("        /// <param name=\"obj\">要序列化的对象</param>");
        sb.AppendLine("        /// <returns>BSON 文档</returns>");
        sb.AppendLine("        private static BsonDocument SerializeComplexObject<T>(T obj)");
        sb.AppendLine("        {");
        sb.AppendLine("            if (obj == null) return new BsonDocument();");
        sb.AppendLine();
        
        // 为每个依赖类型生成类型检查
        foreach (var depType in classInfo.DependentComplexTypes)
        {
            sb.AppendLine($"            // 检查是否是 {depType.ShortName}");
            sb.AppendLine($"            if (obj is {depType.FullyQualifiedName} typed_{depType.SafeMethodName})");
            sb.AppendLine("            {");
            sb.AppendLine($"                return Serialize_{depType.SafeMethodName}(typed_{depType.SafeMethodName});");
            sb.AppendLine("            }");
            sb.AppendLine();
        }
        
        sb.AppendLine("            // 首先尝试使用已注册的 AOT 适配器");
        sb.AppendLine("            if (global::TinyDb.Serialization.AotHelperRegistry.TryGetAdapter<T>(out var adapter))");
        sb.AppendLine("            {");
        sb.AppendLine("                return adapter.ToDocument(obj);");
        sb.AppendLine("            }");
        sb.AppendLine();
        sb.AppendLine("            // 回退到通用序列化（可能使用反射）");
        sb.AppendLine("            return global::TinyDb.Serialization.AotBsonMapper.ToDocument(obj);");
        sb.AppendLine("        }");
        sb.AppendLine();
    }
    
    /// <summary>
    /// 生成带内联方法调用的 DeserializeComplexObject
    /// </summary>
    private static void GenerateDeserializeComplexObjectWithInline(StringBuilder sb, ClassInfo classInfo)
    {
        sb.AppendLine("        /// <summary>");
        sb.AppendLine("        /// 从 BSON 文档反序列化复杂对象（AOT兼容，使用内联反序列化器）");
        sb.AppendLine("        /// </summary>");
        sb.AppendLine("        /// <typeparam name=\"T\">目标类型</typeparam>");
        sb.AppendLine("        /// <param name=\"document\">BSON 文档</param>");
        sb.AppendLine("        /// <returns>反序列化后的对象</returns>");
        sb.AppendLine("        private static T DeserializeComplexObject<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicMethods | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields)] T>(BsonDocument document)");
        sb.AppendLine("        {");
        sb.AppendLine("            if (document == null) return default!;");
        sb.AppendLine();
        
        // 为每个依赖类型生成类型检查
        foreach (var depType in classInfo.DependentComplexTypes)
        {
            sb.AppendLine($"            // 检查是否要反序列化为 {depType.ShortName}");
            sb.AppendLine($"            if (typeof(T) == typeof({depType.FullyQualifiedName}))");
            sb.AppendLine("            {");
            sb.AppendLine($"                return (T)(object)Deserialize_{depType.SafeMethodName}(document);");
            sb.AppendLine("            }");
            sb.AppendLine();
        }
        
        sb.AppendLine("            // 首先尝试使用已注册的 AOT 适配器");
        sb.AppendLine("            if (global::TinyDb.Serialization.AotHelperRegistry.TryGetAdapter<T>(out var adapter))");
        sb.AppendLine("            {");
        sb.AppendLine("                return adapter.FromDocument(document);");
        sb.AppendLine("            }");
        sb.AppendLine();
        sb.AppendLine("            // 回退到通用反序列化（可能使用反射）");
        sb.AppendLine("            return global::TinyDb.Serialization.AotBsonMapper.FromDocument<T>(document);");
        sb.AppendLine("        }");
        sb.AppendLine();
    }
    
    /// <summary>
    /// 生成通用的 SerializeComplexObject 方法（无内联）
    /// </summary>
    private static void GenerateGenericSerializeComplexObject(StringBuilder sb)
    {
        sb.AppendLine("        /// <summary>");
        sb.AppendLine("        /// 序列化复杂对象为 BSON 文档（AOT兼容）");
        sb.AppendLine("        /// </summary>");
        sb.AppendLine("        /// <typeparam name=\"T\">对象类型</typeparam>");
        sb.AppendLine("        /// <param name=\"obj\">要序列化的对象</param>");
        sb.AppendLine("        /// <returns>BSON 文档</returns>");
        sb.AppendLine("        private static BsonDocument SerializeComplexObject<T>(T obj)");
        sb.AppendLine("        {");
        sb.AppendLine("            if (obj == null) return new BsonDocument();");
        sb.AppendLine();
        sb.AppendLine("            // 首先尝试使用已注册的 AOT 适配器");
        sb.AppendLine("            if (global::TinyDb.Serialization.AotHelperRegistry.TryGetAdapter<T>(out var adapter))");
        sb.AppendLine("            {");
        sb.AppendLine("                return adapter.ToDocument(obj);");
        sb.AppendLine("            }");
        sb.AppendLine();
        sb.AppendLine("            // 回退到通用序列化（可能使用反射）");
        sb.AppendLine("            return global::TinyDb.Serialization.AotBsonMapper.ToDocument(obj);");
        sb.AppendLine("        }");
        sb.AppendLine();
    }
    
    /// <summary>
    /// 生成通用的 DeserializeComplexObject 方法（无内联）
    /// </summary>
    private static void GenerateGenericDeserializeComplexObject(StringBuilder sb)
    {
        sb.AppendLine("        /// <summary>");
        sb.AppendLine("        /// 从 BSON 文档反序列化复杂对象（AOT兼容）");
        sb.AppendLine("        /// </summary>");
        sb.AppendLine("        /// <typeparam name=\"T\">目标类型</typeparam>");
        sb.AppendLine("        /// <param name=\"document\">BSON 文档</param>");
        sb.AppendLine("        /// <returns>反序列化后的对象</returns>");
        sb.AppendLine("        private static T DeserializeComplexObject<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicMethods | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields)] T>(BsonDocument document)");
        sb.AppendLine("        {");
        sb.AppendLine("            if (document == null) return default!;");
        sb.AppendLine();
        sb.AppendLine("            // 首先尝试使用已注册的 AOT 适配器");
        sb.AppendLine("            if (global::TinyDb.Serialization.AotHelperRegistry.TryGetAdapter<T>(out var adapter))");
        sb.AppendLine("            {");
        sb.AppendLine("                return adapter.FromDocument(document);");
        sb.AppendLine("            }");
        sb.AppendLine();
        sb.AppendLine("            // 回退到通用反序列化（可能使用反射）");
        sb.AppendLine("            return global::TinyDb.Serialization.AotBsonMapper.FromDocument<T>(document);");
        sb.AppendLine("        }");
        sb.AppendLine();
    }
    
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
        sb.AppendLine("            var document = new BsonDocument();");
        sb.AppendLine();
        
        // 为每个属性生成序列化代码
        foreach (var prop in depType.Properties)
        {
            var bsonFieldName = prop.Name;
            
            if (prop.IsComplexType && !string.IsNullOrEmpty(prop.ComplexTypeFullName))
            {
                // 检测是否是循环引用属性
                if (prop.IsCircularReference)
                {
                    // 循环引用属性：跳过递归序列化，设置为 null 避免栈溢出
                    sb.AppendLine($"            // 注意：属性 {prop.Name} 涉及循环引用，跳过递归序列化以避免栈溢出");
                    sb.AppendLine($"            document = document.Set(\"{bsonFieldName}\", BsonNull.Value);");
                }
                else
                {
                    // 复杂类型使用递归调用
                    if (prop.IsNullable || !prop.IsValueType)
                    {
                        sb.AppendLine($"            if (obj.{prop.Name} == null)");
                        sb.AppendLine($"                document = document.Set(\"{bsonFieldName}\", BsonNull.Value);");
                        sb.AppendLine($"            else");
                        sb.AppendLine($"                document = document.Set(\"{bsonFieldName}\", SerializeComplexObject(obj.{prop.Name}));");
                    }
                    else
                    {
                        sb.AppendLine($"            document = document.Set(\"{bsonFieldName}\", SerializeComplexObject(obj.{prop.Name}));");
                    }
                }
            }
            else
            {
                // 简单类型使用 ConvertToBsonValue
                sb.AppendLine($"            document = document.Set(\"{bsonFieldName}\", ConvertToBsonValue(obj.{prop.Name}));");
            }
        }
        
        sb.AppendLine("            return document;");
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
            sb.AppendLine($"            var result = new {depType.FullyQualifiedName}();");
        }
        else
        {
            sb.AppendLine($"            var result = new {depType.FullyQualifiedName}();");
        }
        sb.AppendLine();
        
        // 为每个属性生成反序列化代码
        foreach (var prop in depType.Properties)
        {
            var bsonFieldName = prop.Name;
            
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
                    if (prop.IsNullable || !prop.IsValueType)
                    {
                        sb.AppendLine($"                    result.{prop.Name} = default;");
                    }
                    sb.AppendLine("                }");
                    sb.AppendLine($"                else if (bson_{prop.Name} is BsonDocument nested_{prop.Name})");
                    sb.AppendLine("                {");
                    sb.AppendLine($"                    result.{prop.Name} = DeserializeComplexObject<{prop.FullyQualifiedTypeName}>(nested_{prop.Name});");
                    sb.AppendLine("                }");
                    sb.AppendLine("            }");
                }
            }
            else
            {
                // 简单类型使用 ConvertFromBsonValue
                sb.AppendLine($"            if (document.TryGetValue(\"{bsonFieldName}\", out var bson_{prop.Name}) && !bson_{prop.Name}.IsNull)");
                sb.AppendLine("            {");
                sb.AppendLine($"                result.{prop.Name} = ConvertFromBsonValue<{prop.FullyQualifiedTypeName}>(bson_{prop.Name});");
                sb.AppendLine("            }");
            }
        }
        
        sb.AppendLine("            return result;");
        sb.AppendLine("        }");
        sb.AppendLine();
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

    /// <summary>
    /// 类型分析结果
    /// </summary>
    private sealed class TypeAnalysisResult
    {
        public bool IsComplexType { get; }
        public bool IsCollection { get; }
        public bool IsDictionary { get; }
        public bool IsArray { get; }
        public string? ElementType { get; }
        public bool IsElementComplexType { get; }
        public bool IsElementValueType { get; }
        public string? DictionaryKeyType { get; }
        public string? DictionaryValueType { get; }
        public bool IsDictionaryValueComplexType { get; }
        public bool IsDictionaryValueValueType { get; }

        public TypeAnalysisResult(
            bool isComplexType,
            bool isCollection,
            bool isDictionary,
            bool isArray,
            string? elementType,
            bool isElementComplexType,
            bool isElementValueType,
            string? dictionaryKeyType,
            string? dictionaryValueType,
            bool isDictionaryValueComplexType,
            bool isDictionaryValueValueType)
        {
            IsComplexType = isComplexType;
            IsCollection = isCollection;
            IsDictionary = isDictionary;
            IsArray = isArray;
            ElementType = elementType;
            IsElementComplexType = isElementComplexType;
            IsElementValueType = isElementValueType;
            DictionaryKeyType = dictionaryKeyType;
            DictionaryValueType = dictionaryValueType;
            IsDictionaryValueComplexType = isDictionaryValueComplexType;
            IsDictionaryValueValueType = isDictionaryValueValueType;
        }
    }

    /// <summary>
    /// 分析属性类型，判断是否是复杂类型、集合类型等
    /// </summary>
    private static TypeAnalysisResult AnalyzePropertyType(ITypeSymbol? typeSymbol)
    {
        if (typeSymbol == null)
        {
            return new TypeAnalysisResult(false, false, false, false, null, false, false, null, null, false, false);
        }

        // 处理可空类型
        if (typeSymbol is INamedTypeSymbol { IsGenericType: true } namedType &&
            namedType.OriginalDefinition.SpecialType == SpecialType.System_Nullable_T)
        {
            typeSymbol = namedType.TypeArguments[0];
        }

        // 检查是否是基本类型
        if (IsPrimitiveOrWellKnownType(typeSymbol))
        {
            return new TypeAnalysisResult(false, false, false, false, null, false, false, null, null, false, false);
        }

        // 检查是否是数组
        if (typeSymbol is IArrayTypeSymbol arrayType)
        {
            var elementType = arrayType.ElementType;
            var elementTypeName = elementType.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
            var isElementComplex = IsComplexObjectType(elementType);
            var isElementValueType = elementType.IsValueType;
            return new TypeAnalysisResult(false, true, false, true, elementTypeName, isElementComplex, isElementValueType, null, null, false, false);
        }

        // 检查是否是字典类型
        if (typeSymbol is INamedTypeSymbol dictType && IsDictionaryType(dictType))
        {
            var typeArgs = GetDictionaryTypeArguments(dictType);
            if (typeArgs != null)
            {
                var keyTypeName = typeArgs.Value.KeyType.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
                var valueTypeName = typeArgs.Value.ValueType.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
                var isValueComplex = IsComplexObjectType(typeArgs.Value.ValueType);
                var isValueValueType = typeArgs.Value.ValueType.IsValueType;
                return new TypeAnalysisResult(false, false, true, false, null, false, false, keyTypeName, valueTypeName, isValueComplex, isValueValueType);
            }
        }

        // 检查是否是集合类型 (List<T>, ICollection<T>, IEnumerable<T> 等)
        if (typeSymbol is INamedTypeSymbol collectionType && IsCollectionType(collectionType))
        {
            var elementType = GetCollectionElementType(collectionType);
            if (elementType != null)
            {
                var elementTypeName = elementType.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
                var isElementComplex = IsComplexObjectType(elementType);
                var isElementValueType = elementType.IsValueType;
                return new TypeAnalysisResult(false, true, false, false, elementTypeName, isElementComplex, isElementValueType, null, null, false, false);
            }
        }

        // 检查是否是复杂对象类型
        if (IsComplexObjectType(typeSymbol))
        {
            return new TypeAnalysisResult(true, false, false, false, null, false, false, null, null, false, false);
        }

        return new TypeAnalysisResult(false, false, false, false, null, false, false, null, null, false, false);
    }

    /// <summary>
    /// 检查是否是基本类型或已知的简单类型
    /// </summary>
    private static bool IsPrimitiveOrWellKnownType(ITypeSymbol typeSymbol)
    {
        // 检查特殊类型
        switch (typeSymbol.SpecialType)
        {
            case SpecialType.System_Boolean:
            case SpecialType.System_Byte:
            case SpecialType.System_SByte:
            case SpecialType.System_Int16:
            case SpecialType.System_UInt16:
            case SpecialType.System_Int32:
            case SpecialType.System_UInt32:
            case SpecialType.System_Int64:
            case SpecialType.System_UInt64:
            case SpecialType.System_Single:
            case SpecialType.System_Double:
            case SpecialType.System_Decimal:
            case SpecialType.System_Char:
            case SpecialType.System_String:
            case SpecialType.System_DateTime:
                return true;
        }

        // 检查已知类型名称
        var typeName = typeSymbol.ToDisplayString();
        return typeName switch
        {
            "System.Guid" or "Guid" => true,
            "System.TimeSpan" or "TimeSpan" => true,
            "System.DateTimeOffset" or "DateTimeOffset" => true,
            "TinyDb.Bson.ObjectId" or "ObjectId" => true,
            "byte[]" or "System.Byte[]" => true,
            _ when typeName.EndsWith("ObjectId", StringComparison.Ordinal) => true,
            _ when typeSymbol.TypeKind == TypeKind.Enum => true,
            _ => false
        };
    }

    /// <summary>
    /// 检查是否是字典类型
    /// </summary>
    private static bool IsDictionaryType(INamedTypeSymbol typeSymbol)
    {
        // 检查是否直接是 Dictionary<,> 或 IDictionary<,>
        var typeName = typeSymbol.OriginalDefinition.ToDisplayString();
        if (typeName.StartsWith("System.Collections.Generic.Dictionary<", StringComparison.Ordinal) ||
            typeName.StartsWith("System.Collections.Generic.IDictionary<", StringComparison.Ordinal))
        {
            return true;
        }

        // 检查是否实现了 IDictionary<,> 接口
        foreach (var iface in typeSymbol.AllInterfaces)
        {
            var ifaceName = iface.OriginalDefinition.ToDisplayString();
            if (ifaceName.StartsWith("System.Collections.Generic.IDictionary<", StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// 获取字典的键值类型
    /// </summary>
    private static (ITypeSymbol KeyType, ITypeSymbol ValueType)? GetDictionaryTypeArguments(INamedTypeSymbol typeSymbol)
    {
        // 如果是泛型字典类型，直接获取类型参数
        if (typeSymbol.IsGenericType && typeSymbol.TypeArguments.Length == 2)
        {
            var typeName = typeSymbol.OriginalDefinition.ToDisplayString();
            if (typeName.StartsWith("System.Collections.Generic.Dictionary<", StringComparison.Ordinal) ||
                typeName.StartsWith("System.Collections.Generic.IDictionary<", StringComparison.Ordinal))
            {
                return (typeSymbol.TypeArguments[0], typeSymbol.TypeArguments[1]);
            }
        }

        // 查找 IDictionary<,> 接口
        foreach (var iface in typeSymbol.AllInterfaces)
        {
            if (iface.IsGenericType && iface.TypeArguments.Length == 2)
            {
                var ifaceName = iface.OriginalDefinition.ToDisplayString();
                if (ifaceName.StartsWith("System.Collections.Generic.IDictionary<", StringComparison.Ordinal))
                {
                    return (iface.TypeArguments[0], iface.TypeArguments[1]);
                }
            }
        }

        return null;
    }

    /// <summary>
    /// 检查是否是集合类型（非字典）
    /// </summary>
    private static bool IsCollectionType(INamedTypeSymbol typeSymbol)
    {
        // 排除字符串
        if (typeSymbol.SpecialType == SpecialType.System_String)
        {
            return false;
        }

        // 排除字典类型
        if (IsDictionaryType(typeSymbol))
        {
            return false;
        }

        // 检查常见集合类型
        var typeName = typeSymbol.OriginalDefinition.ToDisplayString();
        if (typeName.StartsWith("System.Collections.Generic.List<", StringComparison.Ordinal) ||
            typeName.StartsWith("System.Collections.Generic.IList<", StringComparison.Ordinal) ||
            typeName.StartsWith("System.Collections.Generic.ICollection<", StringComparison.Ordinal) ||
            typeName.StartsWith("System.Collections.Generic.IEnumerable<", StringComparison.Ordinal) ||
            typeName.StartsWith("System.Collections.Generic.HashSet<", StringComparison.Ordinal) ||
            typeName.StartsWith("System.Collections.Generic.ISet<", StringComparison.Ordinal))
        {
            return true;
        }

        // 检查是否实现了 ICollection<T> 或 IEnumerable<T>
        foreach (var iface in typeSymbol.AllInterfaces)
        {
            var ifaceName = iface.OriginalDefinition.ToDisplayString();
            if (ifaceName.StartsWith("System.Collections.Generic.ICollection<", StringComparison.Ordinal) ||
                ifaceName.StartsWith("System.Collections.Generic.IEnumerable<", StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// 获取集合类型的元素类型符号
    /// </summary>
    private static ITypeSymbol? GetElementTypeSymbol(ITypeSymbol? typeSymbol)
    {
        if (typeSymbol == null) return null;

        // 处理数组
        if (typeSymbol is IArrayTypeSymbol arrayType)
        {
            return arrayType.ElementType;
        }

        // 处理泛型集合
        if (typeSymbol is INamedTypeSymbol namedType)
        {
            return GetCollectionElementType(namedType);
        }

        return null;
    }

    /// <summary>
    /// 获取字典类型的值类型符号
    /// </summary>
    private static ITypeSymbol? GetDictionaryValueTypeSymbol(ITypeSymbol? typeSymbol)
    {
        if (typeSymbol == null) return null;

        if (typeSymbol is INamedTypeSymbol namedType)
        {
            var typeArgs = GetDictionaryTypeArguments(namedType);
            return typeArgs?.ValueType;
        }

        return null;
    }

    /// <summary>
    /// 获取集合的元素类型
    /// </summary>
    private static ITypeSymbol? GetCollectionElementType(INamedTypeSymbol typeSymbol)
    {
        // 如果是泛型集合类型，直接获取类型参数
        if (typeSymbol.IsGenericType && typeSymbol.TypeArguments.Length == 1)
        {
            return typeSymbol.TypeArguments[0];
        }

        // 查找 IEnumerable<T> 接口
        foreach (var iface in typeSymbol.AllInterfaces)
        {
            if (iface.IsGenericType && iface.TypeArguments.Length == 1)
            {
                var ifaceName = iface.OriginalDefinition.ToDisplayString();
                if (ifaceName.StartsWith("System.Collections.Generic.IEnumerable<", StringComparison.Ordinal))
                {
                    return iface.TypeArguments[0];
                }
            }
        }

        return null;
    }

    /// <summary>
    /// 检查是否是复杂对象类型（类或结构体，非基本类型、非集合、非字典）
    /// </summary>
    private static bool IsComplexObjectType(ITypeSymbol typeSymbol)
    {
        // 排除基本类型
        if (IsPrimitiveOrWellKnownType(typeSymbol))
        {
            return false;
        }

        // 排除数组
        if (typeSymbol is IArrayTypeSymbol)
        {
            return false;
        }

        // 排除集合和字典
        if (typeSymbol is INamedTypeSymbol namedType)
        {
            if (IsDictionaryType(namedType) || IsCollectionType(namedType))
            {
                return false;
            }
        }

        // 类或结构体
        return typeSymbol.TypeKind == TypeKind.Class || typeSymbol.TypeKind == TypeKind.Struct;
    }

    /// <summary>
    /// 检查类型是否有 [Entity] 属性
    /// </summary>
    private static bool HasEntityAttribute(ITypeSymbol typeSymbol)
    {
        return typeSymbol.GetAttributes()
            .Any(attr => attr.AttributeClass?.Name == "EntityAttribute");
    }

    /// <summary>
    /// 收集所有依赖的非Entity复杂类型
    /// </summary>
    private static (List<DependentComplexType> Types, List<CircularReferenceInfo> CircularRefs) CollectDependentComplexTypes(
        List<PropertyInfo> properties, 
        IReadOnlyDictionary<string, ITypeSymbol> typeSymbols)
    {
        var result = new List<DependentComplexType>();
        var circularRefs = new List<CircularReferenceInfo>();
        var visited = new HashSet<string>();
        var toProcess = new Queue<(string FullName, ITypeSymbol Symbol, List<string> Path)>();

        // 首先收集直接依赖的复杂类型
        foreach (var prop in properties)
        {
            // 处理复杂类型属性
            if (prop.IsComplexType && !string.IsNullOrEmpty(prop.FullyQualifiedNonNullableType))
            {
                var typeName = prop.FullyQualifiedNonNullableType;
                if (typeSymbols.TryGetValue(typeName, out var typeSymbol) && 
                    !HasEntityAttribute(typeSymbol) && 
                    !visited.Contains(typeName))
                {
                    visited.Add(typeName);
                    toProcess.Enqueue((typeName, typeSymbol, new List<string> { typeName }));
                }
            }

            // 处理集合元素类型
            if (prop.IsCollection && prop.IsElementComplexType && !string.IsNullOrEmpty(prop.ElementType))
            {
                var typeName = prop.ElementType!;
                if (typeSymbols.TryGetValue(typeName, out var typeSymbol) && 
                    !HasEntityAttribute(typeSymbol) && 
                    !visited.Contains(typeName))
                {
                    visited.Add(typeName);
                    toProcess.Enqueue((typeName, typeSymbol, new List<string> { typeName }));
                }
            }

            // 处理字典值类型
            if (prop.IsDictionary && prop.IsDictionaryValueComplexType && !string.IsNullOrEmpty(prop.DictionaryValueType))
            {
                var typeName = prop.DictionaryValueType!;
                if (typeSymbols.TryGetValue(typeName, out var typeSymbol) && 
                    !HasEntityAttribute(typeSymbol) && 
                    !visited.Contains(typeName))
                {
                    visited.Add(typeName);
                    toProcess.Enqueue((typeName, typeSymbol, new List<string> { typeName }));
                }
            }
        }

        // BFS处理所有依赖类型
        while (toProcess.Count > 0)
        {
            var (fullName, typeSymbol, path) = toProcess.Dequeue();
            var (dependentType, detectedCircularRefs) = AnalyzeDependentType(fullName, typeSymbol, visited, toProcess, path);
            if (dependentType != null)
            {
                result.Add(dependentType);
            }
            circularRefs.AddRange(detectedCircularRefs);
        }

        return (result, circularRefs);
    }

    /// <summary>
    /// 分析一个依赖类型并收集其属性信息
    /// </summary>
    private static (DependentComplexType? Type, List<CircularReferenceInfo> CircularRefs) AnalyzeDependentType(
        string fullName,
        ITypeSymbol typeSymbol,
        HashSet<string> visited,
        Queue<(string FullName, ITypeSymbol Symbol, List<string> Path)> toProcess,
        List<string> currentPath)
    {
        var properties = new List<DependentTypeProperty>();
        var circularRefs = new List<CircularReferenceInfo>();

        // 获取类型的所有公共属性
        foreach (var member in typeSymbol.GetMembers())
        {
            if (member is IPropertySymbol propertySymbol && 
                propertySymbol.DeclaredAccessibility == Accessibility.Public &&
                !propertySymbol.IsStatic &&
                propertySymbol.GetMethod != null &&
                propertySymbol.SetMethod != null)
            {
                // 检查是否有 BsonIgnore 属性
                var hasIgnore = propertySymbol.GetAttributes()
                    .Any(attr => attr.AttributeClass?.Name == "BsonIgnoreAttribute");
                if (hasIgnore) continue;

                var propType = propertySymbol.Type;
                var propTypeName = propType.ToDisplayString(SymbolDisplayFormat.MinimallyQualifiedFormat);
                var propFullTypeName = propType.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
                var isValueType = propType.IsValueType;
                var isNullable = propType.NullableAnnotation == NullableAnnotation.Annotated ||
                                 (propType is INamedTypeSymbol { IsGenericType: true } namedType &&
                                  namedType.OriginalDefinition.SpecialType == SpecialType.System_Nullable_T);

                // 检查属性类型是否是复杂类型
                var isComplex = IsComplexObjectType(propType);
                string? complexFullName = null;
                bool isCircularRef = false;

                if (isComplex && !HasEntityAttribute(propType))
                {
                    complexFullName = propFullTypeName;
                    
                    // 获取实际的类型符号（处理可空类型）
                    var actualPropType = propType;
                    if (propType is INamedTypeSymbol { IsGenericType: true } nullableNamed &&
                        nullableNamed.OriginalDefinition.SpecialType == SpecialType.System_Nullable_T &&
                        nullableNamed.TypeArguments.Length == 1)
                    {
                        actualPropType = nullableNamed.TypeArguments[0];
                    }
                    
                    // 检测循环引用：当前路径中是否已包含此类型
                    if (currentPath.Contains(propFullTypeName))
                    {
                        // 检测到循环引用
                        isCircularRef = true;
                        var cyclePath = new List<string>(currentPath) { propFullTypeName };
                        var cycleStartIndex = currentPath.IndexOf(propFullTypeName);
                        var cycleChain = string.Join(" -> ", cyclePath.Skip(cycleStartIndex));
                        circularRefs.Add(new CircularReferenceInfo(
                            fullName,
                            propFullTypeName,
                            propertySymbol.Name,
                            cycleChain));
                    }
                    // 如果这个类型还没有被访问过且不是循环引用，直接使用属性类型符号加入队列
                    else if (!visited.Contains(propFullTypeName))
                    {
                        visited.Add(propFullTypeName);
                        var newPath = new List<string>(currentPath) { propFullTypeName };
                        toProcess.Enqueue((propFullTypeName, actualPropType, newPath));
                    }
                }

                properties.Add(new DependentTypeProperty(
                    propertySymbol.Name,
                    propTypeName,
                    propFullTypeName,
                    isValueType,
                    isNullable,
                    isComplex && !HasEntityAttribute(propType),
                    complexFullName,
                    isCircularRef));
            }
        }

        if (properties.Count == 0)
        {
            return (null, circularRefs);
        }

        return (new DependentComplexType(
            fullName,
            typeSymbol.Name,
            typeSymbol.IsValueType,
            properties), circularRefs);
    }

}

/// <summary>
/// 类信息
/// </summary>
public class ClassInfo
{
    public string Namespace { get; }
    public string Name { get; }
    public string ContainingTypePath { get; }
    /// <summary>
    /// 用于在代码中引用类型的名称（对于嵌套类，使用 OuterClass.InnerClass 格式）
    /// </summary>
    public string TypeReference
    {
        get
        {
            if (!string.IsNullOrEmpty(ContainingTypePath))
            {
                return $"{ContainingTypePath}.{Name}";
            }
            return Name;
        }
    }
    public string FullName
    {
        get
        {
            var baseName = string.IsNullOrEmpty(Namespace) ? "" : $"{Namespace}.";
            if (!string.IsNullOrEmpty(ContainingTypePath))
            {
                return $"{baseName}{ContainingTypePath}.{Name}";
            }
            return $"{baseName}{Name}";
        }
    }
    public string UniqueFileName
    {
        get
        {
            var parts = new List<string>();
            if (!string.IsNullOrEmpty(Namespace)) parts.Add(Namespace.Replace(".", "_"));
            if (!string.IsNullOrEmpty(ContainingTypePath)) parts.Add(ContainingTypePath);
            parts.Add(Name);
            return string.Join("_", parts);
        }
    }
    /// <summary>
    /// 用于生成帮助器类名的唯一标识（例如 CoverageSprintFinal_MathEntity）
    /// </summary>
    public string HelperClassName
    {
        get
        {
            if (!string.IsNullOrEmpty(ContainingTypePath))
            {
                return $"{ContainingTypePath}_{Name}AotHelper";
            }
            return $"{Name}AotHelper";
        }
    }
    public List<PropertyInfo> Properties { get; }
    public PropertyInfo? IdProperty { get; }
    public string? CollectionName { get; }
    /// <summary>
    /// 类声明的源代码位置（用于诊断报告）
    /// </summary>
    public Location? Location { get; }
    
    /// <summary>
    /// 依赖的非Entity复杂类型（需要生成内联序列化代码）
    /// </summary>
    public List<DependentComplexType> DependentComplexTypes { get; }
    
    /// <summary>
    /// 检测到的循环引用信息
    /// </summary>
    public List<CircularReferenceInfo> CircularReferences { get; }

    public ClassInfo(string @namespace, string name, List<PropertyInfo> properties, PropertyInfo? idProperty, string? collectionName = null, string? containingTypePath = null, Location? location = null, List<DependentComplexType>? dependentComplexTypes = null, List<CircularReferenceInfo>? circularReferences = null)
    {
        Namespace = @namespace;
        Name = name;
        Properties = properties;
        IdProperty = idProperty;
        CollectionName = collectionName;
        ContainingTypePath = containingTypePath ?? string.Empty;
        Location = location;
        DependentComplexTypes = dependentComplexTypes ?? new List<DependentComplexType>();
        CircularReferences = circularReferences ?? new List<CircularReferenceInfo>();
    }
}

/// <summary>
/// 依赖的复杂类型信息（非Entity类型，需要生成内联序列化代码）
/// </summary>
public class DependentComplexType
{
    /// <summary>
    /// 类型的完全限定名（用于生成代码中的类型引用）
    /// </summary>
    public string FullyQualifiedName { get; }
    
    /// <summary>
    /// 类型的简短名称（用于生成方法名等）
    /// </summary>
    public string ShortName { get; }
    
    /// <summary>
    /// 是否是值类型（struct）
    /// </summary>
    public bool IsValueType { get; }
    
    /// <summary>
    /// 类型的属性列表
    /// </summary>
    public List<DependentTypeProperty> Properties { get; }
    
    /// <summary>
    /// 用于生成唯一方法名的安全名称
    /// </summary>
    public string SafeMethodName => FullyQualifiedName
        .Replace("global::", "")
        .Replace(".", "_")
        .Replace("<", "_")
        .Replace(">", "_")
        .Replace(",", "_")
        .Replace(" ", "");

    public DependentComplexType(string fullyQualifiedName, string shortName, bool isValueType, List<DependentTypeProperty> properties)
    {
        FullyQualifiedName = fullyQualifiedName;
        ShortName = shortName;
        IsValueType = isValueType;
        Properties = properties;
    }
}

/// <summary>
/// 依赖类型的属性信息
/// </summary>
public class DependentTypeProperty
{
    public string Name { get; }
    public string TypeName { get; }
    public string FullyQualifiedTypeName { get; }
    public bool IsValueType { get; }
    public bool IsNullable { get; }
    public bool IsComplexType { get; }
    public string? ComplexTypeFullName { get; }
    /// <summary>
    /// 是否是循环引用（属性类型在依赖链中形成循环）
    /// </summary>
    public bool IsCircularReference { get; }

    public DependentTypeProperty(string name, string typeName, string fullyQualifiedTypeName, bool isValueType, bool isNullable, bool isComplexType = false, string? complexTypeFullName = null, bool isCircularReference = false)
    {
        Name = name;
        TypeName = typeName;
        FullyQualifiedTypeName = fullyQualifiedTypeName;
        IsValueType = isValueType;
        IsNullable = isNullable;
        IsComplexType = isComplexType;
        ComplexTypeFullName = complexTypeFullName;
        IsCircularReference = isCircularReference;
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
    
    /// <summary>
    /// 是否是复杂对象类型（非基本类型、非集合、非字典的类或结构体）
    /// </summary>
    public bool IsComplexType { get; }
    
    /// <summary>
    /// 是否是集合类型（List、Array、ICollection等）
    /// </summary>
    public bool IsCollection { get; }
    
    /// <summary>
    /// 是否是字典类型
    /// </summary>
    public bool IsDictionary { get; }
    
    /// <summary>
    /// 是否是数组类型
    /// </summary>
    public bool IsArray { get; }
    
    /// <summary>
    /// 集合/数组的元素类型（完全限定名）
    /// </summary>
    public string? ElementType { get; }
    
    /// <summary>
    /// 集合/数组的元素类型是否是复杂类型
    /// </summary>
    public bool IsElementComplexType { get; }
    
    /// <summary>
    /// 集合/数组的元素类型是否是值类型（struct）
    /// </summary>
    public bool IsElementValueType { get; }
    
    /// <summary>
    /// 字典的键类型
    /// </summary>
    public string? DictionaryKeyType { get; }
    
    /// <summary>
    /// 字典的值类型
    /// </summary>
    public string? DictionaryValueType { get; }
    
    /// <summary>
    /// 字典的值类型是否是复杂类型
    /// </summary>
    public bool IsDictionaryValueComplexType { get; }
    
    /// <summary>
    /// 字典的值类型是否是值类型（struct）
    /// </summary>
    public bool IsDictionaryValueValueType { get; }

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
        string? fullyQualifiedNonNullableType = null,
        bool isComplexType = false,
        bool isCollection = false,
        bool isDictionary = false,
        bool isArray = false,
        string? elementType = null,
        bool isElementComplexType = false,
        bool isElementValueType = false,
        string? dictionaryKeyType = null,
        string? dictionaryValueType = null,
        bool isDictionaryValueComplexType = false,
        bool isDictionaryValueValueType = false)
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
        IsComplexType = isComplexType;
        IsCollection = isCollection;
        IsDictionary = isDictionary;
        IsArray = isArray;
        ElementType = elementType;
        IsElementComplexType = isElementComplexType;
        IsElementValueType = isElementValueType;
        DictionaryKeyType = dictionaryKeyType;
        DictionaryValueType = dictionaryValueType;
        IsDictionaryValueComplexType = isDictionaryValueComplexType;
        IsDictionaryValueValueType = isDictionaryValueValueType;
    }
}

/// <summary>
/// 循环引用信息
/// </summary>
public class CircularReferenceInfo
{
    /// <summary>
    /// 包含循环引用属性的类型名称
    /// </summary>
    public string ContainingTypeName { get; }
    
    /// <summary>
    /// 循环引用的目标类型名称
    /// </summary>
    public string TargetTypeName { get; }
    
    /// <summary>
    /// 导致循环引用的属性名称
    /// </summary>
    public string PropertyName { get; }
    
    /// <summary>
    /// 循环链的描述（如 A -> B -> A）
    /// </summary>
    public string CycleChain { get; }

    public CircularReferenceInfo(string containingTypeName, string targetTypeName, string propertyName, string cycleChain)
    {
        ContainingTypeName = containingTypeName;
        TargetTypeName = targetTypeName;
        PropertyName = propertyName;
        CycleChain = cycleChain;
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
            _ when propertyType.EndsWith("?") => GenerateNullablePropertySerialization(prop, bsonFieldName),
            _ => $"document = document.Set(\"{bsonFieldName}\", ConvertToBsonValue(entity.{propertyName}));"
        };
    }

    /// <summary>
    /// 生成可空属性序列化代码
    /// </summary>
    private static string GenerateNullablePropertySerialization(PropertyInfo prop, string bsonFieldName)
    {
        var propertyName = prop.Name;
        
        // 如果底层类型是复杂类型
        if (prop.IsComplexType)
        {
            return $@"if (entity.{propertyName} == null)
                document = document.Set(""{bsonFieldName}"", BsonNull.Value);
            else
                document = document.Set(""{bsonFieldName}"", SerializeComplexObject(entity.{propertyName}));";
        }
        
        return $"document = document.Set(\"{bsonFieldName}\", entity.{propertyName} == null ? BsonNull.Value : ConvertToBsonValue(entity.{propertyName}));";
    }

    /// <summary>
    /// 生成复杂类型属性的序列化代码
    /// </summary>
    private static string GenerateComplexTypeSerialization(PropertyInfo prop, string bsonFieldName)
    {
        var propertyName = prop.Name;
        var isNullable = prop.IsNullableReferenceType || prop.Type.EndsWith("?");
        
        if (isNullable)
        {
            return $@"if (entity.{propertyName} == null)
                document = document.Set(""{bsonFieldName}"", BsonNull.Value);
            else
                document = document.Set(""{bsonFieldName}"", SerializeComplexObject(entity.{propertyName}));";
        }
        
        return $"document = document.Set(\"{bsonFieldName}\", SerializeComplexObject(entity.{propertyName}));";
    }

    /// <summary>
    /// 生成包含复杂类型元素的集合的序列化代码
    /// </summary>
    private static string GenerateCollectionWithComplexElementSerialization(PropertyInfo prop, string bsonFieldName)
    {
        var propertyName = prop.Name;
        var isNullable = prop.IsNullableReferenceType || prop.Type.EndsWith("?");
        var isElementValueType = prop.IsElementValueType;
        
        var sb = new StringBuilder();
        
        if (isNullable)
        {
            sb.AppendLine($@"if (entity.{propertyName} == null)
                document = document.Set(""{bsonFieldName}"", BsonNull.Value);
            else
            {{");
        }
        
        sb.AppendLine($@"var array_{propertyName} = new BsonArray();
            foreach (var item in entity.{propertyName})
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
        var isNullable = prop.IsNullableReferenceType || prop.Type.EndsWith("?");
        var isValueValueType = prop.IsDictionaryValueValueType;
        
        var sb = new StringBuilder();
        
        if (isNullable)
        {
            sb.AppendLine($@"if (entity.{propertyName} == null)
                document = document.Set(""{bsonFieldName}"", BsonNull.Value);
            else
            {{");
        }
        
        sb.AppendLine($@"var dict_{propertyName} = new BsonDocument();
            foreach (var kvp in entity.{propertyName})
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
        var propertyType = prop.Type;

        // 检查是否是ID属性，如果是则从_id字段读取
        var bsonFieldName = IsIdProperty(prop) ? "_id" : propertyName;

        // 处理复杂类型
        if (prop.IsComplexType)
        {
            return GenerateComplexTypeDeserialization(prop, bsonFieldName);
        }

        // 处理集合中包含复杂类型的情况
        if (prop.IsCollection && prop.IsElementComplexType)
        {
            return GenerateCollectionWithComplexElementDeserialization(prop, bsonFieldName);
        }

        // 处理字典中包含复杂类型值的情况
        if (prop.IsDictionary && prop.IsDictionaryValueComplexType)
        {
            return GenerateDictionaryWithComplexValueDeserialization(prop, bsonFieldName);
        }

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
            _ when propertyType.EndsWith("?") => GenerateNullablePropertyDeserialization(prop, bsonFieldName),
            _ => $"if (document.TryGetValue(\"{bsonFieldName}\", out var bson{propertyName})) entity.{propertyName} = ConvertFromBsonValue<{propertyType}>(bson{propertyName});"
        };
    }

    /// <summary>
    /// 生成可空属性反序列化代码
    /// </summary>
    private static string GenerateNullablePropertyDeserialization(PropertyInfo prop, string bsonFieldName)
    {
        var propertyName = prop.Name;
        var underlyingType = prop.NonNullableType;

        // 如果底层类型是复杂类型
        if (prop.IsComplexType)
        {
            return GenerateComplexTypeDeserialization(prop, bsonFieldName);
        }

        return underlyingType switch
        {
            "int" or "Int32" => $"if (document.TryGetValue(\"{bsonFieldName}\", out var bson{propertyName}) && bson{propertyName} is BsonInt32 int{propertyName}) entity.{propertyName} = int{propertyName}.Value; else entity.{propertyName} = null;",
            "long" or "Int64" => $"if (document.TryGetValue(\"{bsonFieldName}\", out var bson{propertyName}) && bson{propertyName} is BsonInt64 long{propertyName}) entity.{propertyName} = long{propertyName}.Value; else entity.{propertyName} = null;",
            "double" or "Double" => $"if (document.TryGetValue(\"{bsonFieldName}\", out var bson{propertyName}) && bson{propertyName} is BsonDouble dbl{propertyName}) entity.{propertyName} = dbl{propertyName}.Value; else entity.{propertyName} = null;",
            "bool" or "Boolean" => $"if (document.TryGetValue(\"{bsonFieldName}\", out var bson{propertyName}) && bson{propertyName} is BsonBoolean bool{propertyName}) entity.{propertyName} = bool{propertyName}.Value; else entity.{propertyName} = null;",
            "DateTime" => $"if (document.TryGetValue(\"{bsonFieldName}\", out var bson{propertyName}) && bson{propertyName} is BsonDateTime dt{propertyName}) entity.{propertyName} = dt{propertyName}.Value; else entity.{propertyName} = null;",
            "Guid" => $"if (document.TryGetValue(\"{bsonFieldName}\", out var bson{propertyName}) && bson{propertyName} is BsonBinary guid{propertyName}) entity.{propertyName} = new Guid(guid{propertyName}.Bytes); else entity.{propertyName} = null;",
            "ObjectId" => $"if (document.TryGetValue(\"{bsonFieldName}\", out var bson{propertyName}) && bson{propertyName} is BsonObjectId oid{propertyName}) entity.{propertyName} = oid{propertyName}.Value; else entity.{propertyName} = null;",
            _ => $"if (document.TryGetValue(\"{bsonFieldName}\", out var bson{propertyName}) && !bson{propertyName}.IsNull) entity.{propertyName} = ConvertFromBsonValue<{underlyingType}>(bson{propertyName}); else entity.{propertyName} = null;"
        };
    }

    /// <summary>
    /// 生成复杂类型属性的反序列化代码
    /// </summary>
    private static string GenerateComplexTypeDeserialization(PropertyInfo prop, string bsonFieldName)
    {
        var propertyName = prop.Name;
        var propertyType = prop.FullyQualifiedNonNullableType;
        var isNullable = prop.IsNullableReferenceType || prop.Type.EndsWith("?");
        
        var sb = new StringBuilder();
        sb.AppendLine($@"if (document.TryGetValue(""{bsonFieldName}"", out var bson{propertyName}))
            {{
                if (bson{propertyName}.IsNull)
                {{");
        
        if (isNullable)
        {
            sb.AppendLine($"                    entity.{propertyName} = null;");
        }
        
        sb.AppendLine($@"                }}
                else if (bson{propertyName} is BsonDocument nested{propertyName})
                {{
                    entity.{propertyName} = DeserializeComplexObject<{propertyType}>(nested{propertyName});
                }}
            }}");
        
        return sb.ToString();
    }

    /// <summary>
    /// 生成包含复杂类型元素的集合的反序列化代码
    /// </summary>
    private static string GenerateCollectionWithComplexElementDeserialization(PropertyInfo prop, string bsonFieldName)
    {
        var propertyName = prop.Name;
        var elementType = prop.ElementType ?? "object";
        var isArray = prop.IsArray;
        var isNullable = prop.IsNullableReferenceType || prop.Type.EndsWith("?");
        
        var sb = new StringBuilder();
        sb.AppendLine($@"if (document.TryGetValue(""{bsonFieldName}"", out var bson{propertyName}))
            {{
                if (bson{propertyName}.IsNull)
                {{");
        
        if (isNullable)
        {
            sb.AppendLine($"                    entity.{propertyName} = null;");
        }
        
        sb.AppendLine($@"                }}
                else if (bson{propertyName} is BsonArray array{propertyName})
                {{
                    var list_{propertyName} = new System.Collections.Generic.List<{elementType}>();
                    foreach (var item in array{propertyName})
                    {{
                        if (item.IsNull)
                            list_{propertyName}.Add(default!);
                        else if (item is BsonDocument itemDoc)
                            list_{propertyName}.Add(DeserializeComplexObject<{elementType}>(itemDoc));
                    }}");
        
        if (isArray)
        {
            sb.AppendLine($"                    entity.{propertyName} = list_{propertyName}.ToArray();");
        }
        else
        {
            sb.AppendLine($"                    entity.{propertyName} = list_{propertyName};");
        }
        
        sb.AppendLine(@"                }
            }");
        
        return sb.ToString();
    }

    /// <summary>
    /// 生成包含复杂类型值的字典的反序列化代码
    /// </summary>
    private static string GenerateDictionaryWithComplexValueDeserialization(PropertyInfo prop, string bsonFieldName)
    {
        var propertyName = prop.Name;
        var keyType = prop.DictionaryKeyType ?? "string";
        var valueType = prop.DictionaryValueType ?? "object";
        var isNullable = prop.IsNullableReferenceType || prop.Type.EndsWith("?");
        
        var sb = new StringBuilder();
        sb.AppendLine($@"if (document.TryGetValue(""{bsonFieldName}"", out var bson{propertyName}))
            {{
                if (bson{propertyName}.IsNull)
                {{");
        
        if (isNullable)
        {
            sb.AppendLine($"                    entity.{propertyName} = null;");
        }
        
        sb.AppendLine($@"                }}
                else if (bson{propertyName} is BsonDocument dict{propertyName})
                {{
                    var result_{propertyName} = new System.Collections.Generic.Dictionary<{keyType}, {valueType}>();
                    foreach (var kvp in dict{propertyName})
                    {{
                        if (kvp.Value.IsNull)
                            result_{propertyName}[kvp.Key] = default!;
                        else if (kvp.Value is BsonDocument valueDoc)
                            result_{propertyName}[kvp.Key] = DeserializeComplexObject<{valueType}>(valueDoc);
                    }}
                    entity.{propertyName} = result_{propertyName};
                }}
            }}");
        
        return sb.ToString();
    }
}
