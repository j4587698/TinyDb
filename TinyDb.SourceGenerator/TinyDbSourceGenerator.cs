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
    private const string EntityAttributeMetadataName = "TinyDb.Attributes.EntityAttribute";

    /// <summary>
    /// BsonRef 引用类型缺少 Entity 特性的错误诊断描述符
    /// </summary>
    private static readonly DiagnosticDescriptor BsonRefMissingEntityErrorDescriptor = new(
        id: "TINYDB001",
        title: "BsonRef referenced type must have [Entity] attribute",
        messageFormat: "Property '{0}' in type '{1}' uses [BsonRef] but the referenced type '{2}' does not have [Entity] attribute. Add [Entity] attribute to '{2}' or remove [BsonRef] from this property.",
        category: "TinyDb.SourceGenerator",
        DiagnosticSeverity.Error,
        isEnabledByDefault: true,
        description: "Types referenced by [BsonRef] must have [Entity] attribute to ensure AOT compatibility. The Entity attribute causes the source generator to create AOT-compatible serialization code for the type.");

    /// <summary>
    /// 循环引用警告的诊断描述符（非Entity类型）
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
    /// Entity类型间循环引用警告的诊断描述符
    /// </summary>
    private static readonly DiagnosticDescriptor EntityCircularReferenceWarningDescriptor = new(
        id: "TINYDB003",
        title: "Circular reference detected between Entity types",
        messageFormat: "Circular reference detected between Entity types in '{0}': {1}. While this is handled at runtime, it may cause performance overhead and incomplete data during serialization.",
        category: "TinyDb.SourceGenerator",
        DiagnosticSeverity.Warning,
        isEnabledByDefault: true,
        description: "A circular reference was detected between Entity types. The runtime will handle this by breaking the cycle during serialization (returning only the ID for circular references), but this may cause performance overhead due to circular reference tracking. Consider redesigning the data model to avoid circular dependencies between entities.");

    /// <summary>
    /// 初始化生成器
    /// </summary>
    public void Initialize(IncrementalGeneratorInitializationContext context)
    {
        // 查找带有 [Entity] 特性的类声明
        var classDeclarations = context.SyntaxProvider
            .ForAttributeWithMetadataName(
                EntityAttributeMetadataName,
                predicate: static (s, _) => s is ClassDeclarationSyntax || s is StructDeclarationSyntax || s is RecordDeclarationSyntax,
                transform: static (ctx, _) => GetClassInfo(ctx))
            .Where(static m => m is not null)
            .WithComparer(ClassInfoComparer.Instance);

        // 注册源代码生成
        var validClassDeclarations = classDeclarations
            .Where(static classInfo => classInfo is not null && ShouldGenerateMapper(classInfo));

        context.RegisterSourceOutput(validClassDeclarations, static (spc, classInfo) =>
        {
            if (classInfo == null) return;

            var partialClassCode = GeneratePartialClass(classInfo);
            var partialFileName = $"{classInfo.UniqueFileName}_AotHelper.g.cs";
            spc.AddSource(partialFileName, SourceText.From(partialClassCode, Encoding.UTF8));
        });

        context.RegisterSourceOutput(validClassDeclarations.Collect(), static (spc, classes) =>
        {
            if (classes.IsDefaultOrEmpty) return;

            var validClasses = new List<ClassInfo>(classes.Length);

            foreach (var classInfo in classes)
            {
                if (classInfo == null) continue;

                // 嵌套类现在已支持 - 因为复杂类型的 AOT 优化机制同样适用于嵌套的 Entity 类
                // 生成的帮助器类使用唯一的类名（如 OuterClass_InnerClassAotHelper）
                // 并通过 TypeReference（如 OuterClass.InnerClass）正确引用嵌套类型

                if (ShouldGenerateMapper(classInfo))
                {
                    validClasses.Add(classInfo);
                }
            }

            if (validClasses.Count == 0) return;

            foreach (var classInfo in validClasses)
            {
                // 报告非Entity类型的循环引用警告
                foreach (var circularRef in classInfo.CircularReferences)
                {
                    var diagnostic = Diagnostic.Create(
                        CircularReferenceWarningDescriptor,
                        classInfo.Location.ToLocation(),
                        circularRef.ContainingTypeName,
                        circularRef.CycleChain);
                    spc.ReportDiagnostic(diagnostic);
                }
                
                // 报告Entity类型间的循环引用警告
                foreach (var entityCircularRef in classInfo.EntityCircularReferences)
                {
                    var diagnostic = Diagnostic.Create(
                        EntityCircularReferenceWarningDescriptor,
                        classInfo.Location.ToLocation(),
                        entityCircularRef.CurrentEntityName,
                        entityCircularRef.CycleChain);
                    spc.ReportDiagnostic(diagnostic);
                }
                
                // 报告 BsonRef 引用类型缺少 Entity 特性的错误
                foreach (var bsonRefError in classInfo.BsonRefMissingEntityErrors)
                {
                    var diagnostic = Diagnostic.Create(
                        BsonRefMissingEntityErrorDescriptor,
                        bsonRefError.Location.ToLocation() ?? classInfo.Location.ToLocation(),
                        bsonRefError.PropertyName,
                        bsonRefError.ContainingTypeName,
                        bsonRefError.ReferencedTypeName);
                    spc.ReportDiagnostic(diagnostic);
                }
                
                // 使用UniqueFileName来保证文件名唯一
            }

            var registrySource = GenerateRegistrySource(validClasses);
            spc.AddSource("AotHelperRegistry.g.cs", SourceText.From(registrySource, Encoding.UTF8));
        });
    }

    /// <summary>
    /// 获取类信息
    /// </summary>
    private static ClassInfo? GetClassInfo(GeneratorAttributeSyntaxContext context)
    {
        var classDeclaration = (TypeDeclarationSyntax)context.TargetNode;
        var semanticModel = context.SemanticModel;

        // 获取类的完整名称
        var namespaceDeclaration = classDeclaration.FirstAncestorOrSelf<NamespaceDeclarationSyntax>();
        var namespaceName = namespaceDeclaration?.Name.ToString() ?? string.Empty;
        var className = classDeclaration.Identifier.Text;

        // 获取类符号信息
        var classSymbol = context.TargetSymbol as INamedTypeSymbol
            ?? semanticModel.GetDeclaredSymbol(classDeclaration);

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

        // 检查是否是值类型
        var isValueType = classSymbol?.IsValueType ?? false;
        var runtimeFullName = classSymbol != null ? GetRuntimeFullName(classSymbol) : null;

        // 获取Entity属性信息
        var entityAttribute = context.Attributes.FirstOrDefault();

        // 优先从构造函数参数获取Name，否则从命名参数获取
        var collectionName = entityAttribute?.ConstructorArguments.FirstOrDefault().Value?.ToString();
        if (string.IsNullOrEmpty(collectionName))
        {
            collectionName = entityAttribute?.NamedArguments
                .FirstOrDefault(arg => arg.Key == "Name").Value.Value?.ToString();
        }

        // 获取Entity属性中指定的IdProperty名称
        var specifiedIdProperty = entityAttribute?.ConstructorArguments.Length > 1
            ? entityAttribute.ConstructorArguments[1].Value?.ToString()
            : null;
        specifiedIdProperty ??= entityAttribute?.NamedArguments
            .FirstOrDefault(arg => arg.Key == "IdProperty").Value.Value?.ToString();

        // 获取属性信息
        var entityDisplayName = GetEntityMetadataDisplayName(classSymbol, className);
        var entityDescription = GetEntityMetadataDescription(classSymbol);

        var properties = new List<PropertyInfo>();
        PropertyInfo? idProperty = null;
        // 收集所有属性的类型符号，用于后续分析依赖类型
        var typeSymbolMap = new Dictionary<string, ITypeSymbol>();
        // 收集 BsonRef 引用类型缺少 Entity 特性的错误
        var bsonRefMissingEntityErrors = new List<BsonRefMissingEntityInfo>();

        foreach (var member in classDeclaration.Members)
        {
            if (member is PropertyDeclarationSyntax property)
            {
                var propertyName = property.Identifier.Text;
                var propertyType = property.Type.ToString();

                // 检查是否有BsonIgnore属性
                var propertySymbol = semanticModel.GetDeclaredSymbol(property) as IPropertySymbol;

                // 跳过私有属性
                if (propertySymbol == null)
                {
                    continue;
                }

                // AOT 源生成是硬编码访问，不做反射回退，因此仅处理可直接访问的属性。
                if (propertySymbol.DeclaredAccessibility != Accessibility.Public ||
                    propertySymbol.IsStatic ||
                    propertySymbol.IsIndexer ||
                    propertySymbol.GetMethod == null ||
                    propertySymbol.SetMethod == null ||
                    propertySymbol.SetMethod.DeclaredAccessibility != Accessibility.Public ||
                    propertySymbol.SetMethod.IsInitOnly)
                {
                    continue;
                }

                var hasIgnoreAttribute = propertySymbol.GetAttributes()
                    .Any(attr => attr.AttributeClass?.Name == "BsonIgnoreAttribute" || attr.AttributeClass?.Name == "BsonIgnore");

                // 检查是否有 [BsonRef] 特性
                var bsonRefAttribute = propertySymbol.GetAttributes()
                    .FirstOrDefault(attr => attr.AttributeClass?.Name == "BsonRefAttribute");
                var bsonRefCollectionName = bsonRefAttribute?.ConstructorArguments.FirstOrDefault().Value?.ToString();

                var foreignKeyAttribute = propertySymbol.GetAttributes()
                    .FirstOrDefault(attr => attr.AttributeClass?.Name == "ForeignKeyAttribute" ||
                                            attr.AttributeClass?.Name == "ForeignKey");
                var foreignKeyCollectionName = foreignKeyAttribute?.ConstructorArguments.FirstOrDefault().Value?.ToString();
                var propertyMetadataAttribute = GetPropertyMetadataAttribute(propertySymbol);
                
                // 如果有 BsonRef 特性，验证引用类型是否有 Entity 特性
                if (!string.IsNullOrEmpty(bsonRefCollectionName))
                {
                    var refTypeSymbol = GetBsonRefTargetType(propertySymbol.Type);
                    if (refTypeSymbol != null)
                    {
                        var hasEntityAttr = refTypeSymbol.GetAttributes()
                            .Any(attr => attr.AttributeClass?.Name == "EntityAttribute");
                        if (!hasEntityAttr)
                        {
                            bsonRefMissingEntityErrors.Add(new BsonRefMissingEntityInfo(
                                propertyName,
                                className,
                                refTypeSymbol.ToDisplayString(SymbolDisplayFormat.MinimallyQualifiedFormat),
                                DiagnosticLocationInfo.From(property.GetLocation())));
                        }
                    }
                }

                var typeSymbol = propertySymbol?.Type;
                var propIsValueType = typeSymbol?.IsValueType ?? false;
                var isNullableValueType = typeSymbol is INamedTypeSymbol { IsGenericType: true } namedType &&
                                           namedType.OriginalDefinition.SpecialType == SpecialType.System_Nullable_T;
                var isNullableReferenceType = typeSymbol is { IsReferenceType: true } &&
                                              propertySymbol?.NullableAnnotation == NullableAnnotation.Annotated;
                var isEnumType = typeSymbol?.TypeKind == TypeKind.Enum;

                var fullyQualifiedType = typeSymbol?.ToDisplayString(FullyQualifiedNullableDisplayFormat) ?? propertyType;
                var metadataTypeName = typeSymbol != null ? GetStableMetadataTypeName(typeSymbol) : fullyQualifiedType.Replace("global::", "");
                var nonNullableType = propertyType;
                var fullyQualifiedNonNullableType = fullyQualifiedType;

                if (isNullableValueType && typeSymbol is INamedTypeSymbol nullableType && nullableType.TypeArguments.Length == 1)
                {
                    var underlyingType = nullableType.TypeArguments[0];
                    isEnumType = underlyingType.TypeKind == TypeKind.Enum;
                    nonNullableType = underlyingType.ToDisplayString(SymbolDisplayFormat.MinimallyQualifiedFormat);
                    fullyQualifiedNonNullableType = underlyingType.ToDisplayString(FullyQualifiedNullableDisplayFormat);
                    
                    // 记录底层类型符号
                    if (!typeSymbolMap.ContainsKey(fullyQualifiedNonNullableType))
                    {
                        typeSymbolMap[fullyQualifiedNonNullableType] = underlyingType;
                    }
                }
                else if (!propIsValueType && propertyType.EndsWith("?", StringComparison.Ordinal))
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

                var idGenerationInfo = GetIdGenerationInfo(propertySymbol);
                var propInfo = new PropertyInfo(
                    propertyName,
                    propertyType,
                    isId: false,
                    isIgnored: hasIgnoreAttribute,
                    hasIgnoreAttribute: hasIgnoreAttribute,
                    isValueType: propIsValueType,
                    isNullableValueType: isNullableValueType,
                    isNullableReferenceType: isNullableReferenceType,
                    isEnum: isEnumType,
                    nonNullableType: nonNullableType,
                    fullyQualifiedType: fullyQualifiedType,
                    fullyQualifiedNonNullableType: fullyQualifiedNonNullableType,
                    metadataTypeName: metadataTypeName,
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
                    isDictionaryValueValueType: typeAnalysis.IsDictionaryValueValueType,
                    bsonRefCollectionName: bsonRefCollectionName,
                    foreignKeyCollectionName: foreignKeyCollectionName,
                    idGenerationStrategyValue: idGenerationInfo.StrategyValue,
                    idGenerationSequenceName: idGenerationInfo.SequenceName,
                    displayName: GetConstructorString(propertyMetadataAttribute, 0) ?? propertyName,
                    description: GetNamedString(propertyMetadataAttribute, "Description"),
                    order: GetNamedInt(propertyMetadataAttribute, "Order"),
                    required: GetNamedBool(propertyMetadataAttribute, "Required"));
                properties.Add(propInfo);
            }
            // 处理公共字段 (FieldDeclarationSyntax)
            else if (member is FieldDeclarationSyntax field)
            {
                // 跳过静态字段
                if (field.Modifiers.Any(m => m.IsKind(SyntaxKind.ConstKeyword)))
                {
                    continue;
                }

                if (field.Modifiers.Any(m => m.IsKind(SyntaxKind.StaticKeyword)))
                {
                    continue;
                }
                
                // 只处理公共字段
                if (!field.Modifiers.Any(m => m.IsKind(SyntaxKind.PublicKeyword)))
                {
                    continue;
                }

                foreach (var variable in field.Declaration.Variables)
                {
                    var fieldName = variable.Identifier.Text;
                    var fieldType = field.Declaration.Type.ToString();

                    // 获取字段符号
                    var fieldSymbol = semanticModel.GetDeclaredSymbol(variable) as IFieldSymbol;

                    // 检查是否有 BsonIgnore 属性
                    var hasIgnoreAttribute = fieldSymbol?.GetAttributes()
                        .Any(attr => attr.AttributeClass?.Name == "BsonIgnoreAttribute" || attr.AttributeClass?.Name == "BsonIgnore") ?? false;

                    // 检查是否有 [BsonRef] 特性
                    var bsonRefAttribute = fieldSymbol?.GetAttributes()
                        .FirstOrDefault(attr => attr.AttributeClass?.Name == "BsonRefAttribute");
                    var bsonRefCollectionName = bsonRefAttribute?.ConstructorArguments.FirstOrDefault().Value?.ToString();

                    var typeSymbol = fieldSymbol?.Type;
                    var propIsValueType = typeSymbol?.IsValueType ?? false;
                    var isNullableValueType = typeSymbol is INamedTypeSymbol { IsGenericType: true } namedType &&
                                               namedType.OriginalDefinition.SpecialType == SpecialType.System_Nullable_T;
                    var isNullableReferenceType = typeSymbol is { IsReferenceType: true } &&
                                                  fieldSymbol?.NullableAnnotation == NullableAnnotation.Annotated;
                    var isEnumType = typeSymbol?.TypeKind == TypeKind.Enum;

                    var fullyQualifiedType = typeSymbol?.ToDisplayString(FullyQualifiedNullableDisplayFormat) ?? fieldType;
                    var metadataTypeName = typeSymbol != null ? GetStableMetadataTypeName(typeSymbol) : fullyQualifiedType.Replace("global::", "");
                    var nonNullableType = fieldType;
                    var fullyQualifiedNonNullableType = fullyQualifiedType;

                    if (isNullableValueType && typeSymbol is INamedTypeSymbol nullableType && nullableType.TypeArguments.Length == 1)
                    {
                        var underlyingType = nullableType.TypeArguments[0];
                        isEnumType = underlyingType.TypeKind == TypeKind.Enum;
                        nonNullableType = underlyingType.ToDisplayString(SymbolDisplayFormat.MinimallyQualifiedFormat);
                        fullyQualifiedNonNullableType = underlyingType.ToDisplayString(FullyQualifiedNullableDisplayFormat);
                        
                        if (!typeSymbolMap.ContainsKey(fullyQualifiedNonNullableType))
                        {
                            typeSymbolMap[fullyQualifiedNonNullableType] = underlyingType;
                        }
                    }
                    else if (!propIsValueType && fieldType.EndsWith("?", StringComparison.Ordinal))
                    {
                        nonNullableType = fieldType.TrimEnd('?').Trim();
                        fullyQualifiedNonNullableType = fullyQualifiedType.TrimEnd('?').Trim();
                    }

                    // 分析类型的复杂性
                    var typeAnalysis = AnalyzePropertyType(typeSymbol);
                    
                    // 收集类型符号用于依赖分析
                    if (typeSymbol != null && !typeSymbolMap.ContainsKey(fullyQualifiedNonNullableType))
                    {
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
                        fieldName,
                        fieldType,
                        isId: false,
                        isIgnored: hasIgnoreAttribute,
                        hasIgnoreAttribute: hasIgnoreAttribute,
                        isValueType: propIsValueType,
                        isNullableValueType: isNullableValueType,
                        isNullableReferenceType: isNullableReferenceType,
                        isEnum: isEnumType,
                        nonNullableType: nonNullableType,
                        fullyQualifiedType: fullyQualifiedType,
                        fullyQualifiedNonNullableType: fullyQualifiedNonNullableType,
                        metadataTypeName: metadataTypeName,
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
                        isDictionaryValueValueType: typeAnalysis.IsDictionaryValueValueType,
                        bsonRefCollectionName: bsonRefCollectionName);
                    properties.Add(propInfo);
                }
            }
        }

        AddMissingSymbolProperties(classSymbol, className, properties, typeSymbolMap, bsonRefMissingEntityErrors);
        var constructorParameters = AddConstructorBoundProperties(classSymbol, className, properties, typeSymbolMap, bsonRefMissingEntityErrors);

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
        if (idProperty == null)
        {
            // 2. 自动查找 [Id] 标记属性（包含继承链）
            var idAttributePropertyName = FindIdPropertyName(classSymbol);
            idProperty = idAttributePropertyName == null
                ? null
                : properties.FirstOrDefault(p => p.Name == idAttributePropertyName);
            if (idProperty != null)
            {
                idProperty.IsId = true;
            }

            if (idProperty == null)
            {
                var standardIdNames = new[] { "Id", "_id", "ID" };
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
        }

        // 3. 如果还是没有找到，检查是否有[Id]属性标记
        // 收集依赖的非Entity复杂类型
        var (dependentComplexTypes, circularReferences) = CollectDependentComplexTypes(properties, typeSymbolMap);
        
        // 收集Entity类型间的循环引用（检测属性类型中引用了有[Entity]特性的类型）
        var entityCircularReferences = DetectEntityCircularReferences(classSymbol, properties, typeSymbolMap);

        return new ClassInfo(namespaceName, className, isValueType, properties, idProperty, collectionName, entityDisplayName, entityDescription, containingTypePath, DiagnosticLocationInfo.From(classDeclaration.GetLocation()), dependentComplexTypes, circularReferences, entityCircularReferences, bsonRefMissingEntityErrors, constructorParameters, runtimeFullName);
    }

    private static string GetRuntimeFullName(INamedTypeSymbol typeSymbol)
    {
        var names = new Stack<string>();
        var current = typeSymbol;
        while (current != null)
        {
            names.Push(current.MetadataName);
            current = current.ContainingType;
        }

        var typePath = string.Join("+", names);
        var namespaceName = typeSymbol.ContainingNamespace is { IsGlobalNamespace: false } ns
            ? ns.ToDisplayString()
            : string.Empty;

        return string.IsNullOrEmpty(namespaceName) ? typePath : $"{namespaceName}.{typePath}";
    }

    private static string GetStableMetadataTypeName(ITypeSymbol typeSymbol)
    {
        if (typeSymbol is IArrayTypeSymbol arrayType)
        {
            return $"{GetStableMetadataTypeName(arrayType.ElementType)}[]";
        }

        if (typeSymbol is ITypeParameterSymbol typeParameter)
        {
            return typeParameter.Name;
        }

        if (typeSymbol is not INamedTypeSymbol namedType)
        {
            return typeSymbol.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat).Replace("global::", "");
        }

        if (!namedType.IsGenericType)
        {
            return GetRuntimeFullName(namedType);
        }

        var definitionName = GetRuntimeFullName(namedType.ConstructedFrom);
        var tickIndex = definitionName.IndexOf('`');
        if (tickIndex >= 0)
        {
            definitionName = definitionName.Substring(0, tickIndex);
        }

        var typeArguments = namedType.TypeArguments.Select(GetStableMetadataTypeName);
        return $"{definitionName}<{string.Join(",", typeArguments)}>";
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
            
            // 已移除TinyDb核心命名空间的排除逻辑，以便 MetadataDocument 等实体能正常生成
        }

        // 排除编译生成的类
        if (classInfo.Name.StartsWith("<") || classInfo.Name.Contains("Anonymous"))
        {
            return false;
        }

        // 只处理有属性的实体类（ID将通过智能识别获得）
        return classInfo.Properties.Count > 0;
    }

    private static void AddMissingSymbolProperties(
        INamedTypeSymbol? classSymbol,
        string containingTypeName,
        List<PropertyInfo> properties,
        Dictionary<string, ITypeSymbol> typeSymbolMap,
        List<BsonRefMissingEntityInfo> bsonRefMissingEntityErrors)
    {
        if (classSymbol == null)
        {
            return;
        }

        var existingNames = new HashSet<string>(properties.Select(static p => p.Name), StringComparer.Ordinal);
        foreach (var typeSymbol in EnumerateTypeAndBaseTypes(classSymbol))
        {
            foreach (var member in typeSymbol.GetMembers())
            {
                if (member is not IPropertySymbol propertySymbol ||
                    !existingNames.Add(propertySymbol.Name))
                {
                    continue;
                }

                if (TryCreatePropertyInfoFromSymbol(
                        propertySymbol,
                        containingTypeName,
                        typeSymbolMap,
                        bsonRefMissingEntityErrors,
                        requirePublicMutableSetter: true,
                        out var propertyInfo))
                {
                    properties.Add(propertyInfo!);
                }
            }
        }
    }

    private static List<ConstructorParameterInfo> AddConstructorBoundProperties(
        INamedTypeSymbol? classSymbol,
        string containingTypeName,
        List<PropertyInfo> properties,
        Dictionary<string, ITypeSymbol> typeSymbolMap,
        List<BsonRefMissingEntityInfo> bsonRefMissingEntityErrors)
    {
        if (classSymbol == null)
        {
            return new List<ConstructorParameterInfo>();
        }

        var propertySymbols = EnumerateTypeAndBaseTypes(classSymbol)
            .SelectMany(static type => type.GetMembers().OfType<IPropertySymbol>())
            .Where(static property => property.DeclaredAccessibility == Accessibility.Public &&
                                      !property.IsStatic &&
                                      !property.IsIndexer &&
                                      property.GetMethod != null)
            .GroupBy(static property => property.Name, StringComparer.Ordinal)
            .ToDictionary(static group => group.Key, static group => group.First(), StringComparer.Ordinal);

        foreach (var constructor in classSymbol.InstanceConstructors
                     .Where(static ctor => ctor.DeclaredAccessibility == Accessibility.Public && ctor.Parameters.Length > 0)
                     .OrderByDescending(static ctor => ctor.Parameters.Length))
        {
            var parameters = new List<ConstructorParameterInfo>(constructor.Parameters.Length);
            var matchedAll = true;

            foreach (var parameter in constructor.Parameters)
            {
                if (!TryFindConstructorProperty(parameter, propertySymbols, out var propertySymbol))
                {
                    matchedAll = false;
                    break;
                }

                var propertyInfo = properties.FirstOrDefault(p => string.Equals(p.Name, propertySymbol.Name, StringComparison.Ordinal));
                if (propertyInfo == null)
                {
                    if (!TryCreatePropertyInfoFromSymbol(
                            propertySymbol,
                            containingTypeName,
                            typeSymbolMap,
                            bsonRefMissingEntityErrors,
                            requirePublicMutableSetter: false,
                            out propertyInfo))
                    {
                        matchedAll = false;
                        break;
                    }

                    var constructorPropertyInfo = propertyInfo!;
                    properties.Add(constructorPropertyInfo);
                    propertyInfo = constructorPropertyInfo;
                }

                if (propertyInfo.HasIgnoreAttribute)
                {
                    matchedAll = false;
                    break;
                }

                parameters.Add(new ConstructorParameterInfo(parameter.Name, propertyInfo));
            }

            if (matchedAll)
            {
                return parameters;
            }
        }

        return new List<ConstructorParameterInfo>();
    }

    private static bool TryFindConstructorProperty(
        IParameterSymbol parameter,
        IReadOnlyDictionary<string, IPropertySymbol> propertySymbols,
        out IPropertySymbol propertySymbol)
    {
        if (propertySymbols.TryGetValue(parameter.Name, out propertySymbol!) &&
            SymbolEqualityComparer.Default.Equals(propertySymbol.Type, parameter.Type))
        {
            return true;
        }

        foreach (var candidate in propertySymbols.Values)
        {
            if (string.Equals(candidate.Name, parameter.Name, StringComparison.OrdinalIgnoreCase) &&
                SymbolEqualityComparer.Default.Equals(candidate.Type, parameter.Type))
            {
                propertySymbol = candidate;
                return true;
            }
        }

        propertySymbol = null!;
        return false;
    }

    private static IEnumerable<INamedTypeSymbol> EnumerateTypeAndBaseTypes(INamedTypeSymbol typeSymbol)
    {
        for (var current = typeSymbol; current != null && current.SpecialType != SpecialType.System_Object; current = current.BaseType)
        {
            yield return current;
        }
    }

    private static bool TryCreatePropertyInfoFromSymbol(
        IPropertySymbol propertySymbol,
        string containingTypeName,
        Dictionary<string, ITypeSymbol> typeSymbolMap,
        List<BsonRefMissingEntityInfo> bsonRefMissingEntityErrors,
        bool requirePublicMutableSetter,
        out PropertyInfo? propertyInfo)
    {
        propertyInfo = null;
        var canSet = propertySymbol.SetMethod is { DeclaredAccessibility: Accessibility.Public, IsInitOnly: false };
        if (propertySymbol.DeclaredAccessibility != Accessibility.Public ||
            propertySymbol.IsStatic ||
            propertySymbol.IsIndexer ||
            propertySymbol.GetMethod == null ||
            (requirePublicMutableSetter && !canSet))
        {
            return false;
        }

        var attributes = propertySymbol.GetAttributes();
        var hasIgnoreAttribute = attributes
            .Any(attr => attr.AttributeClass?.Name == "BsonIgnoreAttribute" || attr.AttributeClass?.Name == "BsonIgnore");

        var bsonRefAttribute = attributes
            .FirstOrDefault(attr => attr.AttributeClass?.Name == "BsonRefAttribute");
        var bsonRefCollectionName = bsonRefAttribute?.ConstructorArguments.FirstOrDefault().Value?.ToString();

        var foreignKeyAttribute = attributes
            .FirstOrDefault(attr => attr.AttributeClass?.Name == "ForeignKeyAttribute" ||
                                    attr.AttributeClass?.Name == "ForeignKey");
        var foreignKeyCollectionName = foreignKeyAttribute?.ConstructorArguments.FirstOrDefault().Value?.ToString();
        var propertyMetadataAttribute = GetPropertyMetadataAttribute(propertySymbol);

        var typeSymbol = propertySymbol.Type;
        if (!string.IsNullOrEmpty(bsonRefCollectionName))
        {
            var refTypeSymbol = GetBsonRefTargetType(typeSymbol);
            if (refTypeSymbol != null &&
                !refTypeSymbol.GetAttributes().Any(attr => attr.AttributeClass?.Name == "EntityAttribute"))
            {
                bsonRefMissingEntityErrors.Add(new BsonRefMissingEntityInfo(
                    propertySymbol.Name,
                    containingTypeName,
                    refTypeSymbol.ToDisplayString(SymbolDisplayFormat.MinimallyQualifiedFormat),
                    DiagnosticLocationInfo.From(propertySymbol.Locations.FirstOrDefault())));
            }
        }

        var propertyType = typeSymbol.ToDisplayString(FullyQualifiedNullableDisplayFormat);
        var propIsValueType = typeSymbol.IsValueType;
        var isNullableValueType = typeSymbol is INamedTypeSymbol { IsGenericType: true } namedType &&
                                  namedType.OriginalDefinition.SpecialType == SpecialType.System_Nullable_T;
        var isNullableReferenceType = typeSymbol is { IsReferenceType: true } &&
                                      propertySymbol.NullableAnnotation == NullableAnnotation.Annotated;
        var isEnumType = typeSymbol.TypeKind == TypeKind.Enum;
        var nonNullableType = propertyType;
        var fullyQualifiedNonNullableType = propertyType;

        if (isNullableValueType && typeSymbol is INamedTypeSymbol nullableType && nullableType.TypeArguments.Length == 1)
        {
            var underlyingType = nullableType.TypeArguments[0];
            isEnumType = underlyingType.TypeKind == TypeKind.Enum;
            nonNullableType = underlyingType.ToDisplayString(FullyQualifiedNullableDisplayFormat);
            fullyQualifiedNonNullableType = nonNullableType;
            AddTypeSymbolIfMissing(typeSymbolMap, fullyQualifiedNonNullableType, underlyingType);
        }
        else if (!propIsValueType && propertyType.EndsWith("?", StringComparison.Ordinal))
        {
            nonNullableType = propertyType.TrimEnd('?').Trim();
            fullyQualifiedNonNullableType = nonNullableType;
        }

        var typeAnalysis = AnalyzePropertyType(typeSymbol);
        AddTypeSymbols(typeSymbolMap, typeSymbol, isNullableValueType, fullyQualifiedNonNullableType, typeAnalysis);

        var idGenerationInfo = GetIdGenerationInfo(propertySymbol);
        var metadataTypeName = GetStableMetadataTypeName(typeSymbol);
        propertyInfo = new PropertyInfo(
            propertySymbol.Name,
            propertyType,
            isId: false,
            isIgnored: hasIgnoreAttribute,
            hasIgnoreAttribute: hasIgnoreAttribute,
            isValueType: propIsValueType,
            isNullableValueType: isNullableValueType,
            isNullableReferenceType: isNullableReferenceType,
            isEnum: isEnumType,
            nonNullableType: nonNullableType,
            fullyQualifiedType: propertyType,
            fullyQualifiedNonNullableType: fullyQualifiedNonNullableType,
            metadataTypeName: metadataTypeName,
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
            isDictionaryValueValueType: typeAnalysis.IsDictionaryValueValueType,
            bsonRefCollectionName: bsonRefCollectionName,
            foreignKeyCollectionName: foreignKeyCollectionName,
            idGenerationStrategyValue: idGenerationInfo.StrategyValue,
            idGenerationSequenceName: idGenerationInfo.SequenceName,
            canSet: canSet,
            displayName: GetConstructorString(propertyMetadataAttribute, 0) ?? propertySymbol.Name,
            description: GetNamedString(propertyMetadataAttribute, "Description"),
            order: GetNamedInt(propertyMetadataAttribute, "Order"),
            required: GetNamedBool(propertyMetadataAttribute, "Required"));
        return true;
    }

    private static void AddTypeSymbols(
        Dictionary<string, ITypeSymbol> typeSymbolMap,
        ITypeSymbol typeSymbol,
        bool isNullableValueType,
        string fullyQualifiedNonNullableType,
        TypeAnalysisResult typeAnalysis)
    {
        var actualTypeSymbol = typeSymbol;
        if (isNullableValueType && typeSymbol is INamedTypeSymbol nullableType && nullableType.TypeArguments.Length == 1)
        {
            actualTypeSymbol = nullableType.TypeArguments[0];
        }

        AddTypeSymbolIfMissing(typeSymbolMap, fullyQualifiedNonNullableType, actualTypeSymbol);

        if (typeAnalysis.IsCollection && !string.IsNullOrEmpty(typeAnalysis.ElementType))
        {
            var elementTypeSymbol = GetElementTypeSymbol(typeSymbol);
            if (elementTypeSymbol != null)
            {
                AddTypeSymbolIfMissing(typeSymbolMap, typeAnalysis.ElementType!, elementTypeSymbol);
            }
        }

        if (typeAnalysis.IsDictionary && !string.IsNullOrEmpty(typeAnalysis.DictionaryValueType))
        {
            var valueTypeSymbol = GetDictionaryValueTypeSymbol(typeSymbol);
            if (valueTypeSymbol != null)
            {
                AddTypeSymbolIfMissing(typeSymbolMap, typeAnalysis.DictionaryValueType!, valueTypeSymbol);
            }
        }
    }

    private static void AddTypeSymbolIfMissing(
        Dictionary<string, ITypeSymbol> typeSymbolMap,
        string typeName,
        ITypeSymbol typeSymbol)
    {
        if (!typeSymbolMap.ContainsKey(typeName))
        {
            typeSymbolMap[typeName] = typeSymbol;
        }
    }

    private static string? FindIdPropertyName(INamedTypeSymbol? classSymbol)
    {
        if (classSymbol == null)
        {
            return null;
        }

        foreach (var typeSymbol in EnumerateTypeAndBaseTypes(classSymbol))
        {
            foreach (var propertySymbol in typeSymbol.GetMembers().OfType<IPropertySymbol>())
            {
                if (propertySymbol.GetAttributes().Any(attr => attr.AttributeClass?.Name == "IdAttribute"))
                {
                    return propertySymbol.Name;
                }
            }
        }

        return null;
    }

    private static (int StrategyValue, string? SequenceName) GetIdGenerationInfo(ISymbol? symbol)
    {
        var attribute = symbol?.GetAttributes()
            .FirstOrDefault(attr => attr.AttributeClass?.Name == "IdGenerationAttribute");
        if (attribute == null)
        {
            return (0, null);
        }

        var strategyValue = 0;
        if (attribute.ConstructorArguments.Length > 0)
        {
            strategyValue = GetEnumConstantValue(attribute.ConstructorArguments[0]);
        }

        var namedStrategy = attribute.NamedArguments.FirstOrDefault(arg => arg.Key == "Strategy");
        if (namedStrategy.Value.Value != null)
        {
            strategyValue = GetEnumConstantValue(namedStrategy.Value);
        }

        string? sequenceName = null;
        if (attribute.ConstructorArguments.Length > 1)
        {
            sequenceName = attribute.ConstructorArguments[1].Value?.ToString();
        }

        var namedSequence = attribute.NamedArguments.FirstOrDefault(arg => arg.Key == "SequenceName");
        if (namedSequence.Value.Value != null)
        {
            sequenceName = namedSequence.Value.Value?.ToString();
        }

        return (strategyValue, sequenceName);
    }

    private static int GetEnumConstantValue(TypedConstant constant)
    {
        return constant.Value is int value ? value : 0;
    }

    private static string? GetConstructorString(AttributeData? attribute, int index)
    {
        if (attribute == null || attribute.ConstructorArguments.Length <= index) return null;
        return attribute.ConstructorArguments[index].Value?.ToString();
    }

    private static string? GetNamedString(AttributeData? attribute, string name)
    {
        if (attribute == null) return null;

        var argument = attribute.NamedArguments.FirstOrDefault(arg => arg.Key == name);
        return argument.Value.Value?.ToString();
    }

    private static int GetNamedInt(AttributeData? attribute, string name, int defaultValue = 0)
    {
        if (attribute == null) return defaultValue;

        var argument = attribute.NamedArguments.FirstOrDefault(arg => arg.Key == name);
        return argument.Value.Value is int value ? value : defaultValue;
    }

    private static bool GetNamedBool(AttributeData? attribute, string name, bool defaultValue = false)
    {
        if (attribute == null) return defaultValue;

        var argument = attribute.NamedArguments.FirstOrDefault(arg => arg.Key == name);
        return argument.Value.Value is bool value ? value : defaultValue;
    }

    private static string GetEntityMetadataDisplayName(INamedTypeSymbol? classSymbol, string fallback)
    {
        if (classSymbol == null) return fallback;

        foreach (var attribute in classSymbol.GetAttributes().Where(attr => attr.AttributeClass?.Name == "EntityMetadataAttribute"))
        {
            var displayName = GetConstructorString(attribute, 0) ?? GetNamedString(attribute, "DisplayName");
            if (!string.IsNullOrEmpty(displayName)) return displayName!;
        }

        return fallback;
    }

    private static string? GetEntityMetadataDescription(INamedTypeSymbol? classSymbol)
    {
        if (classSymbol == null) return null;

        foreach (var attribute in classSymbol.GetAttributes().Where(attr => attr.AttributeClass?.Name == "EntityMetadataAttribute"))
        {
            var description = GetNamedString(attribute, "Description");
            if (!string.IsNullOrEmpty(description)) return description;
        }

        return null;
    }

    private static AttributeData? GetPropertyMetadataAttribute(ISymbol symbol)
    {
        return symbol.GetAttributes()
            .FirstOrDefault(attr => attr.AttributeClass?.Name == "PropertyMetadataAttribute");
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
        sb.AppendLine("using System.Collections.Generic;");
        sb.AppendLine("using TinyDb.Bson;");
        sb.AppendLine("using TinyDb.Serialization;");
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
        sb.AppendLine($"    internal static class {classInfo.HelperClassName}");
        sb.AppendLine("    {");

        // 生成AOT兼容的ID访问方法
        if (classInfo.IdProperty != null)
        {
            var idProp = classInfo.IdProperty;
            var idAccess = idProp.AccessName;
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
            if (!classInfo.IsValueType)
            {
                sb.AppendLine($"            if (entity == null) return BsonNull.Value;");
            }
            sb.AppendLine($"            // 硬编码ID属性访问 - 避免AOT反射问题");

            if (isObjectId)
            {
                if (idProp.IsNullableValueType)
                {
                    sb.AppendLine($"            return entity.{idAccess}.HasValue ? new BsonObjectId(entity.{idAccess}.Value) : BsonNull.Value;");
                }
                else
                {
                    sb.AppendLine($"            return new BsonObjectId(entity.{idAccess});");
                }
            }
            else if (string.Equals(normalizedIdType, "string", StringComparison.OrdinalIgnoreCase))
            {
                sb.AppendLine($"            return new BsonString(entity.{idAccess} ?? \"\");");
            }
            else if (string.Equals(normalizedIdType, "int", StringComparison.OrdinalIgnoreCase))
            {
                sb.AppendLine($"            return new BsonInt32(entity.{idAccess});");
            }
            else if (string.Equals(normalizedIdType, "long", StringComparison.OrdinalIgnoreCase))
            {
                sb.AppendLine($"            return new BsonInt64(entity.{idAccess});");
            }
            else
            {
                // 对于其他类型，使用BsonConversion辅助方法
                sb.AppendLine($"            return TinyDb.Serialization.BsonConversion.ConvertToBsonValue(entity.{idAccess});");
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
            if (!classInfo.IsValueType)
            {
                sb.AppendLine($"            if (entity == null || id?.IsNull != false) return;");
            }
            else
            {
                sb.AppendLine($"            if (id?.IsNull != false) return;");
            }

            if (!idProp.CanSet)
            {
                sb.AppendLine("            return;");
            }
            else if (isObjectId)
            {
                sb.AppendLine("            if (id is BsonObjectId bsonObjectId)");
                sb.AppendLine($"                entity.{idAccess} = bsonObjectId.Value;");
                sb.AppendLine("            else if (id is BsonString bsonString && ObjectId.TryParse(bsonString.Value, out var parsedObjectId))");
                sb.AppendLine($"                entity.{idAccess} = parsedObjectId;");
            }
            else if (string.Equals(normalizedIdType, "string", StringComparison.OrdinalIgnoreCase))
            {
                sb.AppendLine("            if (id is BsonString bsonString)");
                sb.AppendLine($"                entity.{idAccess} = bsonString.Value ?? \"\";");
                sb.AppendLine("            else if (id != null && !id.IsNull)");
                sb.AppendLine($"                entity.{idAccess} = id.ToString();");
            }
            else if (string.Equals(normalizedIdType, "int", StringComparison.OrdinalIgnoreCase))
            {
                sb.AppendLine("            if (id is BsonInt32 bsonInt32)");
                sb.AppendLine($"                entity.{idAccess} = bsonInt32.Value;");
            }
            else if (string.Equals(normalizedIdType, "long", StringComparison.OrdinalIgnoreCase))
            {
                sb.AppendLine("            if (id is BsonInt64 bsonInt64)");
                sb.AppendLine($"                entity.{idAccess} = bsonInt64.Value;");
            }
            else if (string.Equals(normalizedIdType, "Guid", StringComparison.Ordinal))
            {
                sb.AppendLine("            if (id is BsonBinary bsonBinary)");
                sb.AppendLine($"                entity.{idAccess} = new Guid(bsonBinary.Bytes);");
                sb.AppendLine("            else if (id is BsonString bsonGuidString && Guid.TryParse(bsonGuidString.Value, out var parsedGuid))");
                sb.AppendLine($"                entity.{idAccess} = parsedGuid;");
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
            if (!classInfo.IsValueType)
            {
                sb.AppendLine($"            if (entity == null) return false;");
            }
            sb.AppendLine($"            // 硬编码ID属性验证 - 避免AOT反射问题");

            if (isObjectId)
            {
                sb.AppendLine($"            return entity.{idAccess} != ObjectId.Empty;");
            }
            else
            {
                if (string.Equals(normalizedIdType, "string", StringComparison.OrdinalIgnoreCase))
                {
                    sb.AppendLine($"            return !string.IsNullOrWhiteSpace(entity.{idAccess});");
                }
                else if (string.Equals(normalizedIdType, "Guid", StringComparison.Ordinal))
                {
                    if (idProp.IsNullableValueType)
                    {
                        sb.AppendLine($"            return entity.{idAccess}.HasValue && entity.{idAccess}.Value != Guid.Empty;");
                    }
                    else
                    {
                        sb.AppendLine($"            return entity.{idAccess} != Guid.Empty;");
                    }
                }
                else if (idProp.IsNullableValueType)
                {
                    sb.AppendLine($"            return entity.{idAccess}.HasValue && !System.Collections.Generic.EqualityComparer<{idProp.FullyQualifiedNonNullableType}>.Default.Equals(entity.{idAccess}.Value, default);");
                }
                else if (idProp.IsValueType)
                {
                    sb.AppendLine($"            return !System.Collections.Generic.EqualityComparer<{idProp.FullyQualifiedNonNullableType}>.Default.Equals(entity.{idAccess}, default);");
                }
                else
                {
                    sb.AppendLine($"            return entity.{idAccess} != null;");
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

        AppendIdAdapterMetadata(sb, classInfo);

        // 生成完整的序列化方法
        sb.AppendLine();
        sb.AppendLine("        /// <summary>");
        sb.AppendLine("        /// 将实体序列化为BSON文档（AOT兼容）");
        sb.AppendLine("        /// </summary>");
        sb.AppendLine("        /// <param name=\"entity\">实体实例</param>");
        sb.AppendLine("        /// <returns>BSON文档</returns>");
        sb.AppendLine($"        public static BsonDocument ToDocument({classInfo.TypeReference} entity)");
        sb.AppendLine("        {");
        if (!classInfo.IsValueType)
        {
            sb.AppendLine("            if (entity == null) throw new ArgumentNullException(nameof(entity));");
        }
        sb.AppendLine("            var documentBuilder = new BsonDocumentBuilder();");
        sb.AppendLine();

        // 为每个属性生成序列化代码
        var constructorPropertyNames = new HashSet<string>(
            classInfo.ConstructorParameters.Select(static p => p.Property.Name),
            StringComparer.Ordinal);
        foreach (var prop in classInfo.Properties.Where(p => !p.HasIgnoreAttribute))
        {
            var bsonCode = RewriteDocumentSetToBuilder(SourceGeneratorHelpers.GeneratePropertySerialization(prop));
            sb.AppendLine($"            // 序列化属性: {prop.Name}");
            sb.AppendLine(bsonCode);
            sb.AppendLine();
        }

        // 确保包含集合名称字段
        sb.AppendLine("            // 确保包含集合名称字段");
        sb.AppendLine("            if (!documentBuilder.ContainsKey(\"_collection\"))");
        sb.AppendLine("            {");
        sb.AppendLine($"                documentBuilder.Set(\"_collection\", {ToCSharpStringLiteral(classInfo.CollectionName ?? classInfo.Name)});");
        sb.AppendLine("            }");

        sb.AppendLine("            return documentBuilder.Build();");
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
        if (classInfo.ConstructorParameters.Count > 0)
        {
            foreach (var parameter in classInfo.ConstructorParameters)
            {
                var prop = parameter.Property;
                var bsonFieldName = GetBsonFieldName(prop);
                var localName = $"ctor_{prop.Name}";
                var bsonLocalName = $"bsonCtor_{prop.Name}";
                sb.AppendLine($"            {prop.FullyQualifiedType} {localName} = document.TryGetValue(\"{bsonFieldName}\", out var {bsonLocalName})");
                sb.AppendLine($"                ? ConvertFromBsonValue<{prop.FullyQualifiedType}>({bsonLocalName})!");
                sb.AppendLine("                : default!;");
            }

            var arguments = string.Join(", ", classInfo.ConstructorParameters.Select(p => $"ctor_{p.Property.Name}"));
            sb.AppendLine($"            var entity = new {classInfo.TypeReference}({arguments});");
        }
        else
        {
            sb.AppendLine($"            var entity = new {classInfo.TypeReference}();");
        }
        sb.AppendLine();

        // 为每个属性生成反序列化代码
        foreach (var prop in classInfo.Properties.Where(p => !p.HasIgnoreAttribute && p.CanSet && !constructorPropertyNames.Contains(p.Name)))
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
        if (!classInfo.IsValueType)
        {
            sb.AppendLine("            if (entity == null) throw new ArgumentNullException(nameof(entity));");
        }
        sb.AppendLine("            return propertyName switch");
        sb.AppendLine("            {");

        foreach (var prop in classInfo.Properties.Where(p => !p.HasIgnoreAttribute))
        {
            sb.AppendLine($"                \"{prop.Name}\" => entity.{prop.AccessName},");
        }

        sb.AppendLine("                _ => null");
        sb.AppendLine("            };");
        sb.AppendLine("        }");
        sb.AppendLine();

        sb.AppendLine("        public static bool TrySetPropertyValue(" + classInfo.TypeReference + " entity, string propertyName, object? value)");
        sb.AppendLine("        {");
        if (!classInfo.IsValueType)
        {
            sb.AppendLine("            if (entity == null) throw new ArgumentNullException(nameof(entity));");
        }
        sb.AppendLine("            switch (propertyName)");
        sb.AppendLine("            {");

        foreach (var prop in classInfo.Properties.Where(p => !p.HasIgnoreAttribute && p.CanSet))
        {
            sb.AppendLine($"                case \"{prop.Name}\":");
            sb.AppendLine($"                    entity.{prop.AccessName} = value is null ? default! : ({prop.FullyQualifiedType})value;");
            sb.AppendLine("                    return true;");
        }

        sb.AppendLine("                default:");
        sb.AppendLine("                    return false;");
        sb.AppendLine("            }");
        sb.AppendLine("        }");
        sb.AppendLine();

        GenerateForeignKeyReferences(sb, classInfo);

        sb.AppendLine("    }");

        // 关闭命名空间
        if (!string.IsNullOrEmpty(classInfo.Namespace))
        {
            sb.AppendLine("}");
        }

        return sb.ToString();
    }

    private static string RewriteDocumentSetToBuilder(string source)
    {
        return source.Replace("document = document.Set(", "documentBuilder.Set(");
    }

    private static string GetBsonFieldName(PropertyInfo prop)
    {
        return prop.IsId ? "_id" : ToCamelCase(prop.Name);
    }

    private static void GenerateForeignKeyReferences(StringBuilder sb, ClassInfo classInfo)
    {
        var references = classInfo.Properties
            .Where(p => !p.HasIgnoreAttribute && !string.IsNullOrEmpty(p.ForeignKeyCollectionName))
            .Select(p => new { ForeignKey = p, Target = ResolveForeignKeyTarget(classInfo, p) })
            .ToList();

        sb.AppendLine("        public static readonly IReadOnlyList<AotForeignKeyReference> ForeignKeyReferences = new AotForeignKeyReference[]");
        sb.AppendLine("        {");

        foreach (var reference in references)
        {
            var targetName = reference.Target?.Name is { Length: > 0 } name
                ? ToCSharpStringLiteral(name)
                : "null";
            var targetType = reference.Target != null
                ? $"typeof({reference.Target.FullyQualifiedNonNullableType})"
                : "null";

            sb.AppendLine(
                $"            new AotForeignKeyReference({ToCSharpStringLiteral(reference.ForeignKey.Name)}, {ToCSharpStringLiteral(reference.ForeignKey.ForeignKeyCollectionName!)}, {targetName}, {targetType}),");
        }

        sb.AppendLine("        };");
    }

    private static void AppendIdAdapterMetadata(StringBuilder sb, ClassInfo classInfo)
    {
        sb.AppendLine();

        if (classInfo.IdProperty == null)
        {
            sb.AppendLine("        public static string? IdPropertyName => null;");
            sb.AppendLine("        public static Type? IdPropertyType => null;");
            sb.AppendLine("        public static global::TinyDb.Attributes.IdGenerationStrategy IdGenerationStrategy => global::TinyDb.Attributes.IdGenerationStrategy.None;");
            sb.AppendLine("        public static string? IdGenerationSequenceName => null;");
            sb.AppendLine($"        public static bool GenerateIdIfNeeded({classInfo.TypeReference} entity) => false;");
            return;
        }

        var idProperty = classInfo.IdProperty;
        var idType = idProperty.FullyQualifiedNonNullableType;
        var strategy = ToIdGenerationStrategyExpression(idProperty.IdGenerationStrategyValue);
        var sequenceName = idProperty.IdGenerationSequenceName == null
            ? "null"
            : ToCSharpStringLiteral(idProperty.IdGenerationSequenceName);

        sb.AppendLine($"        public static string? IdPropertyName => {ToCSharpStringLiteral(idProperty.Name)};");
        sb.AppendLine($"        public static Type? IdPropertyType => typeof({idType});");
        sb.AppendLine($"        public static global::TinyDb.Attributes.IdGenerationStrategy IdGenerationStrategy => {strategy};");
        sb.AppendLine($"        public static string? IdGenerationSequenceName => {sequenceName};");
        sb.AppendLine();
        sb.AppendLine($"        public static bool GenerateIdIfNeeded({classInfo.TypeReference} entity)");
        sb.AppendLine("        {");
        if (!classInfo.IsValueType)
        {
            sb.AppendLine("            if (entity == null) return false;");
        }

        if (!idProperty.CanSet)
        {
            sb.AppendLine("            return false;");
            sb.AppendLine("        }");
            return;
        }

        sb.AppendLine("            if (HasValidId(entity)) return false;");
        sb.AppendLine();
        sb.AppendLine("            if (IdGenerationStrategy != global::TinyDb.Attributes.IdGenerationStrategy.None)");
        sb.AppendLine("            {");
        AppendConfiguredIdGeneration(sb, classInfo, idProperty);
        sb.AppendLine("            }");
        sb.AppendLine();
        AppendDefaultIdGeneration(sb, classInfo, idProperty);
        sb.AppendLine("        }");
    }

    private static void AppendConfiguredIdGeneration(StringBuilder sb, ClassInfo classInfo, PropertyInfo idProperty)
    {
        var idType = idProperty.FullyQualifiedNonNullableType;
        var idAccess = idProperty.AccessName;
        var defaultSequenceName = ToCSharpStringLiteral($"{classInfo.Name}_{idProperty.Name}");
        var normalizedIdType = NormalizeTypeName(idProperty.NonNullableType);

        sb.AppendLine("                if (IdGenerationStrategy == global::TinyDb.Attributes.IdGenerationStrategy.IdentityInt ||");
        sb.AppendLine("                    IdGenerationStrategy == global::TinyDb.Attributes.IdGenerationStrategy.IdentityLong)");
        sb.AppendLine("                {");
        sb.AppendLine("                    return false;");
        sb.AppendLine("                }");

        sb.AppendLine($"                var generatedId = global::TinyDb.IdGeneration.AutoIdGenerator.CreateIdValue(typeof({idType}), IdGenerationStrategy, IdGenerationSequenceName, {defaultSequenceName});");
        sb.AppendLine("                if (generatedId == null) return false;");

        if (string.Equals(normalizedIdType, "int", StringComparison.OrdinalIgnoreCase))
        {
            sb.AppendLine($"                entity.{idAccess} = (int)generatedId;");
        }
        else if (string.Equals(normalizedIdType, "long", StringComparison.OrdinalIgnoreCase))
        {
            sb.AppendLine($"                entity.{idAccess} = (long)generatedId;");
        }
        else if (string.Equals(normalizedIdType, "Guid", StringComparison.Ordinal))
        {
            sb.AppendLine($"                entity.{idAccess} = (Guid)generatedId;");
        }
        else if (string.Equals(normalizedIdType, "string", StringComparison.OrdinalIgnoreCase))
        {
            sb.AppendLine($"                entity.{idAccess} = (string)generatedId;");
        }
        else if (normalizedIdType.EndsWith("ObjectId", StringComparison.Ordinal))
        {
            sb.AppendLine($"                entity.{idAccess} = (ObjectId)generatedId;");
        }
        else
        {
            sb.AppendLine("                return false;");
            return;
        }

        sb.AppendLine("                return true;");
    }

    private static void AppendDefaultIdGeneration(StringBuilder sb, ClassInfo classInfo, PropertyInfo idProperty)
    {
        var normalizedIdType = NormalizeTypeName(idProperty.NonNullableType);
        var idAccess = idProperty.AccessName;

        if (string.Equals(normalizedIdType, "int", StringComparison.OrdinalIgnoreCase))
        {
            sb.AppendLine("            return false;");
        }
        else if (string.Equals(normalizedIdType, "long", StringComparison.OrdinalIgnoreCase))
        {
            sb.AppendLine("            return false;");
        }
        else if (string.Equals(normalizedIdType, "Guid", StringComparison.Ordinal))
        {
            sb.AppendLine($"            entity.{idAccess} = global::TinyDb.IdGeneration.AutoIdGenerator.CreateGuidV7();");
            sb.AppendLine("            return true;");
        }
        else if (string.Equals(normalizedIdType, "string", StringComparison.OrdinalIgnoreCase))
        {
            sb.AppendLine($"            entity.{idAccess} = global::TinyDb.IdGeneration.AutoIdGenerator.CreateGuidV7().ToString();");
            sb.AppendLine("            return true;");
        }
        else if (normalizedIdType.EndsWith("ObjectId", StringComparison.Ordinal))
        {
            sb.AppendLine($"            entity.{idAccess} = ObjectId.NewObjectId();");
            sb.AppendLine("            return true;");
        }
        else
        {
            sb.AppendLine("            return false;");
        }
    }

    private static string ToIdGenerationStrategyExpression(int value)
    {
        return value switch
        {
            1 => "global::TinyDb.Attributes.IdGenerationStrategy.ObjectId",
            2 => "global::TinyDb.Attributes.IdGenerationStrategy.IdentityInt",
            3 => "global::TinyDb.Attributes.IdGenerationStrategy.IdentityLong",
            4 => "global::TinyDb.Attributes.IdGenerationStrategy.GuidV7",
            5 => "global::TinyDb.Attributes.IdGenerationStrategy.GuidV4",
            _ => "global::TinyDb.Attributes.IdGenerationStrategy.None"
        };
    }

    private static PropertyInfo? ResolveForeignKeyTarget(ClassInfo classInfo, PropertyInfo foreignKeyProperty)
    {
        if (CanStoreReference(foreignKeyProperty))
        {
            return foreignKeyProperty;
        }

        if (foreignKeyProperty.Name.EndsWith("Id", StringComparison.Ordinal) &&
            foreignKeyProperty.Name.Length > 2)
        {
            var navigationName = foreignKeyProperty.Name.Substring(0, foreignKeyProperty.Name.Length - 2);
            return classInfo.Properties.FirstOrDefault(p =>
                !p.HasIgnoreAttribute &&
                p.Name == navigationName &&
                CanStoreReference(p));
        }

        return null;
    }

    private static bool CanStoreReference(PropertyInfo property)
    {
        var normalized = NormalizeTypeName(property.FullyQualifiedNonNullableType);
        return normalized is "object" or "System.Object" ||
               normalized.EndsWith("BsonDocument", StringComparison.Ordinal) ||
               normalized.EndsWith("BsonValue", StringComparison.Ordinal) ||
               !IsSimpleReferenceValueType(property);
    }

    private static bool IsSimpleReferenceValueType(PropertyInfo property)
    {
        if (property.IsEnum) return true;

        var normalized = NormalizeTypeName(property.NonNullableType);
        return property.IsValueType ||
               normalized is "string" or "decimal" or "double" or "float" or "bool" or "byte" or "sbyte" or
                   "short" or "ushort" or "int" or "uint" or "long" or "ulong" or "char" or "DateTime" or
                   "DateTimeOffset" or "TimeSpan" or "Guid" ||
               normalized.EndsWith("ObjectId", StringComparison.Ordinal);
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
            sb.AppendLine($"            (entity, propertyName) => {helperFullName}.GetPropertyValue(entity, propertyName),");
            sb.AppendLine($"            (entity, propertyName, value) => {helperFullName}.TrySetPropertyValue(entity, propertyName, value),");
            sb.AppendLine($"            {helperFullName}.ForeignKeyReferences,");
            sb.AppendLine($"            {helperFullName}.IdPropertyName,");
            sb.AppendLine($"            {helperFullName}.IdPropertyType,");
            sb.AppendLine($"            {helperFullName}.IdGenerationStrategy,");
            sb.AppendLine($"            {helperFullName}.IdGenerationSequenceName,");
            sb.AppendLine($"            entity => {helperFullName}.GenerateIdIfNeeded(entity)));");
            AppendMetadataRegistryRegistration(sb, classInfo);
        }

        sb.AppendLine("    }");
        sb.AppendLine("}");

        return sb.ToString();
    }

    private static void AppendMetadataRegistryRegistration(StringBuilder sb, ClassInfo classInfo)
    {
        sb.AppendLine($"        global::TinyDb.Metadata.MetadataRegistry.Register(typeof({classInfo.FullName}), new global::TinyDb.Metadata.EntityMetadata");
        sb.AppendLine("        {");
        sb.AppendLine($"            TypeName = {ToCSharpStringLiteral(classInfo.RuntimeFullName)},");
        sb.AppendLine($"            CollectionName = {ToCSharpStringLiteral(classInfo.CollectionName ?? classInfo.Name)},");
        sb.AppendLine($"            DisplayName = {ToCSharpStringLiteral(classInfo.DisplayName)},");
        sb.AppendLine($"            Description = {ToCSharpStringLiteral(classInfo.Description)},");
        sb.AppendLine("            Properties = new List<global::TinyDb.Metadata.PropertyMetadata>");
        sb.AppendLine("            {");

        foreach (var property in classInfo.Properties.Where(static p => !p.HasIgnoreAttribute))
        {
            sb.AppendLine("                new global::TinyDb.Metadata.PropertyMetadata");
            sb.AppendLine("                {");
            sb.AppendLine($"                    PropertyName = {ToCSharpStringLiteral(property.Name)},");
            sb.AppendLine($"                    PropertyType = {ToCSharpStringLiteral(property.MetadataTypeName)},");
            sb.AppendLine($"                    DisplayName = {ToCSharpStringLiteral(property.DisplayName)},");
            sb.AppendLine($"                    Description = {ToCSharpStringLiteral(property.Description)},");
            sb.AppendLine($"                    Order = {property.Order},");
            sb.AppendLine($"                    Required = {(property.Required ? "true" : "false")},");
            sb.AppendLine($"                    IsPrimaryKey = {(property.IsId ? "true" : "false")},");
            sb.AppendLine($"                    ForeignKeyCollection = {ToCSharpStringLiteral(property.ForeignKeyCollectionName)}");
            sb.AppendLine("                },");
        }

        sb.AppendLine("            }");
        sb.AppendLine("        });");
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
        sb.AppendLine("            var documentBuilder = new BsonDocumentBuilder();");

        foreach (var property in classInfo.Properties)
        {
            sb.AppendLine($"            documentBuilder.Set(\"{property.Name}\", ConvertToBsonValue(entity.{property.AccessName}));");
        }

        sb.AppendLine("            return documentBuilder.Build();");
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
            sb.AppendLine($"                entity.{property.AccessName} = ConvertFromBsonValue<{property.Type}>(doc[\"{property.Name}\"]);");
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
        sb.AppendLine("        /// 辅助方法：将BSON值转换为目标类型");
        sb.AppendLine("        /// </summary>");
        sb.AppendLine("        private static T ConvertFromBsonValue<T>(BsonValue value)");
        sb.AppendLine("        {");
        sb.AppendLine("            return global::TinyDb.Serialization.BsonConversion.FromBsonValue<T>(value)!;");
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
        sb.AppendLine("        private static BsonDocument SerializeComplexObject<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicMethods | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields)] T>(T obj)");
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
        
        sb.AppendLine("            // 通过 AotBsonMapper.ToDocument 来序列化，以支持循环引用检测");
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
        sb.AppendLine("        private static BsonDocument SerializeComplexObject<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicMethods | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields)] T>(T obj)");
        sb.AppendLine("        {");
        sb.AppendLine("            if (obj == null) return new BsonDocument();");
        sb.AppendLine();
        sb.AppendLine("            // 通过 AotBsonMapper.ToDocument 来序列化，以支持循环引用检测");
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
        sb.AppendLine("            var documentBuilder = new BsonDocumentBuilder();");
        sb.AppendLine();
        
        // 为每个属性生成序列化代码
        foreach (var prop in depType.Properties)
        {
            var bsonFieldName = ToCamelCase(prop.Name);
            
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
                        sb.AppendLine($"            if (obj.{prop.Name} == null)");
                        sb.AppendLine($"                documentBuilder.Set(\"{bsonFieldName}\", BsonNull.Value);");
                        sb.AppendLine($"            else");
                        sb.AppendLine($"                documentBuilder.Set(\"{bsonFieldName}\", SerializeComplexObject(obj.{prop.Name}));");
                    }
                    else
                    {
                        sb.AppendLine($"            documentBuilder.Set(\"{bsonFieldName}\", SerializeComplexObject(obj.{prop.Name}));");
                    }
                }
            }
            else if (prop.IsCollection && prop.IsElementComplexType)
            {
                if (prop.IsNullable)
                {
                    sb.AppendLine($"            if (obj.{prop.Name} == null)");
                    sb.AppendLine($"                documentBuilder.Set(\"{bsonFieldName}\", BsonNull.Value);");
                    sb.AppendLine($"            else");
                    sb.AppendLine($"            {{");
                }

                sb.AppendLine($"            var array_{prop.Name} = new BsonArray();");
                sb.AppendLine($"            foreach (var item in obj.{prop.Name})");
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
            else if (prop.IsDictionary && prop.IsDictionaryValueComplexType)
            {
                if (prop.IsNullable)
                {
                    sb.AppendLine($"            if (obj.{prop.Name} == null)");
                    sb.AppendLine($"                documentBuilder.Set(\"{bsonFieldName}\", BsonNull.Value);");
                    sb.AppendLine($"            else");
                    sb.AppendLine($"            {{");
                }

                sb.AppendLine($"            var dict_{prop.Name} = new BsonDocument();");
                sb.AppendLine($"            foreach (var kvp in obj.{prop.Name})");
                sb.AppendLine($"            {{");

                if (prop.IsDictionaryValueValueType)
                {
                    sb.AppendLine($"                dict_{prop.Name} = dict_{prop.Name}.Set(kvp.Key.ToString(), SerializeComplexObject(kvp.Value));");
                }
                else
                {
                    sb.AppendLine($"                if (kvp.Value == null)");
                    sb.AppendLine($"                    dict_{prop.Name} = dict_{prop.Name}.Set(kvp.Key.ToString(), BsonNull.Value);");
                    sb.AppendLine($"                else");
                    sb.AppendLine($"                    dict_{prop.Name} = dict_{prop.Name}.Set(kvp.Key.ToString(), SerializeComplexObject(kvp.Value));");
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
                sb.AppendLine($"            documentBuilder.Set(\"{bsonFieldName}\", ConvertToBsonValue(obj.{prop.Name}));");
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
            var bsonFieldName = ToCamelCase(prop.Name);
            
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
                    sb.AppendLine($"                    result.{prop.AccessName} = DeserializeComplexObject<{prop.FullyQualifiedTypeName}>(nested_{prop.Name});");
                    sb.AppendLine("                }");
                    sb.AppendLine("            }");
                }
            }
            else if (prop.IsCollection)
            {
                var elementType = prop.ElementType ?? "object";

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
                sb.AppendLine($"                    var list_{prop.Name} = new System.Collections.Generic.List<{elementType}>();");
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
            else if (prop.IsDictionary && (prop.DictionaryKeyType == null ||
                                           prop.DictionaryKeyType == "string" ||
                                           prop.DictionaryKeyType == "System.String" ||
                                           prop.DictionaryKeyType == "global::System.String"))
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
                sb.AppendLine("                        if (kvp.Value.IsNull)");
                sb.AppendLine($"                            result_{prop.Name}[kvp.Key] = default!;");

                if (prop.IsDictionaryValueComplexType)
                {
                    sb.AppendLine("                        else if (kvp.Value is BsonDocument valueDoc)");
                    sb.AppendLine($"                            result_{prop.Name}[kvp.Key] = DeserializeComplexObject<{valueType}>(valueDoc);");
                    sb.AppendLine("                        else");
                    sb.AppendLine($"                            result_{prop.Name}[kvp.Key] = ConvertFromBsonValue<{valueType}>(kvp.Value);");
                }
                else
                {
                    sb.AppendLine("                        else");
                    sb.AppendLine($"                            result_{prop.Name}[kvp.Key] = ConvertFromBsonValue<{valueType}>(kvp.Value);");
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

    /// <summary>
    /// 生成AOT兼容的ID访问器
    /// </summary>
    private static void GenerateAotIdAccessor(StringBuilder sb, ClassInfo classInfo)
    {
        if (classInfo.IdProperty == null) return;

        var idProperty = classInfo.IdProperty;
        var idAccess = idProperty.AccessName;
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
            sb.AppendLine($"            return new BsonObjectId(entity.{idAccess});");
        }
        else
        {
            sb.AppendLine($"            return ConvertToBsonValue(entity.{idAccess});");
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
            sb.AppendLine($"                entity.{idAccess} = bsonObjectId.Value;");
        }
        else
        {
            sb.AppendLine($"            entity.{idAccess} = ConvertFromBsonValue<{idProperty.Type}>(id);");
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
            sb.AppendLine($"            return entity.{idAccess} != ObjectId.Empty;");
        }
        else
        {
            if (string.Equals(normalizedIdType, "string", StringComparison.OrdinalIgnoreCase))
            {
                sb.AppendLine($"            return !string.IsNullOrWhiteSpace(entity.{idAccess});");
            }
            else if (string.Equals(normalizedIdType, "Guid", StringComparison.Ordinal))
            {
                if (idProperty.IsNullableValueType)
                {
                    sb.AppendLine($"            return entity.{idAccess}.HasValue && entity.{idAccess}.Value != Guid.Empty;");
                }
                else
                {
                    sb.AppendLine($"            return entity.{idAccess} != Guid.Empty;");
                }
            }
            else if (idProperty.IsNullableValueType)
            {
                sb.AppendLine($"            return entity.{idAccess}.HasValue && !System.Collections.Generic.EqualityComparer<{idProperty.FullyQualifiedNonNullableType}>.Default.Equals(entity.{idAccess}.Value, default);");
            }
            else if (idProperty.IsValueType)
            {
                sb.AppendLine($"            return !System.Collections.Generic.EqualityComparer<{idProperty.FullyQualifiedNonNullableType}>.Default.Equals(entity.{idAccess}, default);");
            }
            else
            {
                sb.AppendLine($"            return entity.{idAccess} != null;");
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

    internal static string ToCSharpStringLiteral(string? value)
    {
        if (value == null) return "null";

        var sb = new StringBuilder(value.Length + 2);
        sb.Append('"');

        foreach (var ch in value)
        {
            switch (ch)
            {
                case '\\':
                    sb.Append(@"\\");
                    break;
                case '"':
                    sb.Append("\\\"");
                    break;
                case '\0':
                    sb.Append(@"\0");
                    break;
                case '\a':
                    sb.Append(@"\a");
                    break;
                case '\b':
                    sb.Append(@"\b");
                    break;
                case '\f':
                    sb.Append(@"\f");
                    break;
                case '\r':
                    sb.Append(@"\r");
                    break;
                case '\n':
                    sb.Append(@"\n");
                    break;
                case '\t':
                    sb.Append(@"\t");
                    break;
                case '\v':
                    sb.Append(@"\v");
                    break;
                default:
                    if (char.IsControl(ch))
                    {
                        sb.Append(@"\u");
                        sb.Append(((int)ch).ToString("x4"));
                    }
                    else
                    {
                        sb.Append(ch);
                    }
                    break;
            }
        }

        sb.Append('"');
        return sb.ToString();
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
    private static readonly SymbolDisplayFormat FullyQualifiedNullableDisplayFormat =
        SymbolDisplayFormat.FullyQualifiedFormat.WithMiscellaneousOptions(
            SymbolDisplayFormat.FullyQualifiedFormat.MiscellaneousOptions |
            SymbolDisplayMiscellaneousOptions.IncludeNullableReferenceTypeModifier);

    private static string ToFullyQualifiedNonNullableTypeName(ITypeSymbol typeSymbol)
    {
        if (typeSymbol is INamedTypeSymbol { IsGenericType: true } namedType &&
            namedType.OriginalDefinition.SpecialType == SpecialType.System_Nullable_T &&
            namedType.TypeArguments.Length == 1)
        {
            typeSymbol = namedType.TypeArguments[0];
        }

        return typeSymbol
            .ToDisplayString(FullyQualifiedNullableDisplayFormat)
            .TrimEnd('?');
    }

    private static bool TryGetTypeSymbol(
        IReadOnlyDictionary<string, ITypeSymbol> typeSymbols,
        string typeName,
        out ITypeSymbol typeSymbol)
    {
        if (typeSymbols.TryGetValue(typeName, out typeSymbol))
        {
            return true;
        }

        if (typeName.EndsWith("?", StringComparison.Ordinal) &&
            typeSymbols.TryGetValue(typeName.TrimEnd('?'), out typeSymbol))
        {
            return true;
        }

        typeSymbol = null!;
        return false;
    }

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
            var isElementComplex = IsComplexObjectType(elementType);
            var elementTypeName = elementType.ToDisplayString(FullyQualifiedNullableDisplayFormat);
            var isElementValueType = elementType.IsValueType;
            return new TypeAnalysisResult(false, true, false, true, elementTypeName, isElementComplex, isElementValueType, null, null, false, false);
        }

        // 检查是否是字典类型
        if (typeSymbol is INamedTypeSymbol dictType && IsDictionaryType(dictType))
        {
            var typeArgs = GetDictionaryTypeArguments(dictType);
            if (typeArgs != null)
            {
                var isValueComplex = IsComplexObjectType(typeArgs.Value.ValueType);
                var keyTypeName = typeArgs.Value.KeyType.ToDisplayString(FullyQualifiedNullableDisplayFormat);
                var valueTypeName = typeArgs.Value.ValueType.ToDisplayString(FullyQualifiedNullableDisplayFormat);
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
                var isElementComplex = IsComplexObjectType(elementType);
                var elementTypeName = elementType.ToDisplayString(FullyQualifiedNullableDisplayFormat);
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
            case SpecialType.System_Object:
                return true;
        }

        // 检查已知类型名称
        var typeName = typeSymbol.ToDisplayString();
        var isWellKnown = typeName switch
        {
            "System.Object" or "object" => true,
            "System.Guid" or "Guid" => true,
            "System.TimeSpan" or "TimeSpan" => true,
            "System.DateTimeOffset" or "DateTimeOffset" => true,
            "TinyDb.Bson.ObjectId" or "ObjectId" => true,
            "byte[]" or "System.Byte[]" => true,
            _ when typeName.EndsWith("ObjectId", StringComparison.Ordinal) => true,
            _ when typeSymbol.TypeKind == TypeKind.Enum => true,
            _ => false
        };

        if (isWellKnown)
        {
            return true;
        }

        return IsBsonValueType(typeSymbol);
    }

    private static bool IsBsonValueType(ITypeSymbol typeSymbol)
    {
        for (var current = typeSymbol as INamedTypeSymbol; current != null; current = current.BaseType)
        {
            var fullName = current.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
            if (fullName is "global::TinyDb.Bson.BsonValue" or "TinyDb.Bson.BsonValue")
            {
                return true;
            }
        }

        return false;
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
    /// 获取 BsonRef 属性引用的目标类型（处理集合类型时获取元素类型）
    /// </summary>
    private static ITypeSymbol? GetBsonRefTargetType(ITypeSymbol typeSymbol)
    {
        // 处理数组类型
        if (typeSymbol is IArrayTypeSymbol arrayType)
        {
            return arrayType.ElementType;
        }

        // 处理泛型集合类型（List<T>, ICollection<T> 等）
        if (typeSymbol is INamedTypeSymbol namedType && namedType.IsGenericType)
        {
            var typeName = namedType.OriginalDefinition.ToDisplayString();
            
            // 检查是否是集合类型
            if (typeName.StartsWith("System.Collections.Generic.List<", StringComparison.Ordinal) ||
                typeName.StartsWith("System.Collections.Generic.IList<", StringComparison.Ordinal) ||
                typeName.StartsWith("System.Collections.Generic.ICollection<", StringComparison.Ordinal) ||
                typeName.StartsWith("System.Collections.Generic.IEnumerable<", StringComparison.Ordinal) ||
                typeName.StartsWith("System.Collections.Generic.HashSet<", StringComparison.Ordinal))
            {
                if (namedType.TypeArguments.Length == 1)
                {
                    return namedType.TypeArguments[0];
                }
            }
        }

        // 非集合类型，直接返回类型本身
        return typeSymbol;
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
            if (IsDictionaryType(namedType) || IsCollectionType(namedType) || ImplementsNonGenericEnumerableOrDictionary(namedType))
            {
                return false;
            }
        }

        // 类或结构体
        return typeSymbol.TypeKind == TypeKind.Class || typeSymbol.TypeKind == TypeKind.Struct;
    }

    private static bool ImplementsNonGenericEnumerableOrDictionary(INamedTypeSymbol typeSymbol)
    {
        var selfName = typeSymbol.ToDisplayString();
        if (selfName is "System.Collections.IDictionary" or "System.Collections.IEnumerable")
        {
            return true;
        }

        foreach (var iface in typeSymbol.AllInterfaces)
        {
            var ifaceName = iface.ToDisplayString();
            if (ifaceName is "System.Collections.IDictionary" or "System.Collections.IEnumerable")
            {
                return true;
            }
        }

        return false;
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
    /// 检测Entity类型间的循环引用
    /// </summary>
    /// <param name="currentClassSymbol">当前Entity类的符号</param>
    /// <param name="properties">当前类的属性列表</param>
    /// <param name="typeSymbols">类型符号映射</param>
    /// <returns>检测到的Entity循环引用列表</returns>
    private static List<EntityCircularReferenceInfo> DetectEntityCircularReferences(
        INamedTypeSymbol? currentClassSymbol,
        List<PropertyInfo> properties,
        IReadOnlyDictionary<string, ITypeSymbol> typeSymbols)
    {
        var result = new List<EntityCircularReferenceInfo>();
        if (currentClassSymbol == null) return result;

        var currentTypeName = currentClassSymbol.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
        var visited = new HashSet<string> { currentTypeName };
        var toProcess = new Queue<(ITypeSymbol Symbol, string PropertyName, List<string> Path)>();

        // 遍历当前类的所有属性，查找引用了Entity类型的属性
        foreach (var prop in properties)
        {
            if (prop.HasIgnoreAttribute) continue;

            ITypeSymbol? propTypeSymbol = null;
            
            // 获取属性的类型符号
            if (prop.IsComplexType && TryGetTypeSymbol(typeSymbols, prop.FullyQualifiedNonNullableType, out var directType))
            {
                propTypeSymbol = directType;
            }
            else if (prop.IsCollection && prop.IsElementComplexType && !string.IsNullOrEmpty(prop.ElementType) &&
                     TryGetTypeSymbol(typeSymbols, prop.ElementType!, out var elementType))
            {
                propTypeSymbol = elementType;
            }
            else if (prop.IsDictionary && prop.IsDictionaryValueComplexType && !string.IsNullOrEmpty(prop.DictionaryValueType) &&
                     TryGetTypeSymbol(typeSymbols, prop.DictionaryValueType!, out var valueType))
            {
                propTypeSymbol = valueType;
            }

            if (propTypeSymbol != null && HasEntityAttribute(propTypeSymbol))
            {
                var propTypeName = propTypeSymbol.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
                
                // 检查是否直接引用回当前类（自引用）
                if (propTypeName == currentTypeName)
                {
                    var cycleChain = $"{GetShortTypeName(currentTypeName)} -> {prop.Name} -> {GetShortTypeName(currentTypeName)}";
                    result.Add(new EntityCircularReferenceInfo(
                        GetShortTypeName(currentTypeName),
                        GetShortTypeName(propTypeName),
                        prop.Name,
                        cycleChain));
                }
                else if (!visited.Contains(propTypeName))
                {
                    visited.Add(propTypeName);
                    var path = new List<string> { currentTypeName, propTypeName };
                    toProcess.Enqueue((propTypeSymbol, prop.Name, path));
                }
            }
        }

        // BFS检测更深层的循环引用
        while (toProcess.Count > 0)
        {
            var (typeSymbol, originPropertyName, path) = toProcess.Dequeue();
            
            // 获取该Entity类型的所有公共属性
            foreach (var member in typeSymbol.GetMembers())
            {
                if (member is IPropertySymbol propertySymbol &&
                    propertySymbol.DeclaredAccessibility == Accessibility.Public &&
                    !propertySymbol.IsStatic &&
                    !propertySymbol.IsIndexer &&
                    propertySymbol.GetMethod != null)
                {
                    // 检查是否有BsonIgnore
                    var hasIgnore = propertySymbol.GetAttributes()
                        .Any(attr => attr.AttributeClass?.Name == "BsonIgnoreAttribute");
                    if (hasIgnore) continue;

                    var propType = GetActualType(propertySymbol.Type);
                    if (propType != null && HasEntityAttribute(propType))
                    {
                        var propTypeName = propType.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
                        
                        // 检查是否形成循环（回到当前类或路径中的任意类）
                        if (propTypeName == currentTypeName)
                        {
                            // 找到循环引用回当前类
                            var cycleChain = string.Join(" -> ", path.Select(GetShortTypeName)) + " -> " + GetShortTypeName(currentTypeName);
                            result.Add(new EntityCircularReferenceInfo(
                                GetShortTypeName(currentTypeName),
                                GetShortTypeName(propTypeName),
                                originPropertyName,
                                cycleChain));
                        }
                        else if (path.Contains(propTypeName))
                        {
                            // 路径中已包含此类型，存在循环但不是回到当前类
                            // 这种情况可以选择是否报告，这里也报告
                            var cycleStartIndex = path.IndexOf(propTypeName);
                            var cyclePath = path.Skip(cycleStartIndex).ToList();
                            cyclePath.Add(propTypeName);
                            var cycleChain = string.Join(" -> ", cyclePath.Select(GetShortTypeName));
                            result.Add(new EntityCircularReferenceInfo(
                                GetShortTypeName(currentTypeName),
                                GetShortTypeName(propTypeName),
                                originPropertyName,
                                cycleChain));
                        }
                        else if (!visited.Contains(propTypeName))
                        {
                            visited.Add(propTypeName);
                            var newPath = new List<string>(path) { propTypeName };
                            toProcess.Enqueue((propType, originPropertyName, newPath));
                        }
                    }
                }
            }
        }

        return result;
    }

    /// <summary>
    /// 获取实际的类型（处理可空类型和集合类型）
    /// </summary>
    private static ITypeSymbol? GetActualType(ITypeSymbol typeSymbol)
    {
        // 处理可空值类型
        if (typeSymbol is INamedTypeSymbol { IsGenericType: true } namedType &&
            namedType.OriginalDefinition.SpecialType == SpecialType.System_Nullable_T &&
            namedType.TypeArguments.Length == 1)
        {
            return namedType.TypeArguments[0];
        }

        // 处理数组
        if (typeSymbol is IArrayTypeSymbol arrayType)
        {
            return arrayType.ElementType;
        }

        // 处理泛型集合（List<T>, ICollection<T> 等）
        if (typeSymbol is INamedTypeSymbol genericType && genericType.IsGenericType)
        {
            var typeName = genericType.OriginalDefinition.ToDisplayString();
            
            // 检查是否是常见的单元素泛型集合
            if (typeName.StartsWith("System.Collections.Generic.List<", StringComparison.Ordinal) ||
                typeName.StartsWith("System.Collections.Generic.IList<", StringComparison.Ordinal) ||
                typeName.StartsWith("System.Collections.Generic.ICollection<", StringComparison.Ordinal) ||
                typeName.StartsWith("System.Collections.Generic.IEnumerable<", StringComparison.Ordinal) ||
                typeName.StartsWith("System.Collections.Generic.HashSet<", StringComparison.Ordinal))
            {
                if (genericType.TypeArguments.Length == 1)
                {
                    return genericType.TypeArguments[0];
                }
            }
            
            // 检查字典类型，返回值类型
            if (typeName.StartsWith("System.Collections.Generic.Dictionary<", StringComparison.Ordinal) ||
                typeName.StartsWith("System.Collections.Generic.IDictionary<", StringComparison.Ordinal))
            {
                if (genericType.TypeArguments.Length == 2)
                {
                    return genericType.TypeArguments[1]; // 返回值类型
                }
            }
        }

        // 检查是否是复杂对象类型
        if (IsComplexObjectType(typeSymbol))
        {
            return typeSymbol;
        }

        return null;
    }

    /// <summary>
    /// 获取类型的简短名称（移除 global:: 前缀和命名空间）
    /// </summary>
    private static string GetShortTypeName(string fullTypeName)
    {
        var name = fullTypeName.Replace("global::", "");
        var lastDot = name.LastIndexOf('.');
        return lastDot >= 0 ? name.Substring(lastDot + 1) : name;
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
                if (TryGetTypeSymbol(typeSymbols, typeName, out var typeSymbol))
                {
                    QueueDirectDependency(typeSymbol);
                }
            }

            // 处理集合元素类型
            if (prop.IsCollection && prop.IsElementComplexType && !string.IsNullOrEmpty(prop.ElementType))
            {
                var typeName = prop.ElementType!;
                if (TryGetTypeSymbol(typeSymbols, typeName, out var typeSymbol))
                {
                    QueueDirectDependency(typeSymbol);
                }
            }

            // 处理字典值类型
            if (prop.IsDictionary && prop.IsDictionaryValueComplexType && !string.IsNullOrEmpty(prop.DictionaryValueType))
            {
                var typeName = prop.DictionaryValueType!;
                if (TryGetTypeSymbol(typeSymbols, typeName, out var typeSymbol))
                {
                    QueueDirectDependency(typeSymbol);
                }
            }
        }

        // BFS处理所有依赖类型
        void QueueDirectDependency(ITypeSymbol typeSymbol)
        {
            if (HasEntityAttribute(typeSymbol))
            {
                return;
            }

            var typeName = ToFullyQualifiedNonNullableTypeName(typeSymbol);
            if (visited.Add(typeName))
            {
                toProcess.Enqueue((typeName, typeSymbol, new List<string> { typeName }));
            }
        }

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
                !propertySymbol.IsIndexer &&
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

                var typeAnalysis = AnalyzePropertyType(propType);
                var isComplex = typeAnalysis.IsComplexType;
                string? complexFullName = null;
                bool isCircularRef = false;

                void TrackDependentType(ITypeSymbol? dependencyTypeSymbol)
                {
                    if (dependencyTypeSymbol == null)
                    {
                        return;
                    }

                    if (HasEntityAttribute(dependencyTypeSymbol))
                    {
                        return;
                    }

                    var dependencyFullName = ToFullyQualifiedNonNullableTypeName(dependencyTypeSymbol);

                    if (currentPath.Contains(dependencyFullName))
                    {
                        isCircularRef = true;
                        var cyclePath = new List<string>(currentPath) { dependencyFullName };
                        var cycleStartIndex = currentPath.IndexOf(dependencyFullName);
                        var cycleChain = string.Join(" -> ", cyclePath.Skip(cycleStartIndex));
                        circularRefs.Add(new CircularReferenceInfo(
                            fullName,
                            dependencyFullName,
                            propertySymbol.Name,
                            cycleChain));
                        return;
                    }

                    if (!visited.Contains(dependencyFullName))
                    {
                        visited.Add(dependencyFullName);
                        var newPath = new List<string>(currentPath) { dependencyFullName };
                        toProcess.Enqueue((dependencyFullName, dependencyTypeSymbol, newPath));
                    }
                }

                if (isComplex && !HasEntityAttribute(propType))
                {
                    var actualPropType = GetActualType(propType) ?? propType;
                    complexFullName = actualPropType.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
                    TrackDependentType(actualPropType);
                }

                if (typeAnalysis.IsCollection && typeAnalysis.IsElementComplexType)
                {
                    var elementTypeSymbol = GetElementTypeSymbol(propType);
                    var actualElementType = elementTypeSymbol != null ? (GetActualType(elementTypeSymbol) ?? elementTypeSymbol) : null;
                    TrackDependentType(actualElementType);
                }

                if (typeAnalysis.IsDictionary && typeAnalysis.IsDictionaryValueComplexType)
                {
                    var valueTypeSymbol = GetDictionaryValueTypeSymbol(propType);
                    var actualValueType = valueTypeSymbol != null ? (GetActualType(valueTypeSymbol) ?? valueTypeSymbol) : null;
                    TrackDependentType(actualValueType);
                }

                properties.Add(new DependentTypeProperty(
                    propertySymbol.Name,
                    propTypeName,
                    propFullTypeName,
                    isValueType,
                    isNullable,
                    isComplex && !HasEntityAttribute(propType),
                    complexFullName,
                    typeAnalysis.IsCollection,
                    typeAnalysis.IsDictionary,
                    typeAnalysis.IsArray,
                    typeAnalysis.ElementType,
                    typeAnalysis.IsElementComplexType,
                    typeAnalysis.IsElementValueType,
                    typeAnalysis.DictionaryKeyType,
                    typeAnalysis.DictionaryValueType,
                    typeAnalysis.IsDictionaryValueComplexType,
                    typeAnalysis.IsDictionaryValueValueType,
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

    /// <summary>
    /// 将属性名转换为 camelCase
    /// </summary>
    /// <param name="name">属性名</param>
    /// <returns>camelCase 格式的名称</returns>
    private static string ToCamelCase(string name)
    {
        if (string.IsNullOrEmpty(name)) return name;

        var firstLetter = 0;
        while (firstLetter < name.Length && name[firstLetter] == '_')
        {
            firstLetter++;
        }

        if (firstLetter >= name.Length || char.IsLower(name[firstLetter]))
        {
            return name;
        }

        var chars = name.ToCharArray();
        for (var i = firstLetter; i < chars.Length && char.IsUpper(chars[i]); i++)
        {
            if (i > firstLetter && i + 1 < chars.Length && char.IsLower(chars[i + 1]))
            {
                break;
            }

            chars[i] = char.ToLowerInvariant(chars[i]);
        }

        return new string(chars);
    }

}

public readonly struct DiagnosticLocationInfo : IEquatable<DiagnosticLocationInfo>
{
    public string FilePath { get; }
    public TextSpan TextSpan { get; }
    public LinePositionSpan LineSpan { get; }

    private DiagnosticLocationInfo(string filePath, TextSpan textSpan, LinePositionSpan lineSpan)
    {
        FilePath = filePath;
        TextSpan = textSpan;
        LineSpan = lineSpan;
    }

    public static DiagnosticLocationInfo? From(Location? location)
    {
        if (location == null || location == Location.None || !location.IsInSource)
        {
            return null;
        }

        var lineSpan = location.GetLineSpan();
        return new DiagnosticLocationInfo(lineSpan.Path ?? string.Empty, location.SourceSpan, lineSpan.Span);
    }

    public Location? ToLocation()
    {
        return string.IsNullOrEmpty(FilePath)
            ? null
            : Location.Create(FilePath, TextSpan, LineSpan);
    }

    public bool Equals(DiagnosticLocationInfo other)
    {
        return string.Equals(FilePath, other.FilePath, StringComparison.Ordinal) &&
               TextSpan.Equals(other.TextSpan) &&
               LineSpan.Equals(other.LineSpan);
    }

    public override bool Equals(object? obj)
    {
        return obj is DiagnosticLocationInfo other && Equals(other);
    }

    public override int GetHashCode()
    {
        unchecked
        {
            var hash = 17;
            hash = hash * 31 + StringComparer.Ordinal.GetHashCode(FilePath ?? string.Empty);
            hash = hash * 31 + TextSpan.GetHashCode();
            hash = hash * 31 + LineSpan.GetHashCode();
            return hash;
        }
    }
}

internal static class DiagnosticLocationInfoExtensions
{
    public static Location? ToLocation(this DiagnosticLocationInfo? location)
    {
        return location.HasValue ? location.Value.ToLocation() : null;
    }
}

    public sealed class ConstructorParameterInfo
    {
        public string ParameterName { get; }
        public PropertyInfo Property { get; }

        public ConstructorParameterInfo(string parameterName, PropertyInfo property)
        {
            ParameterName = parameterName;
            Property = property;
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
        public bool IsValueType { get; }
        /// <summary>
        /// 用于在代码中引用类型的名称（对于嵌套类，使用 OuterClass.InnerClass 格式）
        /// </summary>
        public string TypeReference
        {
            get
            {
                if (!string.IsNullOrEmpty(ContainingTypePath))
                {
                    // ContainingTypePath 使用下划线分隔（如 OuterClass_MiddleClass），
                    // 需要转换为点分隔（如 OuterClass.MiddleClass）以便在代码中引用嵌套类型
                    var dotSeparatedPath = ContainingTypePath.Replace("_", ".");
                    return $"{dotSeparatedPath}.{Name}";
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
                    // ContainingTypePath 使用下划线分隔，需要转换为点分隔
                    var dotSeparatedPath = ContainingTypePath.Replace("_", ".");
                    return $"{baseName}{dotSeparatedPath}.{Name}";
                }
                return $"{baseName}{Name}";
            }
        }
        public string RuntimeFullName { get; }
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
        public string DisplayName { get; }
        public string? Description { get; }
        /// <summary>
        /// 类声明的源代码位置（用于诊断报告）
        /// </summary>
        public DiagnosticLocationInfo? Location { get; }
        
        /// <summary>
        /// 依赖的非Entity复杂类型（需要生成内联序列化代码）
        /// </summary>
        public List<DependentComplexType> DependentComplexTypes { get; }
        
        /// <summary>
        /// 检测到的循环引用信息（非Entity类型）
        /// </summary>
        public List<CircularReferenceInfo> CircularReferences { get; }
        
        /// <summary>
        /// 检测到的Entity类型间循环引用信息
        /// </summary>
        public List<EntityCircularReferenceInfo> EntityCircularReferences { get; }
        
        /// <summary>
        /// BsonRef 引用类型缺少 Entity 特性的错误列表
        /// </summary>
        public List<BsonRefMissingEntityInfo> BsonRefMissingEntityErrors { get; }
        public List<ConstructorParameterInfo> ConstructorParameters { get; }

        public ClassInfo(string @namespace, string name, bool isValueType, List<PropertyInfo> properties, PropertyInfo? idProperty, string? collectionName = null, string? displayName = null, string? description = null, string? containingTypePath = null, DiagnosticLocationInfo? location = null, List<DependentComplexType>? dependentComplexTypes = null, List<CircularReferenceInfo>? circularReferences = null, List<EntityCircularReferenceInfo>? entityCircularReferences = null, List<BsonRefMissingEntityInfo>? bsonRefMissingEntityErrors = null, List<ConstructorParameterInfo>? constructorParameters = null, string? runtimeFullName = null)
        {
            Namespace = @namespace;
            Name = name;
            IsValueType = isValueType;
            Properties = properties;
            IdProperty = idProperty;
            CollectionName = collectionName;
            DisplayName = string.IsNullOrEmpty(displayName) ? name : displayName!;
            Description = string.IsNullOrEmpty(description) ? null : description;
            ContainingTypePath = containingTypePath ?? string.Empty;
            RuntimeFullName = runtimeFullName ?? FullName;
            Location = location;
            DependentComplexTypes = dependentComplexTypes ?? new List<DependentComplexType>();
            CircularReferences = circularReferences ?? new List<CircularReferenceInfo>();
            EntityCircularReferences = entityCircularReferences ?? new List<EntityCircularReferenceInfo>();
            BsonRefMissingEntityErrors = bsonRefMissingEntityErrors ?? new List<BsonRefMissingEntityInfo>();
            ConstructorParameters = constructorParameters ?? new List<ConstructorParameterInfo>();
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
    public string AccessName => SyntaxFacts.GetKeywordKind(Name) != SyntaxKind.None ||
                                SyntaxFacts.GetContextualKeywordKind(Name) != SyntaxKind.None
        ? "@" + Name
        : Name;
    public string TypeName { get; }
    public string FullyQualifiedTypeName { get; }
    public bool IsValueType { get; }
    public bool IsNullable { get; }
    public bool IsComplexType { get; }
    public string? ComplexTypeFullName { get; }
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
    /// <summary>
    /// 是否是循环引用（属性类型在依赖链中形成循环）
    /// </summary>
    public bool IsCircularReference { get; }

    public DependentTypeProperty(
        string name,
        string typeName,
        string fullyQualifiedTypeName,
        bool isValueType,
        bool isNullable,
        bool isComplexType = false,
        string? complexTypeFullName = null,
        bool isCollection = false,
        bool isDictionary = false,
        bool isArray = false,
        string? elementType = null,
        bool isElementComplexType = false,
        bool isElementValueType = false,
        string? dictionaryKeyType = null,
        string? dictionaryValueType = null,
        bool isDictionaryValueComplexType = false,
        bool isDictionaryValueValueType = false,
        bool isCircularReference = false)
    {
        Name = name;
        TypeName = typeName;
        FullyQualifiedTypeName = fullyQualifiedTypeName;
        IsValueType = isValueType;
        IsNullable = isNullable;
        IsComplexType = isComplexType;
        ComplexTypeFullName = complexTypeFullName;
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
        IsCircularReference = isCircularReference;
    }
}

/// <summary>
/// 属性信息
/// </summary>
public class PropertyInfo
{
    public string Name { get; }
    public string AccessName => SyntaxFacts.GetKeywordKind(Name) != SyntaxKind.None ||
                                SyntaxFacts.GetContextualKeywordKind(Name) != SyntaxKind.None
        ? "@" + Name
        : Name;
    public string Type { get; }
    public bool IsId { get; set; }
    public bool IsIgnored { get; }
    public bool HasIgnoreAttribute { get; set; }
    public string DisplayName { get; }
    public string? Description { get; }
    public int Order { get; }
    public bool Required { get; }
    public bool IsValueType { get; }
    public bool IsNullableValueType { get; }
    public bool IsNullableReferenceType { get; }
    public bool IsEnum { get; }
    public string NonNullableType { get; }
    public string FullyQualifiedType { get; }
    public string FullyQualifiedNonNullableType { get; }
    public string MetadataTypeName { get; }
    
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
    
    /// <summary>
    /// BsonRef 引用的集合名称（如果属性标记了 [BsonRef] 特性）
    /// </summary>
    public string? BsonRefCollectionName { get; }
    public string? ForeignKeyCollectionName { get; }
    public int IdGenerationStrategyValue { get; }
    public string? IdGenerationSequenceName { get; }
    public bool CanSet { get; }
    
    /// <summary>
    /// 是否是 DbRef 引用类型
    /// </summary>
    public bool IsDbRef => !string.IsNullOrEmpty(BsonRefCollectionName);
    public bool HasForeignKey => !string.IsNullOrEmpty(ForeignKeyCollectionName);

    public PropertyInfo(
        string name,
        string type,
        bool isId = false,
        bool isIgnored = false,
        bool hasIgnoreAttribute = false,
        bool isValueType = false,
        bool isNullableValueType = false,
        bool isNullableReferenceType = false,
        bool isEnum = false,
        string? nonNullableType = null,
        string? fullyQualifiedType = null,
        string? fullyQualifiedNonNullableType = null,
        string? metadataTypeName = null,
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
        bool isDictionaryValueValueType = false,
        string? bsonRefCollectionName = null,
        string? foreignKeyCollectionName = null,
        int idGenerationStrategyValue = 0,
        string? idGenerationSequenceName = null,
        bool canSet = true,
        string? displayName = null,
        string? description = null,
        int order = 0,
        bool required = false)
    {
        Name = name;
        Type = type;
        IsId = isId;
        IsIgnored = isIgnored;
        HasIgnoreAttribute = hasIgnoreAttribute;
        DisplayName = string.IsNullOrEmpty(displayName) ? name : displayName!;
        Description = string.IsNullOrEmpty(description) ? null : description;
        Order = order;
        Required = required;
        IsValueType = isValueType;
        IsNullableValueType = isNullableValueType;
        IsNullableReferenceType = isNullableReferenceType;
        IsEnum = isEnum;
        NonNullableType = nonNullableType ?? type.TrimEnd('?');
        FullyQualifiedType = fullyQualifiedType ?? type;
        FullyQualifiedNonNullableType = fullyQualifiedNonNullableType ?? NonNullableType;
        MetadataTypeName = metadataTypeName ?? FullyQualifiedType.Replace("global::", "");
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
        BsonRefCollectionName = bsonRefCollectionName;
        ForeignKeyCollectionName = foreignKeyCollectionName;
        IdGenerationStrategyValue = idGenerationStrategyValue;
        IdGenerationSequenceName = idGenerationSequenceName;
        CanSet = canSet;
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
/// Entity类型间的循环引用信息
/// </summary>
public class EntityCircularReferenceInfo
{
    /// <summary>
    /// 当前Entity类型名称
    /// </summary>
    public string CurrentEntityName { get; }
    
    /// <summary>
    /// 引用的目标Entity类型名称
    /// </summary>
    public string TargetEntityName { get; }
    
    /// <summary>
    /// 导致循环引用的属性名称
    /// </summary>
    public string PropertyName { get; }
    
    /// <summary>
    /// 循环链的描述（如 EntityA -> EntityB -> EntityA）
    /// </summary>
    public string CycleChain { get; }

    public EntityCircularReferenceInfo(string currentEntityName, string targetEntityName, string propertyName, string cycleChain)
    {
        CurrentEntityName = currentEntityName;
        TargetEntityName = targetEntityName;
        PropertyName = propertyName;
        CycleChain = cycleChain;
    }
}

/// <summary>
/// BsonRef 引用类型缺少 Entity 特性的错误信息
/// </summary>
public class BsonRefMissingEntityInfo
{
    /// <summary>
    /// 属性名称
    /// </summary>
    public string PropertyName { get; }
    
    /// <summary>
    /// 包含该属性的类型名称
    /// </summary>
    public string ContainingTypeName { get; }
    
    /// <summary>
    /// 被引用的类型名称（缺少 Entity 特性）
    /// </summary>
    public string ReferencedTypeName { get; }
    
    /// <summary>
    /// 属性声明的位置（用于诊断报告）
    /// </summary>
    public DiagnosticLocationInfo? Location { get; }

    public BsonRefMissingEntityInfo(string propertyName, string containingTypeName, string referencedTypeName, DiagnosticLocationInfo? location = null)
    {
        PropertyName = propertyName;
        ContainingTypeName = containingTypeName;
        ReferencedTypeName = referencedTypeName;
        Location = location;
    }
}

/// <summary>
/// 生成属性序列化代码的辅助方法
/// </summary>
internal sealed class ClassInfoComparer : IEqualityComparer<ClassInfo?>
{
    public static readonly ClassInfoComparer Instance = new();

    public bool Equals(ClassInfo? x, ClassInfo? y)
    {
        if (ReferenceEquals(x, y)) return true;
        if (x is null || y is null) return false;

        return StringEquals(x.Namespace, y.Namespace) &&
               StringEquals(x.Name, y.Name) &&
               StringEquals(x.ContainingTypePath, y.ContainingTypePath) &&
               x.IsValueType == y.IsValueType &&
               StringEquals(x.RuntimeFullName, y.RuntimeFullName) &&
               StringEquals(x.CollectionName, y.CollectionName) &&
               StringEquals(x.DisplayName, y.DisplayName) &&
               StringEquals(x.Description, y.Description) &&
               Nullable.Equals(x.Location, y.Location) &&
               StringEquals(x.IdProperty?.Name, y.IdProperty?.Name) &&
               ListEquals(x.Properties, y.Properties, PropertyEquals) &&
               ListEquals(x.DependentComplexTypes, y.DependentComplexTypes, DependentComplexTypeEquals) &&
               ListEquals(x.CircularReferences, y.CircularReferences, CircularReferenceEquals) &&
               ListEquals(x.EntityCircularReferences, y.EntityCircularReferences, EntityCircularReferenceEquals) &&
               ListEquals(x.BsonRefMissingEntityErrors, y.BsonRefMissingEntityErrors, BsonRefMissingEntityEquals) &&
               ListEquals(x.ConstructorParameters, y.ConstructorParameters, ConstructorParameterEquals);
    }

    public int GetHashCode(ClassInfo? obj)
    {
        if (obj is null) return 0;

        unchecked
        {
            var hash = 17;
            hash = Add(hash, obj.Namespace);
            hash = Add(hash, obj.Name);
            hash = Add(hash, obj.ContainingTypePath);
            hash = Add(hash, obj.IsValueType);
            hash = Add(hash, obj.RuntimeFullName);
            hash = Add(hash, obj.CollectionName);
            hash = Add(hash, obj.DisplayName);
            hash = Add(hash, obj.Description);
            hash = Add(hash, obj.Location);
            hash = Add(hash, obj.IdProperty?.Name);
            hash = AddList(hash, obj.Properties, GetPropertyHashCode);
            hash = AddList(hash, obj.DependentComplexTypes, GetDependentComplexTypeHashCode);
            hash = AddList(hash, obj.CircularReferences, GetCircularReferenceHashCode);
            hash = AddList(hash, obj.EntityCircularReferences, GetEntityCircularReferenceHashCode);
            hash = AddList(hash, obj.BsonRefMissingEntityErrors, GetBsonRefMissingEntityHashCode);
            hash = AddList(hash, obj.ConstructorParameters, GetConstructorParameterHashCode);
            return hash;
        }
    }

    private static bool ListEquals<T>(IReadOnlyList<T> x, IReadOnlyList<T> y, Func<T, T, bool> comparer)
    {
        if (ReferenceEquals(x, y)) return true;
        if (x.Count != y.Count) return false;

        for (var i = 0; i < x.Count; i++)
        {
            if (!comparer(x[i], y[i])) return false;
        }

        return true;
    }

    private static int AddList<T>(int hash, IReadOnlyList<T> values, Func<T, int> getHashCode)
    {
        unchecked
        {
            hash = Add(hash, values.Count);
            for (var i = 0; i < values.Count; i++)
            {
                hash = hash * 31 + getHashCode(values[i]);
            }

            return hash;
        }
    }

    private static bool PropertyEquals(PropertyInfo x, PropertyInfo y)
    {
        return StringEquals(x.Name, y.Name) &&
               StringEquals(x.Type, y.Type) &&
               x.IsId == y.IsId &&
               x.IsIgnored == y.IsIgnored &&
               x.HasIgnoreAttribute == y.HasIgnoreAttribute &&
               StringEquals(x.DisplayName, y.DisplayName) &&
               StringEquals(x.Description, y.Description) &&
               x.Order == y.Order &&
               x.Required == y.Required &&
               x.IsValueType == y.IsValueType &&
               x.IsNullableValueType == y.IsNullableValueType &&
               x.IsNullableReferenceType == y.IsNullableReferenceType &&
               x.IsEnum == y.IsEnum &&
               StringEquals(x.NonNullableType, y.NonNullableType) &&
               StringEquals(x.FullyQualifiedType, y.FullyQualifiedType) &&
               StringEquals(x.FullyQualifiedNonNullableType, y.FullyQualifiedNonNullableType) &&
               StringEquals(x.MetadataTypeName, y.MetadataTypeName) &&
               x.IsComplexType == y.IsComplexType &&
               x.IsCollection == y.IsCollection &&
               x.IsDictionary == y.IsDictionary &&
               x.IsArray == y.IsArray &&
               StringEquals(x.ElementType, y.ElementType) &&
               x.IsElementComplexType == y.IsElementComplexType &&
               x.IsElementValueType == y.IsElementValueType &&
               StringEquals(x.DictionaryKeyType, y.DictionaryKeyType) &&
               StringEquals(x.DictionaryValueType, y.DictionaryValueType) &&
               x.IsDictionaryValueComplexType == y.IsDictionaryValueComplexType &&
               x.IsDictionaryValueValueType == y.IsDictionaryValueValueType &&
               StringEquals(x.BsonRefCollectionName, y.BsonRefCollectionName) &&
               StringEquals(x.ForeignKeyCollectionName, y.ForeignKeyCollectionName) &&
               x.IdGenerationStrategyValue == y.IdGenerationStrategyValue &&
               StringEquals(x.IdGenerationSequenceName, y.IdGenerationSequenceName) &&
               x.CanSet == y.CanSet;
    }

    private static int GetPropertyHashCode(PropertyInfo value)
    {
        unchecked
        {
            var hash = 17;
            hash = Add(hash, value.Name);
            hash = Add(hash, value.Type);
            hash = Add(hash, value.IsId);
            hash = Add(hash, value.IsIgnored);
            hash = Add(hash, value.HasIgnoreAttribute);
            hash = Add(hash, value.DisplayName);
            hash = Add(hash, value.Description);
            hash = Add(hash, value.Order);
            hash = Add(hash, value.Required);
            hash = Add(hash, value.IsValueType);
            hash = Add(hash, value.IsNullableValueType);
            hash = Add(hash, value.IsNullableReferenceType);
            hash = Add(hash, value.IsEnum);
            hash = Add(hash, value.NonNullableType);
            hash = Add(hash, value.FullyQualifiedType);
            hash = Add(hash, value.FullyQualifiedNonNullableType);
            hash = Add(hash, value.MetadataTypeName);
            hash = Add(hash, value.IsComplexType);
            hash = Add(hash, value.IsCollection);
            hash = Add(hash, value.IsDictionary);
            hash = Add(hash, value.IsArray);
            hash = Add(hash, value.ElementType);
            hash = Add(hash, value.IsElementComplexType);
            hash = Add(hash, value.IsElementValueType);
            hash = Add(hash, value.DictionaryKeyType);
            hash = Add(hash, value.DictionaryValueType);
            hash = Add(hash, value.IsDictionaryValueComplexType);
            hash = Add(hash, value.IsDictionaryValueValueType);
            hash = Add(hash, value.BsonRefCollectionName);
            hash = Add(hash, value.ForeignKeyCollectionName);
            hash = Add(hash, value.IdGenerationStrategyValue);
            hash = Add(hash, value.IdGenerationSequenceName);
            hash = Add(hash, value.CanSet);
            return hash;
        }
    }

    private static bool DependentComplexTypeEquals(DependentComplexType x, DependentComplexType y)
    {
        return StringEquals(x.FullyQualifiedName, y.FullyQualifiedName) &&
               StringEquals(x.ShortName, y.ShortName) &&
               x.IsValueType == y.IsValueType &&
               ListEquals(x.Properties, y.Properties, DependentTypePropertyEquals);
    }

    private static int GetDependentComplexTypeHashCode(DependentComplexType value)
    {
        unchecked
        {
            var hash = 17;
            hash = Add(hash, value.FullyQualifiedName);
            hash = Add(hash, value.ShortName);
            hash = Add(hash, value.IsValueType);
            hash = AddList(hash, value.Properties, GetDependentTypePropertyHashCode);
            return hash;
        }
    }

    private static bool DependentTypePropertyEquals(DependentTypeProperty x, DependentTypeProperty y)
    {
        return StringEquals(x.Name, y.Name) &&
               StringEquals(x.TypeName, y.TypeName) &&
               StringEquals(x.FullyQualifiedTypeName, y.FullyQualifiedTypeName) &&
               x.IsValueType == y.IsValueType &&
               x.IsNullable == y.IsNullable &&
               x.IsComplexType == y.IsComplexType &&
               StringEquals(x.ComplexTypeFullName, y.ComplexTypeFullName) &&
               x.IsCollection == y.IsCollection &&
               x.IsDictionary == y.IsDictionary &&
               x.IsArray == y.IsArray &&
               StringEquals(x.ElementType, y.ElementType) &&
               x.IsElementComplexType == y.IsElementComplexType &&
               x.IsElementValueType == y.IsElementValueType &&
               StringEquals(x.DictionaryKeyType, y.DictionaryKeyType) &&
               StringEquals(x.DictionaryValueType, y.DictionaryValueType) &&
               x.IsDictionaryValueComplexType == y.IsDictionaryValueComplexType &&
               x.IsDictionaryValueValueType == y.IsDictionaryValueValueType &&
               x.IsCircularReference == y.IsCircularReference;
    }

    private static int GetDependentTypePropertyHashCode(DependentTypeProperty value)
    {
        unchecked
        {
            var hash = 17;
            hash = Add(hash, value.Name);
            hash = Add(hash, value.TypeName);
            hash = Add(hash, value.FullyQualifiedTypeName);
            hash = Add(hash, value.IsValueType);
            hash = Add(hash, value.IsNullable);
            hash = Add(hash, value.IsComplexType);
            hash = Add(hash, value.ComplexTypeFullName);
            hash = Add(hash, value.IsCollection);
            hash = Add(hash, value.IsDictionary);
            hash = Add(hash, value.IsArray);
            hash = Add(hash, value.ElementType);
            hash = Add(hash, value.IsElementComplexType);
            hash = Add(hash, value.IsElementValueType);
            hash = Add(hash, value.DictionaryKeyType);
            hash = Add(hash, value.DictionaryValueType);
            hash = Add(hash, value.IsDictionaryValueComplexType);
            hash = Add(hash, value.IsDictionaryValueValueType);
            hash = Add(hash, value.IsCircularReference);
            return hash;
        }
    }

    private static bool CircularReferenceEquals(CircularReferenceInfo x, CircularReferenceInfo y)
    {
        return StringEquals(x.ContainingTypeName, y.ContainingTypeName) &&
               StringEquals(x.TargetTypeName, y.TargetTypeName) &&
               StringEquals(x.PropertyName, y.PropertyName) &&
               StringEquals(x.CycleChain, y.CycleChain);
    }

    private static int GetCircularReferenceHashCode(CircularReferenceInfo value)
    {
        unchecked
        {
            var hash = 17;
            hash = Add(hash, value.ContainingTypeName);
            hash = Add(hash, value.TargetTypeName);
            hash = Add(hash, value.PropertyName);
            hash = Add(hash, value.CycleChain);
            return hash;
        }
    }

    private static bool EntityCircularReferenceEquals(EntityCircularReferenceInfo x, EntityCircularReferenceInfo y)
    {
        return StringEquals(x.CurrentEntityName, y.CurrentEntityName) &&
               StringEquals(x.TargetEntityName, y.TargetEntityName) &&
               StringEquals(x.PropertyName, y.PropertyName) &&
               StringEquals(x.CycleChain, y.CycleChain);
    }

    private static int GetEntityCircularReferenceHashCode(EntityCircularReferenceInfo value)
    {
        unchecked
        {
            var hash = 17;
            hash = Add(hash, value.CurrentEntityName);
            hash = Add(hash, value.TargetEntityName);
            hash = Add(hash, value.PropertyName);
            hash = Add(hash, value.CycleChain);
            return hash;
        }
    }

    private static bool BsonRefMissingEntityEquals(BsonRefMissingEntityInfo x, BsonRefMissingEntityInfo y)
    {
        return StringEquals(x.PropertyName, y.PropertyName) &&
               StringEquals(x.ContainingTypeName, y.ContainingTypeName) &&
               StringEquals(x.ReferencedTypeName, y.ReferencedTypeName) &&
               Nullable.Equals(x.Location, y.Location);
    }

    private static int GetBsonRefMissingEntityHashCode(BsonRefMissingEntityInfo value)
    {
        unchecked
        {
            var hash = 17;
            hash = Add(hash, value.PropertyName);
            hash = Add(hash, value.ContainingTypeName);
            hash = Add(hash, value.ReferencedTypeName);
            hash = Add(hash, value.Location);
            return hash;
        }
    }

    private static bool ConstructorParameterEquals(ConstructorParameterInfo x, ConstructorParameterInfo y)
    {
        return StringEquals(x.ParameterName, y.ParameterName) &&
               StringEquals(x.Property.Name, y.Property.Name);
    }

    private static int GetConstructorParameterHashCode(ConstructorParameterInfo value)
    {
        unchecked
        {
            var hash = 17;
            hash = Add(hash, value.ParameterName);
            hash = Add(hash, value.Property.Name);
            return hash;
        }
    }

    private static bool StringEquals(string? x, string? y)
    {
        return string.Equals(x, y, StringComparison.Ordinal);
    }

    private static int Add(int hash, string? value)
    {
        unchecked
        {
            return hash * 31 + (value == null ? 0 : StringComparer.Ordinal.GetHashCode(value));
        }
    }

    private static int Add(int hash, bool value)
    {
        unchecked
        {
            return hash * 31 + (value ? 1 : 0);
        }
    }

    private static int Add(int hash, int value)
    {
        unchecked
        {
            return hash * 31 + value;
        }
    }

    private static int Add(int hash, DiagnosticLocationInfo? value)
    {
        unchecked
        {
            return hash * 31 + (value.HasValue ? value.Value.GetHashCode() : 0);
        }
    }
}

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
        var bsonFieldName = IsIdProperty(prop) ? "_id" : ToCamelCase(propertyName);

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
        var propertyName = prop.Name;
        var propertyAccess = prop.AccessName;
        
        // 如果底层类型是复杂类型
        if (prop.IsComplexType)
        {
            return $@"if (entity.{propertyAccess} == null)
                document = document.Set(""{bsonFieldName}"", BsonNull.Value);
            else
                document = document.Set(""{bsonFieldName}"", SerializeComplexObject(entity.{propertyAccess}));";
        }
        
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
        var bsonFieldName = IsIdProperty(prop) ? "_id" : ToCamelCase(propertyName);

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
        if (prop.IsComplexType)
        {
            return GenerateComplexTypeDeserialization(prop, bsonFieldName);
        }

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

    /// <summary>
    /// 将属性名转换为 camelCase
    /// </summary>
    /// <param name="name">属性名</param>
    /// <returns>camelCase 格式的名称</returns>
    private static string ToCamelCase(string name)
    {
        if (string.IsNullOrEmpty(name)) return name;

        var firstLetter = 0;
        while (firstLetter < name.Length && name[firstLetter] == '_')
        {
            firstLetter++;
        }

        if (firstLetter >= name.Length || char.IsLower(name[firstLetter]))
        {
            return name;
        }

        var chars = name.ToCharArray();
        for (var i = firstLetter; i < chars.Length && char.IsUpper(chars[i]); i++)
        {
            if (i > firstLetter && i + 1 < chars.Length && char.IsLower(chars[i + 1]))
            {
                break;
            }

            chars[i] = char.ToLowerInvariant(chars[i]);
        }

        return new string(chars);
    }
}
