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

}
