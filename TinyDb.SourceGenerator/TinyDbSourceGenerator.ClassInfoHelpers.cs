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


    private static void AddMissingSymbolFields(
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
        foreach (var member in classSymbol.GetMembers())
        {
            if (member is not IFieldSymbol fieldSymbol ||
                !existingNames.Add(fieldSymbol.Name))
            {
                continue;
            }

            if (TryCreatePropertyInfoFromFieldSymbol(
                    fieldSymbol,
                    containingTypeName,
                    typeSymbolMap,
                    bsonRefMissingEntityErrors,
                    out var propertyInfo))
            {
                properties.Add(propertyInfo!);
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


    private static bool HasModifier(SyntaxTokenList modifiers, SyntaxKind kind)
    {
        foreach (var modifier in modifiers)
        {
            if (modifier.IsKind(kind))
            {
                return true;
            }
        }

        return false;
    }


    private static bool TryCreatePropertyInfoFromFieldSymbol(
        IFieldSymbol fieldSymbol,
        string containingTypeName,
        Dictionary<string, ITypeSymbol> typeSymbolMap,
        List<BsonRefMissingEntityInfo> bsonRefMissingEntityErrors,
        out PropertyInfo? propertyInfo)
    {
        propertyInfo = null;
        if (fieldSymbol.DeclaredAccessibility != Accessibility.Public ||
            fieldSymbol.IsStatic ||
            fieldSymbol.IsConst)
        {
            return false;
        }

        var attributes = fieldSymbol.GetAttributes();
        var hasIgnoreAttribute = HasAttribute(attributes, "BsonIgnoreAttribute", "BsonIgnore");

        var bsonRefAttribute = FindAttribute(attributes, "BsonRefAttribute");
        var bsonRefCollectionName = GetConstructorString(bsonRefAttribute, 0);

        var foreignKeyAttribute = FindAttribute(attributes, "ForeignKeyAttribute", "ForeignKey");
        var foreignKeyCollectionName = GetConstructorString(foreignKeyAttribute, 0);
        var propertyMetadataAttribute = GetPropertyMetadataAttribute(fieldSymbol);

        var typeSymbol = fieldSymbol.Type;
        if (!string.IsNullOrEmpty(bsonRefCollectionName))
        {
            var refTypeSymbol = GetBsonRefTargetType(typeSymbol);
            if (refTypeSymbol != null &&
                !HasAttribute(refTypeSymbol.GetAttributes(), "EntityAttribute"))
            {
                bsonRefMissingEntityErrors.Add(new BsonRefMissingEntityInfo(
                    fieldSymbol.Name,
                    containingTypeName,
                    refTypeSymbol.ToDisplayString(SymbolDisplayFormat.MinimallyQualifiedFormat),
                    DiagnosticLocationInfo.From(fieldSymbol.Locations.Length > 0 ? fieldSymbol.Locations[0] : null)));
            }
        }

        var fieldType = typeSymbol.ToDisplayString(FullyQualifiedNullableDisplayFormat);
        var propIsValueType = typeSymbol.IsValueType;
        var isNullableValueType = typeSymbol is INamedTypeSymbol { IsGenericType: true } namedType &&
                                  namedType.OriginalDefinition.SpecialType == SpecialType.System_Nullable_T;
        var isNullableReferenceType = typeSymbol is { IsReferenceType: true } &&
                                      fieldSymbol.NullableAnnotation == NullableAnnotation.Annotated;
        var isEnumType = typeSymbol.TypeKind == TypeKind.Enum;
        var nonNullableType = fieldType;
        var fullyQualifiedNonNullableType = fieldType;

        if (isNullableValueType && typeSymbol is INamedTypeSymbol nullableType && nullableType.TypeArguments.Length == 1)
        {
            var underlyingType = nullableType.TypeArguments[0];
            isEnumType = underlyingType.TypeKind == TypeKind.Enum;
            nonNullableType = underlyingType.ToDisplayString(FullyQualifiedNullableDisplayFormat);
            fullyQualifiedNonNullableType = nonNullableType;
            AddTypeSymbolIfMissing(typeSymbolMap, fullyQualifiedNonNullableType, underlyingType);
        }
        else if (!propIsValueType && fieldType.EndsWith("?", StringComparison.Ordinal))
        {
            nonNullableType = fieldType.TrimEnd('?').Trim();
            fullyQualifiedNonNullableType = nonNullableType;
        }

        var typeAnalysis = AnalyzePropertyType(typeSymbol);
        AddTypeSymbols(typeSymbolMap, typeSymbol, isNullableValueType, fullyQualifiedNonNullableType, typeAnalysis);

        var idGenerationInfo = GetIdGenerationInfo(fieldSymbol);
        var metadataTypeName = GetStableMetadataTypeName(typeSymbol);
        propertyInfo = new PropertyInfo(
            fieldSymbol.Name,
            fieldType,
            isId: false,
            isIgnored: hasIgnoreAttribute,
            hasIgnoreAttribute: hasIgnoreAttribute,
            isValueType: propIsValueType,
            isNullableValueType: isNullableValueType,
            isNullableReferenceType: isNullableReferenceType,
            isEnum: isEnumType,
            nonNullableType: nonNullableType,
            fullyQualifiedType: fieldType,
            fullyQualifiedNonNullableType: fullyQualifiedNonNullableType,
            metadataTypeName: metadataTypeName,
            isComplexType: typeAnalysis.IsComplexType,
            isCollection: typeAnalysis.IsCollection,
            isDictionary: typeAnalysis.IsDictionary,
            isArray: typeAnalysis.IsArray,
            arrayRank: typeAnalysis.ArrayRank,
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
            displayName: GetConstructorString(propertyMetadataAttribute, 0) ?? fieldSymbol.Name,
            description: GetNamedString(propertyMetadataAttribute, "Description"),
            order: GetNamedInt(propertyMetadataAttribute, "Order"),
            required: GetNamedBool(propertyMetadataAttribute, "Required"));
        return true;
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
        var hasIgnoreAttribute = HasAttribute(attributes, "BsonIgnoreAttribute", "BsonIgnore");

        var bsonRefAttribute = FindAttribute(attributes, "BsonRefAttribute");
        var bsonRefCollectionName = GetConstructorString(bsonRefAttribute, 0);

        var foreignKeyAttribute = FindAttribute(attributes, "ForeignKeyAttribute", "ForeignKey");
        var foreignKeyCollectionName = GetConstructorString(foreignKeyAttribute, 0);
        var propertyMetadataAttribute = GetPropertyMetadataAttribute(propertySymbol);

        var typeSymbol = propertySymbol.Type;
        if (!string.IsNullOrEmpty(bsonRefCollectionName))
        {
            var refTypeSymbol = GetBsonRefTargetType(typeSymbol);
            if (refTypeSymbol != null &&
                !HasAttribute(refTypeSymbol.GetAttributes(), "EntityAttribute"))
            {
                bsonRefMissingEntityErrors.Add(new BsonRefMissingEntityInfo(
                    propertySymbol.Name,
                    containingTypeName,
                    refTypeSymbol.ToDisplayString(SymbolDisplayFormat.MinimallyQualifiedFormat),
                    DiagnosticLocationInfo.From(propertySymbol.Locations.Length > 0 ? propertySymbol.Locations[0] : null)));
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
            arrayRank: typeAnalysis.ArrayRank,
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
                if (HasAttribute(propertySymbol.GetAttributes(), "IdAttribute"))
                {
                    return propertySymbol.Name;
                }
            }
        }

        return null;
    }


    private static (int StrategyValue, string? SequenceName) GetIdGenerationInfo(ISymbol? symbol)
    {
        var attribute = symbol == null
            ? null
            : FindAttribute(symbol.GetAttributes(), "IdGenerationAttribute");
        if (attribute == null)
        {
            return (0, null);
        }

        var strategyValue = 0;
        if (attribute.ConstructorArguments.Length > 0)
        {
            strategyValue = GetEnumConstantValue(attribute.ConstructorArguments[0]);
        }

        if (TryGetNamedArgument(attribute, "Strategy", out var namedStrategy) &&
            namedStrategy.Value != null)
        {
            strategyValue = GetEnumConstantValue(namedStrategy);
        }

        string? sequenceName = null;
        if (attribute.ConstructorArguments.Length > 1)
        {
            sequenceName = attribute.ConstructorArguments[1].Value?.ToString();
        }

        if (TryGetNamedArgument(attribute, "SequenceName", out var namedSequence) &&
            namedSequence.Value != null)
        {
            sequenceName = namedSequence.Value?.ToString();
        }

        return (strategyValue, sequenceName);
    }
}
