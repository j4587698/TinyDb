using System;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Text;
using System.Collections.Immutable;
using System.Text;
using System.Threading;

namespace TinyDb.SourceGenerator;

public partial class TinyDbSourceGenerator
{

    /// <summary>
    /// 获取类信息
    /// </summary>
    private static ClassInfo? GetClassInfo(GeneratorAttributeSyntaxContext context, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var classDeclaration = (TypeDeclarationSyntax)context.TargetNode;
        var semanticModel = context.SemanticModel;

        var className = classDeclaration.Identifier.Text;
        var typeParameterList = classDeclaration.TypeParameterList?.ToString() ?? string.Empty;
        var typeParameterConstraints = string.Join(" ", classDeclaration.ConstraintClauses.Select(static clause => clause.ToString()));

        // 获取类符号信息
        var classSymbol = context.TargetSymbol as INamedTypeSymbol
            ?? semanticModel.GetDeclaredSymbol(classDeclaration, cancellationToken);
        var namespaceName = classSymbol?.ContainingNamespace is { IsGlobalNamespace: false } containingNamespace
            ? containingNamespace.ToDisplayString()
            : string.Empty;
        var metadataName = classSymbol?.MetadataName ?? className;
        var isGenericType = classSymbol?.IsGenericType ?? classDeclaration.TypeParameterList != null;

        // 获取包含类的全名（用于生成唯一的文件名）
        var containingTypeNames = new List<string>();
        var containingTypeDisplayNames = new List<string>();
        var containingType = classSymbol?.ContainingType;
        while (containingType != null)
        {
            cancellationToken.ThrowIfCancellationRequested();

            containingTypeNames.Insert(0, containingType.MetadataName.Replace('`', '_'));
            containingTypeDisplayNames.Insert(0, containingType.MetadataName.Replace('`', '_'));
            containingType = containingType.ContainingType;
        }
        var containingTypePath = string.Join("_", containingTypeNames);
        var containingTypeDisplayPath = string.Join(".", containingTypeDisplayNames);

        // 检查是否是值类型
        var isValueType = classSymbol?.IsValueType ?? false;
        var runtimeFullName = classSymbol != null ? GetRuntimeFullName(classSymbol) : null;
        var fullyQualifiedTypeReference = classSymbol?.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);

        // 获取Entity属性信息
        var entityAttribute = GetFirstAttribute(context.Attributes);

        // 优先从构造函数参数获取Name，否则从命名参数获取
        var collectionName = GetConstructorString(entityAttribute, 0);
        if (string.IsNullOrEmpty(collectionName))
        {
            collectionName = GetNamedString(entityAttribute, "Name");
        }

        // 获取Entity属性中指定的IdProperty名称
        var specifiedIdProperty = GetConstructorString(entityAttribute, 1);
        specifiedIdProperty ??= GetNamedString(entityAttribute, "IdProperty");

        // 获取属性信息
        var entityDisplayName = GetEntityMetadataDisplayName(classSymbol, className);
        var entityDescription = GetEntityMetadataDescription(classSymbol);

        var properties = new List<PropertyInfo>();
        PropertyInfo? idProperty = null;
        string? invalidIdPropertyName = null;
        var invalidIdPropertyLocation = DiagnosticLocationInfo.From(classDeclaration.GetLocation());
        // 收集所有属性的类型符号，用于后续分析依赖类型
        var typeSymbolMap = new Dictionary<string, ITypeSymbol>();
        // 收集 BsonRef 引用类型缺少 Entity 特性的错误
        var bsonRefMissingEntityErrors = new List<BsonRefMissingEntityInfo>();

        AddMissingSymbolProperties(classSymbol, className, properties, typeSymbolMap, bsonRefMissingEntityErrors, cancellationToken);
        AddMissingSymbolFields(classSymbol, className, properties, typeSymbolMap, bsonRefMissingEntityErrors, cancellationToken);
        var constructorParameters = AddConstructorBoundProperties(
            classSymbol,
            className,
            properties,
            typeSymbolMap,
            bsonRefMissingEntityErrors,
            cancellationToken);

        // 智能ID识别逻辑
        if (!string.IsNullOrEmpty(specifiedIdProperty))
        {
            // 1. 优先使用Entity属性中指定的ID属性
            idProperty = properties.FirstOrDefault(p => p.Name == specifiedIdProperty);
            if (idProperty != null)
            {
                idProperty.IsId = true;
            }
            else
            {
                invalidIdPropertyName = specifiedIdProperty;
            }
        }
        if (idProperty == null && invalidIdPropertyName == null)
        {
            // 2. 自动查找 [Id] 标记属性（包含继承链）
            var idAttributePropertyName = FindIdPropertyName(classSymbol, cancellationToken);
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
                    cancellationToken.ThrowIfCancellationRequested();

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
        var (dependentComplexTypes, circularReferences) = CollectDependentComplexTypes(properties, typeSymbolMap, cancellationToken);

        // 收集Entity类型间的循环引用（检测属性类型中引用了有[Entity]特性的类型）
        var entityCircularReferences = DetectEntityCircularReferences(classSymbol, properties, typeSymbolMap, cancellationToken);

        return new ClassInfo(
            namespaceName,
            className,
            isValueType,
            properties,
            idProperty,
            collectionName,
            entityDisplayName,
            entityDescription,
            containingTypePath,
            DiagnosticLocationInfo.From(classDeclaration.GetLocation()),
            dependentComplexTypes,
            circularReferences,
            entityCircularReferences,
            bsonRefMissingEntityErrors,
            invalidIdPropertyName,
            invalidIdPropertyLocation,
            constructorParameters,
            runtimeFullName,
            fullyQualifiedTypeReference,
            metadataName,
            isGenericType,
            typeParameterList,
            typeParameterConstraints,
            containingTypeDisplayPath);
    }

}
