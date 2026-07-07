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
}
