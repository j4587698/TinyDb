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
    /// 检测Entity类型间的循环引用
    /// </summary>
    /// <param name="currentClassSymbol">当前Entity类的符号</param>
    /// <param name="properties">当前类的属性列表</param>
    /// <param name="typeSymbols">类型符号映射</param>
    /// <returns>检测到的Entity循环引用列表</returns>
    private static List<EntityCircularReferenceInfo> DetectEntityCircularReferences(
        INamedTypeSymbol? currentClassSymbol,
        List<PropertyInfo> properties,
        IReadOnlyDictionary<string, ITypeSymbol> typeSymbols,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var result = new List<EntityCircularReferenceInfo>();
        if (currentClassSymbol == null) return result;

        var currentTypeName = currentClassSymbol.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
        var emittedCycles = new HashSet<string>(StringComparer.Ordinal);
        var enqueuedPaths = new HashSet<string>(StringComparer.Ordinal);
        var toProcess = new Queue<(ITypeSymbol Symbol, string PropertyName, List<string> Path)>();

        void AddCycle(string targetTypeName, string originPropertyName, string cycleChain)
        {
            var key = $"{targetTypeName}|{originPropertyName}|{cycleChain}";
            if (!emittedCycles.Add(key))
            {
                return;
            }

            result.Add(new EntityCircularReferenceInfo(
                GetShortTypeName(currentTypeName),
                GetShortTypeName(targetTypeName),
                originPropertyName,
                cycleChain));
        }

        void EnqueuePath(ITypeSymbol symbol, string originPropertyName, List<string> path)
        {
            var key = originPropertyName + "|" + string.Join("|", path);
            if (enqueuedPaths.Add(key))
            {
                toProcess.Enqueue((symbol, originPropertyName, path));
            }
        }

        // 遍历当前类的所有属性，查找引用了Entity类型的属性
        foreach (var prop in properties)
        {
            cancellationToken.ThrowIfCancellationRequested();

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

            if (propTypeSymbol != null && HasEntityAttribute(propTypeSymbol, cancellationToken))
            {
                var propTypeName = propTypeSymbol.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);

                // 检查是否直接引用回当前类（自引用）
                if (propTypeName == currentTypeName)
                {
                    var cycleChain = $"{GetShortTypeName(currentTypeName)} -> {prop.Name} -> {GetShortTypeName(currentTypeName)}";
                    AddCycle(propTypeName, prop.Name, cycleChain);
                }
                else
                {
                    var path = new List<string> { currentTypeName, propTypeName };
                    EnqueuePath(propTypeSymbol, prop.Name, path);
                }
            }
        }

        // BFS检测更深层的循环引用
        while (toProcess.Count > 0)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var (typeSymbol, originPropertyName, path) = toProcess.Dequeue();

            // 获取该Entity类型的所有公共属性
            foreach (var member in typeSymbol.GetMembers())
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (member is IPropertySymbol propertySymbol &&
                    propertySymbol.DeclaredAccessibility == Accessibility.Public &&
                    !propertySymbol.IsStatic &&
                    !propertySymbol.IsIndexer &&
                    propertySymbol.GetMethod != null)
                {
                    // 检查是否有BsonIgnore
                    var hasIgnore = HasAttribute(propertySymbol.GetAttributes(), "BsonIgnoreAttribute");
                    if (hasIgnore) continue;

                    var propType = GetActualType(propertySymbol.Type, cancellationToken);
                    if (propType != null && HasEntityAttribute(propType, cancellationToken))
                    {
                        var propTypeName = propType.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);

                        // 检查是否形成循环（回到当前类或路径中的任意类）
                        if (propTypeName == currentTypeName)
                        {
                            // 找到循环引用回当前类
                            var cycleChain = string.Join(" -> ", path.Select(GetShortTypeName)) + " -> " + GetShortTypeName(currentTypeName);
                            AddCycle(propTypeName, originPropertyName, cycleChain);
                        }
                        else if (path.Contains(propTypeName))
                        {
                            // 路径中已包含此类型，存在循环但不是回到当前类
                            // 这种情况可以选择是否报告，这里也报告
                            var cycleStartIndex = path.IndexOf(propTypeName);
                            var cyclePath = path.Skip(cycleStartIndex).ToList();
                            cyclePath.Add(propTypeName);
                            var cycleChain = string.Join(" -> ", cyclePath.Select(GetShortTypeName));
                            AddCycle(propTypeName, originPropertyName, cycleChain);
                        }
                        else
                        {
                            var newPath = new List<string>(path) { propTypeName };
                            EnqueuePath(propType, originPropertyName, newPath);
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
    private static ITypeSymbol? GetActualType(ITypeSymbol typeSymbol, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

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
        if (IsComplexObjectType(typeSymbol, cancellationToken))
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
        var genericDepth = 0;
        for (var i = name.Length - 1; i >= 0; i--)
        {
            switch (name[i])
            {
                case '>':
                    genericDepth++;
                    break;
                case '<':
                    genericDepth--;
                    break;
                case '.' when genericDepth == 0:
                    return name.Substring(i + 1);
            }
        }

        return name;
    }


    /// <summary>
    /// 收集所有依赖的非Entity复杂类型
    /// </summary>
    private static (List<DependentComplexType> Types, List<CircularReferenceInfo> CircularRefs) CollectDependentComplexTypes(
        List<PropertyInfo> properties,
        IReadOnlyDictionary<string, ITypeSymbol> typeSymbols,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var result = new List<DependentComplexType>();
        var circularRefs = new List<CircularReferenceInfo>();
        var processed = new HashSet<string>();
        var visiting = new HashSet<string>();
        var emittedCircularRefs = new HashSet<string>(StringComparer.Ordinal);

        void AddCircularRefs(IEnumerable<CircularReferenceInfo> refs)
        {
            foreach (var circularRef in refs)
            {
                var key = $"{circularRef.ContainingTypeName}|{circularRef.TargetTypeName}|{circularRef.PropertyName}|{circularRef.CycleChain}";
                if (emittedCircularRefs.Add(key))
                {
                    circularRefs.Add(circularRef);
                }
            }
        }

        void VisitDependency(ITypeSymbol typeSymbol, List<string> path)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (HasEntityAttribute(typeSymbol, cancellationToken))
            {
                return;
            }

            var typeName = ToFullyQualifiedNonNullableTypeName(typeSymbol);
            if (processed.Contains(typeName) || visiting.Contains(typeName))
            {
                return;
            }

            var currentPath = path.Count > 0 && string.Equals(path[path.Count - 1], typeName, StringComparison.Ordinal)
                ? path
                : new List<string>(path) { typeName };

            visiting.Add(typeName);
            try
            {
                var (dependentType, detectedCircularRefs) = AnalyzeDependentType(
                    typeName,
                    typeSymbol,
                    visiting,
                    processed,
                    VisitDependency,
                    currentPath,
                    cancellationToken);

                if (dependentType != null)
                {
                    result.Add(dependentType);
                }

                AddCircularRefs(detectedCircularRefs);
                processed.Add(typeName);
            }
            finally
            {
                visiting.Remove(typeName);
            }
        }

        void VisitRootDependency(ITypeSymbol typeSymbol)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (HasEntityAttribute(typeSymbol, cancellationToken))
            {
                return;
            }

            var typeName = ToFullyQualifiedNonNullableTypeName(typeSymbol);
            VisitDependency(typeSymbol, new List<string> { typeName });
        }

        // 首先收集直接依赖的复杂类型
        foreach (var prop in properties)
        {
            cancellationToken.ThrowIfCancellationRequested();

            // 处理复杂类型属性
            if (prop.IsComplexType && !string.IsNullOrEmpty(prop.FullyQualifiedNonNullableType))
            {
                var typeName = prop.FullyQualifiedNonNullableType;
                if (TryGetTypeSymbol(typeSymbols, typeName, out var typeSymbol))
                {
                    VisitRootDependency(typeSymbol);
                }
            }

            // 处理集合元素类型
            if (prop.IsCollection && prop.IsElementComplexType && !string.IsNullOrEmpty(prop.ElementType))
            {
                var typeName = prop.ElementType!;
                if (TryGetTypeSymbol(typeSymbols, typeName, out var typeSymbol))
                {
                    VisitRootDependency(typeSymbol);
                }
            }

            // 处理字典值类型
            if (prop.IsDictionary && prop.IsDictionaryValueComplexType && !string.IsNullOrEmpty(prop.DictionaryValueType))
            {
                var typeName = prop.DictionaryValueType!;
                if (TryGetTypeSymbol(typeSymbols, typeName, out var typeSymbol))
                {
                    VisitRootDependency(typeSymbol);
                }
            }
        }

        // Dependencies are processed recursively above so each active path can detect cycles.
        return (result, circularRefs);
    }


    /// <summary>
    /// 分析一个依赖类型并收集其属性信息
    /// </summary>
    private static (DependentComplexType? Type, List<CircularReferenceInfo> CircularRefs) AnalyzeDependentType(
        string fullName,
        ITypeSymbol typeSymbol,
        HashSet<string> visiting,
        HashSet<string> processed,
        Action<ITypeSymbol, List<string>> visitDependency,
        List<string> currentPath,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var properties = new List<DependentTypeProperty>();
        var propertySymbols = new Dictionary<string, IPropertySymbol>(StringComparer.Ordinal);
        var propertyInfos = new Dictionary<string, DependentTypeProperty>(StringComparer.Ordinal);
        var circularRefs = new List<CircularReferenceInfo>();

        // 获取类型的所有公共属性
        foreach (var member in typeSymbol.GetMembers())
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (member is IPropertySymbol propertySymbol &&
                propertySymbol.DeclaredAccessibility == Accessibility.Public &&
                !propertySymbol.IsStatic &&
                !propertySymbol.IsIndexer &&
                propertySymbol.GetMethod != null &&
                propertySymbol.SetMethod is { DeclaredAccessibility: Accessibility.Public } setter)
            {
                // 检查是否有 BsonIgnore 属性
                var hasIgnore = HasAttribute(propertySymbol.GetAttributes(), "BsonIgnoreAttribute");
                if (hasIgnore) continue;

                var propType = propertySymbol.Type;
                var propTypeName = propType.ToDisplayString(SymbolDisplayFormat.MinimallyQualifiedFormat);
                var propFullTypeName = propType.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
                var isValueType = propType.IsValueType;
                var isNullable = propType.NullableAnnotation == NullableAnnotation.Annotated ||
                                 (propType is INamedTypeSymbol { IsGenericType: true } namedType &&
                                  namedType.OriginalDefinition.SpecialType == SpecialType.System_Nullable_T);

                var typeAnalysis = AnalyzePropertyType(propType, cancellationToken);
                var isComplex = typeAnalysis.IsComplexType;
                string? complexFullName = null;
                bool isCircularRef = false;
                var isInitOnly = setter.IsInitOnly;

                void TrackDependentType(ITypeSymbol? dependencyTypeSymbol)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    if (dependencyTypeSymbol == null)
                    {
                        return;
                    }

                    if (HasEntityAttribute(dependencyTypeSymbol, cancellationToken))
                    {
                        return;
                    }

                    var dependencyFullName = ToFullyQualifiedNonNullableTypeName(dependencyTypeSymbol);

                    if (visiting.Contains(dependencyFullName))
                    {
                        isCircularRef = true;
                        var cyclePath = new List<string>(currentPath) { dependencyFullName };
                        var cycleStartIndex = currentPath.IndexOf(dependencyFullName);
                        if (cycleStartIndex < 0)
                        {
                            cycleStartIndex = 0;
                        }
                        var cycleChain = string.Join(" -> ", cyclePath.Skip(cycleStartIndex));
                        circularRefs.Add(new CircularReferenceInfo(
                            fullName,
                            dependencyFullName,
                            propertySymbol.Name,
                            cycleChain));
                        return;
                    }

                    if (!processed.Contains(dependencyFullName))
                    {
                        var newPath = new List<string>(currentPath) { dependencyFullName };
                        visitDependency(dependencyTypeSymbol, newPath);
                    }
                }

                if (isComplex && !HasEntityAttribute(propType, cancellationToken))
                {
                    var actualPropType = GetActualType(propType, cancellationToken) ?? propType;
                    complexFullName = actualPropType.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
                    TrackDependentType(actualPropType);
                }

                if (typeAnalysis.IsCollection && typeAnalysis.IsElementComplexType)
                {
                    var elementTypeSymbol = GetElementTypeSymbol(propType, cancellationToken);
                    var actualElementType = elementTypeSymbol != null
                        ? (GetActualType(elementTypeSymbol, cancellationToken) ?? elementTypeSymbol)
                        : null;
                    TrackDependentType(actualElementType);
                }

                if (typeAnalysis.IsDictionary && typeAnalysis.IsDictionaryValueComplexType)
                {
                    var valueTypeSymbol = GetDictionaryValueTypeSymbol(propType, cancellationToken);
                    var actualValueType = valueTypeSymbol != null
                        ? (GetActualType(valueTypeSymbol, cancellationToken) ?? valueTypeSymbol)
                        : null;
                    TrackDependentType(actualValueType);
                }

                var propertyInfo = new DependentTypeProperty(
                    propertySymbol.Name,
                    propTypeName,
                    propFullTypeName,
                    isValueType,
                    isNullable,
                    isComplex && !HasEntityAttribute(propType, cancellationToken),
                    complexFullName,
                    typeAnalysis.IsCollection,
                    typeAnalysis.IsDictionary,
                    typeAnalysis.IsArray,
                    typeAnalysis.ArrayRank,
                    typeAnalysis.ElementType,
                    typeAnalysis.IsElementComplexType,
                    typeAnalysis.IsElementValueType,
                    typeAnalysis.DictionaryKeyType,
                    typeAnalysis.DictionaryValueType,
                    typeAnalysis.IsDictionaryValueComplexType,
                    typeAnalysis.IsDictionaryValueValueType,
                    isInitOnly,
                    isCircularRef);

                properties.Add(propertyInfo);
                propertySymbols[propertySymbol.Name] = propertySymbol;
                propertyInfos[propertySymbol.Name] = propertyInfo;
            }
        }

        if (properties.Count == 0)
        {
            return (null, circularRefs);
        }

        var constructorParameters = CollectDependentConstructorParameters(
            typeSymbol,
            propertySymbols,
            propertyInfos,
            cancellationToken);

        return (new DependentComplexType(
            fullName,
            typeSymbol.Name,
            typeSymbol.IsValueType,
            HasAccessibleParameterlessConstructor(typeSymbol, cancellationToken),
            properties,
            constructorParameters), circularRefs);
    }

    private static List<DependentConstructorParameterInfo> CollectDependentConstructorParameters(
        ITypeSymbol typeSymbol,
        IReadOnlyDictionary<string, IPropertySymbol> propertySymbols,
        IReadOnlyDictionary<string, DependentTypeProperty> propertyInfos,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (typeSymbol is not INamedTypeSymbol namedType)
        {
            return new List<DependentConstructorParameterInfo>();
        }

        foreach (var constructor in namedType.InstanceConstructors
                     .Where(static ctor => ctor.DeclaredAccessibility == Accessibility.Public && ctor.Parameters.Length > 0)
                     .OrderByDescending(static ctor => ctor.Parameters.Length))
        {
            cancellationToken.ThrowIfCancellationRequested();

            var parameters = new List<DependentConstructorParameterInfo>(constructor.Parameters.Length);
            var matchedAll = true;

            foreach (var parameter in constructor.Parameters)
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (!TryFindConstructorProperty(parameter, propertySymbols, cancellationToken, out var propertySymbol) ||
                    !propertyInfos.TryGetValue(propertySymbol.Name, out var propertyInfo))
                {
                    matchedAll = false;
                    break;
                }

                parameters.Add(new DependentConstructorParameterInfo(parameter.Name, propertyInfo));
            }

            if (matchedAll)
            {
                return parameters;
            }
        }

        return new List<DependentConstructorParameterInfo>();
    }

    private static bool HasAccessibleParameterlessConstructor(ITypeSymbol typeSymbol, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (typeSymbol.IsValueType)
        {
            return true;
        }

        if (typeSymbol is not INamedTypeSymbol namedType)
        {
            return false;
        }

        foreach (var constructor in namedType.InstanceConstructors)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (constructor.Parameters.Length == 0 &&
                constructor.DeclaredAccessibility is Accessibility.Public or Accessibility.Internal or Accessibility.ProtectedOrInternal)
            {
                return true;
            }
        }

        return false;
    }
}
