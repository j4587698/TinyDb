using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Microsoft.CodeAnalysis;

namespace TinyDb.SourceGenerator;

public partial class TinyDbSourceGenerator
{
    /// <summary>
    /// 类型分析结果
    /// </summary>
    private sealed class TypeAnalysisResult
    {
        public bool IsComplexType { get; }
        public bool IsCollection { get; }
        public bool IsDictionary { get; }
        public bool IsArray { get; }
        public int ArrayRank { get; }
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
            int arrayRank,
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
            ArrayRank = arrayRank;
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
            return ToFullyQualifiedNonNullableTypeName(namedType.TypeArguments[0]);
        }

        if (typeSymbol is IArrayTypeSymbol arrayType)
        {
            return ToFullyQualifiedNonNullableTypeName(arrayType.ElementType) + CreateArrayRankSuffix(arrayType.Rank);
        }

        if (typeSymbol is INamedTypeSymbol { IsGenericType: true } genericType)
        {
            var definitionName = genericType.OriginalDefinition.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
            var genericMarkerIndex = definitionName.IndexOf('<');
            if (genericMarkerIndex >= 0)
            {
                definitionName = definitionName.Substring(0, genericMarkerIndex);
            }

            var arguments = string.Join(", ", genericType.TypeArguments.Select(ToFullyQualifiedNonNullableTypeName));
            return $"{definitionName}<{arguments}>";
        }

        return typeSymbol.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
    }

    private static string CreateArrayRankSuffix(int rank)
    {
        return rank <= 1
            ? "[]"
            : "[" + new string(',', rank - 1) + "]";
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
            typeSymbols.TryGetValue(typeName.Substring(0, typeName.Length - 1), out typeSymbol))
        {
            return true;
        }

        typeSymbol = null!;
        return false;
    }

    private static TypeAnalysisResult AnalyzePropertyType(ITypeSymbol? typeSymbol, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (typeSymbol == null)
        {
            return new TypeAnalysisResult(false, false, false, false, 0, null, false, false, null, null, false, false);
        }

        // 处理可空类型
        if (typeSymbol is INamedTypeSymbol { IsGenericType: true } namedType &&
            namedType.OriginalDefinition.SpecialType == SpecialType.System_Nullable_T)
        {
            typeSymbol = namedType.TypeArguments[0];
        }

        // 检查是否是基本类型
        if (IsPrimitiveOrWellKnownType(typeSymbol, cancellationToken))
        {
            return new TypeAnalysisResult(false, false, false, false, 0, null, false, false, null, null, false, false);
        }

        // 检查是否是数组
        if (typeSymbol is IArrayTypeSymbol arrayType)
        {
            var elementType = arrayType.ElementType;
            var isElementComplex = IsComplexObjectType(elementType, cancellationToken);
            var elementTypeName = elementType.ToDisplayString(FullyQualifiedNullableDisplayFormat);
            var isElementValueType = elementType.IsValueType;
            return new TypeAnalysisResult(false, true, false, true, arrayType.Rank, elementTypeName, isElementComplex, isElementValueType, null, null, false, false);
        }

        // 检查是否是字典类型
        if (typeSymbol is INamedTypeSymbol dictType && IsDictionaryType(dictType, cancellationToken))
        {
            var typeArgs = GetDictionaryTypeArguments(dictType, cancellationToken);
            if (typeArgs != null)
            {
                var isValueComplex = IsComplexObjectType(typeArgs.Value.ValueType, cancellationToken);
                var keyTypeName = typeArgs.Value.KeyType.ToDisplayString(FullyQualifiedNullableDisplayFormat);
                var valueTypeName = typeArgs.Value.ValueType.ToDisplayString(FullyQualifiedNullableDisplayFormat);
                var isValueValueType = typeArgs.Value.ValueType.IsValueType;
                return new TypeAnalysisResult(false, false, true, false, 0, null, false, false, keyTypeName, valueTypeName, isValueComplex, isValueValueType);
            }
        }

        // 检查是否是集合类型 (List<T>, ICollection<T>, IEnumerable<T> 等)
        if (typeSymbol is INamedTypeSymbol collectionType && IsCollectionType(collectionType, cancellationToken))
        {
            var elementType = GetCollectionElementType(collectionType, cancellationToken);
            if (elementType != null)
            {
                var isElementComplex = IsComplexObjectType(elementType, cancellationToken);
                var elementTypeName = elementType.ToDisplayString(FullyQualifiedNullableDisplayFormat);
                var isElementValueType = elementType.IsValueType;
                return new TypeAnalysisResult(false, true, false, false, 0, elementTypeName, isElementComplex, isElementValueType, null, null, false, false);
            }
        }

        // 检查是否是复杂对象类型
        if (IsComplexObjectType(typeSymbol, cancellationToken))
        {
            return new TypeAnalysisResult(true, false, false, false, 0, null, false, false, null, null, false, false);
        }

        return new TypeAnalysisResult(false, false, false, false, 0, null, false, false, null, null, false, false);
    }

    /// <summary>
    /// 检查是否是基本类型或已知的简单类型
    /// </summary>
    private static bool IsPrimitiveOrWellKnownType(ITypeSymbol typeSymbol, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

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
        if (IsTinyDbObjectIdType(typeSymbol))
        {
            return true;
        }

        var typeName = typeSymbol.ToDisplayString();
        var isWellKnown = typeName switch
        {
            "System.Object" or "object" => true,
            "System.Guid" or "Guid" => true,
            "System.TimeSpan" or "TimeSpan" => true,
            "System.DateTimeOffset" or "DateTimeOffset" => true,
            "byte[]" or "System.Byte[]" => true,
            _ when typeSymbol.TypeKind == TypeKind.Enum => true,
            _ => false
        };

        if (isWellKnown)
        {
            return true;
        }

        return IsBsonValueType(typeSymbol, cancellationToken);
    }

    private static bool IsTinyDbObjectIdType(ITypeSymbol typeSymbol)
    {
        if (typeSymbol is INamedTypeSymbol { IsGenericType: true } namedType &&
            namedType.OriginalDefinition.SpecialType == SpecialType.System_Nullable_T &&
            namedType.TypeArguments.Length == 1)
        {
            typeSymbol = namedType.TypeArguments[0];
        }

        var fullName = typeSymbol.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
        return string.Equals(fullName, "global::TinyDb.Bson.ObjectId", StringComparison.Ordinal);
    }

    private static bool IsBsonValueType(ITypeSymbol typeSymbol, CancellationToken cancellationToken)
    {
        for (var current = typeSymbol as INamedTypeSymbol; current != null; current = current.BaseType)
        {
            cancellationToken.ThrowIfCancellationRequested();

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
    private static bool IsDictionaryType(INamedTypeSymbol typeSymbol, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

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
            cancellationToken.ThrowIfCancellationRequested();

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
    private static (ITypeSymbol KeyType, ITypeSymbol ValueType)? GetDictionaryTypeArguments(
        INamedTypeSymbol typeSymbol,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

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
            cancellationToken.ThrowIfCancellationRequested();

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
    private static bool IsCollectionType(INamedTypeSymbol typeSymbol, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        // 排除字符串
        if (typeSymbol.SpecialType == SpecialType.System_String)
        {
            return false;
        }

        // 排除字典类型
        if (IsDictionaryType(typeSymbol, cancellationToken))
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
            cancellationToken.ThrowIfCancellationRequested();

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
    private static ITypeSymbol? GetElementTypeSymbol(ITypeSymbol? typeSymbol, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (typeSymbol == null) return null;

        // 处理数组
        if (typeSymbol is IArrayTypeSymbol arrayType)
        {
            return arrayType.ElementType;
        }

        // 处理泛型集合
        if (typeSymbol is INamedTypeSymbol namedType)
        {
            return GetCollectionElementType(namedType, cancellationToken);
        }

        return null;
    }

    /// <summary>
    /// 获取字典类型的值类型符号
    /// </summary>
    private static ITypeSymbol? GetDictionaryValueTypeSymbol(ITypeSymbol? typeSymbol, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (typeSymbol == null) return null;

        if (typeSymbol is INamedTypeSymbol namedType)
        {
            var typeArgs = GetDictionaryTypeArguments(namedType, cancellationToken);
            return typeArgs?.ValueType;
        }

        return null;
    }

    /// <summary>
    /// 获取集合的元素类型
    /// </summary>
    private static ITypeSymbol? GetCollectionElementType(INamedTypeSymbol typeSymbol, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        // 如果是泛型集合类型，直接获取类型参数
        if (typeSymbol.IsGenericType && typeSymbol.TypeArguments.Length == 1)
        {
            return typeSymbol.TypeArguments[0];
        }

        // 查找 IEnumerable<T> 接口
        foreach (var iface in typeSymbol.AllInterfaces)
        {
            cancellationToken.ThrowIfCancellationRequested();

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
    private static bool IsComplexObjectType(ITypeSymbol typeSymbol, CancellationToken cancellationToken = default)
    {
        // 排除基本类型
        if (IsPrimitiveOrWellKnownType(typeSymbol, cancellationToken))
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
            if (IsDictionaryType(namedType, cancellationToken) ||
                IsCollectionType(namedType, cancellationToken) ||
                ImplementsNonGenericEnumerableOrDictionary(namedType, cancellationToken))
            {
                return false;
            }
        }

        // 类或结构体
        return typeSymbol.TypeKind == TypeKind.Class || typeSymbol.TypeKind == TypeKind.Struct;
    }

    private static bool ImplementsNonGenericEnumerableOrDictionary(INamedTypeSymbol typeSymbol, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var selfName = typeSymbol.ToDisplayString();
        if (selfName is "System.Collections.IDictionary" or "System.Collections.IEnumerable")
        {
            return true;
        }

        foreach (var iface in typeSymbol.AllInterfaces)
        {
            cancellationToken.ThrowIfCancellationRequested();

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
    private static bool HasEntityAttribute(ITypeSymbol typeSymbol, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        return HasAttribute(typeSymbol.GetAttributes(), "EntityAttribute");
    }

}
