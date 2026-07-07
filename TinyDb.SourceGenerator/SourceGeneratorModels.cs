using System.Collections.Generic;
using Microsoft.CodeAnalysis.CSharp;

namespace TinyDb.SourceGenerator;

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
        public string MetadataName { get; }
        public string ContainingTypePath { get; }
        public bool IsGenericType { get; }
        public string TypeParameterList { get; }
        public string TypeParameterConstraints { get; }
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
                    return FullyQualifiedTypeReference;
                }
                return FullyQualifiedTypeReference;
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
        public string FullyQualifiedTypeReference { get; }
        public string RuntimeFullName { get; }
        public string UniqueFileName { get; }
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

        public ClassInfo(string @namespace, string name, bool isValueType, List<PropertyInfo> properties, PropertyInfo? idProperty, string? collectionName = null, string? displayName = null, string? description = null, string? containingTypePath = null, DiagnosticLocationInfo? location = null, List<DependentComplexType>? dependentComplexTypes = null, List<CircularReferenceInfo>? circularReferences = null, List<EntityCircularReferenceInfo>? entityCircularReferences = null, List<BsonRefMissingEntityInfo>? bsonRefMissingEntityErrors = null, List<ConstructorParameterInfo>? constructorParameters = null, string? runtimeFullName = null, string? fullyQualifiedTypeReference = null, string? metadataName = null, bool isGenericType = false, string? typeParameterList = null, string? typeParameterConstraints = null)
        {
            Namespace = @namespace;
            Name = name;
            MetadataName = metadataName ?? name;
            IsValueType = isValueType;
            IsGenericType = isGenericType;
            TypeParameterList = typeParameterList ?? string.Empty;
            TypeParameterConstraints = typeParameterConstraints ?? string.Empty;
            Properties = properties;
            IdProperty = idProperty;
            CollectionName = collectionName;
            DisplayName = string.IsNullOrEmpty(displayName) ? name : displayName!;
            Description = string.IsNullOrEmpty(description) ? null : description;
            ContainingTypePath = containingTypePath ?? string.Empty;
            FullyQualifiedTypeReference = fullyQualifiedTypeReference ?? FullName;
            RuntimeFullName = runtimeFullName ?? FullName;
            UniqueFileName = CreateUniqueFileName(Namespace, ContainingTypePath, MetadataName);
            Location = location;
            DependentComplexTypes = dependentComplexTypes ?? new List<DependentComplexType>();
            CircularReferences = circularReferences ?? new List<CircularReferenceInfo>();
            EntityCircularReferences = entityCircularReferences ?? new List<EntityCircularReferenceInfo>();
            BsonRefMissingEntityErrors = bsonRefMissingEntityErrors ?? new List<BsonRefMissingEntityInfo>();
            ConstructorParameters = constructorParameters ?? new List<ConstructorParameterInfo>();
        }

        private static string CreateUniqueFileName(string @namespace, string containingTypePath, string name)
        {
            name = SanitizeHintNamePart(name);
            if (string.IsNullOrEmpty(@namespace))
            {
                return string.IsNullOrEmpty(containingTypePath)
                    ? name
                    : $"{containingTypePath}_{name}";
            }

            var namespacePart = @namespace.Replace(".", "_");
            return string.IsNullOrEmpty(containingTypePath)
                ? $"{namespacePart}_{name}"
                : $"{namespacePart}_{containingTypePath}_{name}";
        }

        private static string SanitizeHintNamePart(string value)
        {
            return value
                .Replace('`', '_')
                .Replace('<', '_')
                .Replace('>', '_')
                .Replace(',', '_')
                .Replace('.', '_');
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
