using System;
using System.Collections.Generic;

namespace TinyDb.SourceGenerator;

/// <summary>
/// Compares generator input models for incremental source generation.
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
