using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;
using TinyDb.Bson;

namespace TinyDb.Serialization;

public static partial class AotBsonMapper
{
    /// <summary>
    /// 判断是否为集合类型
    /// </summary>
    private static bool IsCollectionType(Type type) =>
        type != null &&
        type != typeof(string) &&
        type != typeof(byte[]) &&
        typeof(IEnumerable).IsAssignableFrom(type) &&
        !IsDictionaryType(type);

    /// <summary>
    /// 将集合转换为BsonArray
    /// </summary>
    private static BsonArray ConvertCollectionToBsonArray(object collection)
    {
        if (collection == null) throw new ArgumentNullException(nameof(collection));

        if (collection is not IEnumerable enumerable)
        {
            throw new ArgumentException("集合类型必须实现 IEnumerable 接口。", nameof(collection));
        }

        var serializingObjects = GetOrCreateSerializingObjects();
        if (!serializingObjects.Add(collection))
        {
            throw new InvalidOperationException("Circular reference detected while serializing a collection.");
        }

        try
        {
            var builder = ImmutableList.CreateBuilder<BsonValue>();

            foreach (var item in enumerable)
            {
                if (item == null)
                {
                    builder.Add(BsonNull.Value);
                    continue;
                }

                var bsonValue = ConvertToBsonValue(item);
                builder.Add(bsonValue);
            }

            return new BsonArray(builder);
        }
        finally
        {
            serializingObjects.Remove(collection);
            if (serializingObjects.Count == 0)
            {
                SerializingObjects.Value = null;
            }
        }
    }

    private static object? TryWrapWithTargetCollection([DynamicallyAccessedMembers(TypeInspectionRequirements)] Type targetCollectionType, object sourceCollection)
    {
        if (targetCollectionType == null) throw new ArgumentNullException(nameof(targetCollectionType));
        if (sourceCollection == null) throw new ArgumentNullException(nameof(sourceCollection));

        return null;
    }

    private static object? TryCreateCollectionFromListCtor([DynamicallyAccessedMembers(TypeInspectionRequirements)] Type collectionType, BsonArray array)
    {
        if (collectionType == null) throw new ArgumentNullException(nameof(collectionType));
        if (array == null) return null;

        return null;
    }

    private static T[] ConvertArray<T>(BsonArray array, Func<BsonValue, T> convert)
    {
        var values = new T[array.Count];
        for (int i = 0; i < array.Count; i++)
        {
            values[i] = convert(array[i]);
        }

        return values;
    }

    private static T ConvertScalarArrayValue<T>(BsonValue bsonValue)
    {
        if (bsonValue == null || bsonValue.IsNull)
        {
            return default!;
        }

        return (T)ConvertPrimitiveValue(bsonValue, typeof(T));
    }

    [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "Array element types are restricted to scalars/enums, BsonValue, or recursively supported containers; their array code is available.")]
    [UnconditionalSuppressMessage("TrimAnalysis", "IL2067", Justification = "Element types are restricted to scalars/enums, BsonValue, or recursively supported containers that do not require adapter metadata.")]
    private static Array ConvertArrayOfElements(BsonArray array, Type elementType)
    {
        var values = Array.CreateInstance(elementType, array.Count);
        for (var i = 0; i < array.Count; i++)
        {
            values.SetValue(ConvertFromBsonValue(array[i], elementType), i);
        }

        return values;
    }

    [UnconditionalSuppressMessage("TrimAnalysis", "IL2067", Justification = "Element types are restricted to scalars/enums, BsonValue, or recursively supported containers that do not require adapter metadata.")]
    private static IList ConvertListOfElements(BsonArray array, [DynamicallyAccessedMembers(TypeInspectionRequirements)] Type listType, Type elementType)
    {
        var list = (IList)Activator.CreateInstance(listType)!;
        foreach (var bsonValue in array)
        {
            list.Add(ConvertFromBsonValue(bsonValue, elementType));
        }

        return list;
    }

    [UnconditionalSuppressMessage("TrimAnalysis", "IL2067", Justification = "Element types are restricted to scalars/enums, BsonValue, or recursively supported containers that do not require adapter metadata.")]
    [UnconditionalSuppressMessage("TrimAnalysis", "IL2072", Justification = "Element types are restricted to scalars/enums, BsonValue, or recursively supported containers that do not require adapter metadata.")]
    private static object ConvertHashSetOfElements(BsonArray array, [DynamicallyAccessedMembers(TypeInspectionRequirements)] Type hashSetType, Type elementType)
    {
        var values = ConvertArrayOfElements(array, elementType);
        return Activator.CreateInstance(hashSetType, new object?[] { values })!;
    }

    private static object? ConvertCollection([DynamicallyAccessedMembers(TypeInspectionRequirements)] Type collectionType, BsonArray array)
    {
        if (array == null) return null;

        if (collectionType.IsArray)
        {
            if (collectionType.GetArrayRank() != 1)
            {
                throw new NotSupportedException($"AOT fallback does not support multi-dimensional array type '{collectionType.FullName}'.");
            }

            if (collectionType == typeof(int[])) return ConvertArray(array, ConvertToInt32);
            if (collectionType == typeof(long[])) return ConvertArray(array, static value => ConvertScalarArrayValue<long>(value));
            if (collectionType == typeof(short[])) return ConvertArray(array, static value => ConvertScalarArrayValue<short>(value));
            if (collectionType == typeof(ushort[])) return ConvertArray(array, static value => ConvertScalarArrayValue<ushort>(value));
            if (collectionType == typeof(uint[])) return ConvertArray(array, static value => ConvertScalarArrayValue<uint>(value));
            if (collectionType == typeof(ulong[])) return ConvertArray(array, static value => ConvertScalarArrayValue<ulong>(value));
            if (collectionType == typeof(sbyte[])) return ConvertArray(array, static value => ConvertScalarArrayValue<sbyte>(value));
            if (collectionType == typeof(float[])) return ConvertArray(array, static value => ConvertScalarArrayValue<float>(value));
            if (collectionType == typeof(double[])) return ConvertArray(array, static value => ConvertScalarArrayValue<double>(value));
            if (collectionType == typeof(decimal[])) return ConvertArray(array, static value => ConvertScalarArrayValue<decimal>(value));
            if (collectionType == typeof(bool[])) return ConvertArray(array, ConvertToBoolean);
            if (collectionType == typeof(char[])) return ConvertArray(array, static value => ConvertScalarArrayValue<char>(value));
            if (collectionType == typeof(string[])) return ConvertArray(array, ConvertToNullableString);
            if (collectionType == typeof(DateTime[])) return ConvertArray(array, static value => ConvertScalarArrayValue<DateTime>(value));
            if (collectionType == typeof(Guid[])) return ConvertArray(array, static value => ConvertScalarArrayValue<Guid>(value));
            if (collectionType == typeof(ObjectId[])) return ConvertArray(array, static value => ConvertScalarArrayValue<ObjectId>(value));
            if (collectionType == typeof(object[])) return ConvertArray(array, UnwrapBsonValue);

            var arrayElementType = collectionType.GetElementType();
            if (arrayElementType != null && AotFallbackTypes.IsSupportedElementType(arrayElementType))
            {
                return ConvertArrayOfElements(array, arrayElementType);
            }

            throw new NotSupportedException($"AOT fallback does not support array type '{collectionType.FullName}'.");
        }

        if (collectionType.IsInterface || collectionType.IsAbstract)
        {
            throw new NotSupportedException($"AOT 回退模式不支持接口/抽象集合类型 {collectionType.FullName}，请使用具体的 List<T> 或 ArrayList。");
        }

        if (collectionType == typeof(List<int>))
        {
            var list = new List<int>(array.Count);
            foreach (var bsonValue in array)
            {
                list.Add(ConvertToInt32(bsonValue));
            }

            return list;
        }

        if (collectionType == typeof(List<string>))
        {
            var list = new List<string?>(array.Count);
            foreach (var bsonValue in array)
            {
                list.Add(ConvertToNullableString(bsonValue));
            }

            return list;
        }

        if (collectionType == typeof(List<object>))
        {
            var list = new List<object?>(array.Count);
            foreach (var bsonValue in array)
            {
                list.Add(ConvertFromBsonValue(bsonValue, typeof(object)));
            }

            return list;
        }

        if (collectionType.IsGenericType &&
            collectionType.GetGenericTypeDefinition() == typeof(HashSet<>))
        {
            var hashSetElementType = collectionType.GetGenericArguments()[0];
            if (AotFallbackTypes.IsSupportedElementType(hashSetElementType))
            {
                return ConvertHashSetOfElements(array, collectionType, hashSetElementType);
            }
        }

        if (collectionType.IsGenericType &&
            collectionType.GetGenericTypeDefinition() == typeof(List<>))
        {
            var elementType = collectionType.GetGenericArguments()[0];
            if (AotFallbackTypes.IsSupportedElementType(elementType))
            {
                return ConvertListOfElements(array, collectionType, elementType);
            }
        }

        if (collectionType.IsGenericType &&
            collectionType.GetGenericArguments().Length == 1 &&
            typeof(IEnumerable).IsAssignableFrom(collectionType))
        {
            var elementType = collectionType.GetGenericArguments()[0];
            if (AotFallbackTypes.IsSupportedElementType(elementType))
            {
                var values = ConvertArrayOfElements(array, elementType);

                // Stack/ConcurrentStack 的 IEnumerable 构造函数按枚举顺序压栈，
                // 逆序后构造可保持序列化前后的枚举顺序一致。
                if (collectionType.GetGenericTypeDefinition() == typeof(Stack<>) ||
                    collectionType.GetGenericTypeDefinition() == typeof(System.Collections.Concurrent.ConcurrentStack<>))
                {
                    Array.Reverse(values);
                }

                if (AotCollectionFactory.TryCreateFromEnumerable(collectionType, values, out var instance))
                {
                    return instance;
                }
            }
        }

        throw new NotSupportedException($"AOT fallback does not support collection type '{collectionType.FullName}'.");
    }

}
