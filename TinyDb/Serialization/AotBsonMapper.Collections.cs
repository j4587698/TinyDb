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

        throw new NotSupportedException($"AOT fallback does not support collection type '{collectionType.FullName}'.");
    }

}
