using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using TinyDb.Bson;

namespace TinyDb.Serialization;

public static partial class AotBsonMapper
{
    /// <summary>
    /// 判断是否为Dictionary类型
    /// </summary>
    private static bool IsDictionaryType(Type type) => type != null && typeof(IDictionary).IsAssignableFrom(type);

    /// <summary>
    /// 将Dictionary转换为BsonDocument
    /// </summary>
    private static BsonDocument ConvertDictionaryToBsonDocument(object dictionary)
    {
        if (dictionary == null) throw new ArgumentNullException(nameof(dictionary));

        if (dictionary is not IDictionary rawDictionary)
        {
            throw new ArgumentException($"对象 {dictionary.GetType().FullName} 未实现 IDictionary 接口，无法在AOT回退模式下进行序列化。", nameof(dictionary));
        }

        var builder = new BsonDocumentBuilder(rawDictionary.Count);

        foreach (DictionaryEntry entry in rawDictionary)
        {
            if (entry.Key is not string key)
            {
                throw new NotSupportedException("AOT 回退仅支持字符串键的字典。");
            }

            var bsonValue = entry.Value != null
                ? ConvertToBsonValue(entry.Value)
                : BsonNull.Value;
            builder.Set(key, bsonValue);
        }

        return builder.Build();
    }

    private static object? ConvertDictionary([DynamicallyAccessedMembers(TypeInspectionRequirements)] Type dictionaryType, BsonDocument document)
    {
        if (document == null) return null;

        if (dictionaryType.IsInterface || dictionaryType.IsAbstract)
        {
            throw new NotSupportedException($"AOT 回退模式不支持接口/抽象字典类型 {dictionaryType.FullName}，请使用具体的 Dictionary<TKey, TValue>。");
        }

        if (!dictionaryType.IsGenericType || dictionaryType.GetGenericArguments().Length != 2)
        {
            throw new NotSupportedException($"字典类型 {dictionaryType.FullName} 不是有效的泛型字典类型。");
        }

        var args = dictionaryType.GetGenericArguments();
        var keyType = args[0];

        if (keyType != typeof(string))
        {
            throw new NotSupportedException($"AOT 回退模式仅支持字符串键的字典，但实际键类型为 {keyType.FullName}。");
        }

        if (dictionaryType == typeof(Dictionary<string, int>))
        {
            var typedDictionary = new Dictionary<string, int>(document.Count, StringComparer.Ordinal);
            foreach (var element in document.Entries)
            {
                typedDictionary[element.Key] = ConvertToInt32(element.Value);
            }

            return typedDictionary;
        }

        if (dictionaryType == typeof(Dictionary<string, string>))
        {
            var typedDictionary = new Dictionary<string, string?>(document.Count, StringComparer.Ordinal);
            foreach (var element in document.Entries)
            {
                typedDictionary[element.Key] = ConvertToNullableString(element.Value);
            }

            return typedDictionary;
        }

        if (dictionaryType == typeof(Dictionary<string, object>))
        {
            var typedDictionary = new Dictionary<string, object?>(document.Count, StringComparer.Ordinal);
            foreach (var element in document.Entries)
            {
                typedDictionary[element.Key] = UnwrapBsonValue(element.Value);
            }

            return typedDictionary;
        }

        throw new NotSupportedException($"AOT fallback does not support dictionary type '{dictionaryType.FullName}'.");
    }

}
