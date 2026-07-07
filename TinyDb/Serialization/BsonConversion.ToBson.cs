using System;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text.Json;
using TinyDb.Bson;

namespace TinyDb.Serialization;

public static partial class BsonConversion
{

    /// <summary>
    /// 将对象转换为 BsonValue。
    /// </summary>
    /// <param name="value">要转换的对象。</param>
    /// <returns>转换后的 BsonValue。</returns>
    public static BsonValue ToBsonValue(object value)
    {
        return value switch
        {
            null => BsonNull.Value,
            BsonValue bson => bson,
            byte[] bytes => new BsonBinary(bytes),
            ReadOnlyMemory<byte> rom => CreateBinary(rom),
            Memory<byte> mem => CreateBinary(mem),
            byte b => BsonInt32.FromValue(b),      // 使用缓存
            sbyte sb => BsonInt32.FromValue(sb),   // 使用缓存
            short s => BsonInt32.FromValue(s),     // 小整数使用缓存
            ushort us => BsonInt32.FromValue(us),  // 小整数使用缓存
            uint ui => new BsonInt64(ui),
            ulong ul => ul <= long.MaxValue ? new BsonInt64((long)ul) : new BsonDecimal128((decimal)ul),
            string str => new BsonString(str),
            int i => BsonInt32.FromValue(i),       // 使用缓存
            long l => new BsonInt64(l),
            double d => new BsonDouble(d),
            float f => new BsonDouble(f),
            decimal dec => new BsonDecimal128(dec),
            bool b => BsonBoolean.FromValue(b),    // 使用单例
            DateTime dt => new BsonDateTime(dt),
            Guid guid => new BsonBinary(guid),
            ObjectId oid => new BsonObjectId(oid),
            JsonElement jsonElement => ConvertJsonElementToBsonValue(jsonElement),
            JsonDocument jsonDocument => ConvertJsonElementToBsonValue(jsonDocument.RootElement),
            Enum enumValue => ConvertEnumToBsonValue(enumValue),
            System.Collections.IDictionary dict => ConvertDictionaryToBsonDocument(dict),
            System.Collections.IEnumerable enumerable => ConvertEnumerableToBsonArray(enumerable),
            _ => ConvertComplexObjectToBsonValue(value)
        };
    }


    /// <summary>
    /// 将枚举转换为BsonValue
    /// </summary>
    private static BsonValue ConvertEnumToBsonValue(Enum enumValue)
    {
        // 将枚举转换为其基础类型（通常是int32）或字符串表示
        var underlyingType = Enum.GetUnderlyingType(enumValue.GetType());
        var convertedValue = Convert.ChangeType(enumValue, underlyingType);
        return ToBsonValue(convertedValue!);
    }


    private static BsonBinary CreateBinary(ReadOnlyMemory<byte> memory)
    {
        return MemoryMarshal.TryGetArray(memory, out var segment) &&
               segment.Array != null &&
               segment.Offset == 0 &&
               segment.Count == segment.Array.Length
            ? new BsonBinary(segment.Array)
            : new BsonBinary(memory.Span);
    }


    private static BsonValue ConvertJsonElementToBsonValue(JsonElement element)
    {
        return element.ValueKind switch
        {
            JsonValueKind.Undefined => BsonNull.Value,
            JsonValueKind.Null => BsonNull.Value,
            JsonValueKind.String => new BsonString(element.GetString() ?? string.Empty),
            JsonValueKind.True => BsonBoolean.True,
            JsonValueKind.False => BsonBoolean.False,
            JsonValueKind.Number => ConvertJsonNumberToBsonValue(element),
            JsonValueKind.Array => ConvertJsonArrayToBsonArray(element),
            JsonValueKind.Object => ConvertJsonObjectToBsonDocument(element),
            _ => new BsonString(element.GetRawText())
        };
    }


    private static BsonValue ConvertJsonNumberToBsonValue(JsonElement element)
    {
        if (element.TryGetInt32(out var intValue))
        {
            return BsonInt32.FromValue(intValue);
        }

        if (element.TryGetInt64(out var longValue))
        {
            return new BsonInt64(longValue);
        }

        if (element.TryGetDecimal(out var decimalValue))
        {
            return new BsonDecimal128(decimalValue);
        }

        return new BsonDouble(element.GetDouble());
    }


    private static BsonDocument ConvertJsonObjectToBsonDocument(JsonElement element)
    {
        var document = new BsonDocument();
        foreach (var property in element.EnumerateObject())
        {
            document = document.Set(property.Name, ConvertJsonElementToBsonValue(property.Value));
        }

        return document;
    }


    private static BsonArray ConvertJsonArrayToBsonArray(JsonElement element)
    {
        var values = new List<BsonValue>();
        foreach (var item in element.EnumerateArray())
        {
            values.Add(ConvertJsonElementToBsonValue(item));
        }

        return new BsonArray(values);
    }


    /// <summary>
    /// 将复杂对象转换为BsonValue（通过AotBsonMapper）
    /// </summary>
    private static BsonValue ConvertComplexObjectToBsonValue(object value)
    {
        EnterConversion(value);
        try
        {
        var type = value.GetType();

        // 检查是否为复杂对象类型
        if (IsComplexObjectType(type))
        {
            // 使用注册的AOT适配器（AOT兼容）
            if (AotHelperRegistry.TryGetUntypedAdapter(type, out var adapter))
            {
                return adapter.ToDocumentUntyped(value);
            }

            throw new InvalidOperationException(
                $"Type '{type.FullName}' must have [Entity] attribute for AOT serialization. " +
                $"Add [Entity] attribute to the type to enable source generator support.");
        }

        throw new NotSupportedException($"Unsupported type for BSON conversion: {type.FullName}");
        }
        finally
        {
            ExitConversion(value);
        }
    }


    /// <summary>
    /// 将Dictionary转换为BsonDocument
    /// </summary>
    private static BsonDocument ConvertDictionaryToBsonDocument(System.Collections.IDictionary dictionary)
    {
        EnterConversion(dictionary);
        try
        {
        var document = new BsonDocument();
        foreach (System.Collections.DictionaryEntry entry in dictionary)
        {
            if (entry.Key is not string key)
            {
                throw new NotSupportedException("Only string keys are supported for BSON documents.");
            }

            var value = ToBsonValue(entry.Value!);
            document = document.Set(key, value);
        }
        return document;
        }
        finally
        {
            ExitConversion(dictionary);
        }
    }


    /// <summary>
    /// 将Enumerable转换为BsonArray
    /// </summary>
    private static BsonArray ConvertEnumerableToBsonArray(System.Collections.IEnumerable enumerable)
    {
        EnterConversion(enumerable);
        try
        {
        var list = new List<BsonValue>();
        foreach (var item in enumerable)
        {
            list.Add(ToBsonValue(item!));
        }
        return new BsonArray(list);
        }
        finally
        {
            ExitConversion(enumerable);
        }
    }


    private static void EnterConversion(object value)
    {
        if (++_conversionDepth > MaxConversionDepth)
        {
            _conversionDepth--;
            throw new InvalidOperationException($"BSON conversion nesting depth exceeds {MaxConversionDepth}.");
        }

        var serializingObjects = _serializingObjects ??= new HashSet<object>(ReferenceEqualityComparer.Instance);
        if (!serializingObjects.Add(value))
        {
            _conversionDepth--;
            throw new InvalidOperationException("Circular reference detected during BSON conversion.");
        }
    }


    private static void ExitConversion(object value)
    {
        _serializingObjects?.Remove(value);
        if (_serializingObjects is { Count: 0 })
        {
            _serializingObjects = null;
        }

        _conversionDepth--;
    }


    /// <summary>
    /// 将值转换为BsonValue（用于源代码生成器）
    /// </summary>
    public static BsonValue ConvertToBsonValue(object value)
    {
        return ToBsonValue(value);
    }

}
