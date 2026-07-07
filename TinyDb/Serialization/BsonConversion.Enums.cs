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

    public static TEnum FromBsonValueEnum<TEnum>(BsonValue bsonValue)
        where TEnum : struct, Enum
    {
        if (bsonValue == null) throw new ArgumentNullException(nameof(bsonValue));

        if (bsonValue is BsonString stringValue)
        {
            return Enum.Parse<TEnum>(stringValue.Value, ignoreCase: true);
        }

        if (bsonValue.IsNull)
        {
            return default;
        }

        var enumType = typeof(TEnum);
        var underlyingType = Enum.GetUnderlyingType(enumType);

        if (underlyingType == typeof(int))
        {
            var value = bsonValue switch
            {
                BsonInt32 i32 => i32.Value,
                BsonInt64 i64 => checked((int)i64.Value),
                BsonDouble dbl => Convert.ToInt32(dbl.Value),
                _ => Convert.ToInt32(bsonValue.ToString())
            };
            return CastEnum<int, TEnum>(value);
        }

        if (underlyingType == typeof(long))
        {
            var value = bsonValue switch
            {
                BsonInt64 i64 => i64.Value,
                BsonInt32 i32 => i32.Value,
                BsonDouble dbl => Convert.ToInt64(dbl.Value),
                _ => Convert.ToInt64(bsonValue.ToString())
            };
            return CastEnum<long, TEnum>(value);
        }

        if (underlyingType == typeof(short))
        {
            var value = bsonValue switch
            {
                BsonInt32 i32 => (short)i32.Value,
                BsonInt64 i64 => checked((short)i64.Value),
                BsonDouble dbl => Convert.ToInt16(dbl.Value),
                _ => Convert.ToInt16(bsonValue.ToString())
            };
            return CastEnum<short, TEnum>(value);
        }

        if (underlyingType == typeof(byte))
        {
            var value = bsonValue switch
            {
                BsonInt32 i32 => (byte)i32.Value,
                BsonInt64 i64 => checked((byte)i64.Value),
                BsonDouble dbl => Convert.ToByte(dbl.Value),
                _ => Convert.ToByte(bsonValue.ToString())
            };
            return CastEnum<byte, TEnum>(value);
        }

        if (underlyingType == typeof(uint))
        {
            var value = bsonValue switch
            {
                BsonInt32 i32 => checked((uint)i32.Value),
                BsonInt64 i64 => checked((uint)i64.Value),
                BsonDouble dbl => Convert.ToUInt32(dbl.Value),
                _ => Convert.ToUInt32(bsonValue.ToString())
            };
            return CastEnum<uint, TEnum>(value);
        }

        if (underlyingType == typeof(ulong))
        {
            var value = bsonValue switch
            {
                BsonInt64 i64 => checked((ulong)i64.Value),
                BsonInt32 i32 => checked((ulong)i32.Value),
                BsonDouble dbl => Convert.ToUInt64(dbl.Value),
                _ => Convert.ToUInt64(bsonValue.ToString())
            };
            return CastEnum<ulong, TEnum>(value);
        }

        if (underlyingType == typeof(ushort))
        {
            var value = bsonValue switch
            {
                BsonInt32 i32 => checked((ushort)i32.Value),
                BsonInt64 i64 => checked((ushort)i64.Value),
                BsonDouble dbl => Convert.ToUInt16(dbl.Value),
                _ => Convert.ToUInt16(bsonValue.ToString())
            };
            return CastEnum<ushort, TEnum>(value);
        }

        if (underlyingType == typeof(sbyte))
        {
            var value = bsonValue switch
            {
                BsonInt32 i32 => checked((sbyte)i32.Value),
                BsonInt64 i64 => checked((sbyte)i64.Value),
                BsonDouble dbl => Convert.ToSByte(dbl.Value),
                _ => Convert.ToSByte(bsonValue.ToString())
            };
            return CastEnum<sbyte, TEnum>(value);
        }

        return Enum.Parse<TEnum>(bsonValue.ToString(), ignoreCase: true);
    }


    private static TEnum CastEnum<TUnderlying, TEnum>(TUnderlying value)
        where TEnum : struct, Enum
    {
        return Unsafe.As<TUnderlying, TEnum>(ref value);
    }

}
