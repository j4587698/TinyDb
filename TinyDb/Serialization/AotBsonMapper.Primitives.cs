using System;
using System.Globalization;
using TinyDb.Bson;

namespace TinyDb.Serialization;

public static partial class AotBsonMapper
{
    private static int ConvertToInt32(BsonValue bsonValue)
    {
        if (bsonValue == null || bsonValue.IsNull)
        {
            return default;
        }

        return bsonValue switch
        {
            BsonInt32 i32 => i32.Value,
            BsonInt64 i64 => checked((int)i64.Value),
            BsonDouble dbl => Convert.ToInt32(dbl.Value),
            BsonDecimal128 dec => checked((int)dec.Value.ToDecimal()),
            BsonString s => int.Parse(s.Value, CultureInfo.InvariantCulture),
            _ => Convert.ToInt32(bsonValue.ToString(), CultureInfo.InvariantCulture)
        };
    }

    private static string? ConvertToNullableString(BsonValue bsonValue)
    {
        return bsonValue == null || bsonValue.IsNull ? null : bsonValue.ToString();
    }

    private static bool ConvertToBoolean(BsonValue bsonValue)
    {
        if (bsonValue == null || bsonValue.IsNull)
        {
            return default;
        }

        return bsonValue switch
        {
            BsonBoolean b => b.Value,
            BsonString s => bool.Parse(s.Value),
            BsonInt32 i => Convert.ToBoolean(i.Value),
            BsonInt64 l => Convert.ToBoolean(l.Value),
            BsonDouble d => Convert.ToBoolean(d.Value),
            BsonDecimal128 dec => Convert.ToBoolean(dec.Value),
            _ => Convert.ToBoolean(bsonValue.ToString())
        };
    }


    private static object ConvertPrimitiveValue(BsonValue bsonValue, Type targetType)
    {
        if (targetType == typeof(BsonValue) || targetType.IsAssignableFrom(bsonValue.GetType()))
        {
            return bsonValue;
        }

        return targetType switch
        {
            var t when t == typeof(string) => bsonValue.ToString(),
            var t when t == typeof(bool) => bsonValue switch
            {
                BsonBoolean b => b.Value,
                BsonString s => bool.Parse(s.Value),
                BsonInt32 i => Convert.ToBoolean(i.Value),
                BsonInt64 l => Convert.ToBoolean(l.Value),
                BsonDouble d => Convert.ToBoolean(d.Value),
                BsonDecimal128 dec => Convert.ToBoolean(dec.Value),
                _ => Convert.ToBoolean(bsonValue.ToString())
            },
            var t when t == typeof(int) => bsonValue switch
            {
                BsonInt32 i32 => i32.Value,
                BsonInt64 i64 => checked((int)i64.Value),
                BsonDouble dbl => Convert.ToInt32(dbl.Value),
                BsonDecimal128 dec => checked((int)dec.Value.ToDecimal()),
                BsonString s => int.Parse(s.Value, CultureInfo.InvariantCulture),
                _ => Convert.ToInt32(bsonValue.ToString(), CultureInfo.InvariantCulture)
            },
            var t when t == typeof(long) => bsonValue switch
            {
                BsonInt64 i64 => i64.Value,
                BsonInt32 i32 => i32.Value,
                BsonDouble dbl => Convert.ToInt64(dbl.Value),
                BsonDecimal128 dec => checked((long)dec.Value.ToDecimal()),
                BsonString s => long.Parse(s.Value, CultureInfo.InvariantCulture),
                _ => Convert.ToInt64(bsonValue.ToString(), CultureInfo.InvariantCulture)
            },
            var t when t == typeof(byte) => bsonValue switch
            {
                BsonInt32 i32 => checked((byte)i32.Value),
                BsonInt64 i64 => checked((byte)i64.Value),
                BsonDouble dbl => checked((byte)dbl.Value),
                BsonDecimal128 dec => checked((byte)dec.Value.ToDecimal()),
                BsonString s => byte.Parse(s.Value, CultureInfo.InvariantCulture),
                _ => Convert.ToByte(bsonValue.ToString(), CultureInfo.InvariantCulture)
            },
            var t when t == typeof(sbyte) => bsonValue switch
            {
                BsonInt32 i32 => checked((sbyte)i32.Value),
                BsonInt64 i64 => checked((sbyte)i64.Value),
                BsonDouble dbl => checked((sbyte)dbl.Value),
                BsonDecimal128 dec => checked((sbyte)dec.Value.ToDecimal()),
                BsonString s => sbyte.Parse(s.Value, CultureInfo.InvariantCulture),
                _ => Convert.ToSByte(bsonValue.ToString(), CultureInfo.InvariantCulture)
            },
            var t when t == typeof(uint) => bsonValue switch
            {
                BsonInt32 i32 => checked((uint)i32.Value),
                BsonInt64 i64 => checked((uint)i64.Value),
                BsonDouble dbl => checked((uint)dbl.Value),
                BsonDecimal128 dec => checked((uint)dec.Value.ToDecimal()),
                BsonString s => uint.Parse(s.Value, CultureInfo.InvariantCulture),
                _ => Convert.ToUInt32(bsonValue.ToString(), CultureInfo.InvariantCulture)
            },
            var t when t == typeof(ulong) => bsonValue switch
            {
                BsonInt64 i64 => checked((ulong)i64.Value),
                BsonInt32 i32 => checked((ulong)i32.Value),
                BsonDouble dbl => checked((ulong)dbl.Value),
                BsonDecimal128 dec => checked((ulong)dec.Value.ToDecimal()),
                BsonString s => ulong.Parse(s.Value, CultureInfo.InvariantCulture),
                _ => Convert.ToUInt64(bsonValue.ToString(), CultureInfo.InvariantCulture)
            },
            var t when t == typeof(ushort) => bsonValue switch
            {
                BsonInt32 i32 => checked((ushort)i32.Value),
                BsonInt64 i64 => checked((ushort)i64.Value),
                BsonDouble dbl => checked((ushort)dbl.Value),
                BsonDecimal128 dec => checked((ushort)dec.Value.ToDecimal()),
                BsonString s => ushort.Parse(s.Value, CultureInfo.InvariantCulture),
                _ => Convert.ToUInt16(bsonValue.ToString(), CultureInfo.InvariantCulture)
            },
            var t when t == typeof(double) => bsonValue switch
            {
                BsonDouble dbl => dbl.Value,
                BsonInt64 i64 => i64.Value,
                BsonInt32 i32 => i32.Value,
                BsonDecimal128 dec => (double)dec.Value.ToDecimal(),
                BsonString s => double.Parse(s.Value, CultureInfo.InvariantCulture),
                _ => Convert.ToDouble(bsonValue.ToString(), CultureInfo.InvariantCulture)
            },
            var t when t == typeof(float) =>
                Convert.ToSingle(ConvertPrimitiveValue(bsonValue, typeof(double)), CultureInfo.InvariantCulture),
            var t when t == typeof(decimal) => bsonValue switch
            {
                BsonDecimal128 dec => dec.Value.ToDecimal(),
                BsonDouble dbl => Convert.ToDecimal(dbl.Value, CultureInfo.InvariantCulture),
                BsonInt32 i32 => (decimal)i32.Value,
                BsonInt64 i64 => (decimal)i64.Value,
                BsonString s => decimal.Parse(s.Value, CultureInfo.InvariantCulture),
                _ => Convert.ToDecimal(bsonValue.ToString(), CultureInfo.InvariantCulture)
            },
            var t when t == typeof(short) => bsonValue switch
            {
                BsonInt32 i32 => checked((short)i32.Value),
                BsonInt64 i64 => checked((short)i64.Value),
                BsonDouble dbl => checked((short)dbl.Value),
                BsonDecimal128 dec => checked((short)dec.Value.ToDecimal()),
                BsonString s => short.Parse(s.Value, CultureInfo.InvariantCulture),
                _ => Convert.ToInt16(bsonValue.ToString(), CultureInfo.InvariantCulture)
            },
            var t when t == typeof(char) => bsonValue switch
            {
                BsonInt32 i32 => checked((char)i32.Value),
                BsonString s when s.Value.Length > 0 => s.Value[0],
                _ => Convert.ToChar(bsonValue.ToString(), CultureInfo.InvariantCulture)
            },
            var t when t == typeof(DateTime) => bsonValue switch
            {
                BsonDateTime date => date.Value,
                BsonString s => DateTime.Parse(s.Value, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind),
                _ => DateTime.Parse(bsonValue.ToString(), CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind)
            },
            var t when t == typeof(DateTimeOffset) => bsonValue switch
            {
                BsonString s => DateTimeOffset.Parse(s.Value, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind),
                BsonDateTime date => new DateTimeOffset(date.Value),
                _ => DateTimeOffset.Parse(bsonValue.ToString(), CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind)
            },
            var t when t == typeof(TimeSpan) => bsonValue switch
            {
                BsonInt64 i64 => TimeSpan.FromTicks(i64.Value),
                BsonInt32 i32 => TimeSpan.FromTicks(i32.Value),
                BsonString s => TimeSpan.Parse(s.Value, CultureInfo.InvariantCulture),
                _ => TimeSpan.Parse(bsonValue.ToString(), CultureInfo.InvariantCulture)
            },
            var t when t == typeof(Guid) => bsonValue switch
            {
                BsonBinary binary when binary.SubType is BsonBinary.BinarySubType.Uuid or BsonBinary.BinarySubType.UuidLegacy => new Guid(binary.Bytes),
                BsonString s => Guid.Parse(s.Value),
                _ => Guid.Parse(bsonValue.ToString())
            },
            var t when t == typeof(ObjectId) => bsonValue switch
            {
                BsonObjectId objectId => objectId.Value,
                BsonString s => ObjectId.Parse(s.Value),
                _ => ObjectId.Parse(bsonValue.ToString())
            },
            var t when t.IsEnum => bsonValue switch
            {
                BsonInt32 i32 => Enum.ToObject(t, i32.Value),
                BsonInt64 i64 => Enum.ToObject(t, i64.Value),
                BsonDouble dbl => Enum.ToObject(t, Convert.ToInt64(dbl.Value)),
                BsonDecimal128 dec => Enum.ToObject(t, Convert.ToInt64(dec.Value.ToDecimal())),
                BsonString s when long.TryParse(s.Value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed) =>
                    Enum.ToObject(t, parsed),
                BsonString s =>
                    throw new InvalidOperationException($"Cannot convert '{s.Value}' to enum '{t.FullName}' in AOT mode. Use AotBsonMapper.ConvertEnumValue<TEnum>() for name-based parsing."),
                _ => throw new InvalidOperationException($"Cannot convert {bsonValue.GetType().Name} to enum '{t.FullName}' in AOT mode.")
            },
            var t when t == typeof(byte[]) => bsonValue switch
            {
                BsonBinary binary => binary.Bytes,
                BsonString s => Convert.FromBase64String(s.Value),
                _ => throw new InvalidOperationException($"Cannot convert {bsonValue.GetType().Name} to byte[].")
            },
            _ => throw new NotSupportedException($"Unsupported conversion from BSON type '{bsonValue.GetType().Name}' to '{targetType.FullName}'. 为此类型注册源生成器适配器以获得完整支持。")
        };
    }}
