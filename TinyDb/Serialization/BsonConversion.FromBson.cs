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

    public static T? FromBsonValue<T>(BsonValue bsonValue)
    {
        if (bsonValue == null) throw new ArgumentNullException(nameof(bsonValue));

        if (bsonValue.IsNull)
        {
            return default;
        }

        var targetType = typeof(T);
        object? converted;

        if (targetType == typeof(object))
        {
            converted = bsonValue switch
            {
                BsonInt32 i32 => i32.Value,
                BsonInt64 i64 => i64.Value,
                BsonString str => str.Value,
                BsonBoolean b => b.Value,
                BsonDouble d => d.Value,
                BsonDecimal128 dec => dec.Value.ToDecimal(),
                BsonDateTime dt => dt.Value,
                BsonObjectId oid => oid.Value,
                BsonDocument doc => doc.ToDictionary(),
                BsonArray arr => arr.Select(static x => FromBsonValue<object>(x)).ToList(),
                BsonBinary bin => bin.SubType == BsonBinary.BinarySubType.Uuid ? new Guid(bin.Bytes) : bin.Bytes,
                _ => bsonValue.RawValue
            };
        }
        else if (targetType == typeof(string))
        {
            converted = bsonValue is BsonString bsonString ? bsonString.Value : bsonValue.ToString();
        }
        else if (targetType == typeof(int))
        {
            converted = bsonValue switch
            {
                BsonInt32 i32 => i32.Value,
                BsonInt64 i64 => checked((int)i64.Value),
                BsonDouble dbl => Convert.ToInt32(dbl.Value),
                _ => Convert.ToInt32(bsonValue.ToString(), CultureInfo.InvariantCulture)
            };
        }
        else if (targetType == typeof(long))
        {
            converted = bsonValue switch
            {
                BsonInt64 i64 => i64.Value,
                BsonInt32 i32 => i32.Value,
                BsonDouble dbl => Convert.ToInt64(dbl.Value),
                _ => Convert.ToInt64(bsonValue.ToString(), CultureInfo.InvariantCulture)
            };
        }
        else if (targetType == typeof(double))
        {
            converted = bsonValue is BsonDouble dbl ? dbl.Value : Convert.ToDouble(bsonValue.ToString(), CultureInfo.InvariantCulture);
        }
        else if (targetType == typeof(float))
        {
            converted = bsonValue is BsonDouble dbl ? (float)dbl.Value : Convert.ToSingle(bsonValue.ToString(), CultureInfo.InvariantCulture);
        }
        else if (targetType == typeof(decimal))
        {
            converted = bsonValue switch
            {
                BsonDecimal128 dec128 => dec128.Value.ToDecimal(),
                BsonDouble dbl => Convert.ToDecimal(dbl.Value),
                BsonInt32 i32 => Convert.ToDecimal(i32.Value),
                BsonInt64 i64 => Convert.ToDecimal(i64.Value),
                _ => Convert.ToDecimal(bsonValue.ToString(), CultureInfo.InvariantCulture)
            };
        }
        else if (targetType == typeof(bool))
        {
            converted = bsonValue is BsonBoolean bl ? bl.Value : Convert.ToBoolean(bsonValue.ToString(), CultureInfo.InvariantCulture);
        }
        else if (targetType == typeof(DateTime))
        {
            converted = bsonValue is BsonDateTime dt
                ? dt.Value
                : DateTime.Parse(bsonValue.ToString(), CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
        }
        else if (targetType == typeof(Guid))
        {
            converted = bsonValue is BsonString guidString
                ? Guid.Parse(guidString.Value)
                : bsonValue is BsonBinary guidBinary && (guidBinary.Bytes.Length == 16 || guidBinary.SubType == BsonBinary.BinarySubType.Uuid || guidBinary.SubType == BsonBinary.BinarySubType.UuidLegacy)
                    ? new Guid(guidBinary.Bytes)
                    : Guid.Parse(bsonValue.ToString());
        }
        else if (targetType == typeof(char))
        {
            converted = bsonValue switch
            {
                BsonInt32 i32 => checked((char)i32.Value),
                BsonString str when str.Value.Length > 0 => str.Value[0],
                _ => Convert.ToChar(bsonValue.ToString(), CultureInfo.InvariantCulture)
            };
        }
        else if (targetType == typeof(ObjectId))
        {
            converted = bsonValue is BsonObjectId oid ? oid.Value : ObjectId.Parse(bsonValue.ToString());
        }
        else if (targetType == typeof(byte))
        {
            converted = bsonValue switch
            {
                BsonInt32 i32 => checked((byte)i32.Value),
                BsonInt64 i64 => checked((byte)i64.Value),
                BsonDouble dbl => checked((byte)dbl.Value),
                BsonString str => byte.Parse(str.Value, CultureInfo.InvariantCulture),
                _ => Convert.ToByte(bsonValue.ToString(), CultureInfo.InvariantCulture)
            };
        }
        else if (targetType == typeof(sbyte))
        {
            converted = bsonValue switch
            {
                BsonInt32 i32 => checked((sbyte)i32.Value),
                BsonInt64 i64 => checked((sbyte)i64.Value),
                BsonDouble dbl => checked((sbyte)dbl.Value),
                BsonString str => sbyte.Parse(str.Value, CultureInfo.InvariantCulture),
                _ => Convert.ToSByte(bsonValue.ToString(), CultureInfo.InvariantCulture)
            };
        }
        else if (targetType == typeof(short))
        {
            converted = bsonValue switch
            {
                BsonInt32 i32 => checked((short)i32.Value),
                BsonInt64 i64 => checked((short)i64.Value),
                BsonDouble dbl => checked((short)dbl.Value),
                BsonString str => short.Parse(str.Value, CultureInfo.InvariantCulture),
                _ => Convert.ToInt16(bsonValue.ToString(), CultureInfo.InvariantCulture)
            };
        }
        else if (targetType == typeof(ushort))
        {
            converted = bsonValue switch
            {
                BsonInt32 i32 => checked((ushort)i32.Value),
                BsonInt64 i64 => checked((ushort)i64.Value),
                BsonDouble dbl => checked((ushort)dbl.Value),
                BsonString str => ushort.Parse(str.Value, CultureInfo.InvariantCulture),
                _ => Convert.ToUInt16(bsonValue.ToString(), CultureInfo.InvariantCulture)
            };
        }
        else if (targetType == typeof(uint))
        {
            converted = bsonValue switch
            {
                BsonInt64 i64 => checked((uint)i64.Value),
                BsonInt32 i32 => checked((uint)i32.Value),
                BsonDouble dbl => checked((uint)dbl.Value),
                BsonString str => uint.Parse(str.Value, CultureInfo.InvariantCulture),
                _ => Convert.ToUInt32(bsonValue.ToString(), CultureInfo.InvariantCulture)
            };
        }
        else if (targetType == typeof(ulong))
        {
            converted = bsonValue switch
            {
                BsonInt64 i64 => checked((ulong)i64.Value),
                BsonInt32 i32 => checked((ulong)i32.Value),
                BsonDouble dbl => checked((ulong)dbl.Value),
                BsonDecimal128 dec => checked((ulong)dec.Value.ToDecimal()),
                BsonString str => ulong.Parse(str.Value, CultureInfo.InvariantCulture),
                _ => Convert.ToUInt64(bsonValue.ToString(), CultureInfo.InvariantCulture)
            };
        }
        else if (targetType == typeof(byte[]))
        {
            converted = bsonValue switch
            {
                BsonBinary bin => bin.Bytes,
                BsonString str => Convert.FromBase64String(str.Value),
                _ => throw new InvalidOperationException($"Cannot convert '{bsonValue.GetType().Name}' to byte[].")
            };
        }
        else if (targetType == typeof(ReadOnlyMemory<byte>))
        {
            converted = bsonValue switch
            {
                BsonBinary bin => new ReadOnlyMemory<byte>(bin.Bytes),
                BsonString str => new ReadOnlyMemory<byte>(Convert.FromBase64String(str.Value)),
                _ => throw new InvalidOperationException($"Cannot convert '{bsonValue.GetType().Name}' to ReadOnlyMemory<byte>.")
            };
        }
        else if (targetType == typeof(Memory<byte>))
        {
            converted = bsonValue switch
            {
                BsonBinary bin => new Memory<byte>(bin.Bytes),
                BsonString str => new Memory<byte>(Convert.FromBase64String(str.Value)),
                _ => throw new InvalidOperationException($"Cannot convert '{bsonValue.GetType().Name}' to Memory<byte>.")
            };
        }
        else if (targetType == typeof(List<int>) && bsonValue is BsonArray intArray)
        {
            var list = new List<int>(intArray.Count);
            foreach (var item in intArray)
            {
                list.Add(FromBsonValue<int>(item));
            }

            converted = list;
        }
        else if (targetType == typeof(List<string>) && bsonValue is BsonArray stringArray)
        {
            var list = new List<string>(stringArray.Count);
            foreach (var item in stringArray)
            {
                list.Add(FromBsonValue<string>(item) ?? string.Empty);
            }

            converted = list;
        }
        else if (targetType == typeof(List<double>) && bsonValue is BsonArray doubleArray)
        {
            var list = new List<double>(doubleArray.Count);
            foreach (var item in doubleArray)
            {
                list.Add(FromBsonValue<double>(item));
            }

            converted = list;
        }
        else if (typeof(BsonValue).IsAssignableFrom(targetType) && targetType.IsInstanceOfType(bsonValue))
        {
            converted = bsonValue;
        }
        else if (targetType.IsEnum)
        {
            converted = ConvertFromBsonValueToEnum(bsonValue, targetType);
        }
        else
        {
            throw new NotSupportedException(
                $"AOT generic conversion does not support target type '{targetType.FullName}'. Use a generated entity, collection, or dictionary conversion path.");
        }

        return (T?)converted;
    }

}
