using System;
using TinyDb.Bson;

namespace TinyDb.Serialization;

/// <summary>
/// 提供在BSON值与CLR值之间转换的基础设施，避免运行时反射构造动态代码。
/// </summary>
public static class BsonConversion
{
    public static BsonValue ToBsonValue(object value)
    {
        return value switch
        {
            null => BsonNull.Value,
            BsonValue bson => bson,
            string str => new BsonString(str),
            int i => new BsonInt32(i),
            long l => new BsonInt64(l),
            double d => new BsonDouble(d),
            float f => new BsonDouble(f),
            decimal dec => new BsonDecimal128(dec),
            bool b => new BsonBoolean(b),
            DateTime dt => new BsonDateTime(dt),
            Guid guid => new BsonBinary(guid, BsonBinary.BinarySubType.Uuid),
            ObjectId oid => new BsonObjectId(oid),
            _ => new BsonString(value.ToString() ?? string.Empty)
        };
    }

    /// <summary>
    /// 将值转换为BsonValue（用于源代码生成器）
    /// </summary>
    public static BsonValue ConvertToBsonValue(object value)
    {
        return ToBsonValue(value);
    }

    public static object? FromBsonValue(BsonValue bsonValue, Type targetType)
    {
        if (bsonValue == null) throw new ArgumentNullException(nameof(bsonValue));
        if (targetType == null) throw new ArgumentNullException(nameof(targetType));

        if (bsonValue.IsNull) return null;

        targetType = Nullable.GetUnderlyingType(targetType) ?? targetType;

        return targetType switch
        {
            var t when t == typeof(string) =>
                bsonValue is BsonString bsonString ? bsonString.Value : bsonValue.ToString(),
            var t when t == typeof(int) =>
                bsonValue switch
                {
                    BsonInt32 i32 => i32.Value,
                    BsonInt64 i64 => checked((int)i64.Value),
                    BsonDouble dbl => Convert.ToInt32(dbl.Value),
                    _ => Convert.ToInt32(bsonValue.ToString())
                },
            var t when t == typeof(long) =>
                bsonValue switch
                {
                    BsonInt64 i64 => i64.Value,
                    BsonInt32 i32 => i32.Value,
                    BsonDouble dbl => Convert.ToInt64(dbl.Value),
                    _ => Convert.ToInt64(bsonValue.ToString())
                },
            var t when t == typeof(double) =>
                bsonValue is BsonDouble dbl ? dbl.Value : Convert.ToDouble(bsonValue.ToString()),
            var t when t == typeof(float) =>
                bsonValue is BsonDouble dbl ? (float)dbl.Value : Convert.ToSingle(bsonValue.ToString()),
            var t when t == typeof(decimal) =>
                bsonValue switch
                {
                    BsonDecimal128 dec128 => dec128.Value,
                    BsonDouble dbl => Convert.ToDecimal(dbl.Value),
                    BsonInt32 i32 => i32.Value,
                    BsonInt64 i64 => i64.Value,
                    _ => Convert.ToDecimal(bsonValue.ToString())
                },
            var t when t == typeof(bool) =>
                bsonValue is BsonBoolean bl ? bl.Value : Convert.ToBoolean(bsonValue.ToString()),
            var t when t == typeof(DateTime) =>
                bsonValue is BsonDateTime dt ? dt.Value : Convert.ToDateTime(bsonValue.ToString()),
            var t when t == typeof(Guid) =>
                bsonValue is BsonBinary bin && bin.Bytes.Length == 16
                    ? new Guid(bin.Bytes)
                    : Guid.Parse(bsonValue.ToString()),
            var t when t == typeof(ObjectId) =>
                bsonValue is BsonObjectId oid ? oid.Value : ObjectId.Parse(bsonValue.ToString()),
            _ => bsonValue.ToString()
        };
    }
}
