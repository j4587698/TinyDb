using System;
using System.Buffers.Binary;
using System.Linq.Expressions;
using System.Text;
using TinyDb.Bson;

namespace TinyDb.Query;

/// <summary>
/// 二进制谓词求值器 - 在原始字节流上直接执行谓词判断，零分配、零反射。
/// </summary>
public static class BinaryPredicateEvaluator
{
    private static readonly DateTime UnixEpochUtc = new(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    /// <summary>
    /// 尝试对原始 BSON 字节执行比较。
    /// </summary>
    /// <returns>如果可以在二进制层完成比较则返回 true，否则返回 false（由上层回退到反序列化后求值）。</returns>
    public static bool TryEvaluate(ReadOnlySpan<byte> data, int offset, BsonType type, ExpressionType op, object? targetValue, byte[]? targetStringUtf8Bytes, out bool result)
    {
        result = false;

        if (targetValue is BsonValue bv) targetValue = bv.RawValue;

        try
        {
            switch (type)
            {
                case BsonType.Int32:
                    if (!TryConvertInt32(targetValue, out var iTarget)) return false;
                    if (offset < 0 || offset + 4 > data.Length) return false;
                    int iValue = BinaryPrimitives.ReadInt32LittleEndian(data.Slice(offset, 4));
                    result = Compare(iValue, iTarget, op);
                    return true;
                 
                case BsonType.Int64:
                    if (!TryConvertInt64(targetValue, out var lTarget)) return false;
                    if (offset < 0 || offset + 8 > data.Length) return false;
                    long lValue = BinaryPrimitives.ReadInt64LittleEndian(data.Slice(offset, 8));
                    result = Compare(lValue, lTarget, op);
                    return true;

                case BsonType.Timestamp:
                    if (!TryConvertInt64(targetValue, out var tsTarget)) return false;
                    if (offset < 0 || offset + 8 > data.Length) return false;
                    long tsValue = BinaryPrimitives.ReadInt64LittleEndian(data.Slice(offset, 8));
                    result = Compare(tsValue, tsTarget, op);
                    return true;

                case BsonType.Double:
                    if (!TryConvertDouble(targetValue, out var dTarget)) return false;
                    if (offset < 0 || offset + 8 > data.Length) return false;
                    double dValue = BitConverter.ToDouble(data.Slice(offset, 8));
                    result = Compare(dValue, dTarget, op);
                    return true;

                case BsonType.DateTime:
                    if (!TryConvertDateTime(targetValue, out var dtTarget)) return false;
                    if (offset < 0 || offset + 8 > data.Length) return false;
                    long ms = BinaryPrimitives.ReadInt64LittleEndian(data.Slice(offset, 8));
                    var dtValue = UnixEpochUtc.AddMilliseconds(ms);
                    result = Compare(dtValue, dtTarget, op);
                    return true;

                case BsonType.Decimal128:
                    if (!TryConvertDecimal(targetValue, out var decTarget)) return false;
                    if (offset < 0 || offset + 16 > data.Length) return false;
                    var lo = BinaryPrimitives.ReadUInt64LittleEndian(data.Slice(offset, 8));
                    var hi = BinaryPrimitives.ReadUInt64LittleEndian(data.Slice(offset + 8, 8));
                    var dec128 = new Decimal128(lo, hi);
                    result = Compare(dec128.ToDecimal(), decTarget, op);
                    return true;

                case BsonType.ObjectId:
                    if (!TryConvertObjectId(targetValue, out var oidTarget)) return false;
                    if (offset < 0 || offset + 12 > data.Length) return false;
                    int cmp = CompareBytes(data.Slice(offset, 12), oidTarget.ToByteArray());
                    result = cmp switch
                    {
                        _ when op == ExpressionType.Equal => cmp == 0,
                        _ when op == ExpressionType.NotEqual => cmp != 0,
                        _ when op == ExpressionType.GreaterThan => cmp > 0,
                        _ when op == ExpressionType.GreaterThanOrEqual => cmp >= 0,
                        _ when op == ExpressionType.LessThan => cmp < 0,
                        _ when op == ExpressionType.LessThanOrEqual => cmp <= 0,
                        _ => false
                    };
                    return true;

                case BsonType.Boolean:
                    if (!TryConvertBoolean(targetValue, out var bTarget)) return false;
                    if (offset < 0 || offset + 1 > data.Length) return false;
                    bool bValue = data[offset] != 0;
                    if (op == ExpressionType.Equal) { result = bValue == bTarget; return true; }
                    if (op == ExpressionType.NotEqual) { result = bValue != bTarget; return true; }
                    return false;

                case BsonType.String:
                    if (op != ExpressionType.Equal && op != ExpressionType.NotEqual) return false;
                    if (targetValue == null) return false;
                    if (offset < 0 || offset + 4 > data.Length) return false;
                    int len = BinaryPrimitives.ReadInt32LittleEndian(data.Slice(offset, 4));
                    if (len <= 0 || offset + 4 + len > data.Length) return false;

                    // 直接比较 UTF8 字节，避免中间 string 分配
                    var targetBytes = targetStringUtf8Bytes
                        ?? (targetValue is string s ? Encoding.UTF8.GetBytes(s) : Encoding.UTF8.GetBytes(targetValue.ToString() ?? ""));
                     
                    // BSON 字符串包含 \0 结尾，长度比实际内容大 1
                    var sourceSpan = data.Slice(offset + 4, len - 1);
                    bool isEqual = sourceSpan.SequenceEqual(targetBytes);
                     
                    result = op == ExpressionType.Equal ? isEqual : !isEqual;
                    return true;

                case BsonType.Null:
                    result = EvaluateNullComparison(op, targetValue);
                    return true;

                default:
                    return false;
            }
        }
        catch
        {
            result = false;
            return false;
        }
    }

    /// <summary>
    /// 对原始 BSON 字节执行比较（无法比较时返回 false）。
    /// </summary>
    public static bool Evaluate(ReadOnlySpan<byte> data, int offset, BsonType type, ExpressionType op, object targetValue)
    {
        return TryEvaluate(data, offset, type, op, targetValue, null, out var result) && result;
    }

    private static bool Compare<T>(T left, T right, ExpressionType op) where T : IComparable<T>
    {
        int res = left.CompareTo(right);
        return op switch
        {
            ExpressionType.Equal => res == 0,
            ExpressionType.NotEqual => res != 0,
            ExpressionType.GreaterThan => res > 0,
            ExpressionType.GreaterThanOrEqual => res >= 0,
            ExpressionType.LessThan => res < 0,
            ExpressionType.LessThanOrEqual => res <= 0,
            _ => false
        };
    }

    private static bool EvaluateNullComparison(ExpressionType op, object? targetValue)
    {
        if (targetValue is BsonValue bv) targetValue = bv.RawValue;

        bool rightIsNull = targetValue == null;
        return op switch
        {
            ExpressionType.Equal => rightIsNull,
            ExpressionType.NotEqual => !rightIsNull,
            ExpressionType.GreaterThan => false,
            ExpressionType.GreaterThanOrEqual => rightIsNull,
            ExpressionType.LessThan => !rightIsNull,
            ExpressionType.LessThanOrEqual => true,
            _ => false
        };
    }

    private static bool TryConvertInt32(object? value, out int result)
    {
        result = default;
        if (value == null) return false;
        try
        {
            if (value is Decimal128 d128) { result = (int)d128.ToDecimal(); return true; }
            result = Convert.ToInt32(value);
            return true;
        }
        catch
        {
            return false;
        }
    }

    private static bool TryConvertInt64(object? value, out long result)
    {
        result = default;
        if (value == null) return false;
        try
        {
            if (value is Decimal128 d128) { result = (long)d128.ToDecimal(); return true; }
            result = Convert.ToInt64(value);
            return true;
        }
        catch
        {
            return false;
        }
    }

    private static bool TryConvertDouble(object? value, out double result)
    {
        result = default;
        if (value == null) return false;
        try
        {
            if (value is Decimal128 d128) { result = (double)d128.ToDecimal(); return true; }
            result = Convert.ToDouble(value);
            return true;
        }
        catch
        {
            return false;
        }
    }

    private static bool TryConvertDecimal(object? value, out decimal result)
    {
        result = default;
        if (value == null) return false;
        try
        {
            if (value is Decimal128 d128) { result = d128.ToDecimal(); return true; }
            result = Convert.ToDecimal(value);
            return true;
        }
        catch
        {
            return false;
        }
    }

    private static bool TryConvertBoolean(object? value, out bool result)
    {
        result = default;
        if (value == null) return false;
        try
        {
            result = value is bool b ? b : Convert.ToBoolean(value);
            return true;
        }
        catch
        {
            return false;
        }
    }

    private static bool TryConvertDateTime(object? value, out DateTime result)
    {
        result = default;
        if (value == null) return false;

        try
        {
            if (value is DateTime dt) { result = dt; return true; }
            if (value is DateTimeOffset dto) { result = dto.UtcDateTime; return true; }
            result = Convert.ToDateTime(value);
            return true;
        }
        catch
        {
            return false;
        }
    }

    private static bool TryConvertObjectId(object? value, out ObjectId result)
    {
        result = default;
        if (value == null) return false;

        if (value is ObjectId oid)
        {
            result = oid;
            return true;
        }

        if (value is byte[] bytes && bytes.Length == 12)
        {
            result = new ObjectId(bytes);
            return true;
        }

        if (value is string s && ObjectId.TryParse(s, out var parsed))
        {
            result = parsed;
            return true;
        }

        return false;
    }

    private static int CompareBytes(ReadOnlySpan<byte> left, ReadOnlySpan<byte> right)
    {
        int lenDiff = left.Length.CompareTo(right.Length);
        if (lenDiff != 0) return lenDiff;

        for (int i = 0; i < left.Length; i++)
        {
            int diff = left[i].CompareTo(right[i]);
            if (diff != 0) return diff;
        }

        return 0;
    }
}
