using System.Globalization;

namespace TinyDb.Bson;

internal static class BsonValueComparer
{
    public static IEqualityComparer<BsonValue> EqualityComparer { get; } = new BsonValueEqualityComparer();

    public static bool ValueEquals(BsonValue? left, BsonValue? right)
    {
        return Compare(left, right) == 0;
    }

    public static int Compare(BsonValue? left, BsonValue? right)
    {
        if (ReferenceEquals(left, right)) return 0;
        if (left is null) return -1;
        if (right is null) return 1;
        if (left.IsNull && right.IsNull) return 0;
        if (left.IsNull) return -1;
        if (right.IsNull) return 1;

        var leftType = left.BsonType;
        var rightType = right.BsonType;

        if (left.IsNumeric && right.IsNumeric)
        {
            return CompareNumeric(left, right);
        }

        if (leftType != rightType)
        {
            return GetTypeOrder(leftType).CompareTo(GetTypeOrder(rightType));
        }

        return leftType switch
        {
            BsonType.MinKey or BsonType.MaxKey or BsonType.Null => 0,
            BsonType.String => string.Compare(((BsonString)left).Value, ((BsonString)right).Value, StringComparison.Ordinal),
            BsonType.Boolean => ((BsonBoolean)left).Value.CompareTo(((BsonBoolean)right).Value),
            BsonType.DateTime => BsonDateTime.GetComparableTicks(((BsonDateTime)left).Value)
                .CompareTo(BsonDateTime.GetComparableTicks(((BsonDateTime)right).Value)),
            BsonType.ObjectId => ((BsonObjectId)left).Value.CompareTo(((BsonObjectId)right).Value),
            BsonType.Binary => ((BsonBinary)left).CompareTo((BsonBinary)right),
            BsonType.Timestamp => ((BsonTimestamp)left).Value.CompareTo(((BsonTimestamp)right).Value),
            BsonType.Symbol => string.Compare(((BsonSymbol)left).Name, ((BsonSymbol)right).Name, StringComparison.Ordinal),
            _ => string.Compare(left.ToString(), right.ToString(), StringComparison.Ordinal)
        };
    }

    public static int GetHashCode(BsonValue? value)
    {
        if (value is null || value.IsNull) return 0;
        if (value.IsNumeric)
        {
            if (TryGetDecimal(value, out var decimalValue)) return decimalValue.GetHashCode();
            if (TryGetDouble(value, out var doubleValue)) return double.IsNaN(doubleValue) ? int.MinValue : doubleValue.GetHashCode();
            return HashCode.Combine(GetTypeOrder(value.BsonType), value.ToString());
        }

        return value.BsonType switch
        {
            BsonType.String => StringComparer.Ordinal.GetHashCode(((BsonString)value).Value),
            BsonType.Boolean => ((BsonBoolean)value).Value.GetHashCode(),
            BsonType.DateTime => BsonDateTime.GetComparableTicks(((BsonDateTime)value).Value).GetHashCode(),
            BsonType.ObjectId => ((BsonObjectId)value).Value.GetHashCode(),
            BsonType.Binary => ((BsonBinary)value).GetHashCode(),
            BsonType.Timestamp => ((BsonTimestamp)value).Value.GetHashCode(),
            BsonType.Symbol => StringComparer.Ordinal.GetHashCode(((BsonSymbol)value).Name),
            _ => HashCode.Combine(GetTypeOrder(value.BsonType), value.ToString())
        };
    }

    public static int GetTypeOrder(BsonType bsonType)
    {
        return bsonType switch
        {
            BsonType.MinKey => 0,
            BsonType.Null => 1,
            BsonType.Boolean => 2,
            BsonType.Int32 => 3,
            BsonType.Int64 => 3,
            BsonType.Double => 3,
            BsonType.Decimal128 => 3,
            BsonType.String => 7,
            BsonType.ObjectId => 8,
            BsonType.DateTime => 9,
            BsonType.Binary => 10,
            BsonType.Array => 11,
            BsonType.Document => 12,
            BsonType.RegularExpression => 13,
            BsonType.JavaScript => 14,
            BsonType.JavaScriptWithScope => 15,
            BsonType.Timestamp => 16,
            BsonType.Symbol => 17,
            BsonType.Undefined => 18,
            BsonType.MaxKey => 19,
            _ => 20
        };
    }

    private static int CompareNumeric(BsonValue left, BsonValue right)
    {
        // 同型整数快速路径：BsonInt32/BsonInt64 的 ToDecimal 为无损转换，直接比较与 decimal 路径严格等价，
        // 但避免了两次 int/long→decimal 扩展与 128 位比较。整数是索引键的绝对主力，命中此路径收益最大。
        if (left is BsonInt32 leftInt32 && right is BsonInt32 rightInt32)
        {
            return leftInt32.Value.CompareTo(rightInt32.Value);
        }

        if (left is BsonInt64 leftInt64 && right is BsonInt64 rightInt64)
        {
            return leftInt64.Value.CompareTo(rightInt64.Value);
        }

        if (left is BsonDouble leftDoubleValue && right is BsonDouble rightDoubleValue)
        {
            return leftDoubleValue.Value.CompareTo(rightDoubleValue.Value);
        }

        if (TryGetDecimal(left, out var leftDecimal) &&
            TryGetDecimal(right, out var rightDecimal))
        {
            return leftDecimal.CompareTo(rightDecimal);
        }

        if (left is BsonDecimal128 leftDecimal128 && right is BsonDecimal128 rightDecimal128)
        {
            return leftDecimal128.Value.CompareTo(rightDecimal128.Value);
        }

        if (TryGetDouble(left, out var leftDouble) &&
            TryGetDouble(right, out var rightDouble))
        {
            return leftDouble.CompareTo(rightDouble);
        }

        var typeComparison = GetTypeOrder(left.BsonType).CompareTo(GetTypeOrder(right.BsonType));
        if (typeComparison != 0) return typeComparison;
        return string.Compare(left.ToString(), right.ToString(), StringComparison.Ordinal);
    }

    private static bool TryGetDecimal(BsonValue value, out decimal result)
    {
        try
        {
            result = value.ToDecimal(CultureInfo.InvariantCulture);
            return true;
        }
        catch (Exception ex) when (ex is InvalidCastException or FormatException or OverflowException)
        {
            result = default;
            return false;
        }
    }

    private static bool TryGetDouble(BsonValue value, out double result)
    {
        try
        {
            result = value.ToDouble(CultureInfo.InvariantCulture);
            return true;
        }
        catch (Exception ex) when (ex is InvalidCastException or FormatException or OverflowException)
        {
            result = default;
            return false;
        }
    }

    private sealed class BsonValueEqualityComparer : IEqualityComparer<BsonValue>
    {
        public bool Equals(BsonValue? x, BsonValue? y)
        {
            return ValueEquals(x, y);
        }

        public int GetHashCode(BsonValue obj)
        {
            return BsonValueComparer.GetHashCode(obj);
        }
    }
}
