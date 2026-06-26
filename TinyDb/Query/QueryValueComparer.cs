using TinyDb.Bson;

namespace TinyDb.Query;

internal static class QueryValueComparer
{
    public static int Compare(object? left, object? right)
    {
        if (TryCompare(left, right, out var comparison))
        {
            return comparison;
        }

        left = UnwrapBsonValue(left);
        right = UnwrapBsonValue(right);

        var typeComparison = string.Compare(
            left?.GetType().FullName,
            right?.GetType().FullName,
            StringComparison.Ordinal);

        if (typeComparison != 0)
        {
            return typeComparison;
        }

        return string.Compare(left?.ToString(), right?.ToString(), StringComparison.Ordinal);
    }

    public static bool EvaluateComparison(object? left, object? right, System.Linq.Expressions.ExpressionType op)
    {
        if (TryCompare(left, right, out var comparison))
        {
            return Evaluate(comparison, op);
        }

        return op switch
        {
            System.Linq.Expressions.ExpressionType.Equal => ValuesEqual(left, right),
            System.Linq.Expressions.ExpressionType.NotEqual => !ValuesEqual(left, right),
            _ => false
        };
    }

    public static bool TryCompare(object? left, object? right, out int comparison)
    {
        left = UnwrapBsonValue(left);
        right = UnwrapBsonValue(right);
        comparison = 0;

        if (left == null && right == null)
        {
            comparison = 0;
            return true;
        }
        if (left == null)
        {
            comparison = -1;
            return true;
        }
        if (right == null)
        {
            comparison = 1;
            return true;
        }

        if (IsNumericType(left) && IsNumericType(right))
        {
            comparison = CompareNumeric(left, right);
            return true;
        }

        if (left is DateTime leftDateTime && right is DateTime rightDateTime)
        {
            comparison = NormalizeDateTime(leftDateTime).CompareTo(NormalizeDateTime(rightDateTime));
            return true;
        }

        if (left is byte[] b1 && right is byte[] b2)
        {
            comparison = b1.Length.CompareTo(b2.Length);
            if (comparison != 0) return true;

            for (int i = 0; i < b1.Length; i++)
            {
                comparison = b1[i].CompareTo(b2[i]);
                if (comparison != 0) return true;
            }

            return true;
        }

        if (left is string leftStr && right is string rightStr)
        {
            comparison = string.Compare(leftStr, rightStr, StringComparison.Ordinal);
            return true;
        }

        if (left.GetType() == right.GetType() && left is IComparable leftComparable)
        {
            try
            {
                comparison = leftComparable.CompareTo(right);
                return true;
            }
            catch (ArgumentException)
            {
            }
        }

        return false;
    }

    private static object? UnwrapBsonValue(object? value)
    {
        return value is BsonValue bsonValue ? bsonValue.RawValue : value;
    }

    private static bool ValuesEqual(object? left, object? right)
    {
        left = UnwrapBsonValue(left);
        right = UnwrapBsonValue(right);

        if (left is byte[] leftBytes && right is byte[] rightBytes)
        {
            return leftBytes.AsSpan().SequenceEqual(rightBytes);
        }

        return Equals(left, right);
    }

    public static bool Evaluate(int comparison, System.Linq.Expressions.ExpressionType op)
    {
        return op switch
        {
            System.Linq.Expressions.ExpressionType.Equal => comparison == 0,
            System.Linq.Expressions.ExpressionType.NotEqual => comparison != 0,
            System.Linq.Expressions.ExpressionType.GreaterThan => comparison > 0,
            System.Linq.Expressions.ExpressionType.GreaterThanOrEqual => comparison >= 0,
            System.Linq.Expressions.ExpressionType.LessThan => comparison < 0,
            System.Linq.Expressions.ExpressionType.LessThanOrEqual => comparison <= 0,
            _ => false
        };
    }

    private static DateTime NormalizeDateTime(DateTime value)
    {
        return value.Kind switch
        {
            DateTimeKind.Utc => value,
            DateTimeKind.Local => value.ToUniversalTime(),
            _ => DateTime.SpecifyKind(value, DateTimeKind.Utc)
        };
    }

    private static bool IsNumericType(object value)
    {
        return value is sbyte || value is byte || value is short || value is ushort ||
               value is int || value is uint || value is long || value is ulong ||
               value is float || value is double || value is decimal || value is Decimal128;
    }

    private static int CompareNumeric(object left, object right)
    {
        if (TryToDecimal(left, out var leftDecimal) &&
            TryToDecimal(right, out var rightDecimal))
        {
            return leftDecimal.CompareTo(rightDecimal);
        }

        return ToDouble(left).CompareTo(ToDouble(right));
    }

    private static bool TryToDecimal(object value, out decimal result)
    {
        try
        {
            if (value is Decimal128 d128)
            {
                result = d128.ToDecimal();
                return true;
            }

            if (value is double d && (double.IsNaN(d) || double.IsInfinity(d)))
            {
                result = default;
                return false;
            }

            if (value is float f && (float.IsNaN(f) || float.IsInfinity(f)))
            {
                result = default;
                return false;
            }

            result = Convert.ToDecimal(value);
            return true;
        }
        catch (Exception ex) when (ex is InvalidCastException or FormatException or OverflowException)
        {
            result = default;
            return false;
        }
    }

    private static double ToDouble(object value)
    {
        if (value is Decimal128 d128) return (double)d128.ToDecimal();
        return Convert.ToDouble(value);
    }
}
