using System;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;
using TinyDb.Bson;
using TinyDb.Serialization;

namespace TinyDb.Query;

public static class ExpressionEvaluator
{
    public static bool Evaluate<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(QueryExpression expression, T entity)
        where T : class, new()
    {
        return expression switch
        {
            ConstantExpression constExpr => EvaluateConstantExpression(constExpr),
            BinaryExpression binaryExpr => (bool?)EvaluateBinaryExpression(binaryExpr, entity) ?? false,
            MemberExpression memberExpr => EvaluateMemberExpression(memberExpr, entity),
            UnaryExpression unaryExpr => EvaluateUnaryBooleanExpression(unaryExpr, entity),
            ParameterExpression paramExpr => true,
            FunctionExpression funcExpr => (bool?)EvaluateFunctionExpression(funcExpr, entity) ?? false,
            _ => throw new NotSupportedException($"Expression type {expression.GetType().Name} is not supported")
        };
    }

    public static bool Evaluate(QueryExpression expression, BsonDocument document)
    {
        return expression switch
        {
            ConstantExpression constExpr => EvaluateConstantExpression(constExpr),
            BinaryExpression binaryExpr => (bool?)EvaluateBinaryExpression(binaryExpr, document) ?? false,
            MemberExpression memberExpr => EvaluateMemberExpression(memberExpr, document),
            UnaryExpression unaryExpr => EvaluateUnaryBooleanExpression(unaryExpr, document),
            ParameterExpression paramExpr => true,
            FunctionExpression funcExpr => (bool?)EvaluateFunctionExpression(funcExpr, document) ?? false,
            _ => throw new NotSupportedException($"Expression type {expression.GetType().Name} is not supported")
        };
    }

    private static bool EvaluateConstantExpression(ConstantExpression expression)
    {
        if (expression.Value is bool boolValue)
        {
            return boolValue;
        }
        throw new InvalidOperationException($"Constant expression must be boolean, got {expression.Value?.GetType().Name}");
    }

    public static object? EvaluateValue<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(QueryExpression expression, T entity)
        where T : class, new()
    {
        return expression switch
        {
            ConstantExpression constExpr => constExpr.Value,
            MemberExpression memberExpr => GetMemberValue(memberExpr, entity),
            UnaryExpression unaryExpr => EvaluateUnaryExpression(unaryExpr, entity),
            ParameterExpression => entity,
            BinaryExpression binaryExpr => EvaluateBinaryExpression(binaryExpr, entity),
            FunctionExpression funcExpr => EvaluateFunctionExpression(funcExpr, entity),
            ConstructorExpression ctorExpr => EvaluateConstructorExpression(ctorExpr, entity),
            MemberInitQueryExpression memberInitExpr => EvaluateMemberInitExpression(memberInitExpr, entity),
            _ => throw new NotSupportedException($"Expression type {expression.GetType().Name} is not supported for value evaluation")
        };
    }

    public static object? EvaluateValue(QueryExpression expression, BsonDocument document)
    {
        return expression switch
        {
            ConstantExpression constExpr => constExpr.Value,
            MemberExpression memberExpr => GetMemberValue(memberExpr, (object)document),
            UnaryExpression unaryExpr => EvaluateUnaryExpression(unaryExpr, (object)document),
            ParameterExpression => document,
            BinaryExpression binaryExpr => EvaluateBinaryExpression(binaryExpr, (object)document),
            FunctionExpression funcExpr => EvaluateFunctionExpression(funcExpr, (object)document),
            ConstructorExpression ctorExpr => EvaluateConstructorExpression(ctorExpr, (object)document),
            MemberInitQueryExpression memberInitExpr => EvaluateMemberInitExpression(memberInitExpr, (object)document),
            _ => throw new NotSupportedException($"Expression type {expression.GetType().Name} is not supported for value evaluation")
        };
    }

    public static object? EvaluateValue(QueryExpression expression, object entity)
    {
        if (entity is BsonDocument doc) return EvaluateValue(expression, doc);

        return expression switch
        {
            ConstantExpression constExpr => constExpr.Value,
            MemberExpression memberExpr => GetMemberValue(memberExpr, entity),
            UnaryExpression unaryExpr => EvaluateUnaryExpression(unaryExpr, entity),
            ParameterExpression => entity,
            BinaryExpression binaryExpr => EvaluateBinaryExpression(binaryExpr, entity),
            FunctionExpression funcExpr => EvaluateFunctionExpression(funcExpr, entity),
            ConstructorExpression ctorExpr => EvaluateConstructorExpression(ctorExpr, entity),
            MemberInitQueryExpression memberInitExpr => EvaluateMemberInitExpression(memberInitExpr, entity),
            _ => throw new NotSupportedException($"Expression type {expression.GetType().Name} is not supported for value evaluation")
        };
    }


    private static object? EvaluateConstructorExpression<T>(ConstructorExpression expression, T entity)
    {
        var args = expression.Arguments.Select(a => EvaluateValue(a, (object)entity!)).ToArray();
        return Activator.CreateInstance(expression.Type, args);
    }

    [UnconditionalSuppressMessage("TrimAnalysis", "IL2072", Justification = "Type and properties come from expression tree.")]
    [UnconditionalSuppressMessage("TrimAnalysis", "IL2075", Justification = "Property access via reflection for AOT projection.")]
    private static object? EvaluateMemberInitExpression<T>(MemberInitQueryExpression expression, T entity)
    {
        // Create instance using parameterless constructor
        var instance = Activator.CreateInstance(expression.Type);
        if (instance == null) return null;
        
        // Set each property
        foreach (var (memberName, valueExpr) in expression.Bindings)
        {
            var value = EvaluateValue(valueExpr, (object)entity!);
            var prop = expression.Type.GetProperty(memberName);
            if (prop != null && prop.CanWrite)
            {
                // Handle type conversion if needed
                if (value != null && !prop.PropertyType.IsInstanceOfType(value))
                {
                    try
                    {
                        value = Convert.ChangeType(value, prop.PropertyType);
                    }
                    catch
                    {
                        // Keep original value if conversion fails
                    }
                }
                prop.SetValue(instance, value);
            }
        }
        
        return instance;
    }

    private static bool EvaluateUnaryBooleanExpression<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(UnaryExpression expression, T entity)
        where T : class, new()
    {
        var val = EvaluateValue(expression, entity);
        return val is bool b && b;
    }

    private static bool EvaluateUnaryBooleanExpression(UnaryExpression expression, BsonDocument document)
    {
        var val = EvaluateValue(expression, document);
        return val is bool b && b;
    }

    private static object? EvaluateUnaryExpression<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(UnaryExpression expression, T entity)
        where T : class, new()
    {
        var value = EvaluateValue(expression.Operand, entity);
        return EvaluateUnary(expression.NodeType, value, expression.Type);
    }

    private static object? EvaluateUnaryExpression(UnaryExpression expression, object entity)
    {
        var value = EvaluateValue(expression.Operand, entity);
        return EvaluateUnary(expression.NodeType, value, expression.Type);
    }

    private static object? EvaluateUnary(ExpressionType nodeType, object? value, [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicParameterlessConstructor)] Type targetType)
    {
        if (nodeType == ExpressionType.Convert)
        {
            if (value == null) return null;
            if (IsNumericType(value) && IsNumericType(Activator.CreateInstance(targetType) ?? 0))
            {
                return Convert.ChangeType(value, targetType);
            }
            return Convert.ChangeType(value, targetType);
        }
        else if (nodeType == ExpressionType.Not)
        {
            if (value is bool b) return !b;
            throw new InvalidOperationException("Not operator requires boolean operand");
        }
        throw new NotSupportedException($"Unary operation {nodeType} is not supported");
    }

    private static bool EvaluateMemberExpression<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(MemberExpression expression, T entity)
        where T : class, new()
    {
        var value = GetMemberValue(expression, entity);
        return value is bool boolValue ? boolValue : value != null;
    }

    private static bool EvaluateMemberExpression(MemberExpression expression, BsonDocument document)
    {
        var value = GetMemberValue(expression, document);
        return value is bool boolValue ? boolValue : value != null;
    }

    private static object? EvaluateBinaryExpression<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(BinaryExpression expression, T entity)
        where T : class, new()
    {
        if (expression.NodeType == ExpressionType.AndAlso)
        {
            var left = EvaluateValue(expression.Left, entity);
            if (left is bool b && !b) return false;
            var right = EvaluateValue(expression.Right, entity);
            return right is bool rb && rb;
        }
        if (expression.NodeType == ExpressionType.OrElse)
        {
            var left = EvaluateValue(expression.Left, entity);
            if (left is bool b && b) return true;
            var right = EvaluateValue(expression.Right, entity);
            return right is bool rb && rb;
        }

        var leftValue = EvaluateValue(expression.Left, entity);
        var rightValue = EvaluateValue(expression.Right, entity);

        return EvaluateBinary(expression.NodeType, leftValue, rightValue);
    }

    private static object? EvaluateBinaryExpression(BinaryExpression expression, object entity)
    {
        if (expression.NodeType == ExpressionType.AndAlso)
        {
            var left = EvaluateValue(expression.Left, entity);
            if (left is bool b && !b) return false;
            var right = EvaluateValue(expression.Right, entity);
            return right is bool rb && rb;
        }
        if (expression.NodeType == ExpressionType.OrElse)
        {
            var left = EvaluateValue(expression.Left, entity);
            if (left is bool b && b) return true;
            var right = EvaluateValue(expression.Right, entity);
            return right is bool rb && rb;
        }

        var leftValue = EvaluateValue(expression.Left, entity);
        var rightValue = EvaluateValue(expression.Right, entity);

        return EvaluateBinary(expression.NodeType, leftValue, rightValue);
    }

    private static object? EvaluateBinary(ExpressionType nodeType, object? leftValue, object? rightValue)
    {
        switch (nodeType)
        {
            case ExpressionType.Equal: return Compare(leftValue, rightValue) == 0;
            case ExpressionType.NotEqual: return Compare(leftValue, rightValue) != 0;
            case ExpressionType.GreaterThan: return Compare(leftValue, rightValue) > 0;
            case ExpressionType.GreaterThanOrEqual: return Compare(leftValue, rightValue) >= 0;
            case ExpressionType.LessThan: return Compare(leftValue, rightValue) < 0;
            case ExpressionType.LessThanOrEqual: return Compare(leftValue, rightValue) <= 0;
            case ExpressionType.AndAlso: return leftValue is bool leftBool && rightValue is bool rightBool && leftBool && rightBool;
            case ExpressionType.OrElse: return leftValue is bool leftBool2 && rightValue is bool rightBool2 && (leftBool2 || rightBool2);

            case ExpressionType.Add:
                // Handle string concatenation
                if (leftValue is string || rightValue is string)
                {
                    return (leftValue?.ToString() ?? "") + (rightValue?.ToString() ?? "");
                }
                return EvaluateMathOp(leftValue, rightValue, (a, b) => a + b, (a, b) => a + b);
            case ExpressionType.Subtract: return EvaluateMathOp(leftValue, rightValue, (a, b) => a - b, (a, b) => a - b);
            case ExpressionType.Multiply: return EvaluateMathOp(leftValue, rightValue, (a, b) => a * b, (a, b) => a * b);
            case ExpressionType.Divide: return EvaluateMathOp(leftValue, rightValue, (a, b) => a / b, (a, b) => a / b);

            default: throw new NotSupportedException($"Binary operation {nodeType} is not supported");
        }
    }

    private static object? EvaluateMathOp(object? left, object? right, Func<double, double, double> doubleFunc, Func<decimal, decimal, decimal> decimalFunc)
    {
        if (left == null || right == null) return null;

        if (left is decimal || right is decimal || left is Decimal128 || right is Decimal128)
        {
            return decimalFunc(ToDecimal(left), ToDecimal(right));
        }

        var dResult = doubleFunc(ToDouble(left), ToDouble(right));

        if ((left is int || left is long) && (right is int || right is long) && dResult == Math.Floor(dResult))
        {
            if (dResult >= int.MinValue && dResult <= int.MaxValue) return (int)dResult;
            return (long)dResult;
        }

        return dResult;
    }

    private static decimal ToDecimal(object val)
    {
        if (val is Decimal128 d128) return d128.ToDecimal();
        return Convert.ToDecimal(val);
    }

    private static double ToDouble(object val)
    {
        return Convert.ToDouble(val);
    }

    private static object? GetMemberValue<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(MemberExpression expression, T entity)
        where T : class, new()
    {
        object? target = entity;
        if (expression.Expression != null && expression.Expression.NodeType != ExpressionType.Parameter)
        {
            target = EvaluateValue(expression.Expression, entity);
        }

        if (target == null) return null;

        // BsonDocument stores data as dictionary entries, not C# properties
        // Always use GetMemberValueFromTarget for proper dictionary-style access
        if (target is BsonDocument)
        {
            return GetMemberValueFromTarget(expression.MemberName, target);
        }

        // Optimization: Try to use AOT adapter or EntityMetadata if target is T
        var type = target.GetType();
        if (type == typeof(T))
        {
            if (AotHelperRegistry.TryGetAdapter<T>(out var adapter))
            {
                return adapter.GetPropertyValue((T)target, expression.MemberName);
            }
            if (EntityMetadata<T>.TryGetProperty(expression.MemberName, out var property))
            {
                return property.GetValue(target);
            }
        }

        return GetMemberValueFromTarget(expression.MemberName, target);
    }

    private static object? GetMemberValue(MemberExpression expression, object entity)
    {
        object? target = entity;
        if (expression.Expression != null && expression.Expression.NodeType != ExpressionType.Parameter)
        {
            target = EvaluateValue(expression.Expression, entity);
        }

        if (target == null) return null;

        return GetMemberValueFromTarget(expression.MemberName, target);
    }

    private static object? GetMemberValueFromTarget(string memberName, object target)
    {
        // Check BsonDocument first - it implements IEnumerable but needs special handling for property access
        if (target is BsonDocument doc)
        {
            var camelName = ToCamelCase(memberName);

            if (doc.TryGetValue(camelName, out var val)) return val.RawValue;
            if (doc.TryGetValue(memberName, out val)) return val.RawValue;
            if (memberName == "Id" && doc.TryGetValue("_id", out val)) return val.RawValue;
            return null;
        }

        // Check AotGrouping for Key and Count
        if (target is IGrouping<object, object> grouping)
        {
            if (memberName == "Key") return grouping.Key;
            // Count is handled via method call, but provide fallback
            if (memberName == "Count" && target is QueryPipeline.AotGrouping aotGroup) return aotGroup.Count;
        }

        // Check BsonArray Count before ICollection/IEnumerable Count
        if (target is BsonArray arr && memberName == "Count")
        {
            return arr.Count;
        }

        // Check BsonValue wrapper for arrays Count
        if (target is BsonValue bv && bv.IsArray && memberName == "Count")
        {
            return ((BsonArray)bv.RawValue!).Count;
        }

        // Standard type checks
        if (target is string str)
        {
            if (memberName == "Length") return str.Length;
        }
        else if (target is DateTime dt)
        {
            return memberName switch
            {
                "Year" => dt.Year,
                "Month" => dt.Month,
                "Day" => dt.Day,
                "Hour" => dt.Hour,
                "Minute" => dt.Minute,
                "Second" => dt.Second,
                "Date" => dt.Date,
                "DayOfWeek" => (int)dt.DayOfWeek,
                _ => null
            };
        }
        else if (target is System.Collections.ICollection col)
        {
            if (memberName == "Count") return col.Count;
        }
        else if (target is System.Collections.IEnumerable en)
        {
            if (memberName == "Count")
            {
                int count = 0;
                var enumerator = en.GetEnumerator();
                while (enumerator.MoveNext()) count++;
                return count;
            }
        }

        // Fallback to reflection for other properties
        var type = target.GetType();
        var prop = GetPropertySafe(type, memberName);
        return prop?.GetValue(target);
    }

    [UnconditionalSuppressMessage("TrimAnalysis", "IL2075", Justification = "Fallback reflection for non-AOT scenarios. AOT apps should use Source Generator.")]
    [UnconditionalSuppressMessage("TrimAnalysis", "IL2070", Justification = "Fallback reflection for non-AOT scenarios.")]
    private static PropertyInfo? GetPropertySafe(Type type, string name)
    {
        return type.GetProperty(name);
    }

    private static object? EvaluateFunctionExpression<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(FunctionExpression expression, T entity)
        where T : class, new()
    {
        var targetValue = expression.Target != null ? EvaluateValue(expression.Target, entity) : null;
        var argValues = expression.Arguments.Select(a => EvaluateValue(a, entity)).ToArray();

        return EvaluateFunction(expression.FunctionName, targetValue, argValues);
    }

    private static object? EvaluateFunctionExpression(FunctionExpression expression, object entity)
    {
        var targetValue = expression.Target != null ? EvaluateValue(expression.Target, entity) : null;
        var argValues = expression.Arguments.Select(a => EvaluateValue(a, entity)).ToArray();

        return EvaluateFunction(expression.FunctionName, targetValue, argValues);
    }

    private static object? EvaluateFunction(string functionName, object? targetValue, object?[] args)
    {
        if (targetValue is string str)
        {
            return functionName switch
            {
                "Contains" => args.Length == 1 && args[0] is string s ? str.Contains(s) : false,
                "StartsWith" => args.Length == 1 && args[0] is string s ? str.StartsWith(s) : false,
                "EndsWith" => args.Length == 1 && args[0] is string s ? str.EndsWith(s) : false,
                "ToLower" => str.ToLower(),
                "ToUpper" => str.ToUpper(),
                "Trim" => str.Trim(),
                "Substring" => args.Length switch
                {
                    1 => str.Substring(Convert.ToInt32(args[0])),
                    2 => str.Substring(Convert.ToInt32(args[0]), Convert.ToInt32(args[1])),
                    _ => throw new ArgumentException("Invalid arguments for Substring")
                },
                "Replace" => args.Length == 2 && args[0] is string o && args[1] is string n ? str.Replace(o, n) : str,
                _ => throw new NotSupportedException($"String function '{functionName}' is not supported")
            };
        }

        if (targetValue is System.Collections.IEnumerable enumerable)
        {
            if (functionName == "Contains" && args.Length == 1)
            {
                foreach (var item in enumerable)
                {
                    var actualItem = item is BsonValue bv ? bv.RawValue : item;
                    if (Equals(actualItem, args[0])) return true;
                }
                return false;
            }
            if (functionName == "Count")
            {
                if (targetValue is System.Collections.ICollection col) return col.Count;
                if (targetValue is BsonArray ba) return ba.Count;
                int count = 0;
                var e = enumerable.GetEnumerator();
                while (e.MoveNext()) count++;
                return count;
            }
        }

        if (targetValue == null)
        {
            switch (functionName)
            {
                case "Abs": return EvaluateMathOneArg(args, Math.Abs, Math.Abs, Math.Abs);
                case "Ceiling": return EvaluateMathOneArg(args, Math.Ceiling, Math.Ceiling, d => (decimal)Math.Ceiling((double)d));
                case "Floor": return EvaluateMathOneArg(args, Math.Floor, Math.Floor, d => (decimal)Math.Floor((double)d));
                case "Round":
                    if (args.Length == 1) return EvaluateMathOneArg(args, Math.Round, Math.Round, Math.Round);
                    if (args.Length == 2) return EvaluateMathTwoArgs(args, (v, p) => Math.Round(v, (int)p), (v, p) => Math.Round(v, (int)p), (v, p) => Math.Round(v, (int)p));
                    break;
                case "Min": return EvaluateMathTwoArgs(args, Math.Min, Math.Min, Math.Min);
                case "Max": return EvaluateMathTwoArgs(args, Math.Max, Math.Max, Math.Max);
                case "Pow": return args.Length == 2 ? Math.Pow(Convert.ToDouble(args[0]), Convert.ToDouble(args[1])) : 0.0;
                case "Sqrt": return args.Length == 1 ? Math.Sqrt(Convert.ToDouble(args[0])) : 0.0;
            }
        }

        if (targetValue is DateTime dt)
        {
            return functionName switch
            {
                "AddDays" => dt.AddDays(Convert.ToDouble(args[0])),
                "AddHours" => dt.AddHours(Convert.ToDouble(args[0])),
                "AddMinutes" => dt.AddMinutes(Convert.ToDouble(args[0])),
                "AddSeconds" => dt.AddSeconds(Convert.ToDouble(args[0])),
                "AddYears" => dt.AddYears(Convert.ToInt32(args[0])),
                "AddMonths" => dt.AddMonths(Convert.ToInt32(args[0])),
                "ToString" => dt.ToString(),
                _ => throw new NotSupportedException($"DateTime function '{functionName}' is not supported")
            };
        }

        if (functionName == "ToString" && args.Length == 0 && targetValue != null)
        {
            return targetValue.ToString();
        }

        throw new NotSupportedException($"Function '{functionName}' is not supported for type {targetValue?.GetType().Name ?? "null"}");
    }

    private static object EvaluateMathOneArg(object?[] args, Func<double, double> doubleFunc, Func<decimal, decimal> decimalFunc, Func<decimal, decimal>? decimalFuncFallback = null)
    {
        if (args.Length != 1) throw new ArgumentException("Function requires 1 argument");
        var val = args[0];
        if (val is int i) return (int)doubleFunc(i);
        if (val is long l) return (long)doubleFunc(l);
        if (val is double d) return doubleFunc(d);
        if (val is decimal dec) return decimalFunc(dec);
        if (val is Decimal128 d128) return new Decimal128(decimalFunc(d128.ToDecimal()));

        return doubleFunc(Convert.ToDouble(val));
    }

    private static object EvaluateMathTwoArgs(object?[] args, Func<double, double, double> doubleFunc, Func<decimal, decimal, decimal> decimalFunc, Func<decimal, decimal, decimal>? decimalFuncFallback = null)
    {
        if (args.Length != 2) throw new ArgumentException("Function requires 2 arguments");
        var v1 = args[0];
        var v2 = args[1];

        if (v1 == null || v2 == null) return 0.0;

        if (v1 is decimal || v2 is decimal || v1 is Decimal128 || v2 is Decimal128)
            return decimalFunc(ToDecimal(v1), ToDecimal(v2));

        return doubleFunc(ToDouble(v1), ToDouble(v2));
    }

    private static bool EvaluateStartsWithFunction(object? targetValue, object? argumentValue)
    {
        if (targetValue is string targetStr && argumentValue is string argStr)
        {
            return targetStr.StartsWith(argStr);
        }

        throw new NotSupportedException($"StartsWith operation is not supported for types {targetValue?.GetType().Name} and {argumentValue?.GetType().Name}");
    }

    private static bool EvaluateEndsWithFunction(object? targetValue, object? argumentValue)
    {
        if (targetValue is string targetStr && argumentValue is string argStr)
        {
            return targetStr.EndsWith(argStr);
        }

        throw new NotSupportedException($"EndsWith operation is not supported for types {targetValue?.GetType().Name} and {argumentValue?.GetType().Name}");
    }

    private static int Compare(object? left, object? right)
    {
        if (left is BsonValue bl) left = bl.RawValue;
        if (right is BsonValue br) right = br.RawValue;

        if (left == null && right == null) return 0;
        if (left == null) return -1;
        if (right == null) return 1;

        if (IsNumericType(left) && IsNumericType(right))
        {
            if (left is Decimal128 ld) left = ld.ToDecimal();
            if (right is Decimal128 rd) right = rd.ToDecimal();

            if (left is decimal || right is decimal)
            {
                return ToDecimal(left).CompareTo(ToDecimal(right));
            }
            return ToDouble(left).CompareTo(ToDouble(right));
        }

        if (left is byte[] b1 && right is byte[] b2)
        {
            var lenDiff = b1.Length.CompareTo(b2.Length);
            if (lenDiff != 0) return lenDiff;
            for (int i = 0; i < b1.Length; i++)
            {
                var bDiff = b1[i].CompareTo(b2[i]);
                if (bDiff != 0) return bDiff;
            }
            return 0;
        }

        if (left is IComparable leftComparable)
        {
            try
            {
                return leftComparable.CompareTo(right);
            }
            catch (ArgumentException)
            {
            }
        }

        if (left is string leftStr && right is string rightStr)
        {
            return string.Compare(leftStr, rightStr, StringComparison.Ordinal);
        }

        return string.Compare(left.ToString(), right.ToString(), StringComparison.Ordinal);
    }

    private static bool IsNumericType(object value)
    {
        return value is sbyte || value is byte || value is short || value is ushort ||
               value is int || value is uint || value is long || value is ulong ||
               value is float || value is double || value is decimal || value is Decimal128;
    }

    private static string ToCamelCase(string name)
    {
        if (string.IsNullOrEmpty(name)) return name;
        if (name.Length == 1) return name.ToLowerInvariant();
        return char.ToLowerInvariant(name[0]) + name.Substring(1);
    }
}