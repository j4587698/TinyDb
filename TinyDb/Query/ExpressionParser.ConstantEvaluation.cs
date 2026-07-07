using System;
using System.Linq.Expressions;

namespace TinyDb.Query;

internal static partial class ExpressionConstantEvaluator
{
    /// <summary>
    /// 尝试在不使用 Compile 的情况下手动求值表达式（AOT 安全）
    /// </summary>
    internal static object? TryEvaluateWithoutCompile(Expression expression)
    {
        return expression switch
        {
            System.Linq.Expressions.ConstantExpression constExpr => constExpr.Value,
            System.Linq.Expressions.MemberExpression memberExpr => TryEvaluateMember(memberExpr),
            System.Linq.Expressions.MethodCallExpression methodCallExpr => TryEvaluateMethodCall(methodCallExpr),
            System.Linq.Expressions.BinaryExpression binaryExpr => TryEvaluateBinary(binaryExpr),
            System.Linq.Expressions.UnaryExpression unaryExpr when unaryExpr.NodeType == ExpressionType.Convert
                => TryEvaluateConvert(unaryExpr),
            System.Linq.Expressions.UnaryExpression unaryExpr when unaryExpr.NodeType == ExpressionType.ArrayLength
                => TryEvaluateArrayLength(unaryExpr),
            System.Linq.Expressions.NewExpression newExpr => TryEvaluateNew(newExpr),
            System.Linq.Expressions.ConditionalExpression conditionalExpr => TryEvaluateConditional(conditionalExpr),
            _ => null
        };
    }

    /// <summary>
    /// 尝试求值数组长度表达式
    /// </summary>
    private static object? TryEvaluateArrayLength(System.Linq.Expressions.UnaryExpression unaryExpr)
    {
        var array = TryEvaluateWithoutCompile(unaryExpr.Operand);
        if (array is Array arr)
        {
            return arr.Length;
        }
        return null;
    }

    /// <summary>
    /// 尝试求值成员访问表达式
    /// </summary>
    private static object? TryEvaluateMember(System.Linq.Expressions.MemberExpression memberExpr)
    {
        object? container = null;

        // 如果有父表达式，先求值父表达式
        if (memberExpr.Expression != null)
        {
            container = TryEvaluateWithoutCompile(memberExpr.Expression);
            if (container == null && memberExpr.Expression is not System.Linq.Expressions.ConstantExpression { Value: null })
            {
                return null; // 无法求值父表达式
            }
        }

        // 获取成员值
        if (memberExpr.Member is System.Reflection.FieldInfo field)
        {
            return field.GetValue(container);
        }

        var prop = (System.Reflection.PropertyInfo)memberExpr.Member;

        if (container is string s && prop.Name == nameof(string.Length))
        {
            return s.Length;
        }

        if (container is DateTime dt)
        {
            return ExpressionKnownMemberEvaluator.EvaluateDateTimeProperty(dt, prop.Name);
        }

        if (container is null && prop.DeclaringType == typeof(System.Environment) && prop.Name == nameof(System.Environment.NewLine))
        {
            return System.Environment.NewLine;
        }

        return null;
    }

    /// <summary>
    /// 尝试求值类型转换表达式
    /// </summary>
    internal static object? TryEvaluateConvert(System.Linq.Expressions.UnaryExpression unaryExpr)
    {
        var operand = TryEvaluateWithoutCompile(unaryExpr.Operand);
        if (operand == null) return null;

        try
        {
            return Convert.ChangeType(operand, unaryExpr.Type);
        }
        catch (Exception ex) when (ex is InvalidCastException or FormatException or OverflowException or ArgumentException)
        {
            return null;
        }
    }

    /// <summary>
    /// 尝试求值 New 表达式（构造函数调用）
    /// </summary>
    [System.Diagnostics.CodeAnalysis.UnconditionalSuppressMessage("TrimAnalysis", "IL2067", Justification = "Constructor comes from expression tree and arguments are evaluated constants")]
    [System.Diagnostics.CodeAnalysis.UnconditionalSuppressMessage("TrimAnalysis", "IL2072", Justification = "Constructor comes from expression tree and arguments are evaluated constants")]
    private static object? TryEvaluateNew(System.Linq.Expressions.NewExpression newExpr)
    {
        // 求值所有构造函数参数
        var args = new object?[newExpr.Arguments.Count];
        for (int i = 0; i < newExpr.Arguments.Count; i++)
        {
            var argValue = TryEvaluateWithoutCompile(newExpr.Arguments[i]);
            // 如果任何参数无法求值，返回 null
            if (argValue == null && newExpr.Arguments[i] is not System.Linq.Expressions.ConstantExpression { Value: null })
            {
                return null;
            }
            args[i] = argValue;
        }

        try
        {
            // 使用构造函数创建实例
            if (newExpr.Constructor != null)
            {
                return newExpr.Constructor.Invoke(args);
            }
            // 对于无参构造函数
            return Activator.CreateInstance(newExpr.Type);
        }
        catch (Exception ex) when (ex is MissingMethodException or MemberAccessException or ArgumentException or InvalidOperationException or System.Reflection.TargetInvocationException)
        {
            return null;
        }
    }

    /// <summary>
    /// 尝试求值三元表达式
    /// </summary>
    internal static object? TryEvaluateConditional(System.Linq.Expressions.ConditionalExpression conditionalExpr)
    {
        var testValue = TryEvaluateWithoutCompile(conditionalExpr.Test);
        if (testValue is bool test)
        {
            return TryEvaluateWithoutCompile(test ? conditionalExpr.IfTrue : conditionalExpr.IfFalse);
        }
        return null;
    }

}
