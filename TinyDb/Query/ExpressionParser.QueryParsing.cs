using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace TinyDb.Query;

public sealed partial class ExpressionParser
{
    [System.Diagnostics.CodeAnalysis.UnconditionalSuppressMessage("TrimAnalysis", "IL2072", Justification = "The type comes from the expression tree and is expected to be valid.")]
    private QueryExpression ParseNewExpression(System.Linq.Expressions.NewExpression newExpr)
    {
        var args = newExpr.Arguments.Select(ParseExpression).ToList();
        return new ConstructorExpression(newExpr.Type, args);
    }

    [System.Diagnostics.CodeAnalysis.UnconditionalSuppressMessage("TrimAnalysis", "IL2072", Justification = "The type comes from the expression tree and is expected to be valid.")]
    private QueryExpression ParseMemberInitExpression(System.Linq.Expressions.MemberInitExpression memberInit)
    {
        var bindings = new List<(string MemberName, QueryExpression Value)>();

        foreach (var binding in memberInit.Bindings)
        {
            if (binding is System.Linq.Expressions.MemberAssignment assignment)
            {
                var memberName = assignment.Member.Name;
                var value = ParseExpression(assignment.Expression);
                bindings.Add((memberName, value));
            }
        }

        return new MemberInitQueryExpression(memberInit.Type, bindings);
    }

    private QueryExpression ParseConditionalExpression(System.Linq.Expressions.ConditionalExpression conditional)
    {
        var test = ParseExpression(conditional.Test);
        var ifTrue = ParseExpression(conditional.IfTrue);
        var ifFalse = ParseExpression(conditional.IfFalse);
        return new ConditionalQueryExpression(test, ifTrue, ifFalse);
    }

    /// <summary>
    /// 参数检查器 - 检查表达式是否包含参数
    /// </summary>
    private class ParameterChecker : ExpressionVisitor
    {
        private bool _hasParameter;

        public static bool Check(Expression expression)
        {
            var checker = new ParameterChecker();
            checker.Visit(expression);
            return checker._hasParameter;
        }

        public override Expression? Visit(Expression? node)
        {
            if (_hasParameter) return node; // 快速退出
            return base.Visit(node);
        }

        protected override Expression VisitParameter(System.Linq.Expressions.ParameterExpression node)
        {
            _hasParameter = true;
            return node;
        }
    }

    /// <summary>
    /// 解析二元表达式
    /// </summary>
    /// <param name="binary">二元表达式</param>
    /// <returns>查询表达式</returns>
    private QueryExpression ParseBinaryExpression(System.Linq.Expressions.BinaryExpression binary)
    {
        var left = ParseExpression(binary.Left);
        var right = ParseExpression(binary.Right);

        return binary.NodeType switch
        {
            ExpressionType.Equal => new BinaryExpression(ExpressionType.Equal, left, right),
            ExpressionType.NotEqual => new BinaryExpression(ExpressionType.NotEqual, left, right),
            ExpressionType.GreaterThan => new BinaryExpression(ExpressionType.GreaterThan, left, right),
            ExpressionType.GreaterThanOrEqual => new BinaryExpression(ExpressionType.GreaterThanOrEqual, left, right),
            ExpressionType.LessThan => new BinaryExpression(ExpressionType.LessThan, left, right),
            ExpressionType.LessThanOrEqual => new BinaryExpression(ExpressionType.LessThanOrEqual, left, right),
            ExpressionType.AndAlso => new BinaryExpression(ExpressionType.AndAlso, left, right),
            ExpressionType.OrElse => new BinaryExpression(ExpressionType.OrElse, left, right),
            ExpressionType.Add => new BinaryExpression(ExpressionType.Add, left, right),
            ExpressionType.Subtract => new BinaryExpression(ExpressionType.Subtract, left, right),
            ExpressionType.Multiply => new BinaryExpression(ExpressionType.Multiply, left, right),
            ExpressionType.Divide => new BinaryExpression(ExpressionType.Divide, left, right),
            _ => throw new NotSupportedException($"Binary operation {binary.NodeType} is not supported")
        };
    }

    /// <summary>
    /// 解析成员表达式
    /// </summary>
    /// <param name="member">成员表达式</param>
    /// <returns>查询表达式</returns>
    internal QueryExpression ParseMemberExpression(System.Linq.Expressions.MemberExpression member)
    {
        // 尝试处理静态成员访问 (例如 DateTime.Now)
        if (member.Expression == null)
        {
            try
            {
                if (member.Member.DeclaringType == typeof(DateTime) && member.Member is System.Reflection.PropertyInfo dateTimeProp)
                {
                    if (dateTimeProp.Name == nameof(DateTime.Now))
                        return new ConstantExpression(DateTime.Now);
                    if (dateTimeProp.Name == nameof(DateTime.UtcNow))
                        return new ConstantExpression(DateTime.UtcNow);
                    if (dateTimeProp.Name == nameof(DateTime.Today))
                        return new ConstantExpression(DateTime.Today);
                }

                if (member.Member is System.Reflection.FieldInfo field && field.IsStatic)
                {
                    return new ConstantExpression(field.GetValue(null));
                }

                throw new NotSupportedException($"Failed to evaluate static member {member.Member.Name}");
            }
            catch (Exception ex) when (ex is NotSupportedException or ArgumentException or InvalidOperationException or System.Reflection.TargetException or System.Reflection.TargetInvocationException)
            {
                // 如果求值失败，抛出异常或作为普通成员访问处理(虽然静态成员必须求值)
                throw new NotSupportedException($"Failed to evaluate static member {member.Member.Name}", ex);
            }
        }

        // 解析子表达式
        var expression = ParseExpression(member.Expression);

        // 如果子表达式是常量，说明这是对变量或闭包的访问，可以立即求值
        if (expression is ConstantExpression constantExpr)
        {
            try
            {
                object? container = constantExpr.Value;
                object? value = null;

                if (member.Member is System.Reflection.FieldInfo field)
                    value = field.GetValue(container);
                else if (member.Member is System.Reflection.PropertyInfo prop)
                    value = EvaluateProperty(container, prop);

                return new ConstantExpression(value);
            }
            catch (Exception ex) when (ex is InvalidOperationException or NotSupportedException or ArgumentException or System.Reflection.TargetException or System.Reflection.TargetInvocationException)
            {
                // 求值失败忽略，继续作为成员表达式处理 (虽然理论上不应该发生)
            }
        }

        return new MemberExpression(member.Member.Name, expression);
    }

    private static object? EvaluateProperty(object? container, System.Reflection.PropertyInfo prop)
    {
        var known = ExpressionKnownMemberEvaluator.EvaluateProperty(container, prop);
        if (known != null)
        {
            return known;
        }

        var value = prop.GetValue(container);
        if (value == null)
        {
            return null;
        }

        return value;
    }

    /// <summary>
    /// 解析参数表达式
    /// </summary>
    /// <param name="parameter">参数表达式</param>
    /// <returns>查询表达式</returns>
    private QueryExpression ParseParameterExpression(System.Linq.Expressions.ParameterExpression parameter)
    {
        return new ParameterExpression(parameter.Name!);
    }

    /// <summary>
    /// 解析一元表达式
    /// </summary>
    /// <param name="unary">一元表达式</param>
    /// <returns>查询表达式</returns>
    [System.Diagnostics.CodeAnalysis.UnconditionalSuppressMessage("TrimAnalysis", "IL2072", Justification = "The type comes from the expression tree and is expected to be a valid entity type handled by the engine.")]
    private QueryExpression ParseUnaryExpression(System.Linq.Expressions.UnaryExpression unary)
    {
        // 对于 ArrayLength，尝试先求值（因为通常是闭包变量）
        var operand = ParseExpression(unary.Operand);

        var nodeType = unary.NodeType;

        if (nodeType == ExpressionType.Not)
            return new UnaryExpression(ExpressionType.Not, operand, typeof(bool));

        if (nodeType == ExpressionType.Negate)
            return new BinaryExpression(ExpressionType.Subtract, new ConstantExpression(0), operand);

        if (nodeType == ExpressionType.Convert)
            return new UnaryExpression(ExpressionType.Convert, operand, unary.Type);

        if (nodeType == ExpressionType.ArrayLength)
            return new UnaryExpression(ExpressionType.ArrayLength, operand, typeof(int));

        throw new NotSupportedException($"Unary operation {unary.NodeType} is not supported");
    }

    /// <summary>
    /// 解析方法调用表达式
    /// </summary>
    /// <param name="methodCall">方法调用表达式</param>
    /// <returns>查询表达式</returns>
    [System.Diagnostics.CodeAnalysis.UnconditionalSuppressMessage("TrimAnalysis", "IL2072", Justification = "The type comes from the expression tree and is expected to be a valid entity type handled by the engine.")]
    private QueryExpression ParseMethodCallExpression(System.Linq.Expressions.MethodCallExpression methodCall)
    {
        var methodName = methodCall.Method.Name;

        // Handle Convert.To...
        if (methodCall.Method.DeclaringType == typeof(Convert) && methodName.StartsWith("To") && methodCall.Arguments.Count == 1)
        {
            var arg = ParseExpression(methodCall.Arguments[0]);
            return new UnaryExpression(ExpressionType.Convert, arg, methodCall.Type);
        }

        // Special handling for Equals (convert to BinaryExpression)
        if (methodName == "Equals" && methodCall.Arguments.Count == 1 && methodCall.Object != null)
        {
            var left = ParseExpression(methodCall.Object);
            var right = ParseExpression(methodCall.Arguments[0]);
            return new BinaryExpression(ExpressionType.Equal, left, right);
        }

        // Special handling for ToString (constant optimization)
        if (methodName == "ToString" && methodCall.Arguments.Count == 0 && methodCall.Object != null)
        {
             var objectExpression = ParseExpression(methodCall.Object);
             // 如果是常量表达式，立即求值
             if (objectExpression is ConstantExpression constantExpr && constantExpr.Value != null)
             {
                 return new ConstantExpression(constantExpr.Value.ToString());
             }
             // 否则作为普通函数调用处理
        }

        // Generic method parsing
        var target = methodCall.Object != null ? ParseExpression(methodCall.Object) : null;
        var args = methodCall.Arguments.Select(ParseExpression).ToList();

        return new FunctionExpression(methodName, target, args);
    }}
