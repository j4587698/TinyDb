using System.Linq.Expressions;
using TinyDb.Query;

namespace TinyDb.Query;

/// <summary>
/// LINQ 表达式解析器
/// </summary>
public sealed class ExpressionParser
{
    /// <summary>
    /// 解析 LINQ 表达式
    /// </summary>
    /// <typeparam name="T">参数类型</typeparam>
    /// <param name="expression">LINQ 表达式</param>
    /// <returns>查询表达式</returns>
    public QueryExpression Parse<T>(Expression<Func<T, bool>> expression)
    {
        if (expression == null) return null!;
        return ParseExpression(expression.Body);
    }

    /// <summary>
    /// 解析表达式
    /// </summary>
    /// <param name="expression">表达式</param>
    /// <returns>查询表达式</returns>
    private QueryExpression ParseExpression(Expression expression)
    {
        // 尝试提前求值：如果表达式不依赖于参数，则将其视为常量进行求值
        // 这解决了闭包、局部变量、复杂数学运算和不受支持的方法调用（只要它们是常量）的问题
        if (!ParameterChecker.Check(expression))
        {
            try
            {
                // 对于简单的常量表达式，直接提取值以避免编译开销
                if (expression is System.Linq.Expressions.ConstantExpression constExpr)
                {
                    return new ConstantExpression(constExpr.Value);
                }

                // 对于其他不包含参数的表达式，编译并执行
                var lambda = System.Linq.Expressions.Expression.Lambda(expression);
                var fn = lambda.Compile();
                var value = fn.DynamicInvoke();
                return new ConstantExpression(value);
            }
            catch
            {
                // 如果求值失败（极少情况），降级为常规解析
            }
        }

        return expression switch
        {
            System.Linq.Expressions.BinaryExpression binary => ParseBinaryExpression(binary),
            System.Linq.Expressions.MemberExpression member => ParseMemberExpression(member),
            System.Linq.Expressions.ConstantExpression constant => ParseConstantExpression(constant),
            System.Linq.Expressions.ParameterExpression parameter => ParseParameterExpression(parameter),
            System.Linq.Expressions.UnaryExpression unary => ParseUnaryExpression(unary),
            System.Linq.Expressions.MethodCallExpression methodCall => ParseMethodCallExpression(methodCall),
            _ => throw new NotSupportedException($"Expression type {expression.NodeType} is not supported")
        };
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
    private QueryExpression ParseMemberExpression(System.Linq.Expressions.MemberExpression member)
    {
        // 尝试处理静态成员访问 (例如 DateTime.Now)
        if (member.Expression == null)
        {
            try
            {
                object? value = null;
                if (member.Member is System.Reflection.FieldInfo field)
                    value = field.GetValue(null);
                else if (member.Member is System.Reflection.PropertyInfo prop)
                    value = prop.GetValue(null);
                
                return new ConstantExpression(value);
            }
            catch
            {
                // 如果求值失败，抛出异常或作为普通成员访问处理(虽然静态成员必须求值)
                throw new NotSupportedException($"Failed to evaluate static member {member.Member.Name}");
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
                    value = prop.GetValue(container);
                
                return new ConstantExpression(value);
            }
            catch
            {
                // 求值失败忽略，继续作为成员表达式处理 (虽然理论上不应该发生)
            }
        }

        return new MemberExpression(member.Member.Name, expression);
    }

    /// <summary>
    /// 解析常量表达式
    /// </summary>
    /// <param name="constant">常量表达式</param>
    /// <returns>查询表达式</returns>
    private QueryExpression ParseConstantExpression(System.Linq.Expressions.ConstantExpression constant)
    {
        return new ConstantExpression(constant.Value);
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
    private QueryExpression ParseUnaryExpression(System.Linq.Expressions.UnaryExpression unary)
    {
        var operand = ParseExpression(unary.Operand);

        return unary.NodeType switch
        {
            ExpressionType.Not => new BinaryExpression(ExpressionType.NotEqual, operand, new ConstantExpression(true)),
            ExpressionType.Negate => new BinaryExpression(ExpressionType.Subtract, new ConstantExpression(0), operand),
            ExpressionType.Convert => ParseExpression(unary.Operand), // 类型转换，直接解析操作数
            _ => throw new NotSupportedException($"Unary operation {unary.NodeType} is not supported")
        };
    }

    /// <summary>
    /// 解析方法调用表达式
    /// </summary>
    /// <param name="methodCall">方法调用表达式</param>
    /// <returns>查询表达式</returns>
    private QueryExpression ParseMethodCallExpression(System.Linq.Expressions.MethodCallExpression methodCall)
    {
        var methodName = methodCall.Method.Name;

        // 处理一些常见的字符串方法
        return methodName switch
        {
            "Contains" => ParseContainsMethod(methodCall),
            "StartsWith" => ParseStartsWithMethod(methodCall),
            "EndsWith" => ParseEndsWithMethod(methodCall),
            "Equals" => ParseEqualsMethod(methodCall),
            "ToString" => ParseToStringMethod(methodCall),
            _ => throw new NotSupportedException($"Method '{methodName}' is not supported in queries")
        };
    }

    /// <summary>
    /// 解析 ToString 方法
    /// </summary>
    /// <param name="methodCall">方法调用表达式</param>
    /// <returns>查询表达式</returns>
    private QueryExpression ParseToStringMethod(System.Linq.Expressions.MethodCallExpression methodCall)
    {
        var objectExpression = ParseExpression(methodCall.Object!);

        // 如果是常量表达式，立即求值
        if (objectExpression is ConstantExpression constantExpr && constantExpr.Value != null)
        {
            return new ConstantExpression(constantExpr.Value.ToString());
        }

        // 如果不是常量，目前只能返回原表达式（这可能导致类型不匹配，但在有限的查询支持下可能足够）
        // 理想情况下应该引入一个 ToStringExpression 类型
        return objectExpression;
    }

    /// <summary>
    /// 解析 Contains 方法
    /// </summary>
    /// <param name="methodCall">方法调用表达式</param>
    /// <returns>查询表达式</returns>
    private QueryExpression ParseContainsMethod(System.Linq.Expressions.MethodCallExpression methodCall)
    {
        if (methodCall.Object == null || methodCall.Arguments.Count != 1)
            throw new InvalidOperationException("Contains method must have one argument");

        var left = ParseExpression(methodCall.Object);
        var right = ParseExpression(methodCall.Arguments[0]);

        // 将 Contains 转换为字符串包含比较
        // 使用自定义的字符串包含操作，通过FunctionExpression处理
        return new FunctionExpression("Contains", left, right);
    }

    /// <summary>
    /// 解析 StartsWith 方法
    /// </summary>
    /// <param name="methodCall">方法调用表达式</param>
    /// <returns>查询表达式</returns>
    private QueryExpression ParseStartsWithMethod(System.Linq.Expressions.MethodCallExpression methodCall)
    {
        if (methodCall.Object == null || methodCall.Arguments.Count != 1)
            throw new InvalidOperationException("StartsWith method must have one argument");

        var targetExpression = ParseExpression(methodCall.Object);
        var argumentExpression = ParseExpression(methodCall.Arguments[0]);

        // 创建一个表示StartsWith操作的函数表达式
        return new FunctionExpression("StartsWith", targetExpression, argumentExpression);
    }

    /// <summary>
    /// 解析 EndsWith 方法
    /// </summary>
    /// <param name="methodCall">方法调用表达式</param>
    /// <returns>查询表达式</returns>
    private QueryExpression ParseEndsWithMethod(System.Linq.Expressions.MethodCallExpression methodCall)
    {
        if (methodCall.Object == null || methodCall.Arguments.Count != 1)
            throw new InvalidOperationException("EndsWith method must have one argument");

        var targetExpression = ParseExpression(methodCall.Object);
        var argumentExpression = ParseExpression(methodCall.Arguments[0]);

        // 创建一个表示EndsWith操作的函数表达式
        return new FunctionExpression("EndsWith", targetExpression, argumentExpression);
    }

    /// <summary>
    /// 解析 Equals 方法
    /// </summary>
    /// <param name="methodCall">方法调用表达式</param>
    /// <returns>查询表达式</returns>
    private QueryExpression ParseEqualsMethod(System.Linq.Expressions.MethodCallExpression methodCall)
    {
        if (methodCall.Object == null || methodCall.Arguments.Count != 1)
            throw new InvalidOperationException("Equals method must have one argument");

        var left = ParseExpression(methodCall.Object);
        var right = ParseExpression(methodCall.Arguments[0]);

        return new BinaryExpression(ExpressionType.Equal, left, right);
    }
}