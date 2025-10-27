using System.Linq.Expressions;
using SimpleDb.Query;

namespace SimpleDb.Query;

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
        if (expression == null) throw new ArgumentNullException(nameof(expression));
        return ParseExpression(expression.Body);
    }

    /// <summary>
    /// 解析表达式
    /// </summary>
    /// <param name="expression">表达式</param>
    /// <returns>查询表达式</returns>
    private QueryExpression ParseExpression(Expression expression)
    {
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
        var memberName = member.Member.Name;
        QueryExpression? expression = null;

        if (member.Expression != null)
        {
            expression = ParseExpression(member.Expression);
        }

        return new MemberExpression(memberName, expression);
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
            "ToString" => ParseExpression(methodCall.Object!),
            _ => throw new NotSupportedException($"Method '{methodName}' is not supported in queries")
        };
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

        // 简化实现：将 Contains 转换为字符串包含比较
        // 实际实现可能需要更复杂的逻辑
        throw new NotSupportedException("Contains method is not yet implemented");
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

        // 简化实现：暂时不支持
        throw new NotSupportedException("StartsWith method is not yet implemented");
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

        // 简化实现：暂时不支持
        throw new NotSupportedException("EndsWith method is not yet implemented");
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