using System.Linq.Expressions;
using TinyDb.Query;

namespace TinyDb.Query;

/// <summary>
/// LINQ 表达式解析器
/// </summary>
public sealed partial class ExpressionParser
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
    public QueryExpression ParseExpression(Expression expression)
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

                // 尝试手动求值（AOT 安全）
                var manualResult = ExpressionConstantEvaluator.TryEvaluateWithoutCompile(expression);
                if (manualResult != null)
                {
                    return new ConstantExpression(manualResult);
                }

                // 对于其他不包含参数的表达式，尝试编译并执行 (仅在支持动态代码的环境下)
            }
            catch (Exception ex) when (ex is InvalidOperationException or NotSupportedException or ArgumentException or FormatException or InvalidCastException or OverflowException or System.Reflection.TargetException or System.Reflection.TargetInvocationException)
            {
                // 如果求值失败（极少情况），降级为常规解析
            }
        }

        return expression switch
        {
            System.Linq.Expressions.LambdaExpression lambda => new ConstantExpression(lambda),
            System.Linq.Expressions.BinaryExpression binary => ParseBinaryExpression(binary),
            System.Linq.Expressions.MemberExpression member => ParseMemberExpression(member),
            System.Linq.Expressions.ParameterExpression parameter => ParseParameterExpression(parameter),
            System.Linq.Expressions.UnaryExpression unary => ParseUnaryExpression(unary),
            System.Linq.Expressions.MethodCallExpression methodCall => ParseMethodCallExpression(methodCall),
            System.Linq.Expressions.NewExpression newExpr => ParseNewExpression(newExpr),
            System.Linq.Expressions.MemberInitExpression memberInit => ParseMemberInitExpression(memberInit),
            System.Linq.Expressions.ConditionalExpression conditional => ParseConditionalExpression(conditional),
            _ => throw new NotSupportedException($"Expression type {expression.NodeType} is not supported")
        };
    }

}
