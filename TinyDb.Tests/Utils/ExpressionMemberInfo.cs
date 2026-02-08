using System;
using System.Linq.Expressions;
using System.Reflection;

namespace TinyDb.Tests.Utils;

internal static class ExpressionMemberInfo
{
    public static PropertyInfo Property<T, TProperty>(Expression<Func<T, TProperty>> expression)
    {
        if (expression == null) throw new ArgumentNullException(nameof(expression));

        var memberExpression = GetMemberExpression(expression);
        return memberExpression.Member as PropertyInfo
               ?? throw new ArgumentException("表达式必须是属性访问。", nameof(expression));
    }

    public static FieldInfo Field<TField>(Expression<Func<TField>> expression)
    {
        if (expression == null) throw new ArgumentNullException(nameof(expression));

        var memberExpression = GetMemberExpression(expression);
        return memberExpression.Member as FieldInfo
               ?? throw new ArgumentException("表达式必须是字段访问。", nameof(expression));
    }

    private static MemberExpression GetMemberExpression(LambdaExpression expression)
    {
        return expression.Body switch
        {
            MemberExpression member => member,
            UnaryExpression { Operand: MemberExpression member } => member,
            _ => throw new ArgumentException("表达式必须是成员访问。", nameof(expression))
        };
    }
}

