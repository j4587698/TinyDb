using System.Linq.Expressions;

namespace TinyDb.Query;

/// <summary>
/// 查询表达式基类
/// </summary>
public abstract class QueryExpression
{
    /// <summary>
    /// 表达式类型
    /// </summary>
    public abstract ExpressionType NodeType { get; }
}

/// <summary>
/// 常量表达式
/// </summary>
public sealed class ConstantExpression : QueryExpression
{
    public override ExpressionType NodeType => ExpressionType.Constant;
    public object? Value { get; }

    public ConstantExpression(object? value)
    {
        Value = value;
    }
}

/// <summary>
/// 二元表达式
/// </summary>
public sealed class BinaryExpression : QueryExpression
{
    public override ExpressionType NodeType { get; }
    public QueryExpression Left { get; }
    public QueryExpression Right { get; }

    public BinaryExpression(ExpressionType nodeType, QueryExpression left, QueryExpression right)
    {
        NodeType = nodeType;
        Left = left;
        Right = right;
    }
}

/// <summary>
/// 成员表达式
/// </summary>
public sealed class MemberExpression : QueryExpression
{
    public override ExpressionType NodeType => ExpressionType.MemberAccess;
    public string MemberName { get; }
    public QueryExpression? Expression { get; }

    public MemberExpression(string memberName, QueryExpression? expression = null)
    {
        MemberName = memberName;
        Expression = expression;
    }
}

/// <summary>
/// 参数表达式
/// </summary>
public sealed class ParameterExpression : QueryExpression
{
    public override ExpressionType NodeType => ExpressionType.Parameter;
    public string Name { get; }

    public ParameterExpression(string name)
    {
        Name = name;
    }
}

/// <summary>
/// 函数表达式（用于处理Contains等字符串方法）
/// </summary>
public sealed class FunctionExpression : QueryExpression
{
    public override ExpressionType NodeType => ExpressionType.Call; // 使用Call类型表示方法调用
    public string FunctionName { get; }
    public QueryExpression Target { get; }
    public QueryExpression Argument { get; }

    public FunctionExpression(string functionName, QueryExpression target, QueryExpression argument)
    {
        FunctionName = functionName;
        Target = target;
        Argument = argument;
    }
}

/// <summary>
/// 一元表达式（如 Convert, Not）
/// </summary>
public sealed class UnaryExpression : QueryExpression
{
    public override ExpressionType NodeType { get; }
    public QueryExpression Operand { get; }
    public Type Type { get; } // Target type for conversion

    public UnaryExpression(ExpressionType nodeType, QueryExpression operand, Type type)
    {
        NodeType = nodeType;
        Operand = operand;
        Type = type;
    }
}
