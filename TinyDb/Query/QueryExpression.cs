using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
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
/// 函数表达式（用于处理方法调用）
/// </summary>
public sealed class FunctionExpression : QueryExpression
{
    public override ExpressionType NodeType => ExpressionType.Call;
    public string FunctionName { get; }
    public QueryExpression? Target { get; }
    public IReadOnlyList<QueryExpression> Arguments { get; }

    public FunctionExpression(string functionName, QueryExpression? target, IEnumerable<QueryExpression> arguments)
    {
        FunctionName = functionName;
        Target = target;
        Arguments = arguments?.ToList() ?? new List<QueryExpression>();
    }
    
    public FunctionExpression(string functionName, QueryExpression target, QueryExpression argument) 
        : this(functionName, target, new[] { argument })
    {
    }
}

/// <summary>
/// 一元表达式（如 Convert, Not）
/// </summary>
public sealed class UnaryExpression : QueryExpression
{
    public override ExpressionType NodeType { get; }
    public QueryExpression Operand { get; }
    
    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicParameterlessConstructor)]
    public Type Type { get; } // Target type for conversion

    public UnaryExpression(ExpressionType nodeType, QueryExpression operand, [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicParameterlessConstructor)] Type type)
    {
        NodeType = nodeType;
        Operand = operand;
        Type = type;
    }
}

/// <summary>
/// 构造函数表达式 (new T(...))
/// </summary>
public sealed class ConstructorExpression : QueryExpression
{
    public override ExpressionType NodeType => ExpressionType.New;
    
    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)]
    public Type Type { get; }
    
    public IReadOnlyList<QueryExpression> Arguments { get; }

    public ConstructorExpression([DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] Type type, IEnumerable<QueryExpression> arguments)
    {
        Type = type;
        Arguments = arguments?.ToList() ?? new List<QueryExpression>();
    }
}

/// <summary>
/// 成员初始化表达式 (new T { Prop = value, ... })
/// </summary>
public sealed class MemberInitQueryExpression : QueryExpression
{
    public override ExpressionType NodeType => ExpressionType.MemberInit;
    
    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties)]
    public Type Type { get; }
    
    public IReadOnlyList<(string MemberName, QueryExpression Value)> Bindings { get; }

    public MemberInitQueryExpression(
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties)] Type type, 
        IEnumerable<(string MemberName, QueryExpression Value)> bindings)
    {
        Type = type;
        Bindings = bindings?.ToList() ?? new List<(string, QueryExpression)>();
    }
}

/// <summary>
/// ä¸‰å…ƒè¡¨è¾¾å¼ (condition ? ifTrue : ifFalse)
/// </summary>
public sealed class ConditionalQueryExpression : QueryExpression
{
    public override ExpressionType NodeType => ExpressionType.Conditional;
    public QueryExpression Test { get; }
    public QueryExpression IfTrue { get; }
    public QueryExpression IfFalse { get; }

    public ConditionalQueryExpression(QueryExpression test, QueryExpression ifTrue, QueryExpression ifFalse)
    {
        Test = test;
        IfTrue = ifTrue;
        IfFalse = ifFalse;
    }
}
