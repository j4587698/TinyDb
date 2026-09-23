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

    /// <summary>
    /// CLR 属性名（例如 <c>Uid</c>）。用于针对实体对象求值。
    /// </summary>
    public string MemberName { get; }

    /// <summary>
    /// 该成员对应的 BSON 存储字段名（例如主键属性 <c>Uid</c> 对应 <c>_id</c>）。
    /// 用于所有面向文档的访问：谓词下推、索引选择、排序键读取、BsonDocument 求值。
    /// </summary>
    public string StorageName { get; }

    public QueryExpression? Expression { get; }

    /// <summary>
    /// 该成员是否直接访问查询参数（即根文档字段），而非嵌套对象的字段。
    /// 只有根成员才允许参与谓词下推与索引选择。
    /// </summary>
    public bool IsRootMember => Expression == null || Expression.NodeType == ExpressionType.Parameter;

    public MemberExpression(string memberName, QueryExpression? expression = null)
        : this(memberName, expression, storageName: null)
    {
    }

    public MemberExpression(string memberName, QueryExpression? expression, string? storageName)
    {
        MemberName = memberName;
        Expression = expression;

        // _id 改名只适用于集合根文档；内嵌复杂对象的字段一律是 camelCase。
        StorageName = storageName ?? (IsRootMember
            ? Serialization.BsonFieldName.ForMember(memberName, declaringType: null)
            : Serialization.BsonFieldName.ToCamelCase(memberName));
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

internal static class RuntimeFunctionNames
{
    public const string DateTimeNow = "$DateTime.Now";
    public const string DateTimeUtcNow = "$DateTime.UtcNow";
    public const string DateTimeToday = "$DateTime.Today";
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
/// 三元表达式 (condition ? ifTrue : ifFalse)
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
