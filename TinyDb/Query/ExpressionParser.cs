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
                var manualResult = TryEvaluateWithoutCompile(expression);
                if (manualResult != null)
                {
                    return new ConstantExpression(manualResult);
                }

                // 对于其他不包含参数的表达式，尝试编译并执行 (仅在支持动态代码的环境下)
            }
            catch
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

    /// <summary>
    /// 尝试在不使用 Compile 的情况下手动求值表达式（AOT 安全）
    /// </summary>
    private static object? TryEvaluateWithoutCompile(Expression expression)
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
            return EvaluateKnownDateTimeProperty(dt, prop.Name);
        }

        if (container is null && prop.DeclaringType == typeof(System.Environment) && prop.Name == nameof(System.Environment.NewLine))
        {
            return System.Environment.NewLine;
        }

        return null;
    }

    /// <summary>
    /// 尝试求值二元表达式
    /// </summary>
    private static object? TryEvaluateMethodCall(System.Linq.Expressions.MethodCallExpression methodCallExpr)
    {
        if (methodCallExpr.Object == null)
        {
            if (methodCallExpr.Method.DeclaringType == typeof(Guid) &&
                methodCallExpr.Method.Name == nameof(Guid.NewGuid) &&
                methodCallExpr.Arguments.Count == 0)
            {
                return Guid.NewGuid();
            }

            if (methodCallExpr.Method.DeclaringType == typeof(string))
            {
                if (methodCallExpr.Method.Name == nameof(string.IsNullOrEmpty) && methodCallExpr.Arguments.Count == 1)
                {
                    var value = TryEvaluateWithoutCompile(methodCallExpr.Arguments[0]) as string;
                    return string.IsNullOrEmpty(value);
                }

                if (methodCallExpr.Method.Name == nameof(string.IsNullOrWhiteSpace) && methodCallExpr.Arguments.Count == 1)
                {
                    var value = TryEvaluateWithoutCompile(methodCallExpr.Arguments[0]) as string;
                    return string.IsNullOrWhiteSpace(value);
                }
            }

            return null;
        }

        var target = TryEvaluateWithoutCompile(methodCallExpr.Object);
        if (methodCallExpr.Object is not System.Linq.Expressions.ConstantExpression { Value: null } && target == null)
        {
            return null;
        }

        var args = new object?[methodCallExpr.Arguments.Count];
        for (int i = 0; i < methodCallExpr.Arguments.Count; i++)
        {
            var argValue = TryEvaluateWithoutCompile(methodCallExpr.Arguments[i]);
            if (argValue == null && methodCallExpr.Arguments[i] is not System.Linq.Expressions.ConstantExpression { Value: null })
            {
                return null;
            }
            args[i] = argValue;
        }

        if (methodCallExpr.Method.Name == nameof(object.ToString) && args.Length == 0)
        {
            return target is null ? null : target.ToString();
        }

        if (target is string s)
        {
            var methodName = methodCallExpr.Method.Name;

            if (methodName == nameof(string.Contains) && args.Length == 1 && args[0] is string containsValue)
                return s.Contains(containsValue);
            if (methodName == nameof(string.StartsWith) && args.Length == 1 && args[0] is string startsWithValue)
                return s.StartsWith(startsWithValue);
            if (methodName == nameof(string.EndsWith) && args.Length == 1 && args[0] is string endsWithValue)
                return s.EndsWith(endsWithValue);

            if (methodName == nameof(string.ToLower) && args.Length == 0)
                return s.ToLowerInvariant();
            if (methodName == nameof(string.ToUpper) && args.Length == 0)
                return s.ToUpperInvariant();
            if (methodName == nameof(string.Trim) && args.Length == 0)
                return s.Trim();

            if (methodName == nameof(string.Substring) && args.Length == 1)
                return s.Substring(Convert.ToInt32(args[0]));
            if (methodName == nameof(string.Substring) && args.Length == 2)
                return s.Substring(Convert.ToInt32(args[0]), Convert.ToInt32(args[1]));

            if (methodName == nameof(string.Replace) && args.Length == 2 && args[0] is string oldValue && args[1] is string newValue)
                return s.Replace(oldValue, newValue);

            return null;
        }

        if (target is DateTime dt)
        {
            return methodCallExpr.Method.Name switch
            {
                nameof(DateTime.AddDays) when args.Length == 1 => dt.AddDays(Convert.ToDouble(args[0])),
                nameof(DateTime.AddHours) when args.Length == 1 => dt.AddHours(Convert.ToDouble(args[0])),
                nameof(DateTime.AddMinutes) when args.Length == 1 => dt.AddMinutes(Convert.ToDouble(args[0])),
                nameof(DateTime.AddSeconds) when args.Length == 1 => dt.AddSeconds(Convert.ToDouble(args[0])),
                nameof(DateTime.AddYears) when args.Length == 1 => dt.AddYears(Convert.ToInt32(args[0])),
                nameof(DateTime.AddMonths) when args.Length == 1 => dt.AddMonths(Convert.ToInt32(args[0])),
                _ => null
            };
        }

        return null;
    }

    private static object? TryEvaluateBinary(System.Linq.Expressions.BinaryExpression binaryExpr)
    {
        var nodeType = binaryExpr.NodeType;

        if (nodeType is ExpressionType.AndAlso or ExpressionType.OrElse)
        {
            var leftValue = TryEvaluateWithoutCompile(binaryExpr.Left);
            if (leftValue is not bool leftBool) return null;

            if (nodeType == ExpressionType.AndAlso && !leftBool) return false;
            if (nodeType == ExpressionType.OrElse && leftBool) return true;

            var rightValue = TryEvaluateWithoutCompile(binaryExpr.Right);
            return rightValue is bool rightBool ? rightBool : null;
        }

        var left = TryEvaluateWithoutCompile(binaryExpr.Left);
        var right = TryEvaluateWithoutCompile(binaryExpr.Right);

        if (left == null) return null;
        if (right == null) return null;

        try
        {
            if (nodeType == ExpressionType.Add) return EvaluateAdd(left, right);
            if (nodeType == ExpressionType.Subtract) return EvaluateSubtract(left, right);
            if (nodeType == ExpressionType.Multiply) return EvaluateMultiply(left, right);
            if (nodeType == ExpressionType.Divide) return EvaluateDivide(left, right);

            if (nodeType == ExpressionType.Equal) return Equals(left, right);
            if (nodeType == ExpressionType.NotEqual) return !Equals(left, right);

            if (TryCompare(left, right, out var compareResult))
            {
                if (nodeType == ExpressionType.GreaterThan) return compareResult > 0;
                if (nodeType == ExpressionType.GreaterThanOrEqual) return compareResult >= 0;
                if (nodeType == ExpressionType.LessThan) return compareResult < 0;
                if (nodeType == ExpressionType.LessThanOrEqual) return compareResult <= 0;
            }

            return null;
        }
        catch
        {
            return null;
        }
    }

    private static bool TryCompare(object left, object right, out int result)
    {
        result = 0;

        if (left is int li && right is int ri) { result = li.CompareTo(ri); return true; }
        if (left is int li2 && right is long rl2) { result = ((long)li2).CompareTo(rl2); return true; }
        if (left is long ll3 && right is int ri3) { result = ll3.CompareTo((long)ri3); return true; }
        if (left is long ll && right is long rl) { result = ll.CompareTo(rl); return true; }

        if (left is double ld && right is double rd) { result = ld.CompareTo(rd); return true; }
        if (left is double ld2 && right is int ri2) { result = ld2.CompareTo((double)ri2); return true; }
        if (left is int li4 && right is double rd2) { result = ((double)li4).CompareTo(rd2); return true; }
        if (left is double ld3 && right is long rl3) { result = ld3.CompareTo((double)rl3); return true; }
        if (left is long ll4 && right is double rd3) { result = ((double)ll4).CompareTo(rd3); return true; }

        if (left is decimal lm && right is decimal rm) { result = lm.CompareTo(rm); return true; }
        if (left is decimal lm2 && right is int ri5) { result = lm2.CompareTo((decimal)ri5); return true; }
        if (left is int li5 && right is decimal rm2) { result = ((decimal)li5).CompareTo(rm2); return true; }
        if (left is decimal lm3 && right is long rl4) { result = lm3.CompareTo((decimal)rl4); return true; }
        if (left is long ll5 && right is decimal rm3) { result = ((decimal)ll5).CompareTo(rm3); return true; }

        if (left is string ls && right is string rs) { result = string.CompareOrdinal(ls, rs); return true; }
        if (left is DateTime ldt && right is DateTime rdt) { result = ldt.CompareTo(rdt); return true; }
        if (left is TimeSpan lts && right is TimeSpan rts) { result = lts.CompareTo(rts); return true; }

        return false;
    }

    private static object? EvaluateAdd(object left, object right) =>
        (left, right) switch
        {
            (int l, int r) => l + r,
            (long l, long r) => l + r,
            (double l, double r) => l + r,
            (decimal l, decimal r) => l + r,
            (int l, long r) => l + r,
            (long l, int r) => l + r,
            (DateTime l, TimeSpan r) => l + r,
            (TimeSpan l, TimeSpan r) => l + r,
            _ => null
        };

    private static object? EvaluateSubtract(object left, object right) =>
        (left, right) switch
        {
            (int l, int r) => l - r,
            (long l, long r) => l - r,
            (double l, double r) => l - r,
            (decimal l, decimal r) => l - r,
            (int l, long r) => l - r,
            (long l, int r) => l - r,
            (DateTime l, TimeSpan r) => l - r,
            (DateTime l, DateTime r) => l - r,
            (TimeSpan l, TimeSpan r) => l - r,
            _ => null
        };

    private static object? EvaluateMultiply(object left, object right) =>
        (left, right) switch
        {
            (int l, int r) => l * r,
            (long l, long r) => l * r,
            (double l, double r) => l * r,
            (decimal l, decimal r) => l * r,
            (int l, long r) => l * r,
            (long l, int r) => l * r,
            (TimeSpan l, double r) => TimeSpan.FromTicks(checked((long)(l.Ticks * r))),
            (double l, TimeSpan r) => TimeSpan.FromTicks(checked((long)(l * r.Ticks))),
            (TimeSpan l, int r) => TimeSpan.FromTicks(checked(l.Ticks * r)),
            (int l, TimeSpan r) => TimeSpan.FromTicks(checked((long)l * r.Ticks)),
            _ => null
        };

    private static object? EvaluateDivide(object left, object right) =>
        (left, right) switch
        {
            (int l, int r) when r != 0 => l / r,
            (long l, long r) when r != 0 => l / r,
            (double l, double r) when r != 0 => l / r,
            (decimal l, decimal r) when r != 0 => l / r,
            (int l, long r) when r != 0 => l / r,
            (long l, int r) when r != 0 => l / r,
            (TimeSpan l, double r) when r != 0 => TimeSpan.FromTicks(checked((long)(l.Ticks / r))),
            (TimeSpan l, int r) when r != 0 => TimeSpan.FromTicks(l.Ticks / r),
            _ => null
        };

    /// <summary>
    /// 尝试求值类型转换表达式
    /// </summary>
    private static object? TryEvaluateConvert(System.Linq.Expressions.UnaryExpression unaryExpr)
    {
        var operand = TryEvaluateWithoutCompile(unaryExpr.Operand);
        if (operand == null) return null;

        try
        {
            return Convert.ChangeType(operand, unaryExpr.Type);
        }
        catch
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
        catch
        {
            return null;
        }
    }

    /// <summary>
    /// å°è¯•æ±‚å€¼ä¸‰å…ƒè¡¨è¾¾å¼
    /// </summary>
    private static object? TryEvaluateConditional(System.Linq.Expressions.ConditionalExpression conditionalExpr)
    {
        var testValue = TryEvaluateWithoutCompile(conditionalExpr.Test);
        if (testValue is bool test)
        {
            return TryEvaluateWithoutCompile(test ? conditionalExpr.IfTrue : conditionalExpr.IfFalse);
        }
        return null;
    }

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
    private QueryExpression ParseMemberExpression(System.Linq.Expressions.MemberExpression member)
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
                    value = EvaluateKnownProperty(container, prop);
                
                if (member.Member is System.Reflection.PropertyInfo && value == null)
                    throw new InvalidOperationException("Constant folding is not supported for this property in AOT-only mode.");

                return new ConstantExpression(value);
            }
            catch
            {
                // 求值失败忽略，继续作为成员表达式处理 (虽然理论上不应该发生)
            }
        }

        return new MemberExpression(member.Member.Name, expression);
    }

    private static object? EvaluateKnownDateTimeProperty(DateTime dt, string propertyName)
    {
        if (propertyName == nameof(DateTime.Year)) return dt.Year;
        if (propertyName == nameof(DateTime.Month)) return dt.Month;
        if (propertyName == nameof(DateTime.Day)) return dt.Day;
        if (propertyName == nameof(DateTime.Hour)) return dt.Hour;
        if (propertyName == nameof(DateTime.Minute)) return dt.Minute;
        if (propertyName == nameof(DateTime.Second)) return dt.Second;
        if (propertyName == nameof(DateTime.Date)) return dt.Date;
        if (propertyName == nameof(DateTime.DayOfWeek)) return (int)dt.DayOfWeek;
        return null;
    }

    private static object? EvaluateKnownProperty(object? container, System.Reflection.PropertyInfo prop)
    {
        if (container is string s && prop.Name == nameof(string.Length))
        {
            return s.Length;
        }

        if (container is DateTime dt)
        {
            return EvaluateKnownDateTimeProperty(dt, prop.Name);
        }

        return null;
    }

    /// <summary>
    /// 解析常量表达式
    /// </summary>
    /// <param name="constant">常量表达式</param>
    /// <returns>查询表达式</returns>
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
    }
}
