using System.Linq;

namespace TinyDb.Query;

internal static class RuntimeQueryExpressionBinder
{
    public static QueryExpression? Bind(QueryExpression? expression)
    {
        return expression switch
        {
            null => null,
            ConstantExpression => expression,
            ParameterExpression => expression,
            BinaryExpression binary => new BinaryExpression(binary.NodeType, Bind(binary.Left)!, Bind(binary.Right)!),
            MemberExpression member => new MemberExpression(member.MemberName, Bind(member.Expression)),
            UnaryExpression unary => new UnaryExpression(unary.NodeType, Bind(unary.Operand)!, unary.Type),
            FunctionExpression function => BindFunction(function),
            ConstructorExpression constructor => new ConstructorExpression(constructor.Type, constructor.Arguments.Select(argument => Bind(argument)!)),
            MemberInitQueryExpression memberInit => new MemberInitQueryExpression(
                memberInit.Type,
                memberInit.Bindings.Select(binding => (binding.MemberName, Bind(binding.Value)!))),
            ConditionalQueryExpression conditional => new ConditionalQueryExpression(
                Bind(conditional.Test)!,
                Bind(conditional.IfTrue)!,
                Bind(conditional.IfFalse)!),
            _ => expression
        };
    }

    private static QueryExpression BindFunction(FunctionExpression function)
    {
        if (function.Target == null &&
            function.Arguments.Count == 0 &&
            TryEvaluateRuntimeFunction(function.FunctionName, out var value))
        {
            return new ConstantExpression(value);
        }

        return new FunctionExpression(
            function.FunctionName,
            Bind(function.Target),
            function.Arguments.Select(argument => Bind(argument)!));
    }

    private static bool TryEvaluateRuntimeFunction(string functionName, out object value)
    {
        switch (functionName)
        {
            case RuntimeFunctionNames.DateTimeNow:
                value = DateTime.Now;
                return true;
            case RuntimeFunctionNames.DateTimeUtcNow:
                value = DateTime.UtcNow;
                return true;
            case RuntimeFunctionNames.DateTimeToday:
                value = DateTime.Today;
                return true;
            default:
                value = null!;
                return false;
        }
    }
}
