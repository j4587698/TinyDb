using System.Diagnostics.CodeAnalysis;
using LinqExp = System.Linq.Expressions;

namespace TinyDb.Query;

internal static class QueryExpressionCallHelpers
{
    public static bool TryGetLambdaArgument(LinqExp.MethodCallExpression call, [NotNullWhen(true)] out LinqExp.LambdaExpression? lambda)
    {
        lambda = null;
        if (call.Arguments.Count < 2) return false;

        if (call.Arguments[1] is LinqExp.UnaryExpression u && u.Operand is LinqExp.LambdaExpression quotedLambda)
        {
            lambda = quotedLambda;
            return true;
        }

        if (call.Arguments[1] is LinqExp.LambdaExpression directLambda)
        {
            lambda = directLambda;
            return true;
        }

        return false;
    }

    public static LinqExp.Expression ReplaceParameter(
        LinqExp.Expression expression,
        LinqExp.ParameterExpression from,
        LinqExp.ParameterExpression to)
    {
        return new ParameterReplaceVisitor(from, to).Visit(expression) ?? expression;
    }

    private sealed class ParameterReplaceVisitor : LinqExp.ExpressionVisitor
    {
        private readonly LinqExp.ParameterExpression _from;
        private readonly LinqExp.ParameterExpression _to;

        public ParameterReplaceVisitor(LinqExp.ParameterExpression from, LinqExp.ParameterExpression to)
        {
            _from = from;
            _to = to;
        }

        protected override LinqExp.Expression VisitParameter(LinqExp.ParameterExpression node)
        {
            return node == _from ? _to : base.VisitParameter(node);
        }
    }
}
