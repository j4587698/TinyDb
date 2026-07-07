using System.Collections;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using TinyDb.Bson;
using LinqExp = System.Linq.Expressions;

namespace TinyDb.Query;

internal static partial class QueryPipeline
{
    public static object? Execute<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] TSource>(QueryExecutor executor, string collectionName, LinqExp.Expression expression)
        where TSource : class
    {
        // 1. 提取可下推到数据库的查询条件 (Where)
        // Note: PredicateExtractor.Extract may use Expression.Lambda for combining predicates,
        // but the actual execution path in AOT uses ExecuteAot which doesn't require dynamic code.
        var (shape, sourceConstant) = QueryShapeExtractor.Extract<TSource>(expression);

        if (sourceConstant != null &&
            TryExecuteCountTerminal(executor, collectionName, expression, shape, out var countResult))
        {
            return countResult;
        }

        // 2. 执行数据库查询 (直接调用泛型方法，无需反射)
        // Predicate is Expression<Func<TSource, bool>>?
        var queryResult = executor.ExecuteShaped(collectionName, shape, out var pushdown);

        // 3. 重写表达式树：将 Queryable 转换为 Enumerable 调用
        if (sourceConstant != null)
        {
            // 优先使用 AOT 兼容路径，确保 AOT 代码路径得到充分测试
            // 只有在 AOT 路径明确不支持的情况下才回退到动态编译路径
            try
            {
                return ExecuteAot(expression, queryResult, pushdown);
            }
            catch (NotSupportedException)
            {
                // AOT 路径不支持此操作，回退到动态编译路径（仅在非 AOT 环境下）
                throw;
            }
        }

        return queryResult;
    }

    private static bool TryExecuteCountTerminal<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] TSource>(
        QueryExecutor executor,
        string collectionName,
        LinqExp.Expression expression,
        QueryShape<TSource> shape,
        out object? result)
        where TSource : class
    {
        result = null;
        if (shape.HasTypeShapingOperator ||
            expression is not LinqExp.MethodCallExpression methodCall ||
            methodCall.Method.DeclaringType != typeof(System.Linq.Queryable))
        {
            return false;
        }

        var methodName = methodCall.Method.Name;
        if (methodName is not ("Count" or "LongCount"))
        {
            return false;
        }

        if (!IsCountFastPathShapeSafe(methodCall, shape))
        {
            return false;
        }

        if (!TryBuildCountPredicate(methodCall, shape.Predicate, out var predicate))
        {
            return false;
        }

        var count = executor.Count(collectionName, predicate);
        if (shape.Skip is { } skip && skip > 0)
        {
            count = Math.Max(0, count - skip);
        }

        if (shape.Take is { } take)
        {
            count = Math.Min(count, Math.Max(0, take));
        }

        if (methodName == "LongCount")
        {
            result = count;
        }
        else
        {
            result = checked((int)count);
        }

        return true;
    }

    private static bool IsCountFastPathShapeSafe<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] TSource>(
        LinqExp.MethodCallExpression terminalCall,
        QueryShape<TSource> shape)
        where TSource : class
    {
        if (terminalCall.Arguments.Count == 2 &&
            (shape.Skip.HasValue || shape.Take.HasValue))
        {
            return false;
        }

        var stack = new Stack<LinqExp.MethodCallExpression>();
        var current = terminalCall.Arguments[0];
        while (current is LinqExp.MethodCallExpression call && call.Method.DeclaringType == typeof(System.Linq.Queryable))
        {
            stack.Push(call);
            current = call.Arguments[0];
        }

        var stage = CountFastPathStage.BeforePagination;
        var seenSkip = false;
        var seenTake = false;

        foreach (var call in stack)
        {
            switch (call.Method.Name)
            {
                case "Where":
                    if (stage != CountFastPathStage.BeforePagination ||
                        !TryGetPredicateLambda<TSource>(call, out _))
                    {
                        return false;
                    }
                    break;

                case "OrderBy":
                case "OrderByDescending":
                case "ThenBy":
                case "ThenByDescending":
                    if (stage != CountFastPathStage.BeforePagination)
                    {
                        return false;
                    }
                    break;

                case "Skip":
                    if (stage != CountFastPathStage.BeforePagination ||
                        seenSkip ||
                        seenTake ||
                        !TryGetIntConstantArgument(call))
                    {
                        return false;
                    }

                    seenSkip = true;
                    stage = CountFastPathStage.AfterSkip;
                    break;

                case "Take":
                    if (seenTake ||
                        !TryGetIntConstantArgument(call))
                    {
                        return false;
                    }

                    seenTake = true;
                    stage = CountFastPathStage.AfterTake;
                    break;

                default:
                    return false;
            }
        }

        return true;
    }

    private static bool TryGetPredicateLambda<TSource>(
        LinqExp.MethodCallExpression call,
        [NotNullWhen(true)] out LinqExp.LambdaExpression? lambda)
        where TSource : class
    {
        lambda = null;
        if (call.Arguments.Count < 2)
        {
            return false;
        }

        if (call.Arguments[1] is LinqExp.UnaryExpression unary &&
            unary.Operand is LinqExp.LambdaExpression quotedLambda)
        {
            lambda = quotedLambda;
        }
        else if (call.Arguments[1] is LinqExp.LambdaExpression directLambda)
        {
            lambda = directLambda;
        }
        else
        {
            return false;
        }

        return lambda.Parameters.Count == 1 &&
               lambda.Parameters[0].Type == typeof(TSource) &&
               lambda.ReturnType == typeof(bool);
    }

    private static bool TryGetIntConstantArgument(LinqExp.MethodCallExpression call)
    {
        return call.Arguments.Count >= 2 &&
               call.Arguments[1] is LinqExp.ConstantExpression { Value: int };
    }

    private enum CountFastPathStage
    {
        BeforePagination,
        AfterSkip,
        AfterTake
    }

    private static bool TryBuildCountPredicate<TSource>(
        LinqExp.MethodCallExpression methodCall,
        LinqExp.Expression<Func<TSource, bool>>? shapePredicate,
        out LinqExp.Expression<Func<TSource, bool>>? predicate)
        where TSource : class
    {
        predicate = shapePredicate;
        if (methodCall.Arguments.Count == 1)
        {
            return true;
        }

        if (methodCall.Arguments.Count != 2 ||
            methodCall.Arguments[1] is not LinqExp.UnaryExpression unary ||
            unary.Operand is not LinqExp.LambdaExpression lambda ||
            lambda.Parameters.Count != 1 ||
            lambda.Parameters[0].Type != typeof(TSource) ||
            lambda.ReturnType != typeof(bool))
        {
            return false;
        }

        var terminalPredicate = (LinqExp.Expression<Func<TSource, bool>>)lambda;
        predicate = shapePredicate == null
            ? terminalPredicate
            : AndAlso(shapePredicate, terminalPredicate);
        return true;
    }

    private static LinqExp.Expression<Func<TSource, bool>> AndAlso<TSource>(
        LinqExp.Expression<Func<TSource, bool>> left,
        LinqExp.Expression<Func<TSource, bool>> right)
        where TSource : class
    {
        var parameter = LinqExp.Expression.Parameter(typeof(TSource), "x");
        var leftBody = new ParameterReplaceVisitor(left.Parameters[0], parameter).Visit(left.Body)!;
        var rightBody = new ParameterReplaceVisitor(right.Parameters[0], parameter).Visit(right.Body)!;
        return LinqExp.Expression.Lambda<Func<TSource, bool>>(
            LinqExp.Expression.AndAlso(leftBody, rightBody),
            parameter);
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
            return ReferenceEquals(node, _from) ? _to : node;
        }
    }

}
