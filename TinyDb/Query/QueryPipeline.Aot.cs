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

    private static object? ExecuteAot<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] TSource>(LinqExp.Expression expression, IEnumerable<TSource> queryResult, QueryPushdownInfo pushdown)
        where TSource : class
    {
        var stack = new Stack<LinqExp.MethodCallExpression>();
        var current = expression;

        while (current is LinqExp.MethodCallExpression m && m.Method.DeclaringType == typeof(System.Linq.Queryable))
        {
            stack.Push(m);
            current = m.Arguments[0];
        }

        IEnumerable source = queryResult;
        Type currentType = typeof(TSource);
        bool isTyped = true;

        var remainingWhereSkips = pushdown.WherePushedCount;
        var remainingOrderSkips = pushdown.OrderPushedCount;
        var remainingSkipSkips = pushdown.SkipPushedCount;
        var remainingTakeSkips = pushdown.TakePushedCount;

        foreach (var m in stack)
        {
            if (QueryPipelineTerminalExecutor.IsTerminal(m.Method.Name))
            {
                return QueryPipelineTerminalExecutor.Execute(source, m);
            }

            var methodName = m.Method.Name;

            if (methodName == "Where")
            {
                if (remainingWhereSkips > 0)
                {
                    remainingWhereSkips--;
                    continue;
                }

                if (isTyped && currentType == typeof(TSource))
                    source = ExecuteWhereGeneric<TSource>((IEnumerable<TSource>)source, m);
                else
                    source = ExecuteWhere(source, m);
            }
            else if (methodName == "Select")
            {
                var selector = (LinqExp.LambdaExpression)((LinqExp.UnaryExpression)m.Arguments[1]).Operand;
                source = ExecuteSelect(source, selector);
                currentType = selector.ReturnType;
                isTyped = false; // Type changed, lost generic context
            }
            else if (methodName == "Skip")
            {
                if (remainingSkipSkips > 0)
                {
                    remainingSkipSkips--;
                    continue;
                }

                if (m.Arguments[1] is LinqExp.ConstantExpression s)
                {
                    var count = (int)s.Value!;
                    if (isTyped && currentType == typeof(TSource))
                        source = ExecuteSkipGeneric<TSource>((IEnumerable<TSource>)source, count);
                    else
                        source = ExecuteSkip(source, count);
                }
            }
            else if (methodName == "Take")
            {
                if (remainingTakeSkips > 0)
                {
                    remainingTakeSkips--;
                    continue;
                }

                if (m.Arguments[1] is LinqExp.ConstantExpression t)
                {
                    var count = (int)t.Value!;
                    if (isTyped && currentType == typeof(TSource))
                        source = ExecuteTakeGeneric<TSource>((IEnumerable<TSource>)source, count);
                    else
                        source = ExecuteTake(source, count);
                }
            }
            else if (methodName == "Distinct")
            {
                if (isTyped && currentType == typeof(TSource))
                    source = ExecuteDistinctGeneric<TSource>((IEnumerable<TSource>)source);
                else
                    source = ExecuteDistinct(source);
            }
            else if (methodName == "OrderBy" || methodName == "OrderByDescending" || methodName == "ThenBy" || methodName == "ThenByDescending")
            {
                if (remainingOrderSkips > 0)
                {
                    remainingOrderSkips--;
                    continue;
                }

                if (isTyped && currentType == typeof(TSource))
                    source = QueryPipelineOrdering.ExecuteGeneric<TSource>((IEnumerable<TSource>)source, m);
                else
                    source = QueryPipelineOrdering.Execute(source, m);
            }
            else if (methodName == "GroupBy")
            {
                // GroupBy in AOT mode - returns AotGrouping objects
                if (isTyped && currentType == typeof(TSource))
                    source = ExecuteGroupByGeneric<TSource>((IEnumerable<TSource>)source, m);
                else
                    source = ExecuteGroupBy(source, m);
                currentType = typeof(AotGrouping);
                isTyped = false;
            }
            else if (methodName == "Sum" || methodName == "Average" || methodName == "Min" || methodName == "Max")
            {
                // These are terminal operations when called on a grouped source
                return QueryPipelineAggregation.Execute(source, m);
            }
            else
            {
                throw new NotSupportedException($"Operation {methodName} is not supported in AOT mode.");
            }
        }
        if (currentType == typeof(AotGrouping))
        {
            throw new NotSupportedException("GroupBy result enumeration requires dynamic code generation. Use Select after GroupBy for AOT compatibility.");
        }

        return source;
    }

    private static IEnumerable<T> ExecuteWhereGeneric<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(IEnumerable<T> source, LinqExp.MethodCallExpression m)
        where T : class
    {
        var lambda = (LinqExp.LambdaExpression)((LinqExp.UnaryExpression)m.Arguments[1]).Operand;
        var parser = new ExpressionParser();
        var queryExpr = parser.ParseExpression(lambda.Body);

        return source.Where(item =>
        {
            if (item == null) return false;
            var result = ExpressionEvaluator.EvaluateValue<T>(queryExpr, item);
            return result is bool b && b;
        });
    }

    private static IEnumerable<T> ExecuteSkipGeneric<T>(IEnumerable<T> source, int count)
    {
        return source.Skip(count);
    }

    private static IEnumerable<T> ExecuteTakeGeneric<T>(IEnumerable<T> source, int count)
    {
        return source.Take(count);
    }

    private static IEnumerable<T> ExecuteDistinctGeneric<T>(IEnumerable<T> source)
    {
        return source.Distinct();
    }

}
