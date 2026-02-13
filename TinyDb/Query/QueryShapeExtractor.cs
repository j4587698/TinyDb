using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using LinqExp = System.Linq.Expressions;

namespace TinyDb.Query;

internal static class QueryShapeExtractor
{
    private static readonly HashSet<string> TerminalMethods = new(StringComparer.Ordinal)
    {
        "Count",
        "LongCount",
        "Any",
        "All",
        "First",
        "FirstOrDefault",
        "Single",
        "SingleOrDefault",
        "Last",
        "LastOrDefault",
        "ElementAt",
        "ElementAtOrDefault"
    };

    public static (QueryShape<TSource> Shape, LinqExp.ConstantExpression? Source) Extract<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] TSource>(
        LinqExp.Expression expression)
        where TSource : class, new()
    {
        if (expression == null) throw new ArgumentNullException(nameof(expression));

        var stack = new Stack<LinqExp.MethodCallExpression>();
        var current = expression;

        while (current is LinqExp.MethodCallExpression m && m.Method.DeclaringType == typeof(Queryable))
        {
            stack.Push(m);
            current = m.Arguments[0];
        }

        LinqExp.ConstantExpression? sourceConstant = current is LinqExp.ConstantExpression c && typeof(IQueryable).IsAssignableFrom(c.Type) ? c : null;

        LinqExp.Expression<Func<TSource, bool>>? predicate = null;
        int pushedWhereCount = 0;
        var sortFields = new List<QuerySortField>();
        int? skip = null;
        int? take = null;
        bool hasTypeShapingOperator = false;

        var stage = ExtractionStage.BeforePagination;

        foreach (var call in stack)
        {
            var methodName = call.Method.Name;

            if (TerminalMethods.Contains(methodName))
            {
                continue;
            }

            switch (methodName)
            {
                case "Where":
                    if (stage != ExtractionStage.BeforePagination) break;
                    if (!TryGetUnaryQuotedLambda(call, out var whereLambda)) break;
                    if (whereLambda.Parameters.Count != 1 || whereLambda.ReturnType != typeof(bool)) break;
                    if (whereLambda.Parameters[0].Type != typeof(TSource)) break;
                    predicate = predicate == null
                        ? (LinqExp.Expression<Func<TSource, bool>>)whereLambda
                        : AndAlso(predicate, (LinqExp.Expression<Func<TSource, bool>>)whereLambda);
                    pushedWhereCount++;
                    break;

                case "OrderBy":
                case "OrderByDescending":
                    if (stage != ExtractionStage.BeforePagination) break;
                    if (!TryExtractSortField<TSource>(call, methodName == "OrderByDescending", out var orderByField))
                    {
                        hasTypeShapingOperator = true;
                        stage = ExtractionStage.Stopped;
                        sortFields.Clear();
                        break;
                    }
                    sortFields.Clear();
                    sortFields.Add(orderByField);
                    break;

                case "ThenBy":
                case "ThenByDescending":
                    if (stage != ExtractionStage.BeforePagination) break;
                    if (!TryExtractSortField<TSource>(call, methodName == "ThenByDescending", out var thenByField))
                    {
                        hasTypeShapingOperator = true;
                        stage = ExtractionStage.Stopped;
                        sortFields.Clear();
                        break;
                    }
                    sortFields.Add(thenByField);
                    break;

                case "Skip":
                    if (stage != ExtractionStage.BeforePagination) break;
                    if (!TryGetIntConstant(call, out var skipValue)) break;
                    skip = skipValue;
                    stage = ExtractionStage.AfterSkip;
                    break;

                case "Take":
                    if (stage == ExtractionStage.Stopped) break;
                    if (stage == ExtractionStage.AfterTake) break;
                    if (!TryGetIntConstant(call, out var takeValue)) break;
                    take = takeValue;
                    stage = ExtractionStage.AfterTake;
                    break;

                case "Select":
                case "SelectMany":
                case "GroupBy":
                case "Join":
                case "Distinct":
                case "OfType":
                case "Cast":
                    hasTypeShapingOperator = true;
                    stage = ExtractionStage.Stopped;
                    break;

                default:
                    hasTypeShapingOperator = true;
                    stage = ExtractionStage.Stopped;
                    break;
            }
        }

        var shape = new QueryShape<TSource>
        {
            Predicate = predicate,
            PushedWhereCount = predicate != null ? pushedWhereCount : 0,
            Sort = sortFields,
            Skip = skip,
            Take = take,
            HasTypeShapingOperator = hasTypeShapingOperator
        };

        return (shape, sourceConstant);
    }

    private static bool TryGetUnaryQuotedLambda(LinqExp.MethodCallExpression call, [NotNullWhen(true)] out LinqExp.LambdaExpression? lambda)
    {
        lambda = null;
        if (call.Arguments.Count < 2) return false;

        if (call.Arguments[1] is LinqExp.UnaryExpression u && u.Operand is LinqExp.LambdaExpression l)
        {
            lambda = l;
            return true;
        }

        if (call.Arguments[1] is LinqExp.LambdaExpression l2)
        {
            lambda = l2;
            return true;
        }

        return false;
    }

    private static bool TryGetIntConstant(LinqExp.MethodCallExpression call, out int value)
    {
        value = default;
        if (call.Arguments.Count < 2) return false;
        if (call.Arguments[1] is not LinqExp.ConstantExpression c) return false;
        if (c.Value is not int i) return false;
        value = i;
        return true;
    }

    private static bool TryExtractSortField<TSource>(LinqExp.MethodCallExpression call, bool descending, out QuerySortField field)
        where TSource : class, new()
    {
        field = default;
        if (!TryGetUnaryQuotedLambda(call, out var lambda)) return false;
        if (lambda.Parameters.Count != 1) return false;
        if (lambda.Parameters[0].Type != typeof(TSource)) return false;

        var body = lambda.Body;
        while (body is LinqExp.UnaryExpression u && (u.NodeType == LinqExp.ExpressionType.Convert || u.NodeType == LinqExp.ExpressionType.ConvertChecked))
        {
            body = u.Operand;
        }

        if (body is not LinqExp.MemberExpression member) return false;
        if (member.Expression is not LinqExp.ParameterExpression) return false;

        var memberName = member.Member.Name;
        var fieldName = NormalizeFieldName(memberName);
        field = new QuerySortField(fieldName, member.Type, descending);
        return true;
    }

    private static string NormalizeFieldName(string memberName)
    {
        if (string.Equals(memberName, "Id", StringComparison.Ordinal))
        {
            return "_id";
        }

        return ToCamelCase(memberName);
    }

    private static string ToCamelCase(string name)
    {
        if (string.IsNullOrEmpty(name)) return name;
        if (name.Length == 1) return name.ToLowerInvariant();
        return char.ToLowerInvariant(name[0]) + name.Substring(1);
    }

    private static LinqExp.Expression<Func<T, bool>> AndAlso<T>(LinqExp.Expression<Func<T, bool>> left, LinqExp.Expression<Func<T, bool>> right)
    {
        var p = left.Parameters[0];
        var rightBody = new ParameterReplaceVisitor(right.Parameters[0], p).Visit(right.Body) ?? right.Body;
        var body = LinqExp.Expression.AndAlso(left.Body, rightBody);
        return LinqExp.Expression.Lambda<Func<T, bool>>(body, p);
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
            if (node == _from) return _to;
            return base.VisitParameter(node);
        }
    }

    private enum ExtractionStage
    {
        BeforePagination,
        AfterSkip,
        AfterTake,
        Stopped
    }
}
