using System.Collections;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using LinqExp = System.Linq.Expressions;

namespace TinyDb.Query;

internal static partial class QueryPipeline
{

    /// <summary>
    /// AOT-compatible grouping that implements IGrouping interface
    /// </summary>
    internal sealed class AotGrouping : IGrouping<object, object>, IEnumerable<object>
    {
        private readonly object? _key;
        private readonly List<object> _elements;

        public AotGrouping(object? key, IEnumerable<object> elements)
        {
            _key = key;
            _elements = elements.ToList();
        }

        public object Key => _key!;
        public int Count => _elements.Count;
        public IEnumerator<object> GetEnumerator() => _elements.GetEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        public decimal Sum(Func<object, object> selector)
        {
            decimal sum = 0;
            foreach (var item in _elements)
            {
                sum = QueryPipelineAggregation.AddAggregateValue(sum, selector(item));
            }
            return sum;
        }

        public decimal Average(Func<object, object> selector)
        {
            if (_elements.Count == 0) return 0;
            return Sum(selector) / _elements.Count;
        }

        public object? Min(Func<object, object> selector)
        {
            object? min = null;
            foreach (var item in _elements)
            {
                var value = selector(item);
                if (value == null) continue;
                if (min == null || QueryValueComparer.Compare(value, min) < 0)
                    min = value;
            }
            return min;
        }

        public object? Max(Func<object, object> selector)
        {
            object? max = null;
            foreach (var item in _elements)
            {
                var value = selector(item);
                if (value == null) continue;
                if (max == null || QueryValueComparer.Compare(value, max) > 0)
                    max = value;
            }
            return max;
        }
    }

    private readonly struct GroupKey : IEquatable<GroupKey>
    {
        public GroupKey(object? value)
        {
            Value = value;
        }

        public object? Value { get; }

        public bool Equals(GroupKey other)
        {
            return QueryValueComparer.Compare(Value, other.Value) == 0;
        }

        public override bool Equals(object? obj)
        {
            return obj is GroupKey other && Equals(other);
        }

        public override int GetHashCode()
        {
            return QueryValueComparer.GetHashCode(Value);
        }
    }

    private static IEnumerable<AotGrouping> ExecuteGroupByGeneric<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(IEnumerable<T> source, LinqExp.MethodCallExpression m)
        where T : class
    {
        var lambda = (LinqExp.LambdaExpression)((LinqExp.UnaryExpression)m.Arguments[1]).Operand;
        var parser = new ExpressionParser();
        var keyExpr = parser.ParseExpression(lambda.Body);

        var groups = new Dictionary<GroupKey, List<object>>();

        foreach (var item in source)
        {
            if (item == null) continue;
            var key = new GroupKey(ExpressionEvaluator.EvaluateValue<T>(keyExpr, item));

            if (!groups.TryGetValue(key, out var list))
            {
                list = new List<object>();
                groups[key] = list;
            }
            list.Add(item);
        }

        foreach (var kvp in groups)
        {
            yield return new AotGrouping(kvp.Key.Value, kvp.Value);
        }
    }

    private static IEnumerable<AotGrouping> ExecuteGroupBy(IEnumerable source, LinqExp.MethodCallExpression m)
    {
        var lambda = (LinqExp.LambdaExpression)((LinqExp.UnaryExpression)m.Arguments[1]).Operand;
        var parser = new ExpressionParser();
        var keyExpr = parser.ParseExpression(lambda.Body);

        var groups = new Dictionary<GroupKey, List<object>>();

        foreach (var item in source)
        {
            if (item == null) continue;
            var key = new GroupKey(ExpressionEvaluator.EvaluateValue(keyExpr, item));

            if (!groups.TryGetValue(key, out var list))
            {
                list = new List<object>();
                groups[key] = list;
            }
            list.Add(item);
        }

        foreach (var kvp in groups)
        {
            yield return new AotGrouping(kvp.Key.Value, kvp.Value);
        }
    }

}
