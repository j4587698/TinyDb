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

internal static class PredicateExtractor
{
    public static (LinqExp.Expression? Predicate, LinqExp.ConstantExpression? Source, bool HasMultiplePredicates) ExtractAot(LinqExp.Expression expression, Type entityType)
    {
        var visitor = new AotExtractionVisitor(entityType);
        visitor.Visit(expression);
        return (visitor.Predicate, visitor.SourceQueryable, visitor.HasMultiplePredicates);
    }

    internal class AotExtractionVisitor : LinqExp.ExpressionVisitor
    {
        private readonly Type _entityType;
        public LinqExp.Expression? Predicate { get; private set; }
        public LinqExp.ConstantExpression? SourceQueryable { get; private set; }
        public bool HasMultiplePredicates { get; private set; }

        public AotExtractionVisitor(Type entityType)
        {
            _entityType = entityType;
        }

        public override LinqExp.Expression? Visit(LinqExp.Expression? node)
        {
            if (node is LinqExp.MethodCallExpression m)
            {
                if (m.Method.Name == "Where" && m.Method.DeclaringType == typeof(System.Linq.Queryable))
                {
                    if (m.Arguments[1] is LinqExp.UnaryExpression u && u.Operand is LinqExp.LambdaExpression l)
                    {
                        if (Predicate == null)
                        {
                            Predicate = l;
                        }
                        else
                        {
                            HasMultiplePredicates = true;
                        }
                    }
                }
            }
            else if (node is LinqExp.ConstantExpression c && typeof(IQueryable).IsAssignableFrom(c.Type))
            {
                SourceQueryable = c;
            }

            return base.Visit(node);
        }
    }
}
