using TinyDb.Bson;
using TinyDb.Serialization;

namespace TinyDb.Query;

internal static class QueryPredicateAnalyzer
{
    public static Dictionary<string, QueryFieldComparison> ExtractComparisonMap(QueryExpression queryExpression)
    {
        var comparisons = new Dictionary<string, QueryFieldComparison>(StringComparer.Ordinal);
        AddComparisons(queryExpression, comparisons);
        return comparisons;
    }

    public static bool TryBuildDisjunctiveClauses(
        QueryExpression expression,
        int maxClauses,
        out List<QueryExpression> clauses)
    {
        clauses = new List<QueryExpression>();
        if (!TryBuildDnfTerms(expression, maxClauses, out var termGroups) || termGroups.Count < 2)
        {
            return false;
        }

        foreach (var terms in termGroups)
        {
            clauses.Add(CombineAndTerms(terms));
        }

        return true;
    }

    public static QueryExpression UnwrapConvert(QueryExpression expression)
    {
        while (expression is UnaryExpression unary &&
               unary.NodeType is System.Linq.Expressions.ExpressionType.Convert or System.Linq.Expressions.ExpressionType.ConvertChecked)
        {
            expression = unary.Operand;
        }

        return expression;
    }

    public static BsonValue? ExtractComparisonValue(QueryExpression queryExpression, string fieldName)
    {
        if (queryExpression is BinaryExpression binaryExpr)
        {
            if (binaryExpr.NodeType == System.Linq.Expressions.ExpressionType.AndAlso)
            {
                return ExtractComparisonValue(binaryExpr.Left, fieldName) ??
                       ExtractComparisonValue(binaryExpr.Right, fieldName);
            }

            var left = UnwrapConvert(binaryExpr.Left);
            var right = UnwrapConvert(binaryExpr.Right);
            if (left is MemberExpression leftMember &&
                string.Equals(leftMember.MemberName, fieldName, StringComparison.OrdinalIgnoreCase))
            {
                return ExtractConstantValue(right);
            }

            if (right is MemberExpression rightMember &&
                string.Equals(rightMember.MemberName, fieldName, StringComparison.OrdinalIgnoreCase))
            {
                return ExtractConstantValue(left);
            }
        }

        return null;
    }

    public static BsonValue? ExtractConstantValue(QueryExpression expression)
    {
        try
        {
            return expression switch
            {
                ConstantExpression constExpr => ConvertToBsonValue(constExpr.Value),
                UnaryExpression { NodeType: System.Linq.Expressions.ExpressionType.Convert or System.Linq.Expressions.ExpressionType.ConvertChecked } unaryExpr
                    => ExtractConstantValue(unaryExpr.Operand),
                MemberExpression memberExpr => EvaluateMemberExpression(memberExpr),
                _ => null
            };
        }
        catch (Exception ex) when (ex is InvalidOperationException or NotSupportedException or InvalidCastException or FormatException or OverflowException or ArgumentException)
        {
            return null;
        }
    }

    public static BsonValue? ConvertToBsonValue(object? value)
    {
        if (value == null) return BsonNull.Value;

        try
        {
            return BsonConversion.ToBsonValue(value);
        }
        catch (Exception ex) when (ex is InvalidOperationException or NotSupportedException or InvalidCastException or FormatException or OverflowException or ArgumentException)
        {
            return new BsonString(value.ToString() ?? string.Empty);
        }
    }

    public static ComparisonType ExtractComparisonType(QueryExpression queryExpression, string fieldName)
    {
        if (queryExpression is BinaryExpression binaryExpr)
        {
            if (binaryExpr.NodeType == System.Linq.Expressions.ExpressionType.AndAlso)
            {
                var leftValue = ExtractComparisonValue(binaryExpr.Left, fieldName);
                if (leftValue != null)
                {
                    return ExtractComparisonType(binaryExpr.Left, fieldName);
                }

                return ExtractComparisonType(binaryExpr.Right, fieldName);
            }

            var isTargetField = false;

            if (binaryExpr.Left is MemberExpression leftMember &&
                string.Equals(leftMember.MemberName, fieldName, StringComparison.OrdinalIgnoreCase))
            {
                isTargetField = true;
            }
            else if (binaryExpr.Right is MemberExpression rightMember &&
                     string.Equals(rightMember.MemberName, fieldName, StringComparison.OrdinalIgnoreCase))
            {
                isTargetField = true;
            }

            if (isTargetField)
            {
                return binaryExpr.NodeType switch
                {
                    System.Linq.Expressions.ExpressionType.Equal => ComparisonType.Equal,
                    System.Linq.Expressions.ExpressionType.NotEqual => ComparisonType.NotEqual,
                    System.Linq.Expressions.ExpressionType.GreaterThan => ComparisonType.GreaterThan,
                    System.Linq.Expressions.ExpressionType.GreaterThanOrEqual => ComparisonType.GreaterThanOrEqual,
                    System.Linq.Expressions.ExpressionType.LessThan => ComparisonType.LessThan,
                    System.Linq.Expressions.ExpressionType.LessThanOrEqual => ComparisonType.LessThanOrEqual,
                    _ => ComparisonType.Equal
                };
            }
        }

        return ComparisonType.Equal;
    }

    private static void AddComparisons(QueryExpression queryExpression, Dictionary<string, QueryFieldComparison> comparisons)
    {
        queryExpression = UnwrapConvert(queryExpression);
        if (TryAddBooleanComparison(queryExpression, comparisons)) return;
        if (queryExpression is not BinaryExpression binaryExpr) return;

        if (binaryExpr.NodeType == System.Linq.Expressions.ExpressionType.AndAlso)
        {
            AddComparisons(binaryExpr.Left, comparisons);
            AddComparisons(binaryExpr.Right, comparisons);
            return;
        }

        if (!IsComparisonNode(binaryExpr.NodeType)) return;

        var left = UnwrapConvert(binaryExpr.Left);
        var right = UnwrapConvert(binaryExpr.Right);

        if (left is MemberExpression leftMember)
        {
            var value = ExtractConstantValue(right);
            if (value != null)
            {
                AddComparison(comparisons, leftMember.MemberName, value, ToComparisonType(binaryExpr.NodeType, reversed: false));
            }

            return;
        }

        if (right is MemberExpression rightMember)
        {
            var value = ExtractConstantValue(left);
            if (value != null)
            {
                AddComparison(comparisons, rightMember.MemberName, value, ToComparisonType(binaryExpr.NodeType, reversed: true));
            }
        }
    }

    private static bool TryAddBooleanComparison(QueryExpression queryExpression, Dictionary<string, QueryFieldComparison> comparisons)
    {
        if (queryExpression is MemberExpression member)
        {
            AddComparison(comparisons, member.MemberName, BsonBoolean.True, ComparisonType.Equal);
            return true;
        }

        if (queryExpression is UnaryExpression { NodeType: System.Linq.Expressions.ExpressionType.Not } unary &&
            UnwrapConvert(unary.Operand) is MemberExpression negatedMember)
        {
            AddComparison(comparisons, negatedMember.MemberName, BsonBoolean.False, ComparisonType.Equal);
            return true;
        }

        return false;
    }

    private static void AddComparison(
        Dictionary<string, QueryFieldComparison> comparisons,
        string fieldName,
        BsonValue value,
        ComparisonType comparisonType)
    {
        if (!comparisons.TryGetValue(fieldName, out var comparison))
        {
            comparison = new QueryFieldComparison();
            comparisons[fieldName] = comparison;
        }

        comparison.Add(value, comparisonType);
    }

    private static bool IsComparisonNode(System.Linq.Expressions.ExpressionType nodeType)
    {
        return nodeType is System.Linq.Expressions.ExpressionType.Equal
            or System.Linq.Expressions.ExpressionType.NotEqual
            or System.Linq.Expressions.ExpressionType.GreaterThan
            or System.Linq.Expressions.ExpressionType.GreaterThanOrEqual
            or System.Linq.Expressions.ExpressionType.LessThan
            or System.Linq.Expressions.ExpressionType.LessThanOrEqual;
    }

    private static ComparisonType ToComparisonType(System.Linq.Expressions.ExpressionType nodeType, bool reversed)
    {
        if (reversed)
        {
            nodeType = nodeType switch
            {
                System.Linq.Expressions.ExpressionType.GreaterThan => System.Linq.Expressions.ExpressionType.LessThan,
                System.Linq.Expressions.ExpressionType.GreaterThanOrEqual => System.Linq.Expressions.ExpressionType.LessThanOrEqual,
                System.Linq.Expressions.ExpressionType.LessThan => System.Linq.Expressions.ExpressionType.GreaterThan,
                System.Linq.Expressions.ExpressionType.LessThanOrEqual => System.Linq.Expressions.ExpressionType.GreaterThanOrEqual,
                _ => nodeType
            };
        }

        return nodeType switch
        {
            System.Linq.Expressions.ExpressionType.Equal => ComparisonType.Equal,
            System.Linq.Expressions.ExpressionType.NotEqual => ComparisonType.NotEqual,
            System.Linq.Expressions.ExpressionType.GreaterThan => ComparisonType.GreaterThan,
            System.Linq.Expressions.ExpressionType.GreaterThanOrEqual => ComparisonType.GreaterThanOrEqual,
            System.Linq.Expressions.ExpressionType.LessThan => ComparisonType.LessThan,
            System.Linq.Expressions.ExpressionType.LessThanOrEqual => ComparisonType.LessThanOrEqual,
            _ => ComparisonType.Equal
        };
    }

    private static bool TryBuildDnfTerms(
        QueryExpression expression,
        int maxClauses,
        out List<List<QueryExpression>> termGroups)
    {
        expression = UnwrapConvert(expression);

        if (expression is BinaryExpression { NodeType: System.Linq.Expressions.ExpressionType.OrElse } or)
        {
            if (!TryBuildDnfTerms(or.Left, maxClauses, out var leftGroups) ||
                !TryBuildDnfTerms(or.Right, maxClauses, out var rightGroups) ||
                leftGroups.Count + rightGroups.Count > maxClauses)
            {
                termGroups = new List<List<QueryExpression>>();
                return false;
            }

            leftGroups.AddRange(rightGroups);
            termGroups = leftGroups;
            return true;
        }

        if (expression is BinaryExpression { NodeType: System.Linq.Expressions.ExpressionType.AndAlso } and)
        {
            if (!TryBuildDnfTerms(and.Left, maxClauses, out var leftGroups) ||
                !TryBuildDnfTerms(and.Right, maxClauses, out var rightGroups) ||
                leftGroups.Count * rightGroups.Count > maxClauses)
            {
                termGroups = new List<List<QueryExpression>>();
                return false;
            }

            var combined = new List<List<QueryExpression>>(leftGroups.Count * rightGroups.Count);
            foreach (var left in leftGroups)
            {
                foreach (var right in rightGroups)
                {
                    var terms = new List<QueryExpression>(left.Count + right.Count);
                    terms.AddRange(left);
                    terms.AddRange(right);
                    combined.Add(terms);
                }
            }

            termGroups = combined;
            return true;
        }

        termGroups = new List<List<QueryExpression>>
        {
            new() { expression }
        };
        return true;
    }

    private static QueryExpression CombineAndTerms(IReadOnlyList<QueryExpression> terms)
    {
        var combined = terms[0];
        for (var i = 1; i < terms.Count; i++)
        {
            combined = new BinaryExpression(System.Linq.Expressions.ExpressionType.AndAlso, combined, terms[i]);
        }

        return combined;
    }

    private static BsonValue? EvaluateMemberExpression(MemberExpression _)
    {
        return null;
    }
}

internal sealed class QueryFieldComparison
{
    private BsonValue? _equalValue;
    private BsonValue? _notEqualValue;
    private BsonValue? _lowerValue;
    private BsonValue? _upperValue;
    private bool _includeLower;
    private bool _includeUpper;

    public void Add(BsonValue value, ComparisonType comparisonType)
    {
        switch (comparisonType)
        {
            case ComparisonType.Equal:
                _equalValue ??= value;
                break;
            case ComparisonType.NotEqual:
                _notEqualValue ??= value;
                break;
            case ComparisonType.GreaterThan:
                SetLower(value, include: false);
                break;
            case ComparisonType.GreaterThanOrEqual:
                SetLower(value, include: true);
                break;
            case ComparisonType.LessThan:
                SetUpper(value, include: false);
                break;
            case ComparisonType.LessThanOrEqual:
                SetUpper(value, include: true);
                break;
        }
    }

    public IndexScanKey ToIndexScanKey(string fieldName)
    {
        if (_equalValue != null)
        {
            return new IndexScanKey
            {
                FieldName = fieldName,
                Value = _equalValue,
                ComparisonType = ComparisonType.Equal
            };
        }

        if (_lowerValue != null && _upperValue != null)
        {
            return new IndexScanKey
            {
                FieldName = fieldName,
                Value = _lowerValue,
                ComparisonType = ComparisonType.Range,
                LowerValue = _lowerValue,
                UpperValue = _upperValue,
                IncludeLower = _includeLower,
                IncludeUpper = _includeUpper
            };
        }

        if (_lowerValue != null)
        {
            return new IndexScanKey
            {
                FieldName = fieldName,
                Value = _lowerValue,
                ComparisonType = _includeLower ? ComparisonType.GreaterThanOrEqual : ComparisonType.GreaterThan
            };
        }

        if (_upperValue != null)
        {
            return new IndexScanKey
            {
                FieldName = fieldName,
                Value = _upperValue,
                ComparisonType = _includeUpper ? ComparisonType.LessThanOrEqual : ComparisonType.LessThan
            };
        }

        return new IndexScanKey
        {
            FieldName = fieldName,
            Value = _notEqualValue ?? BsonNull.Value,
            ComparisonType = ComparisonType.NotEqual
        };
    }

    private void SetLower(BsonValue value, bool include)
    {
        if (_lowerValue == null)
        {
            _lowerValue = value;
            _includeLower = include;
            return;
        }

        var comparison = BsonValueComparer.Compare(value, _lowerValue);
        if (comparison > 0 || (comparison == 0 && _includeLower && !include))
        {
            _lowerValue = value;
            _includeLower = include;
        }
    }

    private void SetUpper(BsonValue value, bool include)
    {
        if (_upperValue == null)
        {
            _upperValue = value;
            _includeUpper = include;
            return;
        }

        var comparison = BsonValueComparer.Compare(value, _upperValue);
        if (comparison < 0 || (comparison == 0 && _includeUpper && !include))
        {
            _upperValue = value;
            _includeUpper = include;
        }
    }
}
