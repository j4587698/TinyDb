using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using TinyDb.Bson;
using TinyDb.Core;
using TinyDb.Index;
using TinyDb.Serialization;

namespace TinyDb.Query;

public sealed partial class QueryExecutor
{
    private bool CollectPredicates(QueryExpression? expr, List<ScanPredicate> predicates)
    {
        if (expr == null) return true;
        if (expr is not BinaryExpression binary) return false;

        if (binary.NodeType == ExpressionType.AndAlso)
        {
            bool left = CollectPredicates(binary.Left, predicates);
            bool right = CollectPredicates(binary.Right, predicates);
            return left && right;
        }

        if (!IsSupportedBinaryPredicate(binary.NodeType)) return false;

        var (member, constant, op) = ExtractComparison(binary);
        if (member == null || constant == null) return false;

        // 只对根对象的字段进行下推，避免嵌套属性导致误过滤。
        if (member.Expression != null && member.Expression.NodeType != ExpressionType.Parameter) return false;

        var memberName = member.MemberName;

        byte[] fieldNameBytes;
        byte[]? alternateFieldNameBytes = null;
        byte[]? secondAlternateFieldNameBytes = null;

        // 与 ExpressionEvaluator 行为保持一致：优先 camelCase，其次原字段名，Id 特殊映射到 _id。
        if (string.Equals(memberName, "Id", StringComparison.OrdinalIgnoreCase))
        {
            fieldNameBytes = System.Text.Encoding.UTF8.GetBytes("id");
            alternateFieldNameBytes = System.Text.Encoding.UTF8.GetBytes("Id");
            secondAlternateFieldNameBytes = System.Text.Encoding.UTF8.GetBytes("_id");
        }
        else
        {
            var camelName = BsonFieldName.ToCamelCase(memberName);
            fieldNameBytes = System.Text.Encoding.UTF8.GetBytes(camelName);

            if (!string.Equals(camelName, memberName, StringComparison.Ordinal))
            {
                alternateFieldNameBytes = System.Text.Encoding.UTF8.GetBytes(memberName);
            }
        }

        predicates.Add(new ScanPredicate(fieldNameBytes, alternateFieldNameBytes, secondAlternateFieldNameBytes, constant.Value, op));
        return true;
    }

    private static bool IsSupportedBinaryPredicate(ExpressionType op)
    {
        return op is ExpressionType.Equal or ExpressionType.NotEqual
            or ExpressionType.GreaterThan or ExpressionType.GreaterThanOrEqual
            or ExpressionType.LessThan or ExpressionType.LessThanOrEqual;
    }

    internal static ExpressionType ReverseComparisonOperator(ExpressionType op)
    {
        return op switch
        {
            ExpressionType.GreaterThan => ExpressionType.LessThan,
            ExpressionType.GreaterThanOrEqual => ExpressionType.LessThanOrEqual,
            ExpressionType.LessThan => ExpressionType.GreaterThan,
            ExpressionType.LessThanOrEqual => ExpressionType.GreaterThanOrEqual,
            _ => op
        };
    }

    internal (MemberExpression? member, ConstantExpression? constant, ExpressionType op) ExtractComparison(BinaryExpression binary)
    {
        // Case 1: Member OP Constant
        if (binary.Left is MemberExpression m1 && binary.Right is ConstantExpression c1) return (m1, c1, binary.NodeType);

        // Case 2: Member OP Convert(Constant)
        if (binary.Left is MemberExpression m2 && binary.Right is UnaryExpression u2 && u2.NodeType == ExpressionType.Convert && u2.Operand is ConstantExpression c2)
        {
            if (TryConvertConstant(c2, u2.Type, out var converted))
            {
                return (m2, converted, binary.NodeType);
            }
        }

        // Case 3: Constant OP Member
        if (binary.Left is ConstantExpression c3 && binary.Right is MemberExpression m3) return (m3, c3, ReverseComparisonOperator(binary.NodeType));

        // Case 4: Convert(Constant) OP Member
        if (binary.Left is UnaryExpression u4 && u4.NodeType == ExpressionType.Convert && u4.Operand is ConstantExpression c4 && binary.Right is MemberExpression m4)
        {
            if (TryConvertConstant(c4, u4.Type, out var converted))
            {
                return (m4, converted, ReverseComparisonOperator(binary.NodeType));
            }
        }

        return (null, null, binary.NodeType);
    }

    internal static bool TryConvertConstant(ConstantExpression constant, Type targetType, out ConstantExpression converted)
    {
        converted = constant;

        object? convertedValue;
        try
        {
            convertedValue = Convert.ChangeType(constant.Value, targetType);
        }
        catch (InvalidCastException)
        {
            return false;
        }
        catch (FormatException)
        {
            return false;
        }
        catch (OverflowException)
        {
            return false;
        }

        converted = new ConstantExpression(convertedValue);
        return true;
    }

    internal static BsonDocument DeserializeDocumentOrThrow(ReadOnlyMemory<byte> slice)
    {
        try
        {
            return BsonSerializer.DeserializeDocument(slice);
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException("Failed to deserialize BSON document from storage slice.", ex);
        }
    }

    // 构建索引扫描范围
    private static QueryExpression? BuildCommittedPostFilter(QueryExecutionPlan executionPlan, QueryExpression? queryExpression)
    {
        if (queryExpression == null ||
            executionPlan.UseIndex == null ||
            executionPlan.IndexScanKeys.Count == 0)
        {
            return queryExpression;
        }

        return RemoveIndexCoveredPredicates(queryExpression, executionPlan.IndexScanKeys, executionPlan.UseIndex.Fields);
    }

    private static QueryExpression? RemoveIndexCoveredPredicates(
        QueryExpression expression,
        IReadOnlyList<IndexScanKey> scanKeys,
        IReadOnlyList<string> indexFields)
    {
        if (expression is BinaryExpression binary &&
            binary.NodeType == System.Linq.Expressions.ExpressionType.AndAlso)
        {
            var left = RemoveIndexCoveredPredicates(binary.Left, scanKeys, indexFields);
            var right = RemoveIndexCoveredPredicates(binary.Right, scanKeys, indexFields);

            if (left == null) return right;
            if (right == null) return left;
            if (ReferenceEquals(left, binary.Left) && ReferenceEquals(right, binary.Right)) return expression;

            return new BinaryExpression(System.Linq.Expressions.ExpressionType.AndAlso, left, right);
        }

        return IsPredicateCoveredByIndex(expression, scanKeys, indexFields) ? null : expression;
    }

    private static bool IsPredicateCoveredByIndex(
        QueryExpression expression,
        IReadOnlyList<IndexScanKey> scanKeys,
        IReadOnlyList<string> indexFields)
    {
        if (expression is not BinaryExpression binary ||
            !TryExtractIndexedComparison(binary, out var fieldName, out var comparisonType, out var value))
        {
            return false;
        }

        var coveredScanKeyCount = GetRangeCoveredScanKeyCount(scanKeys);
        for (var i = 0; i < coveredScanKeyCount; i++)
        {
            var scanKey = scanKeys[i];
            if (!string.Equals(scanKey.FieldName, fieldName, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            var fieldIndex = FindIndexFieldPosition(indexFields, scanKey.FieldName);
            var hasTrailingIndexFields = fieldIndex >= 0 && fieldIndex < indexFields.Count - 1;
            if (IsComparisonCoveredByScanKey(comparisonType, value, scanKey, hasTrailingIndexFields))
            {
                return true;
            }
        }

        return false;
    }

    private static int GetRangeCoveredScanKeyCount(IReadOnlyList<IndexScanKey> scanKeys)
    {
        for (var i = 0; i < scanKeys.Count; i++)
        {
            if (scanKeys[i].ComparisonType != ComparisonType.Equal)
            {
                return i + 1;
            }
        }

        return scanKeys.Count;
    }

    private static int FindIndexFieldPosition(IReadOnlyList<string> indexFields, string fieldName)
    {
        for (var i = 0; i < indexFields.Count; i++)
        {
            if (string.Equals(indexFields[i], fieldName, StringComparison.OrdinalIgnoreCase))
            {
                return i;
            }
        }

        return -1;
    }

    private static bool TryExtractIndexedComparison(
        BinaryExpression expression,
        [NotNullWhen(true)] out string? fieldName,
        out ComparisonType comparisonType,
        [NotNullWhen(true)] out BsonValue? value)
    {
        if (!TryMapComparisonType(expression.NodeType, reversed: false, out var leftComparisonType) ||
            !TryMapComparisonType(expression.NodeType, reversed: true, out var rightComparisonType))
        {
            fieldName = null;
            comparisonType = default;
            value = null;
            return false;
        }

        if (expression.Left is MemberExpression leftMember &&
            TryConvertConstantExpression(expression.Right, out var rightValue))
        {
            fieldName = leftMember.MemberName;
            comparisonType = leftComparisonType;
            value = rightValue;
            return true;
        }

        if (expression.Right is MemberExpression rightMember &&
            TryConvertConstantExpression(expression.Left, out var leftValue))
        {
            fieldName = rightMember.MemberName;
            comparisonType = rightComparisonType;
            value = leftValue;
            return true;
        }

        fieldName = null;
        comparisonType = default;
        value = null;
        return false;
    }

    private static bool TryMapComparisonType(
        System.Linq.Expressions.ExpressionType nodeType,
        bool reversed,
        out ComparisonType comparisonType)
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

        comparisonType = nodeType switch
        {
            System.Linq.Expressions.ExpressionType.Equal => ComparisonType.Equal,
            System.Linq.Expressions.ExpressionType.GreaterThan => ComparisonType.GreaterThan,
            System.Linq.Expressions.ExpressionType.GreaterThanOrEqual => ComparisonType.GreaterThanOrEqual,
            System.Linq.Expressions.ExpressionType.LessThan => ComparisonType.LessThan,
            System.Linq.Expressions.ExpressionType.LessThanOrEqual => ComparisonType.LessThanOrEqual,
            _ => default
        };

        return nodeType is System.Linq.Expressions.ExpressionType.Equal
            or System.Linq.Expressions.ExpressionType.GreaterThan
            or System.Linq.Expressions.ExpressionType.GreaterThanOrEqual
            or System.Linq.Expressions.ExpressionType.LessThan
            or System.Linq.Expressions.ExpressionType.LessThanOrEqual;
    }

    private static bool TryConvertConstantExpression(
        QueryExpression expression,
        [NotNullWhen(true)] out BsonValue? value)
    {
        if (expression is ConstantExpression constant)
        {
            try
            {
                value = constant.Value == null ? BsonNull.Value : BsonConversion.ToBsonValue(constant.Value);
                return true;
            }
            catch (Exception ex) when (ex is InvalidOperationException or NotSupportedException or InvalidCastException or FormatException or OverflowException or ArgumentException)
            {
                value = null;
                return false;
            }
        }

        value = null;
        return false;
    }

    private static bool IsComparisonCoveredByScanKey(
        ComparisonType comparisonType,
        BsonValue value,
        IndexScanKey scanKey,
        bool hasTrailingIndexFields)
    {
        if (comparisonType == ComparisonType.NotEqual)
        {
            return false;
        }

        if (comparisonType == ComparisonType.Equal)
        {
            return scanKey.ComparisonType == ComparisonType.Equal &&
                   BsonValueComparer.ValueEquals(scanKey.Value, value);
        }

        if (scanKey.ComparisonType == comparisonType &&
            BsonValueComparer.ValueEquals(scanKey.Value, value))
        {
            if (hasTrailingIndexFields && comparisonType == ComparisonType.GreaterThan)
            {
                return false;
            }

            return true;
        }

        if (scanKey.ComparisonType != ComparisonType.Range)
        {
            return false;
        }

        return comparisonType switch
        {
            ComparisonType.GreaterThan =>
                scanKey.LowerValue != null &&
                !hasTrailingIndexFields &&
                !scanKey.IncludeLower &&
                BsonValueComparer.ValueEquals(scanKey.LowerValue, value),
            ComparisonType.GreaterThanOrEqual =>
                scanKey.LowerValue != null &&
                scanKey.IncludeLower &&
                BsonValueComparer.ValueEquals(scanKey.LowerValue, value),
            ComparisonType.LessThan =>
                scanKey.UpperValue != null &&
                !scanKey.IncludeUpper &&
                BsonValueComparer.ValueEquals(scanKey.UpperValue, value),
            ComparisonType.LessThanOrEqual =>
                scanKey.UpperValue != null &&
                scanKey.IncludeUpper &&
                BsonValueComparer.ValueEquals(scanKey.UpperValue, value),
            _ => false
        };
    }

    internal static IndexScanRange BuildIndexScanRange(QueryExecutionPlan executionPlan)
    {
        if (executionPlan.IndexScanKeys.Count == 0)
        {
            return new IndexScanRange
            {
                MinKey = IndexKey.MinValue,
                MaxKey = IndexKey.MaxValue,
                IncludeMin = true,
                IncludeMax = true
            };
        }

        var minValues = new List<BsonValue>();
        var maxValues = new List<BsonValue>();
        var indexFieldCount = executionPlan.UseIndex?.Fields.Length ?? executionPlan.IndexScanKeys.Count;
        bool includeMin = true;
        bool includeMax = true;
        bool stoppedAtRange = false;
        ComparisonType lastOp = ComparisonType.Equal;
        bool rangeIncludesUpper = false;

        foreach (var key in executionPlan.IndexScanKeys)
        {
            if (key.ComparisonType == ComparisonType.Equal)
            {
                minValues.Add(key.Value);
                maxValues.Add(key.Value);
            }
            else
            {
                stoppedAtRange = true;
                lastOp = key.ComparisonType;
                switch (key.ComparisonType)
                {
                    case ComparisonType.NotEqual:
                        minValues.Add(BsonMinKey.Value);
                        maxValues.Add(BsonMaxKey.Value);
                        includeMin = true;
                        includeMax = true;
                        break;
                    case ComparisonType.GreaterThan:
                        minValues.Add(key.Value);
                        if (HasTrailingIndexFields(minValues.Count, indexFieldCount))
                        {
                            minValues.Add(BsonMaxKey.Value);
                        }

                        maxValues.Add(BsonMaxKey.Value);
                        includeMin = false;
                        break;
                    case ComparisonType.GreaterThanOrEqual:
                        minValues.Add(key.Value);
                        maxValues.Add(BsonMaxKey.Value);
                        includeMin = true;
                        break;
                    case ComparisonType.LessThan:
                        maxValues.Add(key.Value);
                        includeMax = false;
                        break;
                    case ComparisonType.LessThanOrEqual:
                        maxValues.Add(key.Value);
                        includeMax = true;
                        break;
                    case ComparisonType.Range:
                        minValues.Add(key.LowerValue ?? BsonMinKey.Value);
                        if (key.LowerValue != null &&
                            !key.IncludeLower &&
                            HasTrailingIndexFields(minValues.Count, indexFieldCount))
                        {
                            minValues.Add(BsonMaxKey.Value);
                        }

                        maxValues.Add(key.UpperValue ?? BsonMaxKey.Value);
                        includeMin = key.IncludeLower;
                        includeMax = key.IncludeUpper;
                        rangeIncludesUpper = key.IncludeUpper;
                        break;
                }
                break;
            }
        }

        // Pad MaxKey to ensure correct prefix/range matching
        if (!stoppedAtRange)
        {
            // All Equals: Prefix match, so we want everything starting with this prefix
            maxValues.Add(BsonMaxKey.Value);
        }
        else
        {
            // If we ended with LT/LTE, we need to ensure we include children of the boundary
            // e.g. A <= 5 should include (5, 1)
            if (lastOp == ComparisonType.LessThanOrEqual ||
                (lastOp == ComparisonType.Range && rangeIncludesUpper))
            {
                maxValues.Add(BsonMaxKey.Value);
            }
        }

        return new IndexScanRange
        {
            MinKey = CreateIndexKey(minValues),
            MaxKey = CreateIndexKey(maxValues),
            IncludeMin = includeMin,
            IncludeMax = includeMax
        };
    }

    internal static IndexKey? BuildExactIndexKey(QueryExecutionPlan executionPlan)
    {
        if (executionPlan.IndexScanKeys.Count == 0) return null;
        var values = new List<BsonValue>();
        foreach (var key in executionPlan.IndexScanKeys)
        {
            if (key.ComparisonType != ComparisonType.Equal) return null;
            values.Add(key.Value);
        }
        return CreateIndexKey(values);
    }

    private static IndexKey CreateIndexKey(List<BsonValue> values)
    {
        return values.Count == 1
            ? IndexKey.Create(values[0])
            : new IndexKey(values.ToArray());
    }

    private static bool HasTrailingIndexFields(int populatedFieldCount, int indexFieldCount)
    {
        return populatedFieldCount < indexFieldCount;
    }
}
