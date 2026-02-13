using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Linq.Expressions;
using TinyDb.Bson;
using TinyDb.Core;
using TinyDb.Index;
using TinyDb.Serialization;

namespace TinyDb.Query;

public sealed class QueryExecutor
{
    private readonly TinyDbEngine _engine;
    private readonly ExpressionParser _expressionParser;
    private readonly QueryOptimizer _queryOptimizer;

    public QueryExecutor(TinyDbEngine engine)
    {
        _engine = engine ?? throw new ArgumentNullException(nameof(engine));
        _expressionParser = new ExpressionParser();
        _queryOptimizer = new QueryOptimizer(engine);
    }

    public IEnumerable<T> Execute<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(string collectionName, Expression<Func<T, bool>>? expression = null)
        where T : class, new()
    {
        if (string.IsNullOrWhiteSpace(collectionName))
            throw new ArgumentException("Collection name cannot be null or empty", nameof(collectionName));

        var executionPlan = _queryOptimizer.CreateExecutionPlan(collectionName, expression);

        return executionPlan.Strategy switch
        {
            QueryExecutionStrategy.PrimaryKeyLookup => ExecutePrimaryKeyLookup<T>(executionPlan),
            QueryExecutionStrategy.IndexScan => ExecuteIndexScan<T>(executionPlan),
            QueryExecutionStrategy.IndexSeek => ExecuteIndexSeek<T>(executionPlan),
            _ => ExecuteFullTableScan<T>(collectionName, expression)
        };
    }

    // ... (省略 Index 相关方法，保持不变) ...
    internal IEnumerable<BsonDocument> ExecuteIndexScanForTests(QueryExecutionPlan executionPlan) => ExecuteIndexScan<BsonDocument>(executionPlan);
    internal IEnumerable<T> ExecutePrimaryKeyLookupForTests<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(QueryExecutionPlan executionPlan) where T : class, new() => ExecutePrimaryKeyLookup<T>(executionPlan);
    internal IEnumerable<T> ExecuteIndexSeekForTests<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(QueryExecutionPlan executionPlan) where T : class, new() => ExecuteIndexSeek<T>(executionPlan);

    private IEnumerable<T> ExecutePrimaryKeyLookup<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(QueryExecutionPlan executionPlan) where T : class, new()
    {
        if (executionPlan.IndexScanKeys.Count == 0) yield break;
        var doc = _engine.FindById(executionPlan.CollectionName, executionPlan.IndexScanKeys[0].Value);
        if (doc != null)
        {
            bool match = executionPlan.QueryExpression == null || ExpressionEvaluator.Evaluate(executionPlan.QueryExpression, doc);
            if (match)
            {
                var entity = AotBsonMapper.FromDocument<T>(doc);
                if (entity != null) yield return entity;
            }
        }
    }

    private IEnumerable<T> ExecuteIndexScan<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(QueryExecutionPlan executionPlan) where T : class, new()
    {
        var idxMgr = _engine.GetIndexManager(executionPlan.CollectionName);
        if (idxMgr == null || executionPlan.UseIndex == null)
        {
            foreach (var item in ExecuteFullTableScan<T>(executionPlan.CollectionName, (Expression<Func<T, bool>>?)executionPlan.OriginalExpression)) yield return item;
            yield break;
        }
        var index = idxMgr.GetIndex(executionPlan.UseIndex.Name);
        if (index == null)
        {
            foreach (var item in ExecuteFullTableScan<T>(executionPlan.CollectionName, (Expression<Func<T, bool>>?)executionPlan.OriginalExpression)) yield return item;
            yield break;
        }

        var range = BuildIndexScanRange(executionPlan);
        var ids = index.FindRange(range.MinKey, range.MaxKey, range.IncludeMin, range.IncludeMax);
        
        QueryExpression? qe = null;
        if (executionPlan.OriginalExpression != null) qe = _expressionParser.Parse<T>((Expression<Func<T, bool>>)executionPlan.OriginalExpression);

        foreach (var id in ids)
        {
            var doc = _engine.FindById(executionPlan.CollectionName, id);
            if (doc != null && (qe == null || ExpressionEvaluator.Evaluate(qe, doc)))
            {
                var entity = AotBsonMapper.FromDocument<T>(doc);
                if (entity != null) yield return entity;
            }
        }
    }

    private IEnumerable<T> ExecuteIndexSeek<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(QueryExecutionPlan executionPlan) where T : class, new()
    {
        var idxMgr = _engine.GetIndexManager(executionPlan.CollectionName);
        if (idxMgr == null || executionPlan.UseIndex == null)
        {
            foreach (var item in ExecuteFullTableScan<T>(executionPlan.CollectionName, (Expression<Func<T, bool>>?)executionPlan.OriginalExpression)) yield return item;
            yield break;
        }
        var index = idxMgr.GetIndex(executionPlan.UseIndex.Name);
        if (index == null)
        {
            foreach (var item in ExecuteFullTableScan<T>(executionPlan.CollectionName, (Expression<Func<T, bool>>?)executionPlan.OriginalExpression)) yield return item;
            yield break;
        }

        var key = BuildExactIndexKey(executionPlan);
        if (key == null)
        {
            foreach (var item in ExecuteFullTableScan<T>(executionPlan.CollectionName, (Expression<Func<T, bool>>?)executionPlan.OriginalExpression)) yield return item;
            yield break;
        }

        QueryExpression? qe = null;
        if (executionPlan.OriginalExpression != null) qe = _expressionParser.Parse<T>((Expression<Func<T, bool>>)executionPlan.OriginalExpression);

        if (index.IsUnique)
        {
            var id = index.FindExact(key);
            if (id != null)
            {
                var doc = _engine.FindById(executionPlan.CollectionName, id);
                if (doc != null && (qe == null || ExpressionEvaluator.Evaluate(qe, doc)))
                {
                    var entity = AotBsonMapper.FromDocument<T>(doc);
                    if (entity != null) yield return entity;
                }
            }
        }
        else
        {
            foreach (var id in index.Find(key))
            {
                var doc = _engine.FindById(executionPlan.CollectionName, id);
                if (doc != null && (qe == null || ExpressionEvaluator.Evaluate(qe, doc)))
                {
                    var entity = AotBsonMapper.FromDocument<T>(doc);
                    if (entity != null) yield return entity;
                }
            }
        }
    }

    private IEnumerable<T> ExecuteFullTableScan<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(string collectionName, Expression<Func<T, bool>>? expression = null)
        where T : class, new()
    {
        var tx = _engine.GetCurrentTransaction();
        QueryExpression? queryExpression = null;

        if (expression != null)
        {
            try { queryExpression = _expressionParser.Parse(expression); }
            catch (Exception ex) { throw new NotSupportedException("Parse failed", ex); }
        }

        // 1. 准备下推谓词
        var predicates = new List<ScanPredicate>();
        bool fullyPushed = CollectPredicates(queryExpression, predicates);
        var pushDownPredicates = predicates.Count > 0 ? predicates.ToArray() : null;

        // 2. 处理管道
        var rawPipeline = _engine.FindAllRawWithPredicateInfo(collectionName, pushDownPredicates)
            .Select(r => (Doc: TryDeserialize(r.Slice), r.RequiresPostFilter))
            .Where(x => x.Doc != null)
            .Select(x => (Doc: x.Doc!, x.RequiresPostFilter))
            .Where(x => !x.Doc.TryGetValue("_collection", out var c) || c.ToString() == collectionName)
            .Select(x => (Doc: _engine.ResolveLargeDocument(x.Doc), x.RequiresPostFilter));

        // 3. 事务覆盖
        IEnumerable<(BsonDocument Doc, bool RequiresPostFilter)> docs;
        if (tx != null)
        {
            var txOverlay = new ConcurrentDictionary<string, BsonDocument?>();
            foreach (var op in tx.Operations)
            {
                if (op.CollectionName != collectionName) continue;
                var k = op.DocumentId?.ToString() ?? "";
                if (op.OperationType == TransactionOperationType.Delete) txOverlay[k] = null;
                else if (op.NewDocument != null) txOverlay[k] = op.NewDocument;
            }

            if (!txOverlay.IsEmpty)
            {
                docs = rawPipeline.Select(item =>
                    {
                        var id = item.Doc["_id"].ToString();
                        if (txOverlay.TryRemove(id, out var txDoc))
                        {
                            return (Doc: txDoc, RequiresPostFilter: true);
                        }

                        return (Doc: item.Doc, item.RequiresPostFilter);
                    })
                    .Where(x => x.Doc != null)
                    .Select(x => (Doc: x.Doc!, x.RequiresPostFilter))
                    .Concat(txOverlay.Values.Where(v => v != null).Select(v => (Doc: v!, RequiresPostFilter: true)));
            }
            else docs = rawPipeline;
        }
        else docs = rawPipeline;

        // 4. 内存过滤与映射
        var filtered = queryExpression == null
            ? docs.Select(x => x.Doc)
            : fullyPushed
                ? docs.Where(x => !x.RequiresPostFilter || ExpressionEvaluator.Evaluate(queryExpression, x.Doc)).Select(x => x.Doc)
                : docs.Where(x => ExpressionEvaluator.Evaluate(queryExpression, x.Doc)).Select(x => x.Doc);

        return filtered
            .Select(doc => AotBsonMapper.FromDocument<T>(doc))
            .Where(entity => entity != null)!;
    }

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
        if (string.Equals(memberName, "Id", StringComparison.Ordinal))
        {
            fieldNameBytes = System.Text.Encoding.UTF8.GetBytes("id");
            alternateFieldNameBytes = System.Text.Encoding.UTF8.GetBytes("Id");
            secondAlternateFieldNameBytes = System.Text.Encoding.UTF8.GetBytes("_id");
        }
        else
        {
            var camelName = ToCamelCase(memberName);
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

    private static string ToCamelCase(string name)
    {
        if (string.IsNullOrEmpty(name)) return name;
        if (name.Length == 1) return name.ToLowerInvariant();
        return char.ToLowerInvariant(name[0]) + name.Substring(1);
    }

    private static ExpressionType ReverseComparisonOperator(ExpressionType op)
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

    private (MemberExpression? member, ConstantExpression? constant, ExpressionType op) ExtractComparison(BinaryExpression binary)
    {
        // Case 1: Member OP Constant
        if (binary.Left is MemberExpression m1 && binary.Right is ConstantExpression c1) return (m1, c1, binary.NodeType);
        
        // Case 2: Member OP Convert(Constant)
        if (binary.Left is MemberExpression m2 && binary.Right is UnaryExpression u2 && u2.NodeType == ExpressionType.Convert && u2.Operand is ConstantExpression c2)
        {
            try 
            {
                var converted = Convert.ChangeType(c2.Value, u2.Type);
                return (m2, new ConstantExpression(converted), binary.NodeType);
            }
            catch { }
        }

        // Case 3: Constant OP Member
        if (binary.Left is ConstantExpression c3 && binary.Right is MemberExpression m3) return (m3, c3, ReverseComparisonOperator(binary.NodeType));

        // Case 4: Convert(Constant) OP Member
        if (binary.Left is UnaryExpression u4 && u4.NodeType == ExpressionType.Convert && u4.Operand is ConstantExpression c4 && binary.Right is MemberExpression m4)
        {
            try
            {
                var converted = Convert.ChangeType(c4.Value, u4.Type);
                return (m4, new ConstantExpression(converted), ReverseComparisonOperator(binary.NodeType));
            }
            catch { }
        }

        return (null, null, binary.NodeType);
    }

    private static BsonDocument? TryDeserialize(ReadOnlyMemory<byte> slice)
    {
        try { return BsonSerializer.DeserializeDocument(slice); }
        catch { return null; }
    }

    // 构建索引扫描范围
    private static IndexScanRange BuildIndexScanRange(QueryExecutionPlan executionPlan)
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
        bool includeMin = true;
        bool includeMax = true;
        bool stoppedAtRange = false;
        ComparisonType lastOp = ComparisonType.Equal;

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
                    case ComparisonType.GreaterThan:
                        minValues.Add(key.Value);
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
            if (lastOp == ComparisonType.LessThan || lastOp == ComparisonType.LessThanOrEqual)
            {
                maxValues.Add(BsonMaxKey.Value);
            }
        }

        return new IndexScanRange
        {
            MinKey = new IndexKey(minValues.ToArray()),
            MaxKey = new IndexKey(maxValues.ToArray()),
            IncludeMin = includeMin,
            IncludeMax = includeMax
        };
    }

    private static IndexKey? BuildExactIndexKey(QueryExecutionPlan executionPlan)
    {
        if (executionPlan.IndexScanKeys.Count == 0) return null;
        var values = new List<BsonValue>();
        foreach (var key in executionPlan.IndexScanKeys)
        {
            if (key.ComparisonType != ComparisonType.Equal) return null;
            values.Add(key.Value);
        }
        return new IndexKey(values.ToArray());
    }
}
