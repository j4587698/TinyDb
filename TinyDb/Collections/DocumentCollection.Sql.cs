using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using TinyDb.Bson;
using TinyDb.Core;
using TinyDb.Serialization;
using TinyDb.Query;
using TinyDb.Index;
using TinyDb.Attributes;
using TinyDb.IdGeneration;

namespace TinyDb.Collections;

public sealed partial class DocumentCollection<T> where T : class
{
    public IEnumerable<T> Find(string predicate, IReadOnlyDictionary<string, object?>? parameters = null)
    {
        return Find(predicate, parameters, 0, int.MaxValue);
    }

    public IEnumerable<T> FindSql(string sql, IReadOnlyDictionary<string, object?>? parameters = null)
    {
        return Execute<T>(sql, parameters).Rows;
    }

    public IEnumerable<BsonDocument> FindSqlDocuments(string sql, IReadOnlyDictionary<string, object?>? parameters = null)
    {
        return Execute(sql, parameters).Documents;
    }

    public IEnumerable<TProjection> FindSql<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] TProjection>(
        string sql,
        IReadOnlyDictionary<string, object?>? parameters = null)
        where TProjection : class
    {
        return Execute<TProjection>(sql, parameters).Rows;
    }

    public SqlExecutionResult Execute(string sql, IReadOnlyDictionary<string, object?>? parameters = null)
    {
        ThrowIfDisposed();
        if (sql == null) throw new ArgumentNullException(nameof(sql));

        var statement = SqlQueryParser.Parse(sql, parameters);
        return Execute(statement);
    }

    internal SqlExecutionResult Execute(SqlStatement statement)
    {
        ThrowIfDisposed();
        if (statement == null) throw new ArgumentNullException(nameof(statement));

        ValidateSqlCollection(statement);

        return statement switch
        {
            SqlQuerySpec query => new SqlExecutionResult(
                SqlStatementKind.Select,
                0,
                FindDocuments(query, preserveProjectionNames: true).ToList()),
            SqlInsertStatement insert => new SqlExecutionResult(SqlStatementKind.Insert, ExecuteInsert(insert), Array.Empty<BsonDocument>()),
            SqlUpdateStatement update => new SqlExecutionResult(SqlStatementKind.Update, ExecuteUpdate(update), Array.Empty<BsonDocument>()),
            SqlDeleteStatement delete => new SqlExecutionResult(SqlStatementKind.Delete, ExecuteDelete(delete), Array.Empty<BsonDocument>()),
            _ => throw new NotSupportedException($"Unsupported SQL statement '{statement.Kind}'.")
        };
    }

    public SqlExecutionResult<TProjection> Execute<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] TProjection>(
        string sql,
        IReadOnlyDictionary<string, object?>? parameters = null)
        where TProjection : class
    {
        ThrowIfDisposed();
        if (sql == null) throw new ArgumentNullException(nameof(sql));

        var statement = SqlQueryParser.Parse(sql, parameters);
        return Execute<TProjection>(statement);
    }

    internal SqlExecutionResult<TProjection> Execute<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] TProjection>(
        SqlStatement statement)
        where TProjection : class
    {
        ThrowIfDisposed();
        if (statement == null) throw new ArgumentNullException(nameof(statement));

        ValidateSqlCollection(statement);

        return statement switch
        {
            SqlQuerySpec query => new SqlExecutionResult<TProjection>(
                SqlStatementKind.Select,
                0,
                FindDocuments(query, preserveProjectionNames: false)
                    .Select(AotBsonMapper.FromDocument<TProjection>)
                    .ToList()),
            SqlInsertStatement insert => new SqlExecutionResult<TProjection>(SqlStatementKind.Insert, ExecuteInsert(insert), Array.Empty<TProjection>()),
            SqlUpdateStatement update => new SqlExecutionResult<TProjection>(SqlStatementKind.Update, ExecuteUpdate(update), Array.Empty<TProjection>()),
            SqlDeleteStatement delete => new SqlExecutionResult<TProjection>(SqlStatementKind.Delete, ExecuteDelete(delete), Array.Empty<TProjection>()),
            _ => throw new NotSupportedException($"Unsupported SQL statement '{statement.Kind}'.")
        };
    }

    public IEnumerable<T> Find(Expression<Func<T, bool>> predicate, int skip, int limit)
    {
        ThrowIfDisposed();
        if (predicate == null) throw new ArgumentNullException(nameof(predicate));

        ValidatePaginationArguments(skip, limit);
        if (limit == 0) return Enumerable.Empty<T>();

        var query = Query().Where(predicate);
        if (skip > 0)
        {
            query = query.Skip(skip);
        }

        if (limit < int.MaxValue)
        {
            query = query.Take(limit);
        }

        return query;
    }

    public IEnumerable<T> Find(string predicate, IReadOnlyDictionary<string, object?>? parameters, int skip, int limit)
    {
        ThrowIfDisposed();
        if (predicate == null) throw new ArgumentNullException(nameof(predicate));

        var queryExpression = StringQueryParser.Parse(predicate, parameters);
        return Find(queryExpression, skip, limit);
    }

    private IEnumerable<T> Find(SqlQuerySpec query)
    {
        ValidateSqlCollection(query);

        ValidatePaginationArguments(query.Skip, query.Limit);
        if (query.Limit == 0) return Enumerable.Empty<T>();

        var result = _queryExecutor.Execute<T>(_name, query.Predicate);
        var orderBy = CreateStableSqlOrderBy(query);
        if (orderBy.Count > 0)
        {
            result = ApplySqlOrdering(result, orderBy);
        }

        if (query.Skip > 0)
        {
            result = result.Skip(query.Skip);
        }

        if (query.Limit < int.MaxValue)
        {
            result = result.Take(query.Limit);
        }

        return result;
    }

    private IEnumerable<BsonDocument> FindDocuments(SqlQuerySpec query, bool preserveProjectionNames)
    {
        var source = Find(query);
        if (!query.SelectAll)
        {
            return source.Select(item => ProjectSqlDocument(item, query.Projection, preserveProjectionNames));
        }

        if (typeof(T) == typeof(BsonDocument))
        {
            return source.Select(ToSqlDocument);
        }

        var projection = CreateSelectAllProjection();
        return source.Select(item => ProjectSqlDocument(item, projection, preserveProjectionNames));
    }

    private int ExecuteInsert(SqlInsertStatement statement)
    {
        ValidateNoDuplicateSqlStorageFields(statement.Assignments);

        var document = new BsonDocument();
        foreach (var assignment in statement.Assignments)
        {
            document = SetSqlDocumentField(document, assignment);
        }

        var entity = ToEntity(document);
        Insert(entity);
        return 1;
    }

    private int ExecuteUpdate(SqlUpdateStatement statement)
    {
        if (statement.Assignments.Any(static assignment => IsSqlIdFieldPath(assignment.FieldPath)))
        {
            throw new NotSupportedException("SQL UPDATE cannot modify primary key fields.");
        }

        ValidateNoDuplicateSqlStorageFields(statement.Assignments);

        var ids = CollectSqlTargetIds(statement.Predicate, "update");
        var currentTransaction = _engine.GetCurrentTransaction();
        return currentTransaction is Transaction transaction
            ? ExecuteUpdateInTransaction(statement, ids, transaction)
            : ExecuteUpdateCommitted(statement, ids);
    }

    private List<BsonValue> CollectSqlTargetIds(QueryExpression? predicate, string operationName)
    {
        var ids = new List<BsonValue>();
        var seenIds = new HashSet<BsonValue>(BsonValueComparer.EqualityComparer);

        foreach (var item in _queryExecutor.Execute<T>(_name, predicate))
        {
            if (!AotIdAccessor<T>.HasValidId(item))
            {
                throw new ArgumentException($"Entity must have a valid ID for {operationName}", nameof(item));
            }

            var id = AotIdAccessor<T>.GetId(item);
            if (id != null && !id.IsNull && seenIds.Add(id))
            {
                ids.Add(id);
            }
        }

        return ids;
    }

    private int ExecuteUpdateCommitted(SqlUpdateStatement statement, IReadOnlyList<BsonValue> ids)
    {
        var updatedCount = 0;
        var idBatch = new List<BsonValue>(SqlWriteBatchSize);
        var documentBatch = new List<BsonDocument>(SqlWriteBatchSize);

        foreach (var id in ids)
        {
            idBatch.Add(id);
            if (idBatch.Count >= SqlWriteBatchSize)
            {
                updatedCount += FlushSqlUpdateBatch(statement, idBatch, documentBatch);
            }
        }

        updatedCount += FlushSqlUpdateBatch(statement, idBatch, documentBatch);
        return updatedCount;
    }

    private int FlushSqlUpdateBatch(
        SqlUpdateStatement statement,
        List<BsonValue> idBatch,
        List<BsonDocument> documentBatch)
    {
        if (idBatch.Count == 0)
        {
            return 0;
        }

        var documents = _engine.FindByIds(_name, idBatch);
        for (int i = 0; i < documents.Count; i++)
        {
            var document = documents[i];
            if (document == null)
            {
                continue;
            }

            foreach (var assignment in statement.Assignments)
            {
                document = SetSqlDocumentField(document, assignment);
            }

            documentBatch.Add(document);
        }

        idBatch.Clear();
        if (documentBatch.Count == 0)
        {
            return 0;
        }

        var updatedCount = _engine.UpdateDocuments(_name, documentBatch);
        documentBatch.Clear();
        return updatedCount;
    }

    private int ExecuteUpdateInTransaction(
        SqlUpdateStatement statement,
        IReadOnlyList<BsonValue> ids,
        Transaction transaction)
    {
        var updatedCount = 0;
        var idBatch = new List<BsonValue>(SqlWriteBatchSize);
        var prepared = new List<(BsonDocument Document, BsonValue Id)>(SqlWriteBatchSize);
        var originalDocuments = new List<BsonDocument?>(SqlWriteBatchSize);

        foreach (var id in ids)
        {
            idBatch.Add(id);
            if (idBatch.Count >= SqlWriteBatchSize)
            {
                updatedCount += FlushSqlTransactionUpdateBatch(statement, idBatch, prepared, originalDocuments, transaction);
            }
        }

        updatedCount += FlushSqlTransactionUpdateBatch(statement, idBatch, prepared, originalDocuments, transaction);
        return updatedCount;
    }

    private int FlushSqlTransactionUpdateBatch(
        SqlUpdateStatement statement,
        List<BsonValue> idBatch,
        List<(BsonDocument Document, BsonValue Id)> prepared,
        List<BsonDocument?> originalDocuments,
        Transaction transaction)
    {
        if (idBatch.Count == 0)
        {
            return 0;
        }

        var currentDocuments = _engine.FindByIds(_name, idBatch);
        for (int i = 0; i < currentDocuments.Count; i++)
        {
            var document = currentDocuments[i];
            if (document == null)
            {
                continue;
            }

            var originalDocument = document;
            foreach (var assignment in statement.Assignments)
            {
                document = SetSqlDocumentField(document, assignment);
            }

            prepared.Add((document, idBatch[i]));
            originalDocuments.Add(originalDocument);
        }

        idBatch.Clear();
        if (prepared.Count == 0)
        {
            return 0;
        }

        var updatedCount = RecordPreparedUpdatesInTransaction(prepared, originalDocuments, transaction);
        prepared.Clear();
        originalDocuments.Clear();
        return updatedCount;
    }

    private int ExecuteDelete(SqlDeleteStatement statement)
    {
        return Delete(CollectSqlTargetIds(statement.Predicate, "delete"));
    }

    private static BsonDocument SetSqlDocumentField(BsonDocument document, SqlAssignment assignment)
    {
        var target = ResolveSqlStorageField(assignment.FieldPath);
        var value = NormalizeSqlAssignmentValue(assignment.Value, target.PropertyType, assignment.FieldPath);
        return document.Set(target.FieldName, BsonConversion.ToBsonValue(value!));
    }

    private static T ToEntity(BsonDocument document)
    {
        return typeof(T) == typeof(BsonDocument)
            ? (T)(object)document
            : AotBsonMapper.FromDocument<T>(document);
    }

    private void ValidateSqlCollection(SqlStatement statement)
    {
        if (!string.Equals(statement.CollectionName, _name, StringComparison.OrdinalIgnoreCase))
        {
            throw new ArgumentException(
                $"SQL collection '{statement.CollectionName}' does not match collection '{_name}'.",
                nameof(statement));
        }
    }

    private static BsonDocument ToSqlDocument(T item)
    {
        return item is BsonDocument document
            ? document
            : AotBsonMapper.ToDocument(item);
    }

    private static BsonDocument ProjectSqlDocument(T item, IReadOnlyList<SqlProjectionField> projection, bool preserveProjectionNames)
    {
        var document = new BsonDocument();
        foreach (var field in projection)
        {
            var fieldExpression = CreateSqlFieldExpression(field.FieldPath);
            var value = ExpressionEvaluator.EvaluateValue<T>(fieldExpression, item);
            document = document.Set(
                NormalizeSqlProjectionFieldName(field.OutputName, preserveProjectionNames),
                BsonConversion.ToBsonValue(value!));
        }

        return document;
    }

    private static IReadOnlyList<SqlProjectionField> CreateSelectAllProjection()
    {
        var projection = new List<SqlProjectionField>(EntityMetadata<T>.Properties.Count);
        foreach (var property in EntityMetadata<T>.Properties)
        {
            projection.Add(new SqlProjectionField(property.Name, property.Name));
        }

        return projection;
    }

    private static IEnumerable<T> ApplySqlOrdering(IEnumerable<T> source, IReadOnlyList<SqlOrderBy> orderBy)
    {
        IOrderedEnumerable<T>? ordered = null;
        foreach (var sort in orderBy)
        {
            var sortExpression = CreateSqlFieldExpression(sort.FieldPath);
            Func<T, object?> keySelector = item => ExpressionEvaluator.EvaluateValue<T>(sortExpression, item);

            if (ordered == null)
            {
                ordered = sort.Descending
                    ? source.OrderByDescending(keySelector, SqlSortComparer.Instance)
                    : source.OrderBy(keySelector, SqlSortComparer.Instance);
                continue;
            }

            ordered = sort.Descending
                ? ordered.ThenByDescending(keySelector, SqlSortComparer.Instance)
                : ordered.ThenBy(keySelector, SqlSortComparer.Instance);
        }

        return ordered ?? source;
    }

    private static IReadOnlyList<SqlOrderBy> CreateStableSqlOrderBy(SqlQuerySpec query)
    {
        if ((query.Skip <= 0 && query.Limit >= int.MaxValue) || query.OrderBy.Count == 0)
        {
            return query.OrderBy;
        }

        if (query.OrderBy.Any(static sort => IsSqlIdFieldPath(sort.FieldPath)))
        {
            return query.OrderBy;
        }

        var result = new List<SqlOrderBy>(query.OrderBy.Count + 1);
        result.AddRange(query.OrderBy);
        result.Add(new SqlOrderBy(GetSqlIdFieldName(), false));
        return result;
    }

    private static QueryExpression CreateSqlFieldExpression(string fieldPath)
    {
        var segments = fieldPath.Split('.');
        QueryExpression expression = new TinyDb.Query.MemberExpression(
            NormalizeSqlFieldSegment(segments[0]),
            new TinyDb.Query.ParameterExpression("$"));
        for (var i = 1; i < segments.Length; i++)
        {
            expression = new TinyDb.Query.MemberExpression(NormalizeSqlFieldSegment(segments[i]), expression);
        }

        return expression;
    }

    private static string NormalizeSqlFieldSegment(string segment)
    {
        if (string.Equals(segment, "_id", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(segment, "id", StringComparison.OrdinalIgnoreCase))
        {
            return GetSqlIdFieldName();
        }

        if (typeof(T) == typeof(BsonDocument))
        {
            return segment;
        }

        foreach (var property in EntityMetadata<T>.Properties)
        {
            if (string.Equals(property.Name, segment, StringComparison.OrdinalIgnoreCase) ||
                string.Equals(BsonFieldName.ToCamelCase(property.Name), segment, StringComparison.OrdinalIgnoreCase))
            {
                return property.Name;
            }
        }

        return segment;
    }

    private static string NormalizeSqlProjectionFieldName(string name, bool preserveProjectionNames)
    {
        var outputName = name;
        var lastDot = outputName.LastIndexOf('.');
        if (lastDot >= 0)
        {
            outputName = outputName.Substring(lastDot + 1);
        }

        if (preserveProjectionNames)
        {
            return outputName;
        }

        if (string.Equals(outputName, "_id", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(outputName, "id", StringComparison.OrdinalIgnoreCase))
        {
            return "_id";
        }

        if (typeof(T) == typeof(BsonDocument))
        {
            return outputName;
        }

        foreach (var property in EntityMetadata<T>.Properties)
        {
            if (string.Equals(property.Name, outputName, StringComparison.OrdinalIgnoreCase) ||
                string.Equals(BsonFieldName.ToCamelCase(property.Name), outputName, StringComparison.OrdinalIgnoreCase))
            {
                return property == EntityMetadata<T>.IdProperty
                    ? "_id"
                    : BsonFieldName.ToCamelCase(property.Name);
            }
        }

        return BsonFieldName.ToCamelCase(outputName);
    }

    private static string NormalizeSqlStorageFieldName(string fieldPath)
        => ResolveSqlStorageField(fieldPath).FieldName;

    private static (string FieldName, Type? PropertyType) ResolveSqlStorageField(string fieldPath)
    {
        if (fieldPath.IndexOf('.') >= 0)
        {
            throw new NotSupportedException("SQL INSERT/UPDATE currently supports top-level fields only.");
        }

        if (typeof(T) == typeof(BsonDocument))
        {
            return (IsIdFieldPath(fieldPath) ? "_id" : fieldPath, null);
        }

        if (IsIdFieldPath(fieldPath))
        {
            return ("_id", EntityMetadata<T>.IdProperty?.PropertyType);
        }

        foreach (var property in EntityMetadata<T>.Properties)
        {
            if (string.Equals(property.Name, fieldPath, StringComparison.OrdinalIgnoreCase) ||
                string.Equals(BsonFieldName.ToCamelCase(property.Name), fieldPath, StringComparison.OrdinalIgnoreCase))
            {
                var fieldName = property == EntityMetadata<T>.IdProperty
                    ? "_id"
                    : BsonFieldName.ToCamelCase(property.Name);
                return (fieldName, property.PropertyType);
            }
        }

        return (BsonFieldName.ToCamelCase(fieldPath), null);
    }

    private static void ValidateNoDuplicateSqlStorageFields(IReadOnlyList<SqlAssignment> assignments)
    {
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var assignment in assignments)
        {
            var fieldName = ResolveSqlStorageField(assignment.FieldPath).FieldName;
            if (!seen.Add(fieldName))
            {
                throw new FormatException($"SQL DML contains duplicate field '{assignment.FieldPath}'.");
            }
        }
    }

    private static object? NormalizeSqlAssignmentValue(object? value, Type? targetType, string fieldPath)
    {
        if (targetType == null)
        {
            return value;
        }

        var nullableType = Nullable.GetUnderlyingType(targetType);
        var effectiveType = nullableType ?? targetType;
        if (value == null)
        {
            if (nullableType == null && targetType.IsValueType)
            {
                throw new InvalidOperationException(
                    $"SQL assignment for '{fieldPath}' cannot set null to non-nullable type '{targetType.Name}'.");
            }

            return null;
        }

        if (effectiveType.IsInstanceOfType(value))
        {
            return value;
        }

        try
        {
            if (value is BsonValue bsonValue && !typeof(BsonValue).IsAssignableFrom(effectiveType))
            {
                value = GetSqlAssignmentRawValue(bsonValue);
                if (value == null)
                {
                    if (nullableType == null && targetType.IsValueType)
                    {
                        throw new InvalidOperationException(
                            $"SQL assignment for '{fieldPath}' cannot set null to non-nullable type '{targetType.Name}'.");
                    }

                    return null;
                }

                if (effectiveType.IsInstanceOfType(value))
                {
                    return value;
                }
            }

            if (effectiveType.IsEnum)
            {
                return value is string enumText
                    ? Enum.Parse(effectiveType, enumText, ignoreCase: true)
                    : Enum.ToObject(effectiveType, Convert.ChangeType(value, Enum.GetUnderlyingType(effectiveType), CultureInfo.InvariantCulture)!);
            }

            if (effectiveType == typeof(Guid))
            {
                if (value is string guidText)
                {
                    return Guid.Parse(guidText);
                }

                throw new InvalidCastException($"Cannot convert '{value.GetType().Name}' to Guid.");
            }

            if (effectiveType == typeof(DateTime) && value is string dateText)
            {
                return DateTime.Parse(dateText, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
            }

            if (IsNumericType(effectiveType) && IsNumericType(value.GetType()))
            {
                return ConvertSqlNumericValue(value, effectiveType, fieldPath);
            }

            if (effectiveType == typeof(string))
            {
                return Convert.ToString(value, CultureInfo.InvariantCulture);
            }

            return Convert.ChangeType(value, effectiveType, CultureInfo.InvariantCulture);
        }
        catch (Exception ex) when (ex is FormatException or InvalidCastException or OverflowException or ArgumentException)
        {
            throw new InvalidOperationException(
                $"SQL assignment for '{fieldPath}' cannot convert value '{value}' to '{effectiveType.Name}'.",
                ex);
        }
    }

    private static object ConvertSqlNumericValue(object value, Type targetType, string fieldPath)
    {
        if (IsIntegralType(targetType))
        {
            var decimalValue = Convert.ToDecimal(value, CultureInfo.InvariantCulture);
            if (decimal.Truncate(decimalValue) != decimalValue)
            {
                throw new InvalidOperationException(
                    $"SQL assignment for '{fieldPath}' cannot convert fractional value '{value}' to '{targetType.Name}'.");
            }
        }

        return Convert.ChangeType(value, targetType, CultureInfo.InvariantCulture)!;
    }

    private static bool IsNumericType(Type type)
    {
        type = Nullable.GetUnderlyingType(type) ?? type;
        return type == typeof(byte) ||
               type == typeof(sbyte) ||
               type == typeof(short) ||
               type == typeof(ushort) ||
               type == typeof(int) ||
               type == typeof(uint) ||
               type == typeof(long) ||
               type == typeof(ulong) ||
               type == typeof(float) ||
               type == typeof(double) ||
               type == typeof(decimal);
    }

    private static bool IsIntegralType(Type type)
    {
        type = Nullable.GetUnderlyingType(type) ?? type;
        return type == typeof(byte) ||
               type == typeof(sbyte) ||
               type == typeof(short) ||
               type == typeof(ushort) ||
               type == typeof(int) ||
               type == typeof(uint) ||
               type == typeof(long) ||
               type == typeof(ulong);
    }

    private static object? GetSqlAssignmentRawValue(BsonValue bsonValue)
    {
        if (bsonValue.IsNull)
        {
            return null;
        }

        return bsonValue is BsonDecimal128 decimalValue
            ? decimalValue.ToDecimal(CultureInfo.InvariantCulture)
            : bsonValue.RawValue;
    }

    private static bool IsIdFieldPath(string fieldPath)
    {
        return string.Equals(fieldPath, "Id", StringComparison.OrdinalIgnoreCase) ||
               string.Equals(fieldPath, "_id", StringComparison.OrdinalIgnoreCase);
    }

    private static bool IsSqlIdFieldPath(string fieldPath)
    {
        if (fieldPath.IndexOf('.') >= 0)
        {
            return false;
        }

        if (IsIdFieldPath(fieldPath))
        {
            return true;
        }

        if (typeof(T) == typeof(BsonDocument))
        {
            return false;
        }

        var idProperty = EntityMetadata<T>.IdProperty;
        return idProperty != null &&
               (string.Equals(idProperty.Name, fieldPath, StringComparison.OrdinalIgnoreCase) ||
                string.Equals(BsonFieldName.ToCamelCase(idProperty.Name), fieldPath, StringComparison.OrdinalIgnoreCase));
    }

    private static string GetSqlIdFieldName()
    {
        if (typeof(T) == typeof(BsonDocument))
        {
            return "Id";
        }

        return EntityMetadata<T>.IdProperty?.Name ?? "Id";
    }

    private sealed class SqlSortComparer : IComparer<object?>
    {
        public static readonly SqlSortComparer Instance = new();

        public int Compare(object? x, object? y)
        {
            return QueryValueComparer.Compare(x, y);
        }
    }

}
