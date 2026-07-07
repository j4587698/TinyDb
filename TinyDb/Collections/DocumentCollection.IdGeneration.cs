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
    private void EnsureEntityHasId(T entity)
    {
        if (AotIdAccessor<T>.TryGetIdInfo(out var idPropertyName, out var idPropertyType) &&
            (idPropertyType == typeof(int) || idPropertyType == typeof(long)) &&
            !AotIdAccessor<T>.HasValidId(entity))
        {
            AotIdAccessor<T>.TryGetIdGenerationInfo(out _, out var configuredSequenceName);
            var sequenceName = string.IsNullOrWhiteSpace(configuredSequenceName)
                ? idPropertyName
                : configuredSequenceName;
            var identityId = AutoIdGenerator.CreateIdentityValue(_engine, _name, sequenceName, idPropertyType);
            AotIdAccessor<T>.SetId(entity, identityId);
            if (AotIdAccessor<T>.HasValidId(entity))
            {
                EnsureIdIndex();
                return;
            }

            throw CreateIdGenerationFailedException(idPropertyName);
        }

        // 尝试使用新的ID生成系统
        if (AotIdAccessor<T>.GenerateIdIfNeeded(entity))
        {
            if (!AotIdAccessor<T>.HasValidId(entity))
            {
                throw CreateIdGenerationFailedException(idPropertyName);
            }

            // ID生成成功，确保创建了索引
            EnsureIdIndex();
            return;
        }

        // 回退到原有的ObjectId生成逻辑
        var id = GetEntityId(entity);
        if (id == null || id.IsNull)
        {
            var newId = ObjectId.NewObjectId();
            UpdateEntityId(entity, newId);
            EnsureIdIndex();
        }

        if (AotIdAccessor<T>.TryGetIdInfo(out idPropertyName, out _) &&
            !AotIdAccessor<T>.HasValidId(entity))
        {
            throw CreateIdGenerationFailedException(idPropertyName);
        }
    }

    private static InvalidOperationException CreateIdGenerationFailedException(string? idPropertyName)
    {
        return new InvalidOperationException(
            $"ID property '{idPropertyName ?? "Id"}' for entity type '{typeof(T).FullName}' cannot be generated automatically. Provide a non-default ID value or use a writable ID property.");
    }

    /// <summary>
    /// 确保ID索引存在
    /// </summary>
    private void EnsureIdIndex()
    {
        try
        {
            if (AotIdAccessor<T>.TryGetIdInfo(out _, out _))
            {
                var indexName = $"idx_{_name}_id";
                _engine.EnsureIndex(_name, "_id", indexName, unique: true);
            }
        }
        catch (Exception ex)
        {
            if (ex is ObjectDisposedException) throw;

            throw new InvalidOperationException(
                $"Failed to ensure unique _id index for collection '{_name}'.", ex);
        }
    }

    /// <summary>
    /// 获取实体的ID
    /// </summary>
    /// <param name="entity">实体</param>
    /// <returns>ID值</returns>
    private BsonValue GetEntityId(T entity)
    {
        try
        {
            return AotIdAccessor<T>.GetId(entity);
        }
        catch (InvalidOperationException)
        {
            return BsonNull.Value;
        }
    }

    /// <summary>
    /// 更新实体的ID
    /// </summary>
    /// <param name="entity">实体</param>
    /// <param name="id">新的ID值</param>
    private void UpdateEntityId(T entity, BsonValue id)
    {
        if (entity is BsonDocument)
        {
            return;
        }

        if (!AotIdAccessor<T>.TryGetIdInfo(out var idPropertyName, out _))
        {
            return;
        }

        try
        {
            AotIdAccessor<T>.SetId(entity, id);
        }
        catch (Exception ex) when (ex is InvalidOperationException or InvalidCastException or FormatException or OverflowException or ArgumentException)
        {
            throw new InvalidOperationException(
                $"Failed to set ID property '{idPropertyName}' for entity type '{typeof(T).FullName}'.",
                ex);
        }
        var updatedId = AotIdAccessor<T>.GetId(entity);
        if (!BsonValueComparer.ValueEquals(updatedId, id))
        {
            throw new InvalidOperationException(
                $"ID property '{idPropertyName}' for entity type '{typeof(T).FullName}' was not updated to the document ID.");
        }
    }
}
