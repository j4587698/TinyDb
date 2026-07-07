using System;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using TinyDb.Bson;
using TinyDb.Security;
using TinyDb.Collections;
using TinyDb.Index;
using TinyDb.Query;
using TinyDb.Serialization;
using TinyDb.Storage;
using TinyDb.Attributes;
using TinyDb.Utils;

namespace TinyDb.Core;

public sealed partial class TinyDbEngine
{
    /// <summary>
    /// 确保在特定字段上存在索引。
    /// </summary>
    /// <param name="collectionName">集合名称。</param>
    /// <param name="fieldName">要索引的字段。</param>
    /// <param name="indexName">索引的名称。</param>
    /// <param name="unique">索引是否应该是唯一的。</param>
    /// <returns>如果成功则为 true。</returns>
    public bool EnsureIndex(string collectionName, string fieldName, string indexName, bool unique = false, bool sparse = false)
    {
        return EnsureIndex(collectionName, new[] { fieldName }, indexName, unique, sparse);
    }

    internal bool EnsureIndex(string collectionName, string[] fields, string indexName, bool unique = false, bool sparse = false)
    {
        var lockKey = collectionName + "\u001F" + indexName;
        var indexLock = _indexCreationLocks.GetOrAdd(lockKey, _ => new object());
        lock (indexLock)
        {
            var indexManager = GetIndexManager(collectionName);
            var created = indexManager.CreateIndexForBackfill(indexName, fields, unique, sparse);
            if (!created) return false;

            try
            {
                BackfillIndex(collectionName, indexManager, indexName);
                indexManager.PersistCurrentDefinitions(_options.WriteConcern == WriteConcern.Synced);
                return true;
            }
            catch
            {
                indexManager.DropIndex(indexName);
                throw;
            }
        }
    }

    private void BackfillIndex(string collectionName, IndexManager indexManager, string indexName)
    {
        var state = GetCollectionState(collectionName);
        var existingDocuments = ReadAllDocumentsSnapshotFromPageSnapshots(collectionName, state);
        indexManager.RebuildIndex(indexName, existingDocuments);
    }

    public IndexManager GetIndexManager(string c)
    {
        if (_indexManagers.TryGetValue(c, out var existing))
        {
            return existing;
        }

        lock (_collectionRegistryLock)
        {
            if (_indexManagers.TryGetValue(c, out existing))
            {
                return existing;
            }

            var created = CreateIndexManager(c);
            if (_indexManagers.TryAdd(c, created))
            {
                return created;
            }

            created.Dispose();
            return _indexManagers[c];
        }
    }

    private IndexManager CreateIndexManager(string collectionName)
    {
        var persistedDefinitions = LoadPersistedIndexDefinitions(collectionName);
        return new IndexManager(
            collectionName,
            _pageManager,
            persistedDefinitions,
            (definitions, forceFlush) => SavePersistedIndexDefinitions(
                collectionName,
                definitions,
                forceFlush || _options.WriteConcern == WriteConcern.Synced));
    }

    private IReadOnlyList<PersistedIndexDefinition> LoadPersistedIndexDefinitions(string collectionName)
    {
        var metadata = _collectionMetaStore.GetMetadata(collectionName, includeInternal: true);
        if (!metadata.TryGetValue(IndexMetadataKey, out var indexDefinitionsValue) || indexDefinitionsValue is not BsonArray indexDefinitions)
        {
            return Array.Empty<PersistedIndexDefinition>();
        }

        var definitions = new List<PersistedIndexDefinition>(indexDefinitions.Count);
        foreach (var indexDefinitionValue in indexDefinitions)
        {
            if (!TryGetDocument(indexDefinitionValue, out var indexDefinition)) continue;
            if (!TryGetString(indexDefinition, IndexNameKey, out var name)) continue;
            if (!TryGetStringArray(indexDefinition, IndexFieldsKey, out var fields) || fields.Length == 0) continue;
            if (!TryGetUInt32(indexDefinition, IndexRootPageKey, out var rootPageId) || rootPageId == 0) continue;

            var unique = TryGetBoolean(indexDefinition, IndexUniqueKey, out var uniqueValue) && uniqueValue;
            var sparse = TryGetBoolean(indexDefinition, IndexSparseKey, out var sparseValue) && sparseValue;
            var maxKeys = TryGetInt32(indexDefinition, IndexMaxKeysKey, out var maxKeysValue) && maxKeysValue > 0
                ? maxKeysValue
                : 200;

            definitions.Add(new PersistedIndexDefinition(name, fields, unique, sparse, rootPageId, maxKeys));
        }

        return definitions;
    }

    private void SavePersistedIndexDefinitions(string collectionName, IReadOnlyList<PersistedIndexDefinition> definitions, bool forceFlush)
    {
        var metadata = _collectionMetaStore.GetMetadata(collectionName, includeInternal: true);
        if (definitions.Count == 0)
        {
            metadata = metadata.RemoveKey(IndexMetadataKey);
        }
        else
        {
            var indexDefinitions = new BsonArray();
            foreach (var definition in definitions)
            {
                var fields = new BsonArray();
                foreach (var field in definition.Fields)
                {
                    fields = fields.AddValue(new BsonString(field));
                }

                var document = new BsonDocument()
                    .Set(IndexNameKey, new BsonString(definition.Name))
                    .Set(IndexFieldsKey, fields)
                    .Set(IndexUniqueKey, BsonBoolean.FromValue(definition.IsUnique))
                    .Set(IndexSparseKey, BsonBoolean.FromValue(definition.IsSparse))
                    .Set(IndexRootPageKey, new BsonInt64(definition.RootPageId))
                    .Set(IndexMaxKeysKey, BsonInt32.FromValue(definition.MaxKeys));

                indexDefinitions = indexDefinitions.AddValue(document);
            }

            metadata = metadata.Set(IndexMetadataKey, indexDefinitions);
        }

        _collectionMetaStore.UpdateMetadata(collectionName, metadata, forceFlush);
    }

    private static bool TryGetDocument(BsonValue value, [NotNullWhen(true)] out BsonDocument? document)
    {
        if (value is BsonDocument bsonDocument)
        {
            document = bsonDocument;
            return true;
        }

        if (value.IsDocument && value.RawValue is BsonDocument rawDocument)
        {
            document = rawDocument;
            return true;
        }

        document = null;
        return false;
    }

    private static bool TryGetString(BsonDocument document, string key, [NotNullWhen(true)] out string? value)
    {
        if (document.TryGetValue(key, out var bsonValue) && bsonValue is BsonString bsonString)
        {
            value = bsonString.Value;
            return !string.IsNullOrEmpty(value);
        }

        value = null;
        return false;
    }

    private static bool TryGetStringArray(BsonDocument document, string key, out string[] values)
    {
        if (!document.TryGetValue(key, out var bsonValue) || bsonValue is not BsonArray array)
        {
            values = Array.Empty<string>();
            return false;
        }

        var result = new List<string>(array.Count);
        foreach (var item in array)
        {
            if (item is BsonString bsonString && !string.IsNullOrEmpty(bsonString.Value))
            {
                result.Add(bsonString.Value);
            }
        }

        values = result.ToArray();
        return values.Length > 0;
    }

    private static bool TryGetBoolean(BsonDocument document, string key, out bool value)
    {
        if (document.TryGetValue(key, out var bsonValue) && bsonValue is BsonBoolean bsonBoolean)
        {
            value = bsonBoolean.Value;
            return true;
        }

        value = false;
        return false;
    }

    private static bool TryGetUInt32(BsonDocument document, string key, out uint value)
    {
        if (TryGetInt64(document, key, out var longValue) && longValue >= 0 && longValue <= uint.MaxValue)
        {
            value = (uint)longValue;
            return true;
        }

        value = 0;
        return false;
    }

    private static bool TryGetInt32(BsonDocument document, string key, out int value)
    {
        if (TryGetInt64(document, key, out var longValue) && longValue >= int.MinValue && longValue <= int.MaxValue)
        {
            value = (int)longValue;
            return true;
        }

        value = 0;
        return false;
    }

    private static bool TryGetInt64(BsonDocument document, string key, out long value)
    {
        if (document.TryGetValue(key, out var bsonValue))
        {
            switch (bsonValue)
            {
                case BsonInt32 bsonInt32:
                    value = bsonInt32.Value;
                    return true;
                case BsonInt64 bsonInt64:
                    value = bsonInt64.Value;
                    return true;
                case BsonString bsonString when long.TryParse(bsonString.Value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed):
                    value = parsed;
                    return true;
            }
        }

        value = 0;
        return false;
    }

    private void DisposeIndexManagers() { foreach (var m in _indexManagers.Values) m.Dispose(); _indexManagers.Clear(); }
}
