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
    /// 检索或为指定类型创建集合。
    /// 集合名称按以下优先级确定：Entity特性的Name属性 > 类名
    /// </summary>
    /// <typeparam name="T">集合中文档的类型。</typeparam>
    /// <returns>集合实例。</returns>
    public ITinyCollection<T> GetCollection<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>() where T : class
    {
        var n = GetCollectionNameFromEntityAttribute<T>() ?? typeof(T).Name;
        return GetOrCreateCollection<T>(n);
    }


    /// <summary>
    /// 检索或使用特定名称创建集合。
    /// 集合名称按以下优先级确定：name参数 > Entity特性的Name属性 > 类名
    /// </summary>
    /// <typeparam name="T">集合中文档的类型。</typeparam>
    /// <param name="name">集合的名称（可选）。如果为null或空，则使用Entity特性的Name属性或类名。</param>
    /// <returns>集合实例。</returns>
    public ITinyCollection<T> GetCollection<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>(string? name) where T : class
    {
        var n = !string.IsNullOrEmpty(name) ? name : (GetCollectionNameFromEntityAttribute<T>() ?? typeof(T).Name);
        return GetOrCreateCollection<T>(n);
    }


    public IEnumerable<T> QuerySql<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>(
        string sql,
        IReadOnlyDictionary<string, object?>? parameters = null)
        where T : class
    {
        ThrowIfDisposed();
        if (sql == null) throw new ArgumentNullException(nameof(sql));

        var query = SqlQueryParser.ParseSelect(sql, parameters);
        var collection = GetCollection<T>(query.CollectionName);
        return collection is DocumentCollection<T> documentCollection
            ? documentCollection.Execute<T>(query).Rows
            : collection.FindSql(sql, parameters);
    }


    public IEnumerable<BsonDocument> QuerySqlDocuments<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] TSource>(
        string sql,
        IReadOnlyDictionary<string, object?>? parameters = null)
        where TSource : class
    {
        ThrowIfDisposed();
        if (sql == null) throw new ArgumentNullException(nameof(sql));

        var query = SqlQueryParser.ParseSelect(sql, parameters);
        var collection = GetCollection<TSource>(query.CollectionName);
        return collection is DocumentCollection<TSource> documentCollection
            ? documentCollection.Execute(query).Documents
            : collection.FindSqlDocuments(sql, parameters);
    }


    public IEnumerable<TProjection> QuerySql<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] TSource,
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] TProjection>(
        string sql,
        IReadOnlyDictionary<string, object?>? parameters = null)
        where TSource : class
        where TProjection : class
    {
        ThrowIfDisposed();
        if (sql == null) throw new ArgumentNullException(nameof(sql));

        var query = SqlQueryParser.ParseSelect(sql, parameters);
        var collection = GetCollection<TSource>(query.CollectionName);
        return collection is DocumentCollection<TSource> documentCollection
            ? documentCollection.Execute<TProjection>(query).Rows
            : collection.FindSql<TProjection>(sql, parameters);
    }


    public SqlExecutionResult Execute<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] TSource>(
        string sql,
        IReadOnlyDictionary<string, object?>? parameters = null)
        where TSource : class
    {
        ThrowIfDisposed();
        if (sql == null) throw new ArgumentNullException(nameof(sql));

        var statement = SqlQueryParser.Parse(sql, parameters);
        var collection = GetCollection<TSource>(statement.CollectionName);
        return collection is DocumentCollection<TSource> documentCollection
            ? documentCollection.Execute(statement)
            : collection.Execute(sql, parameters);
    }


    public SqlExecutionResult<TProjection> Execute<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] TSource,
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] TProjection>(
        string sql,
        IReadOnlyDictionary<string, object?>? parameters = null)
        where TSource : class
        where TProjection : class
    {
        ThrowIfDisposed();
        if (sql == null) throw new ArgumentNullException(nameof(sql));

        var statement = SqlQueryParser.Parse(sql, parameters);
        var collection = GetCollection<TSource>(statement.CollectionName);
        return collection is DocumentCollection<TSource> documentCollection
            ? documentCollection.Execute<TProjection>(statement)
            : collection.Execute<TProjection>(sql, parameters);
    }


    private ITinyCollection<T> GetOrCreateCollection<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>(string name) where T : class
    {
        if (_collections.TryGetValue(name, out var existing))
        {
            return (ITinyCollection<T>)existing;
        }

        lock (_collectionRegistryLock)
        {
            if (_collections.TryGetValue(name, out existing))
            {
                return (ITinyCollection<T>)existing;
            }

            var isKnownCollection = _collectionMetaStore.IsKnown(name);
            _metadataManager.EnsureSchema(name, typeof(T));
            RegisterCollection(name);
            if (!isKnownCollection)
            {
                var state = CreateEmptyCollectionState();
                state.MarkCacheInitialized();
                _collectionStates.TryAdd(name, state);
            }

            var collection = new DocumentCollection<T>(this, name);
            _collections[name] = collection;
            return collection;
        }
    }

    internal void TrackCollection(IDocumentCollection collection)
    {
        ArgumentNullException.ThrowIfNull(collection);
        _collections[collection.Name] = collection;
    }




    /// <summary>
    /// 获取数据库中所有集合的名称。
    /// </summary>
    /// <returns>集合名称列表。</returns>
    public IEnumerable<string> GetCollectionNames()
        => GetCollectionNames(includeSystemCollections: false);


    public IEnumerable<string> GetCollectionNames(bool includeSystemCollections)
    {
        ThrowIfDisposed();
        EnsureInitialized();
        var all = new HashSet<string>(_collectionMetaStore.GetCollectionNames());
        foreach (var n in _collections.Keys) all.Add(n);

        if (!includeSystemCollections)
        {
            all.RemoveWhere(n => n.StartsWith("__", StringComparison.Ordinal));
        }

        return all.ToList();
    }


    /// <summary>
    /// 检查集合是否存在。
    /// </summary>
    /// <param name="n">集合名称。</param>
    /// <returns>如果集合存在则为 true；否则为 false。</returns>
    public bool CollectionExists(string n) => _collections.ContainsKey(n) || _collectionMetaStore.IsKnown(n);


    /// <summary>
    /// 删除（丢弃）集合及其所有数据。
    /// </summary>
    /// <param name="n">集合名称。</param>
    /// <returns>如果集合被删除则为 true；如果集合不存在则为 false。</returns>
    public bool DropCollection(string n)
    {
        ThrowIfDisposed();
        EnsureInitialized();

        using var collectionLock = EnterCollectionCommitGates(new[] { n });
        bool r = _collections.TryRemove(n, out var col);
        if (r)
        {
            col.DeleteAll();
            col.Dispose();
        }
        if (_collectionMetaStore.IsKnown(n))
        {
            _collectionMetaStore.RemoveCollection(n, true);
            if (_indexManagers.TryRemove(n, out var indexManager)) indexManager.Dispose();
            ClearCollectionRuntimeCaches(n);
            return true;
        }

        if (_indexManagers.TryRemove(n, out var removedIndexManager)) removedIndexManager.Dispose();
        ClearCollectionRuntimeCaches(n);
        return r;
    }


    private void ClearCollectionRuntimeCaches(string collectionName)
    {
        _collectionStates.TryRemove(collectionName, out _);

        var identityPrefix = collectionName + "\0";
        foreach (var key in _identitySequences.Keys)
        {
            if (key.StartsWith(identityPrefix, StringComparison.Ordinal))
            {
                _identitySequences.TryRemove(key, out _);
            }
        }

        var indexLockPrefix = collectionName + "\u001F";
        foreach (var key in _indexCreationLocks.Keys)
        {
            if (key.StartsWith(indexLockPrefix, StringComparison.Ordinal))
            {
                _indexCreationLocks.TryRemove(key, out _);
            }
        }

        _transactionManager.ClearForeignKeyCache();
        _metadataManager.InvalidateMetadata(collectionName);
    }


    private static string? GetCollectionNameFromEntityAttribute<T>() where T : class => typeof(T).GetCustomAttribute<EntityAttribute>()?.Name;

}
