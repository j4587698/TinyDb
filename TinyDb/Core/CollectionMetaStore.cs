using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using TinyDb.Bson;
using TinyDb.Serialization;
using TinyDb.Storage;

namespace TinyDb.Core;

internal sealed class CollectionMetaStore
{
    private readonly PageManager _pageManager;
    private readonly Func<uint> _getCollectionPageId;
    private readonly Action<uint> _setCollectionPageId;
    
    // Map: CollectionName -> Metadata (e.g. { "rootIndexPage": 123 })
    private readonly Dictionary<string, BsonDocument> _collectionsMetadata;
    private readonly object _lock = new();

    public CollectionMetaStore(PageManager pageManager, Func<uint> getCollectionPageId, Action<uint> setCollectionPageId)
    {
        _pageManager = pageManager;
        _getCollectionPageId = getCollectionPageId;
        _setCollectionPageId = setCollectionPageId;
        _collectionsMetadata = new Dictionary<string, BsonDocument>(StringComparer.Ordinal);
    }

    public List<string> GetCollectionNames()
    {
        lock (_lock) return _collectionsMetadata.Keys.ToList();
    }

    public void RegisterCollection(string name, bool forceFlush)
    {
        lock (_lock)
        {
            if (!_collectionsMetadata.ContainsKey(name))
            {
                _collectionsMetadata[name] = new BsonDocument();
                SaveCollections(forceFlush);
            }
        }
    }

    public void RemoveCollection(string name, bool forceFlush)
    {
        lock (_lock)
        {
            if (_collectionsMetadata.Remove(name)) SaveCollections(forceFlush);
        }
    }

    public bool IsKnown(string name)
    {
        lock (_lock) return _collectionsMetadata.ContainsKey(name);
    }
    
    public BsonDocument GetMetadata(string name, bool includeInternal = false)
    {
        lock (_lock)
        {
            if (!_collectionsMetadata.TryGetValue(name, out var doc)) return new BsonDocument();
            if (includeInternal) return doc;

            var filtered = new BsonDocument();
            foreach (var element in doc)
            {
                if (!element.Key.StartsWith("__", StringComparison.Ordinal))
                {
                    filtered = filtered.Set(element.Key, element.Value);
                }
            }

            return filtered;
        }
    }
    
    public void UpdateMetadata(string name, BsonDocument metadata, bool forceFlush)
    {
        lock (_lock)
        {
            if (!_collectionsMetadata.ContainsKey(name))
            {
                // Implicit register? Better strict.
                _collectionsMetadata[name] = metadata;
            }
            else
            {
                _collectionsMetadata[name] = metadata;
            }
            SaveCollections(forceFlush);
        }
    }

    public void LoadCollections()
    {
        var pageId = _getCollectionPageId();
        if (pageId == 0) return;
        try
        {
            var collectionPage = _pageManager.GetPage(pageId);
            const int dataOffset = 247;
            var collectionData = collectionPage.ReadBytes(dataOffset, collectionPage.DataSize - dataOffset);
            if (collectionData.Length < 4) return;

            var bsonLength = BitConverter.ToInt32(collectionData, 0);
            // 新建数据库时该页可能已分配但尚未写入（长度头为 0），按“空元数据”处理。
            if (bsonLength == 0) return;

            if (bsonLength <= 4 || bsonLength > collectionData.Length)
            {
                throw new InvalidDataException(
                    $"Invalid collection metadata BSON length {bsonLength} on page {pageId}.");
            }

            var documentBytes = collectionData.AsSpan(0, bsonLength).ToArray();
            var document = BsonSerializer.DeserializeDocument(documentBytes);
            lock (_lock)
            {
                _collectionsMetadata.Clear();
                foreach (var kvp in document)
                {
                    // Value can be String (Legacy) or Document (New)
                    if (kvp.Value is BsonString)
                    {
                        // Legacy format: Key=Name, Value=Name
                        // We use Key as name.
                        _collectionsMetadata[kvp.Key] = new BsonDocument();
                    }
                    else if (kvp.Value is BsonDocument meta)
                    {
                        _collectionsMetadata[kvp.Key] = meta;
                    }
                }
            }
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException(
                $"Failed to load collection metadata from page {pageId}.", ex);
        }
    }

    public void SaveCollections(bool forceFlush)
    {
        var pageId = _getCollectionPageId();
        var collectionInfo = new BsonDocument();
        lock (_lock)
        {
            if (_collectionsMetadata.Count == 0 && pageId == 0) return;
            foreach (var kvp in _collectionsMetadata)
            {
                // Store Metadata as value
                collectionInfo = collectionInfo.Set(kvp.Key, kvp.Value);
            }
        }
        if (pageId == 0)
        {
            var newPage = _pageManager.NewPage(PageType.Collection);
            pageId = newPage.PageID;
            _setCollectionPageId(pageId);
        }
        var collectionData = BsonSerializer.SerializeDocument(collectionInfo);
        
        
        var collectionPage = _pageManager.GetPage(pageId);
        const int dataOffset = 247;
        
        // 检查溢出
        if (collectionData.Length > collectionPage.PageSize - dataOffset)
        {
            throw new InvalidOperationException($"Collection metadata size ({collectionData.Length} bytes) exceeds page capacity. Multi-page metadata storage is not yet implemented.");
        }

        collectionPage.WriteData(dataOffset, collectionData);
        collectionPage.UpdateStats((ushort)(collectionPage.DataSize - dataOffset - collectionData.Length), 1);
        _pageManager.SavePage(collectionPage, forceFlush: forceFlush);
    }
}
