using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using TinyDb.Bson;

namespace TinyDb.Core;

/// <summary>
/// 文档主键索引接口 (抽象层)
/// </summary>
internal interface IDocumentIndex
{
    int Count { get; }
    bool TryGet(BsonValue key, out DocumentLocation location);
    void Set(BsonValue key, DocumentLocation location);
    bool Remove(BsonValue key);
    void Clear();
    IEnumerable<KeyValuePair<BsonValue, DocumentLocation>> GetAll();
}

/// <summary>
/// 基于内存的文档索引 (现有实现)
/// </summary>
internal sealed class MemoryDocumentIndex : IDocumentIndex
{
    private readonly ConcurrentDictionary<BsonValue, DocumentLocation> _index;

    public MemoryDocumentIndex()
    {
        _index = new ConcurrentDictionary<BsonValue, DocumentLocation>();
    }

    public int Count => _index.Count;

    public bool TryGet(BsonValue key, out DocumentLocation location)
    {
        return _index.TryGetValue(key, out location);
    }

    public void Set(BsonValue key, DocumentLocation location)
    {
        _index[key] = location;
    }

    public bool Remove(BsonValue key)
    {
        return _index.TryRemove(key, out _);
    }

    public void Clear()
    {
        _index.Clear();
    }

    public IEnumerable<KeyValuePair<BsonValue, DocumentLocation>> GetAll()
    {
        return _index;
    }
}
