using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace TinyDb.Core;

/// <summary>
/// 文档主键索引接口 (抽象层)
/// </summary>
internal interface IDocumentIndex
{
    int Count { get; }
    bool TryGet(string key, out DocumentLocation location);
    void Set(string key, DocumentLocation location);
    bool Remove(string key);
    void Clear();
    IEnumerable<KeyValuePair<string, DocumentLocation>> GetAll();
}

/// <summary>
/// 基于内存的文档索引 (现有实现)
/// </summary>
internal sealed class MemoryDocumentIndex : IDocumentIndex
{
    private readonly ConcurrentDictionary<string, DocumentLocation> _index;

    public MemoryDocumentIndex()
    {
        _index = new ConcurrentDictionary<string, DocumentLocation>(StringComparer.Ordinal);
    }

    public int Count => _index.Count;

    public bool TryGet(string key, out DocumentLocation location)
    {
        return _index.TryGetValue(key, out location);
    }

    public void Set(string key, DocumentLocation location)
    {
        _index[key] = location;
    }

    public bool Remove(string key)
    {
        return _index.TryRemove(key, out _);
    }

    public void Clear()
    {
        _index.Clear();
    }

    public IEnumerable<KeyValuePair<string, DocumentLocation>> GetAll()
    {
        return _index;
    }
}
