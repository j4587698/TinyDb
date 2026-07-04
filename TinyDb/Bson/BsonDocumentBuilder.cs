using System.Collections.Generic;

namespace TinyDb.Bson;

/// <summary>
/// BSON 文档构建器，用于高效创建 BsonDocument 实例
/// </summary>
public sealed class BsonDocumentBuilder
{
    private readonly List<KeyValuePair<string, BsonValue>> _items;
    private readonly Dictionary<string, int> _index;

    public BsonDocumentBuilder()
        : this(0)
    {
    }

    public BsonDocumentBuilder(int capacity)
    {
        _items = capacity > 0
            ? new List<KeyValuePair<string, BsonValue>>(capacity)
            : new List<KeyValuePair<string, BsonValue>>();
        _index = capacity > 0
            ? new Dictionary<string, int>(capacity, StringComparer.Ordinal)
            : new Dictionary<string, int>(StringComparer.Ordinal);
    }

    public BsonDocumentBuilder(BsonDocument existing)
    {
        ArgumentNullException.ThrowIfNull(existing);
        _items = new List<KeyValuePair<string, BsonValue>>(existing.Count);
        _index = new Dictionary<string, int>(existing.Count, StringComparer.Ordinal);
        foreach (var item in existing.Entries)
        {
            _index[item.Key] = _items.Count;
            _items.Add(item);
        }
    }

    public BsonDocumentBuilder Set(string key, BsonValue value)
    {
        ArgumentNullException.ThrowIfNull(key);
        if (_index.TryGetValue(key, out var index))
        {
            _items[index] = new KeyValuePair<string, BsonValue>(key, value);
        }
        else
        {
            _index.Add(key, _items.Count);
            _items.Add(new KeyValuePair<string, BsonValue>(key, value));
        }

        return this;
    }

    public BsonDocumentBuilder Remove(string key)
    {
        ArgumentNullException.ThrowIfNull(key);
        if (!_index.TryGetValue(key, out var index))
        {
            return this;
        }

        _items.RemoveAt(index);
        _index.Remove(key);
        for (var i = index; i < _items.Count; i++)
        {
            _index[_items[i].Key] = i;
        }

        return this;
    }

    public bool ContainsKey(string key)
    {
        return _index.ContainsKey(key);
    }

    public BsonDocument Build()
    {
        return new BsonDocument((IReadOnlyList<KeyValuePair<string, BsonValue>>)_items);
    }
}
