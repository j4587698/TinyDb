using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;

namespace TinyDb.Utils;

/// <summary>
/// 线程安全的 LRU (Least Recently Used) 缓存实现
/// </summary>
/// <typeparam name="TKey">键类型</typeparam>
/// <typeparam name="TValue">值类型</typeparam>
public sealed class LRUCache<TKey, TValue> where TKey : notnull
{
    private int _capacity;
    private readonly ConcurrentDictionary<TKey, LinkedListNode<CacheItem>> _cacheMap;
    private readonly LinkedList<CacheItem> _lruList;
    private long _hits;
    private long _misses;

    /// <summary>
    /// 缓存容量
    /// </summary>
    public int Capacity => _capacity;

    /// <summary>
    /// 当前缓存项数量
    /// </summary>
    public int Count => _cacheMap.Count;

    /// <summary>
    /// 缓存命中次数
    /// </summary>
    public long Hits => _hits;

    /// <summary>
    /// 缓存未命中次数
    /// </summary>
    public long Misses => _misses;

    /// <summary>
    /// 缓存命中率
    /// </summary>
    public double HitRatio
    {
        get
        {
            var total = _hits + _misses;
            return total == 0 ? 0.0 : (double)_hits / total;
        }
    }

    /// <summary>
    /// 初始化 LRU 缓存
    /// </summary>
    /// <param name="capacity">缓存容量</param>
    public LRUCache(int capacity)
    {
        if (capacity <= 0) throw new ArgumentOutOfRangeException(nameof(capacity));
        _capacity = capacity;
        _cacheMap = new ConcurrentDictionary<TKey, LinkedListNode<CacheItem>>();
        _lruList = new LinkedList<CacheItem>();
    }

    /// <summary>
    /// 尝试获取值
    /// </summary>
    /// <param name="key">键</param>
    /// <param name="value">值</param>
    /// <returns>是否获取成功</returns>
    public bool TryGetValue(TKey key, out TValue? value)
    {
        value = default;

        if (_cacheMap.TryGetValue(key, out var node))
        {
            value = node.Value.Value;
            TouchInternal(key);
            Interlocked.Increment(ref _hits);
            return true;
        }

        Interlocked.Increment(ref _misses);
        return false;
    }

    /// <summary>
    /// 获取或添加值
    /// </summary>
    /// <param name="key">键</param>
    /// <param name="valueFactory">值工厂函数</param>
    /// <returns>值</returns>
    public TValue GetOrAdd(TKey key, Func<TKey, TValue> valueFactory)
    {
        if (TryGetValue(key, out var value))
        {
            return value!;
        }

        var newValue = valueFactory(key);
        Put(key, newValue);
        return newValue;
    }

    /// <summary>
    /// 添加或更新键值对
    /// </summary>
    /// <param name="key">键</param>
    /// <param name="value">值</param>
    public void Put(TKey key, TValue value)
    {
        lock (_lruList)
        {
            if (_cacheMap.TryGetValue(key, out var existingNode))
            {
                // 更新现有项
                existingNode.Value.Value = value;
                _lruList.Remove(existingNode);
                _lruList.AddFirst(existingNode);
            }
            else
            {
                // 添加新项
                var cacheItem = new CacheItem(key, value);
                var newNode = new LinkedListNode<CacheItem>(cacheItem);

                _lruList.AddFirst(newNode);
                _cacheMap[key] = newNode;

                // 检查是否超出容量
                if (_cacheMap.Count > _capacity)
                {
                    Evict();
                }
            }
        }
    }

    /// <summary>
    /// 尝试移除键值对
    /// </summary>
    /// <param name="key">键</param>
    /// <returns>是否移除成功</returns>
    public bool TryRemove(TKey key)
    {
        lock (_lruList)
        {
            if (_cacheMap.TryRemove(key, out var node))
            {
                _lruList.Remove(node);
                return true;
            }
            return false;
        }
    }

    /// <summary>
    /// 移除指定的键
    /// </summary>
    /// <param name="key">键</param>
    public void Remove(TKey key)
    {
        TryRemove(key);
    }

    /// <summary>
    /// 标记键为最近使用
    /// </summary>
    /// <param name="key">键</param>
    public void Touch(TKey key)
    {
        if (_cacheMap.ContainsKey(key))
        {
            TouchInternal(key);
        }
    }

    /// <summary>
    /// 尝试获取最少使用的项
    /// </summary>
    /// <param name="key">键</param>
    /// <param name="value">值</param>
    /// <returns>是否获取成功</returns>
    public bool TryGetLeastRecentlyUsed(out TKey key, out TValue? value)
    {
        lock (_lruList)
        {
            if (_lruList.Last != null)
            {
                key = _lruList.Last.Value.Key;
                value = _lruList.Last.Value.Value;
                return true;
            }
        }

        key = default!;
        value = default;
        return false;
    }

    /// <summary>
    /// 获取最少使用的N个键
    /// </summary>
    /// <param name="count">数量</param>
    /// <returns>键列表</returns>
    public IList<TKey> GetLeastRecentlyUsed(int count)
    {
        var result = new List<TKey>();

        lock (_lruList)
        {
            var current = _lruList.Last;
            while (current != null && result.Count < count)
            {
                result.Add(current.Value.Key);
                current = current.Previous;
            }
        }

        return result;
    }

    /// <summary>
    /// 清空缓存
    /// </summary>
    public void Clear()
    {
        lock (_lruList)
        {
            _cacheMap.Clear();
            _lruList.Clear();
        }
    }

    /// <summary>
    /// 检查是否包含指定键
    /// </summary>
    /// <param name="key">键</param>
    /// <returns>是否包含</returns>
    public bool ContainsKey(TKey key)
    {
        return _cacheMap.ContainsKey(key);
    }

    /// <summary>
    /// 获取所有的键
    /// </summary>
    /// <returns>键集合</returns>
    public IEnumerable<TKey> GetKeys()
    {
        return _cacheMap.Keys.ToList();
    }

    /// <summary>
    /// 获取所有的值
    /// </summary>
    /// <returns>值集合</returns>
    public IEnumerable<TValue> GetValues()
    {
        lock (_lruList)
        {
            return _lruList.Select(item => item.Value).ToList();
        }
    }

    /// <summary>
    /// 缩减缓存容量
    /// </summary>
    /// <param name="newCapacity">新容量</param>
    public void Trim(int newCapacity)
    {
        if (newCapacity <= 0) throw new ArgumentOutOfRangeException(nameof(newCapacity));

        lock (_lruList)
        {
            while (_cacheMap.Count > newCapacity)
            {
                Evict();
            }
            _capacity = newCapacity;
        }
    }

    /// <summary>
    /// 驱逐最少使用的项
    /// </summary>
    private void Evict()
    {
        if (_lruList.Last != null)
        {
            var lastNode = _lruList.Last;
            var key = lastNode.Value.Key;

            _lruList.RemoveLast();
            _cacheMap.TryRemove(key, out _);
        }
    }

    /// <summary>
    /// 内部方法：标记为最近使用
    /// </summary>
    /// <param name="key">键</param>
    private void TouchInternal(TKey key)
    {
        lock (_lruList)
        {
            if (_cacheMap.TryGetValue(key, out var node))
            {
                _lruList.Remove(node);
                _lruList.AddFirst(node);
            }
        }
    }

    /// <summary>
    /// 缓存项
    /// </summary>
    private sealed class CacheItem
    {
        public TKey Key { get; }
        public TValue Value { get; set; }

        public CacheItem(TKey key, TValue value)
        {
            Key = key;
            Value = value;
        }
    }

    /// <summary>
    /// 重写 ToString 方法
    /// </summary>
    public override string ToString()
    {
        return $"LRUCache[{Count}/{_capacity}] HitRatio={HitRatio:P1}";
    }
}