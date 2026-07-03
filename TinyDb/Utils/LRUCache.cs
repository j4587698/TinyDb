namespace TinyDb.Utils;

/// <summary>
/// 线程安全的 LRU (Least Recently Used) 缓存实现
/// </summary>
/// <typeparam name="TKey">键类型</typeparam>
/// <typeparam name="TValue">值类型</typeparam>
public sealed class LRUCache<TKey, TValue> where TKey : notnull
{
    private int _capacity;
    private readonly Dictionary<TKey, LinkedListNode<CacheItem>> _cacheMap;
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
    public int Count
    {
        get
        {
            lock (_lruList)
            {
                return _cacheMap.Count;
            }
        }
    }

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
        _cacheMap = new Dictionary<TKey, LinkedListNode<CacheItem>>();
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
        CacheValue? valueHolder = null;

        lock (_lruList)
        {
            if (_cacheMap.TryGetValue(key, out var node) && node.List == _lruList)
            {
                valueHolder = node.Value.Value;
                _lruList.Remove(node);
                _lruList.AddFirst(node);
                Interlocked.Increment(ref _hits);
            }
        }

        if (valueHolder != null)
        {
            value = GetValueOrRemoveFailed(key, valueHolder);
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
        ArgumentNullException.ThrowIfNull(valueFactory);
        CacheValue valueHolder;

        lock (_lruList)
        {
            if (_cacheMap.TryGetValue(key, out var node) && node.List == _lruList)
            {
                _lruList.Remove(node);
                _lruList.AddFirst(node);
                Interlocked.Increment(ref _hits);
                valueHolder = node.Value.Value;
            }
            else
            {
                Interlocked.Increment(ref _misses);
                valueHolder = CacheValue.FromFactory(() => valueFactory(key));
                AddNewNode(key, valueHolder);
            }
        }

        return GetValueOrRemoveFailed(key, valueHolder);
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
                existingNode.Value.Value = CreateCompletedValue(value);
                _lruList.Remove(existingNode);
                _lruList.AddFirst(existingNode);
            }
            else
            {
                // 添加新项
                AddNewNode(key, CreateCompletedValue(value));
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
            if (_cacheMap.TryGetValue(key, out var node))
            {
                _cacheMap.Remove(key);
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
        lock (_lruList)
        {
            if (_cacheMap.TryGetValue(key, out var node) && node.List == _lruList)
            {
                _lruList.Remove(node);
                _lruList.AddFirst(node);
            }
        }
    }

    public bool TryTouch(TKey key)
    {
        if (!Monitor.TryEnter(_lruList))
        {
            return false;
        }

        try
        {
            if (_cacheMap.TryGetValue(key, out var node) && node.List == _lruList)
            {
                _lruList.Remove(node);
                _lruList.AddFirst(node);
                return true;
            }

            return false;
        }
        finally
        {
            Monitor.Exit(_lruList);
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
        CacheValue? valueHolder;

        lock (_lruList)
        {
            if (_lruList.Last != null)
            {
                key = _lruList.Last.Value.Key;
                valueHolder = _lruList.Last.Value.Value;
            }
            else
            {
                key = default!;
                value = default;
                return false;
            }
        }

        value = GetValueOrRemoveFailed(key, valueHolder);
        return true;
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
        lock (_lruList)
        {
            return _cacheMap.ContainsKey(key);
        }
    }

    /// <summary>
    /// 获取所有的键
    /// </summary>
    /// <returns>键集合</returns>
    public IEnumerable<TKey> GetKeys()
    {
        lock (_lruList)
        {
            return _cacheMap.Keys.ToList();
        }
    }

    /// <summary>
    /// 获取所有的值
    /// </summary>
    /// <returns>值集合</returns>
    public IEnumerable<TValue> GetValues()
    {
        List<KeyValuePair<TKey, CacheValue>> values;

        lock (_lruList)
        {
            values = _lruList.Select(item => new KeyValuePair<TKey, CacheValue>(item.Key, item.Value)).ToList();
        }

        return values.Select(item => GetValueOrRemoveFailed(item.Key, item.Value)).ToList();
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
            _cacheMap.Remove(key);
        }
    }

    private void AddNewNode(TKey key, CacheValue value)
    {
        var cacheItem = new CacheItem(key, value);
        var newNode = new LinkedListNode<CacheItem>(cacheItem);

        _lruList.AddFirst(newNode);
        _cacheMap[key] = newNode;

        if (_cacheMap.Count > _capacity)
        {
            Evict();
        }
    }

    private static CacheValue CreateCompletedValue(TValue value)
    {
        return CacheValue.FromValue(value);
    }

    private TValue GetValueOrRemoveFailed(TKey key, CacheValue valueHolder)
    {
        try
        {
            return valueHolder.GetValue();
        }
        catch
        {
            RemoveIfSameValueHolder(key, valueHolder);
            throw;
        }
    }

    private void RemoveIfSameValueHolder(TKey key, CacheValue valueHolder)
    {
        lock (_lruList)
        {
            if (_cacheMap.TryGetValue(key, out var node) &&
                node.List == _lruList &&
                ReferenceEquals(node.Value.Value, valueHolder))
            {
                _cacheMap.Remove(key);
                _lruList.Remove(node);
            }
        }
    }

    private sealed class CacheValue
    {
        private readonly object _sync = new();
        private Func<TValue>? _factory;
        private TValue? _value;
        private System.Runtime.ExceptionServices.ExceptionDispatchInfo? _failure;
        private volatile bool _hasValue;

        private CacheValue(TValue value)
        {
            _value = value;
            _hasValue = true;
        }

        private CacheValue(Func<TValue> factory)
        {
            _factory = factory;
        }

        public static CacheValue FromValue(TValue value)
        {
            return new CacheValue(value);
        }

        public static CacheValue FromFactory(Func<TValue> factory)
        {
            return new CacheValue(factory);
        }

        public TValue GetValue()
        {
            if (_hasValue)
            {
                return _value!;
            }

            lock (_sync)
            {
                if (_hasValue)
                {
                    return _value!;
                }

                _failure?.Throw();

                try
                {
                    _value = _factory!();
                    _factory = null;
                    _hasValue = true;
                    return _value!;
                }
                catch (Exception ex)
                {
                    _failure = System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(ex);
                    throw;
                }
            }
        }
    }

    /// <summary>
    /// 缓存项
    /// </summary>
    private sealed class CacheItem
    {
        public TKey Key { get; }
        public CacheValue Value { get; set; }

        public CacheItem(TKey key, CacheValue value)
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
        return $"LRUCache[{Count}/{_capacity}] HitRatio={HitRatio * 100:F1}%";
    }
}
