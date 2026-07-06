using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;
using System.Text;

namespace TinyDb.Bson;

/// <summary>
/// BSON 文档，表示键值对的集合
/// </summary>
public sealed class BsonDocument : BsonValue, IDictionary<string, BsonValue>, IReadOnlyDictionary<string, BsonValue>
{
    internal readonly ImmutableDictionary<string, BsonValue> _elements;
    private readonly ImmutableArray<string> _order;
    private readonly ImmutableArray<KeyValuePair<string, BsonValue>> _entries;
    private string[]? _sortedKeys;

    public override BsonType BsonType => BsonType.Document;
    public override object? RawValue => _elements;
    public override bool IsDocument => true;

    /// <summary>
    /// 获取元素数量
    /// </summary>
    public int Count => _elements.Count;

    /// <summary>
    /// 获取所有键
    /// </summary>
    public IEnumerable<string> Keys => _order;

    /// <summary>
    /// 获取所有值
    /// </summary>
    public IEnumerable<BsonValue> Values => new ValueEnumerable(_entries);

    internal EntryEnumerable Entries => new(_entries);

    /// <summary>
    /// IDictionary.Keys 显式实现
    /// </summary>
    ICollection<string> IDictionary<string, BsonValue>.Keys => _order.ToList();

    /// <summary>
    /// IDictionary.Values 显式实现
    /// </summary>
    ICollection<BsonValue> IDictionary<string, BsonValue>.Values => Values.ToList();

    /// <summary>
    /// 获取是否为只读
    /// </summary>
    bool ICollection<KeyValuePair<string, BsonValue>>.IsReadOnly => true;

    /// <summary>
    /// 获取指定键的值
    /// </summary>
    public BsonValue this[string key]
    {
        get => _elements.TryGetValue(key, out var value) ? value : BsonNull.Value;
        set => throw new NotSupportedException("BsonDocument is immutable. Use Set method to create a new document.");
    }

    /// <summary>
    /// 获取指定键的值，如果不存在则返回默认值
    /// </summary>
    public BsonValue Get(string key, BsonValue? defaultValue = null)
    {
        return _elements.TryGetValue(key, out var value) ? value : defaultValue ?? BsonNull.Value;
    }

    /// <summary>
    /// 尝试获取指定键的值
    /// </summary>
    public bool TryGetValue(string key, [NotNullWhen(true)] out BsonValue? value)
    {
        return _elements.TryGetValue(key, out value);
    }

    /// <summary>
    /// 检查是否包含指定键
    /// </summary>
    public bool ContainsKey(string key) => _elements.ContainsKey(key);

    internal bool TryGetFirstKey([NotNullWhen(true)] out string? key)
    {
        if (_order.Length == 0)
        {
            key = null;
            return false;
        }

        key = _order[0];
        return true;
    }

    /// <summary>
    /// 检查是否包含指定值
    /// </summary>
    public bool Contains(BsonValue value) => _elements.ContainsValue(value);

    /// <summary>
    /// 添加键值对
    /// </summary>
    public void Add(string key, BsonValue value)
    {
        throw new NotSupportedException("BsonDocument is immutable. Use Set method to create a new document.");
    }

    /// <summary>
    /// 设置键值对，返回新的文档
    /// </summary>
    public BsonDocument Set(string key, BsonValue value)
    {
        ArgumentNullException.ThrowIfNull(key);
        var order = _elements.ContainsKey(key) ? _order : _order.Add(key);
        return new BsonDocument(_elements.SetItem(key, value), order, normalizeOrder: false);
    }

    /// <summary>
    /// 移除指定键
    /// </summary>
    public bool Remove(string key)
    {
        throw new NotSupportedException("BsonDocument is immutable. Use Remove method to create a new document.");
    }

    /// <summary>
    /// 移除指定键，返回新的文档
    /// </summary>
    public BsonDocument RemoveKey(string key)
    {
        ArgumentNullException.ThrowIfNull(key);
        if (!_elements.ContainsKey(key))
        {
            return this;
        }

        return new BsonDocument(_elements.Remove(key), _order.Remove(key), normalizeOrder: false);
    }

    /// <summary>
    /// 清空所有元素
    /// </summary>
    public void Clear()
    {
        throw new NotSupportedException("BsonDocument is immutable.");
    }

    /// <summary>
    /// 添加键值对
    /// </summary>
    public void Add(KeyValuePair<string, BsonValue> item)
    {
        throw new NotSupportedException("BsonDocument is immutable. Use Set method to create a new document.");
    }

    /// <summary>
    /// 检查是否包含指定键值对
    /// </summary>
    public bool Contains(KeyValuePair<string, BsonValue> item)
    {
        return _elements.TryGetValue(item.Key, out var value) && value.Equals(item.Value);
    }

    /// <summary>
    /// 复制到数组
    /// </summary>
    public void CopyTo(KeyValuePair<string, BsonValue>[] array, int arrayIndex)
    {
        ArgumentNullException.ThrowIfNull(array);
        foreach (var item in Entries)
        {
            array[arrayIndex++] = item;
        }
    }

    /// <summary>
    /// 移除键值对
    /// </summary>
    public bool Remove(KeyValuePair<string, BsonValue> item)
    {
        throw new NotSupportedException("BsonDocument is immutable.");
    }

    /// <summary>
    /// 获取枚举器
    /// </summary>
    public IEnumerator<KeyValuePair<string, BsonValue>> GetEnumerator() => Entries.GetEnumerator();

    IEnumerator<KeyValuePair<string, BsonValue>> IEnumerable<KeyValuePair<string, BsonValue>>.GetEnumerator() => GetEnumerator();

    System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();

    internal readonly struct EntryEnumerable : IEnumerable<KeyValuePair<string, BsonValue>>
    {
        private readonly ImmutableArray<KeyValuePair<string, BsonValue>> _entries;

        internal EntryEnumerable(ImmutableArray<KeyValuePair<string, BsonValue>> entries)
        {
            _entries = entries;
        }

        public EntryEnumerator GetEnumerator() => new(_entries);

        IEnumerator<KeyValuePair<string, BsonValue>> IEnumerable<KeyValuePair<string, BsonValue>>.GetEnumerator() => GetEnumerator();

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
    }

    internal struct EntryEnumerator : IEnumerator<KeyValuePair<string, BsonValue>>
    {
        private readonly ImmutableArray<KeyValuePair<string, BsonValue>> _entries;
        private int _index;

        internal EntryEnumerator(ImmutableArray<KeyValuePair<string, BsonValue>> entries)
        {
            _entries = entries;
            _index = -1;
        }

        public KeyValuePair<string, BsonValue> Current => _entries[_index];

        object System.Collections.IEnumerator.Current => Current;

        public bool MoveNext()
        {
            var next = _index + 1;
            if (next >= _entries.Length)
            {
                return false;
            }

            _index = next;
            return true;
        }

        public void Reset()
        {
            _index = -1;
        }

        public void Dispose()
        {
        }
    }

    private readonly struct ValueEnumerable : IEnumerable<BsonValue>
    {
        private readonly ImmutableArray<KeyValuePair<string, BsonValue>> _entries;

        internal ValueEnumerable(ImmutableArray<KeyValuePair<string, BsonValue>> entries)
        {
            _entries = entries;
        }

        public ValueEnumerator GetEnumerator() => new(_entries);

        IEnumerator<BsonValue> IEnumerable<BsonValue>.GetEnumerator() => GetEnumerator();

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
    }

    private struct ValueEnumerator : IEnumerator<BsonValue>
    {
        private readonly ImmutableArray<KeyValuePair<string, BsonValue>> _entries;
        private int _index;

        internal ValueEnumerator(ImmutableArray<KeyValuePair<string, BsonValue>> entries)
        {
            _entries = entries;
            _index = -1;
        }

        public BsonValue Current => _entries[_index].Value;

        object System.Collections.IEnumerator.Current => Current;

        public bool MoveNext()
        {
            var next = _index + 1;
            if (next >= _entries.Length)
            {
                return false;
            }

            _index = next;
            return true;
        }

        public void Reset()
        {
            _index = -1;
        }

        public void Dispose()
        {
        }
    }

    /// <summary>
    /// 初始化空的文档
    /// </summary>
    private static object? _aotRef;
    static BsonDocument()
    {
        // Preserve BsonDocumentValue constructor for AOT
        _aotRef = new BsonDocumentValue();
    }

    public BsonDocument() : this(
        ImmutableDictionary<string, BsonValue>.Empty,
        ImmutableArray<string>.Empty,
        normalizeOrder: false)
    {
    }

    /// <summary>
    /// 使用字典初始化文档
    /// </summary>
    public BsonDocument(IDictionary<string, BsonValue> dictionary) : this((IEnumerable<KeyValuePair<string, BsonValue>>)dictionary)
    {
    }

    /// <summary>
    /// 使用不可变字典初始化文档
    /// </summary>
    internal BsonDocument(ImmutableDictionary<string, BsonValue> elements)
        : this(elements, elements.Keys.ToImmutableArray(), normalizeOrder: false)
    {
    }

    private BsonDocument(
        ImmutableDictionary<string, BsonValue> elements,
        ImmutableArray<string> order,
        bool normalizeOrder = true)
    {
        _elements = elements;
        _order = normalizeOrder ? NormalizeOrder(elements, order) : order;
        _entries = CreateEntries(_elements, _order);
    }

    /// <summary>
    /// 使用 Builder 初始化文档（内部高效使用）
    /// </summary>
    internal BsonDocument(ImmutableDictionary<string, BsonValue>.Builder builder)
        : this(builder.ToImmutable())
    {
    }

    internal BsonDocument(IReadOnlyList<KeyValuePair<string, BsonValue>> elements)
    {
        ArgumentNullException.ThrowIfNull(elements);

        var builder = ImmutableDictionary.CreateBuilder<string, BsonValue>();
        var order = ImmutableArray.CreateBuilder<string>(elements.Count);

        for (var i = 0; i < elements.Count; i++)
        {
            var (key, value) = elements[i];
            ArgumentNullException.ThrowIfNull(key);
            if (!builder.ContainsKey(key))
            {
                order.Add(key);
            }

            builder[key] = value;
        }

        _elements = builder.ToImmutable();
        _order = order.ToImmutable();
        _entries = CreateEntries(_elements, _order);
    }

    internal BsonDocument(IEnumerable<KeyValuePair<string, BsonValue>> elements)
    {
        ArgumentNullException.ThrowIfNull(elements);

        var builder = ImmutableDictionary.CreateBuilder<string, BsonValue>();
        var order = ImmutableArray.CreateBuilder<string>();

        foreach (var (key, value) in elements)
        {
            ArgumentNullException.ThrowIfNull(key);
            if (!builder.ContainsKey(key))
            {
                order.Add(key);
            }

            builder[key] = value;
        }

        _elements = builder.ToImmutable();
        _order = order.ToImmutable();
        _entries = CreateEntries(_elements, _order);
    }

    private static ImmutableArray<KeyValuePair<string, BsonValue>> CreateEntries(
        ImmutableDictionary<string, BsonValue> elements,
        ImmutableArray<string> order)
    {
        if (elements.Count == 0)
        {
            return ImmutableArray<KeyValuePair<string, BsonValue>>.Empty;
        }

        var entries = ImmutableArray.CreateBuilder<KeyValuePair<string, BsonValue>>(elements.Count);
        foreach (var key in order)
        {
            if (elements.TryGetValue(key, out var value))
            {
                entries.Add(new KeyValuePair<string, BsonValue>(key, value));
            }
        }

        return entries.ToImmutable();
    }

    private static ImmutableArray<string> NormalizeOrder(
        ImmutableDictionary<string, BsonValue> elements,
        ImmutableArray<string> order)
    {
        if (elements.Count == 0)
        {
            return ImmutableArray<string>.Empty;
        }

        var seen = new HashSet<string>(StringComparer.Ordinal);
        var normalized = ImmutableArray.CreateBuilder<string>(elements.Count);
        foreach (var key in order)
        {
            if (elements.ContainsKey(key) && seen.Add(key))
            {
                normalized.Add(key);
            }
        }

        foreach (var key in elements.Keys)
        {
            if (seen.Add(key))
            {
                normalized.Add(key);
            }
        }

        return normalized.ToImmutable();
    }

    /// <summary>
    /// 创建一个可变构建器
    /// </summary>
    public ImmutableDictionary<string, BsonValue>.Builder ToBuilder()
    {
        return _elements.ToBuilder();
    }

    /// <summary>
    /// 创建包含单个元素的文档
    /// </summary>
    public static BsonDocument Create(string key, BsonValue value)
    {
        ArgumentNullException.ThrowIfNull(key);
        return new BsonDocument(
            ImmutableDictionary<string, BsonValue>.Empty.Add(key, value),
            ImmutableArray.Create(key),
            normalizeOrder: false);
    }

    /// <summary>
    /// 克隆文档
    /// </summary>
    public BsonDocument Clone()
    {
        return this;
    }

    private string[] GetSortedKeys()
    {
        var keys = Volatile.Read(ref _sortedKeys);
        if (keys != null)
        {
            return keys;
        }

        keys = _elements.Keys.OrderBy(k => k, StringComparer.Ordinal).ToArray();
        Volatile.Write(ref _sortedKeys, keys);
        return keys;
    }

    /// <summary>
    /// 比较两个文档
    /// </summary>
    public override int CompareTo(BsonValue? other)
    {
        if (other is null) return 1;
        if (other is BsonDocument otherDoc)
        {
            var countComparison = Count.CompareTo(otherDoc.Count);
            if (countComparison != 0) return countComparison;

            var leftKeys = GetSortedKeys();
            var rightKeys = otherDoc.GetSortedKeys();

            for (var i = 0; i < leftKeys.Length; i++)
            {
                var keyComparison = StringComparer.Ordinal.Compare(leftKeys[i], rightKeys[i]);
                if (keyComparison != 0) return keyComparison;

                var valueComparison = _elements[leftKeys[i]].CompareTo(otherDoc._elements[rightKeys[i]]);
                if (valueComparison != 0) return valueComparison;
            }

            return 0;
        }
        return BsonValueComparer.Compare(this, other);
    }

    /// <summary>
    /// 检查相等性
    /// </summary>
    public override bool Equals(BsonValue? other)
    {
        var otherDoc = other switch
        {
            BsonDocument document => document,
            BsonDocumentValue documentValue => documentValue.Value,
            _ => null
        };

        if (otherDoc is null) return false;
        if (Count != otherDoc.Count) return false;

        // 逐个比较键值对
        foreach (var kvp in Entries)
        {
            if (!otherDoc._elements.TryGetValue(kvp.Key, out var otherValue) || !kvp.Value.Equals(otherValue))
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// 获取哈希码
    /// </summary>
    public override int GetHashCode()
    {
        unchecked
        {
            long sum = 0;
            long sumOfSquares = 0;
            int xor = 0;

            foreach (var kvp in Entries)
            {
                var elementHash = HashCode.Combine(
                    StringComparer.Ordinal.GetHashCode(kvp.Key),
                    BsonValueComparer.GetHashCode(kvp.Value));
                sum += elementHash;
                sumOfSquares += (long)elementHash * elementHash;
                xor ^= elementHash;
            }

            return HashCode.Combine(Count, sum, sumOfSquares, xor);
        }
    }

    /// <summary>
    /// 转换为 JSON 字符串
    /// </summary>
    public override string ToString()
    {
        var sb = new StringBuilder();
        sb.Append('{');

        var first = true;
        foreach (var kvp in Entries)
        {
            if (!first) sb.Append(", ");
            first = false;

            sb.Append('"');
            sb.Append(kvp.Key);
            sb.Append("\": ");
            sb.Append(ToJsonString(kvp.Value));
        }

        sb.Append('}');
        return sb.ToString();
    }

    private static string ToJsonString(BsonValue value)
    {
        return value switch
        {
            BsonString str => $"\"{str.Value.Replace("\"", "\\\"")}\"",
            BsonBoolean boolean => boolean.Value.ToString().ToLowerInvariant(),
            BsonNull => "null",
            BsonDocument doc => doc.ToString(),
            BsonArray array => array.ToString(),
            BsonObjectId oid => $"{{ \"$oid\": \"{oid.Value}\" }}",
            BsonDateTime dt => $"{{ \"$date\": \"{dt.Value:yyyy-MM-ddTHH:mm:ss.fffZ}\" }}",
            _ => value.ToString()
        };
    }

    /// <summary>
    /// 从字典创建文档
    /// </summary>
    public static BsonDocument FromDictionary(Dictionary<string, object?> dictionary)
    {
        var elements = new List<KeyValuePair<string, BsonValue>>(dictionary.Count);

        foreach (var kvp in dictionary)
        {
            elements.Add(new KeyValuePair<string, BsonValue>(kvp.Key, ConvertToBsonValue(kvp.Value)));
        }

        return new BsonDocument(elements);
    }

    /// <summary>
    /// 转换为字典
    /// </summary>
    public Dictionary<string, object?> ToDictionary()
    {
        var result = new Dictionary<string, object?>();

        foreach (var kvp in Entries)
        {
            result[kvp.Key] = ConvertFromBsonValue(kvp.Value);
        }

        return result;
    }

    private static BsonValue ConvertToBsonValue(object? value)
    {
        return value switch
        {
            null => BsonNull.Value,
            string str => str,
            int i => i,
            long l => l,
            double d => d,
            float f => (double)f,
            bool b => b,
            DateTime dt => dt,
            ObjectId oid => oid,
            Dictionary<string, object?> dict => FromDictionary(dict),
            List<object?> list => BsonArray.FromList(list),
            _ => throw new NotSupportedException($"Type {value.GetType()} is not supported")
        };
    }

    private static object? ConvertFromBsonValue(BsonValue value)
    {
        return value switch
        {
            BsonNull => null,
            BsonString str => str.Value,
            BsonInt32 i => i.Value,
            BsonInt64 l => l.Value,
            BsonDouble d => d.Value,
            BsonBoolean b => b.Value,
            BsonDateTime dt => dt.Value,
            BsonObjectId oid => oid.Value,
            BsonDocument doc => doc.ToDictionary(),
            BsonArray array => array.ToList(),
            _ => value.RawValue
        };
    }

    /// <summary>
    /// 隐式转换：从 Dictionary 到 BsonDocument
    /// </summary>
    public static implicit operator BsonDocument(Dictionary<string, object?> dictionary)
    {
        return FromDictionary(dictionary);
    }

    // IConvertible 实现
    public override TypeCode GetTypeCode() => TypeCode.Object;

    public override bool ToBoolean(IFormatProvider? provider) => Count > 0;
    public override byte ToByte(IFormatProvider? provider) => Convert.ToByte(Count, provider);
    public override char ToChar(IFormatProvider? provider) => Convert.ToChar(Count, provider);
    public override DateTime ToDateTime(IFormatProvider? provider) => throw new InvalidCastException();
    public override decimal ToDecimal(IFormatProvider? provider) => Convert.ToDecimal(Count, provider);
    public override double ToDouble(IFormatProvider? provider) => Convert.ToDouble(Count, provider);
    public override short ToInt16(IFormatProvider? provider) => Convert.ToInt16(Count, provider);
    public override int ToInt32(IFormatProvider? provider) => Count;
    public override long ToInt64(IFormatProvider? provider) => Count;
    public override sbyte ToSByte(IFormatProvider? provider) => Convert.ToSByte(Count, provider);
    public override float ToSingle(IFormatProvider? provider) => Convert.ToSingle(Count, provider);
    public override string ToString(IFormatProvider? provider) => ToString();
    public override object ToType(Type conversionType, IFormatProvider? provider)
    {
        if (conversionType == typeof(BsonDocument)) return this;
        if (conversionType == typeof(Dictionary<string, object?>)) return ToDictionary();
        
        if (Type.GetTypeCode(conversionType) != TypeCode.Object)
        {
            return Convert.ChangeType(this, conversionType, provider);
        }
        
        throw new InvalidCastException($"Cannot convert BsonDocument to {conversionType.Name}");
    }
    public override ushort ToUInt16(IFormatProvider? provider) => Convert.ToUInt16(Count, provider);
    public override uint ToUInt32(IFormatProvider? provider) => Convert.ToUInt32(Count, provider);
    public override ulong ToUInt64(IFormatProvider? provider) => Convert.ToUInt64(Count, provider);

    // IEnumerable<KeyValuePair<string, BsonValue>> 实现
    IEnumerable<string> IReadOnlyDictionary<string, BsonValue>.Keys => Keys;
    IEnumerable<BsonValue> IReadOnlyDictionary<string, BsonValue>.Values => Values;
}

/// <summary>
/// BSON 文档值的包装器，用于在 BsonValue 层次结构中表示文档
/// </summary>
internal sealed class BsonDocumentValue : BsonValue
{
    public override BsonType BsonType => BsonType.Document;
    public override object? RawValue => Value;
    public override bool IsDocument => true;

    public BsonDocument Value { get; }

    public BsonDocumentValue(BsonDocument value)
    {
        Value = value ?? throw new ArgumentNullException(nameof(value));
    }

    public BsonDocumentValue()
    {
        Value = new BsonDocument();
    }

    public override int CompareTo(BsonValue? other)
    {
        if (other is null) return 1;
        if (other is BsonDocumentValue otherDoc) return Value.CompareTo(otherDoc.Value);
        if (other is BsonDocument doc) return Value.CompareTo(doc);
        return BsonValueComparer.Compare(this, other);
    }

    public override bool Equals(BsonValue? other)
    {
        if (other is null) return false;
        if (other is BsonDocumentValue otherDoc) return Value.Equals(otherDoc.Value);
        if (other is BsonDocument doc) return Value.Equals(doc);
        return false;
    }

    public override int GetHashCode() => Value.GetHashCode();

    public override string ToString() => Value.ToString();

    // IConvertible 实现 - 委托给内部的 BsonDocument
    public override TypeCode GetTypeCode() => Value.GetTypeCode();
    public override bool ToBoolean(IFormatProvider? provider) => Value.ToBoolean(provider);
    public override byte ToByte(IFormatProvider? provider) => Value.ToByte(provider);
    public override char ToChar(IFormatProvider? provider) => Value.ToChar(provider);
    public override DateTime ToDateTime(IFormatProvider? provider) => Value.ToDateTime(provider);
    public override decimal ToDecimal(IFormatProvider? provider) => Value.ToDecimal(provider);
    public override double ToDouble(IFormatProvider? provider) => Value.ToDouble(provider);
    public override short ToInt16(IFormatProvider? provider) => Value.ToInt16(provider);
    public override int ToInt32(IFormatProvider? provider) => Value.ToInt32(provider);
    public override long ToInt64(IFormatProvider? provider) => Value.ToInt64(provider);
    public override sbyte ToSByte(IFormatProvider? provider) => Value.ToSByte(provider);
    public override float ToSingle(IFormatProvider? provider) => Value.ToSingle(provider);
    public override string ToString(IFormatProvider? provider) => Value.ToString(provider);
    public override object ToType(Type conversionType, IFormatProvider? provider) => Value.ToType(conversionType, provider);
    public override ushort ToUInt16(IFormatProvider? provider) => Value.ToUInt16(provider);
    public override uint ToUInt32(IFormatProvider? provider) => Value.ToUInt32(provider);
    public override ulong ToUInt64(IFormatProvider? provider) => Value.ToUInt64(provider);
}
