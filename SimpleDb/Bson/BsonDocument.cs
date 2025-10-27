using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;
using System.Text;

namespace SimpleDb.Bson;

/// <summary>
/// BSON 文档，表示键值对的集合
/// </summary>
public sealed class BsonDocument : BsonValue, IDictionary<string, BsonValue>, IReadOnlyDictionary<string, BsonValue>
{
    internal readonly ImmutableDictionary<string, BsonValue> _elements;

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
    public IEnumerable<string> Keys => _elements.Keys;

    /// <summary>
    /// 获取所有值
    /// </summary>
    public IEnumerable<BsonValue> Values => _elements.Values;

    /// <summary>
    /// IDictionary.Keys 显式实现
    /// </summary>
    ICollection<string> IDictionary<string, BsonValue>.Keys => _elements.Keys.ToList();

    /// <summary>
    /// IDictionary.Values 显式实现
    /// </summary>
    ICollection<BsonValue> IDictionary<string, BsonValue>.Values => _elements.Values.ToList();

    /// <summary>
    /// 获取是否为只读
    /// </summary>
    bool ICollection<KeyValuePair<string, BsonValue>>.IsReadOnly => true;

    /// <summary>
    /// 获取或设置指定键的值
    /// </summary>
    public BsonValue this[string key]
    {
        get => _elements.TryGetValue(key, out var value) ? value : BsonNull.Value;
        set => Set(key, value);
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
        return new BsonDocument(_elements.SetItem(key, value));
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
        return new BsonDocument(_elements.Remove(key));
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
        Add(item.Key, item.Value);
    }

    /// <summary>
    /// 检查是否包含指定键值对
    /// </summary>
    public bool Contains(KeyValuePair<string, BsonValue> item)
    {
        return _elements.Contains(item);
    }

    /// <summary>
    /// 复制到数组
    /// </summary>
    public void CopyTo(KeyValuePair<string, BsonValue>[] array, int arrayIndex)
    {
        ((ICollection<KeyValuePair<string, BsonValue>>)_elements).CopyTo(array, arrayIndex);
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
    public IEnumerator<KeyValuePair<string, BsonValue>> GetEnumerator()
    {
        return _elements.GetEnumerator();
    }

    System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => _elements.GetEnumerator();

    /// <summary>
    /// 初始化空的文档
    /// </summary>
    public BsonDocument() : this(ImmutableDictionary<string, BsonValue>.Empty)
    {
    }

    /// <summary>
    /// 使用字典初始化文档
    /// </summary>
    public BsonDocument(IDictionary<string, BsonValue> dictionary) : this(dictionary.ToImmutableDictionary())
    {
    }

    /// <summary>
    /// 使用不可变字典初始化文档
    /// </summary>
    private BsonDocument(ImmutableDictionary<string, BsonValue> elements)
    {
        _elements = elements;
    }

    /// <summary>
    /// 创建包含单个元素的文档
    /// </summary>
    public static BsonDocument Create(string key, BsonValue value)
    {
        return new BsonDocument(ImmutableDictionary<string, BsonValue>.Empty.Add(key, value));
    }

    /// <summary>
    /// 克隆文档
    /// </summary>
    public BsonDocument Clone()
    {
        return new BsonDocument(_elements.ToBuilder().ToImmutable());
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

            foreach (var kvp in _elements)
            {
                if (!otherDoc.TryGetValue(kvp.Key, out var otherValue))
                    return 1;

                var valueComparison = kvp.Value.CompareTo(otherValue);
                if (valueComparison != 0) return valueComparison;
            }

            return 0;
        }
        return BsonType.CompareTo(other.BsonType);
    }

    /// <summary>
    /// 检查相等性
    /// </summary>
    public override bool Equals(BsonValue? other)
    {
        return other is BsonDocument otherDoc && _elements.Equals(otherDoc._elements);
    }

    /// <summary>
    /// 获取哈希码
    /// </summary>
    public override int GetHashCode() => _elements.GetHashCode();

    /// <summary>
    /// 转换为 JSON 字符串
    /// </summary>
    public override string ToString()
    {
        var sb = new StringBuilder();
        sb.Append('{');

        var first = true;
        foreach (var kvp in _elements)
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
        var builder = ImmutableDictionary.CreateBuilder<string, BsonValue>();

        foreach (var kvp in dictionary)
        {
            builder[kvp.Key] = ConvertToBsonValue(kvp.Value);
        }

        return new BsonDocument(builder.ToImmutable());
    }

    /// <summary>
    /// 转换为字典
    /// </summary>
    public Dictionary<string, object?> ToDictionary()
    {
        var result = new Dictionary<string, object?>();

        foreach (var kvp in _elements)
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
    public override DateTime ToDateTime(IFormatProvider? provider) => Convert.ToDateTime(this, provider);
    public override decimal ToDecimal(IFormatProvider? provider) => Convert.ToDecimal(Count, provider);
    public override double ToDouble(IFormatProvider? provider) => Convert.ToDouble(Count, provider);
    public override short ToInt16(IFormatProvider? provider) => Convert.ToInt16(Count, provider);
    public override int ToInt32(IFormatProvider? provider) => Count;
    public override long ToInt64(IFormatProvider? provider) => Count;
    public override sbyte ToSByte(IFormatProvider? provider) => Convert.ToSByte(Count, provider);
    public override float ToSingle(IFormatProvider? provider) => Convert.ToSingle(Count, provider);
    public override string ToString(IFormatProvider? provider) => ToString();
    public override object ToType(Type conversionType, IFormatProvider? provider) => Convert.ChangeType(this, conversionType, provider);
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

    public override int CompareTo(BsonValue? other)
    {
        if (other is null) return 1;
        if (other is BsonDocumentValue otherDoc) return Value.CompareTo(otherDoc.Value);
        if (other is BsonDocument doc) return Value.CompareTo(doc);
        return BsonType.CompareTo(other.BsonType);
    }

    public override bool Equals(BsonValue? other)
    {
        return other is BsonDocumentValue otherDoc && Value.Equals(otherDoc.Value);
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