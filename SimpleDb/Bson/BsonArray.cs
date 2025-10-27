using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;
using System.Text;

namespace SimpleDb.Bson;

/// <summary>
/// BSON 数组，表示值的有序集合
/// </summary>
public sealed class BsonArray : BsonValue, IList<BsonValue>, IReadOnlyList<BsonValue>
{
    private readonly ImmutableList<BsonValue> _elements;

    public override BsonType BsonType => BsonType.Array;
    public override object? RawValue => _elements;
    public override bool IsArray => true;

    /// <summary>
    /// 获取元素数量
    /// </summary>
    public int Count => _elements.Count;

    /// <summary>
    /// 获取或设置指定索引的值
    /// </summary>
    public BsonValue this[int index]
    {
        get => _elements[index];
        set => throw new NotSupportedException("BsonArray is immutable. Use Set method to create a new array.");
    }

    /// <summary>
    /// 获取是否为只读
    /// </summary>
    public bool IsReadOnly => true;

    
    /// <summary>
    /// 获取指定索引的值
    /// </summary>
    public BsonValue Get(int index)
    {
        return _elements[index];
    }

    /// <summary>
    /// 设置指定索引的值，返回新的数组
    /// </summary>
    public BsonArray Set(int index, BsonValue value)
    {
        return new BsonArray(_elements.SetItem(index, value));
    }

    /// <summary>
    /// 添加值
    /// </summary>
    public void Add(BsonValue item)
    {
        throw new NotSupportedException("BsonArray is immutable. Use Add method to create a new array.");
    }

    /// <summary>
    /// 添加值，返回新的数组
    /// </summary>
    public BsonArray AddValue(BsonValue value)
    {
        return new BsonArray(_elements.Add(value));
    }

    /// <summary>
    /// 在指定位置插入值
    /// </summary>
    public void Insert(int index, BsonValue item)
    {
        throw new NotSupportedException("BsonArray is immutable. Use Insert method to create a new array.");
    }

    /// <summary>
    /// 在指定位置插入值，返回新的数组
    /// </summary>
    public BsonArray InsertValue(int index, BsonValue value)
    {
        return new BsonArray(_elements.Insert(index, value));
    }

    /// <summary>
    /// 移除指定值
    /// </summary>
    public bool Remove(BsonValue item)
    {
        throw new NotSupportedException("BsonArray is immutable. Use Remove method to create a new array.");
    }

    /// <summary>
    /// 移除指定值，返回新的数组
    /// </summary>
    public BsonArray RemoveValue(BsonValue value)
    {
        return new BsonArray(_elements.Remove(value));
    }

    /// <summary>
    /// 移除指定位置的值
    /// </summary>
    public void RemoveAt(int index)
    {
        throw new NotSupportedException("BsonArray is immutable. Use RemoveAt method to create a new array.");
    }

    /// <summary>
    /// 移除指定位置的值，返回新的数组
    /// </summary>
    public BsonArray RemoveAtValue(int index)
    {
        return new BsonArray(_elements.RemoveAt(index));
    }

    /// <summary>
    /// 清空所有元素
    /// </summary>
    public void Clear()
    {
        throw new NotSupportedException("BsonArray is immutable.");
    }

    /// <summary>
    /// 检查是否包含指定值
    /// </summary>
    public bool Contains(BsonValue item) => _elements.Contains(item);

    /// <summary>
    /// 复制到数组
    /// </summary>
    public void CopyTo(BsonValue[] array, int arrayIndex)
    {
        _elements.CopyTo(array, arrayIndex);
    }

    /// <summary>
    /// 获取值的索引
    /// </summary>
    public int IndexOf(BsonValue item) => _elements.IndexOf(item);

    /// <summary>
    /// 获取枚举器
    /// </summary>
    public IEnumerator<BsonValue> GetEnumerator()
    {
        return _elements.GetEnumerator();
    }

    System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => _elements.GetEnumerator();

    /// <summary>
    /// 初始化空数组
    /// </summary>
    public BsonArray() : this(ImmutableList<BsonValue>.Empty)
    {
    }

    /// <summary>
    /// 使用集合初始化数组
    /// </summary>
    public BsonArray(IEnumerable<BsonValue> values) : this(values.ToImmutableList())
    {
    }

    /// <summary>
    /// 使用不可变列表初始化数组
    /// </summary>
    private BsonArray(ImmutableList<BsonValue> elements)
    {
        _elements = elements;
    }

    /// <summary>
    /// 创建包含单个元素的数组
    /// </summary>
    public static BsonArray Create(BsonValue value)
    {
        return new BsonArray(ImmutableList<BsonValue>.Empty.Add(value));
    }

    /// <summary>
    /// 从对象列表创建数组
    /// </summary>
    public static BsonArray FromList(List<object?> list)
    {
        var builder = ImmutableList.CreateBuilder<BsonValue>();

        foreach (var item in list)
        {
            builder.Add(ConvertToBsonValue(item));
        }

        return new BsonArray(builder.ToImmutable());
    }

    /// <summary>
    /// 比较两个数组
    /// </summary>
    public override int CompareTo(BsonValue? other)
    {
        if (other is null) return 1;
        if (other is BsonArray otherArray)
        {
            var countComparison = Count.CompareTo(otherArray.Count);
            if (countComparison != 0) return countComparison;

            for (int i = 0; i < Count; i++)
            {
                var elementComparison = _elements[i].CompareTo(otherArray._elements[i]);
                if (elementComparison != 0) return elementComparison;
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
        return other is BsonArray otherArray && _elements.SequenceEqual(otherArray._elements);
    }

    /// <summary>
    /// 获取哈希码
    /// </summary>
    public override int GetHashCode()
    {
        var hash = 17;
        foreach (var element in _elements)
        {
            hash = hash * 31 + element.GetHashCode();
        }
        return hash;
    }

    /// <summary>
    /// 转换为 JSON 字符串
    /// </summary>
    public override string ToString()
    {
        var sb = new StringBuilder();
        sb.Append('[');

        var first = true;
        foreach (var element in _elements)
        {
            if (!first) sb.Append(", ");
            first = false;

            sb.Append(ToJsonString(element));
        }

        sb.Append(']');
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
    /// 转换为对象列表
    /// </summary>
    public List<object?> ToList()
    {
        var result = new List<object?>(_elements.Count);

        foreach (var element in _elements)
        {
            result.Add(ConvertFromBsonValue(element));
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
            Dictionary<string, object?> dict => BsonDocument.FromDictionary(dict),
            List<object?> list => FromList(list),
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
    /// 隐式转换：从 List 到 BsonArray
    /// </summary>
    public static implicit operator BsonArray(List<object?> list)
    {
        return FromList(list);
    }

    /// <summary>
    /// 隐式转换：从数组到 BsonArray
    /// </summary>
    public static implicit operator BsonArray(object?[] array)
    {
        return new BsonArray(array.Select(ConvertToBsonValue));
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
}

/// <summary>
/// BSON 数组值的包装器，用于在 BsonValue 层次结构中表示数组
/// </summary>
internal sealed class BsonArrayValue : BsonValue
{
    public override BsonType BsonType => BsonType.Array;
    public override object? RawValue => Value;
    public override bool IsArray => true;

    public BsonArray Value { get; }

    public BsonArrayValue(BsonArray value)
    {
        Value = value ?? throw new ArgumentNullException(nameof(value));
    }

    public override int CompareTo(BsonValue? other)
    {
        if (other is null) return 1;
        if (other is BsonArrayValue otherArray) return Value.CompareTo(otherArray.Value);
        if (other is BsonArray array) return Value.CompareTo(array);
        return BsonType.CompareTo(other.BsonType);
    }

    public override bool Equals(BsonValue? other)
    {
        return other is BsonArrayValue otherArray && Value.Equals(otherArray.Value);
    }

    public override int GetHashCode() => Value.GetHashCode();

    public override string ToString() => Value.ToString();

    // IConvertible 实现 - 委托给内部的 BsonArray
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