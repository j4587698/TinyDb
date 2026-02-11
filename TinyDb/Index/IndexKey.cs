using System;
using System.Collections.Generic;
using System.Linq;
using TinyDb.Bson;

namespace TinyDb.Index;

/// <summary>
/// 索引键，支持多个字段的复合索引
/// </summary>
public sealed class IndexKey : IComparable<IndexKey>, IEquatable<IndexKey>
{
    private readonly BsonValue[] _values;
    private readonly int _hashCode;

    /// <summary>
    /// 索引键值数组
    /// </summary>
    public IReadOnlyList<BsonValue> Values => _values;
    internal ReadOnlySpan<BsonValue> ValuesSpan => _values;

    /// <summary>
    /// 索引键长度（字段数量）
    /// </summary>
    public int Length => _values.Length;

    /// <summary>
    /// 初始化索引键
    /// </summary>
    /// <param name="values">键值数组</param>
    public IndexKey(params BsonValue[] values)
    {
        _values = values ?? throw new ArgumentNullException(nameof(values));
        _hashCode = CalculateHashCode();
    }

    /// <summary>
    /// 获取指定位置的值
    /// </summary>
    /// <param name="index">位置索引</param>
    /// <returns>键值</returns>
    public BsonValue this[int index] => _values[index];

    /// <summary>
    /// 比较两个索引键
    /// </summary>
    /// <param name="other">另一个索引键</param>
    /// <returns>比较结果</returns>
    public int CompareTo(IndexKey? other)
    {
        if (other is null) return 1;
        if (ReferenceEquals(this, other)) return 0;

        // 按字段顺序比较
        var minLength = Math.Min(_values.Length, other._values.Length);
        for (int i = 0; i < minLength; i++)
        {
            var comparison = CompareValues(_values[i], other._values[i]);
            if (comparison != 0) return comparison;
        }

        // 如果所有字段都相等，较短的键排在前面
        return _values.Length.CompareTo(other._values.Length);
    }

    /// <summary>
    /// 获取 BSON 类型的比较顺序
    /// 基于 MongoDB 的类型比较顺序
    /// </summary>
    /// <param name="bsonType">BSON 类型</param>
    /// <returns>类型顺序</returns>
    private static int GetTypeOrder(BsonType bsonType)
    {
        return bsonType switch
        {
            BsonType.MinKey => 0,
            BsonType.Null => 1,
            BsonType.Boolean => 2,
            BsonType.Int32 => 3,
            BsonType.Int64 => 4,
            BsonType.Double => 5,
            BsonType.Decimal128 => 6,
            BsonType.String => 7,
            BsonType.ObjectId => 8,
            BsonType.DateTime => 9,
            BsonType.Binary => 10,
            BsonType.Array => 11,
            BsonType.Document => 12,
            BsonType.RegularExpression => 13,
            BsonType.JavaScript => 14,
            BsonType.JavaScriptWithScope => 15,
            BsonType.Timestamp => 16,
            BsonType.Symbol => 17,
            BsonType.Undefined => 18,
            BsonType.MaxKey => 19,
            _ => 20 // 未知类型
        };
    }

    /// <summary>
    /// 比较两个 BSON 值
    /// </summary>
    /// <param name="left">左值</param>
    /// <param name="right">右值</param>
    /// <returns>比较结果</returns>
    private static int CompareValues(BsonValue left, BsonValue right)
    {
        // 处理 null 值
        if (left.IsNull && right.IsNull) return 0;
        if (left.IsNull) return -1;
        if (right.IsNull) return 1;

        // 处理不同类型的比较
        var leftType = left.BsonType;
        var rightType = right.BsonType;

        if (leftType != rightType)
        {
            // 特殊处理数值类型之间的比较：如果数值不相等，按数值排序
            if (IsNumeric(left) && IsNumeric(right))
            {
                try
                {
                    var lVal = left.ToDecimal(null);
                    var rVal = right.ToDecimal(null);
                    var numComp = lVal.CompareTo(rVal);
                    if (numComp != 0) return numComp;
                    // 如果数值相等但类型不同，降级到按类型顺序排序
                }
                catch { }
            }
            
            return GetTypeOrder(leftType).CompareTo(GetTypeOrder(rightType));
        }

        // 同类型比较
        return leftType switch
        {
            BsonType.String => string.Compare(((BsonString)left).Value, ((BsonString)right).Value, StringComparison.Ordinal),
            BsonType.Int32 => ((BsonInt32)left).Value.CompareTo(((BsonInt32)right).Value),
            BsonType.Int64 => ((BsonInt64)left).Value.CompareTo(((BsonInt64)right).Value),
            BsonType.Double => ((BsonDouble)left).Value.CompareTo(((BsonDouble)right).Value),
            BsonType.Boolean => ((BsonBoolean)left).Value.CompareTo(((BsonBoolean)right).Value),
            BsonType.DateTime => ((BsonDateTime)left).Value.CompareTo(((BsonDateTime)right).Value),       
            BsonType.ObjectId => ((BsonObjectId)left).Value.CompareTo(((BsonObjectId)right).Value),       
            BsonType.Binary => ((BsonBinary)left).CompareTo((BsonBinary)right),
            BsonType.Decimal128 => ((BsonDecimal128)left).Value.CompareTo(((BsonDecimal128)right).Value), 
            _ => string.Compare(left.ToString(), right.ToString(), StringComparison.Ordinal)
        };
    }

    private static bool IsNumeric(BsonValue val)
    {
        return val.BsonType == BsonType.Int32 ||
               val.BsonType == BsonType.Int64 ||
               val.BsonType == BsonType.Double ||
               val.BsonType == BsonType.Decimal128;
    }

    /// <summary>
    /// 检查相等性
    /// </summary>
    /// <param name="other">另一个索引键</param>
    /// <returns>是否相等</returns>
    public bool Equals(IndexKey? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;

        if (_values.Length != other._values.Length) return false;

        for (int i = 0; i < _values.Length; i++)
        {
            if (!Equals(_values[i], other._values[i])) return false;
        }

        return true;
    }

    /// <summary>
    /// 重写 Equals 方法
    /// </summary>
    /// <param name="obj">比较对象</param>
    /// <returns>是否相等</returns>
    public override bool Equals(object? obj)
    {
        return obj is IndexKey other && Equals(other);
    }

    /// <summary>
    /// 计算哈希码
    /// </summary>
    /// <returns>哈希码</returns>
    public override int GetHashCode()
    {
        return _hashCode;
    }

    /// <summary>
    /// 计算哈希码
    /// </summary>
    /// <returns>哈希码</returns>
    private int CalculateHashCode()
    {
        var hash = 17;
        foreach (var value in _values)
        {
            hash = hash * 31 + (value?.GetHashCode() ?? 0);
        }
        return hash;
    }

    /// <summary>
    /// 相等操作符
    /// </summary>
    public static bool operator ==(IndexKey? left, IndexKey? right)
    {
        return Equals(left, right);
    }

    /// <summary>
    /// 不等操作符
    /// </summary>
    public static bool operator !=(IndexKey? left, IndexKey? right)
    {
        return !Equals(left, right);
    }

    /// <summary>
    /// 大于操作符
    /// </summary>
    public static bool operator >(IndexKey left, IndexKey right)
    {
        return left.CompareTo(right) > 0;
    }

    /// <summary>
    /// 小于操作符
    /// </summary>
    public static bool operator <(IndexKey left, IndexKey right)
    {
        return left.CompareTo(right) < 0;
    }

    /// <summary>
    /// 大于等于操作符
    /// </summary>
    public static bool operator >=(IndexKey left, IndexKey right)
    {
        return left.CompareTo(right) >= 0;
    }

    /// <summary>
    /// 小于等于操作符
    /// </summary>
    public static bool operator <=(IndexKey left, IndexKey right)
    {
        return left.CompareTo(right) <= 0;
    }

    /// <summary>
    /// 转换为字符串表示
    /// </summary>
    /// <returns>字符串表示</returns>
    public override string ToString()
    {
        return $"IndexKey[{string.Join(", ", _values.Select(v => v?.ToString() ?? "null"))}]";
    }

    /// <summary>
    /// 克隆索引键
    /// </summary>
    /// <returns>新的索引键</returns>
    public IndexKey Clone()
    {
        return new IndexKey(_values.ToArray());
    }

    /// <summary>
    /// 创建单字段索引键
    /// </summary>
    /// <param name="value">字段值</param>
    /// <returns>索引键</returns>
    public static IndexKey Create(BsonValue value)
    {
        return new IndexKey(value);
    }

    /// <summary>
    /// 创建复合索引键
    /// </summary>
    /// <param name="values">字段值数组</param>
    /// <returns>索引键</returns>
    public static IndexKey Create(params BsonValue[] values)
    {
        return new IndexKey(values);
    }

    /// <summary>
    /// 获取最小索引键（用于范围查询的下界）
    /// </summary>
    public static IndexKey MinValue => new IndexKey(BsonMinKey.Value);

    /// <summary>
    /// 获取最大索引键（用于范围查询的上界）
    /// </summary>
    public static IndexKey MaxValue => new IndexKey(BsonMaxKey.Value);
}
