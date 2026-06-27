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
    /// 比较两个 BSON 值
    /// </summary>
    /// <param name="left">左值</param>
    /// <param name="right">右值</param>
    /// <returns>比较结果</returns>
    private static int CompareValues(BsonValue left, BsonValue right)
    {
        return BsonValueComparer.Compare(left, right);
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
            if (CompareValues(_values[i], other._values[i]) != 0) return false;
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
            hash = hash * 31 + BsonValueComparer.GetHashCode(value);
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
