using System;
using System.Collections.Generic;
using TinyDb.Attributes;

namespace TinyDb.UI.Models;

/// <summary>
/// 用于动态插入的通用实体类
/// </summary>
[Entity]
public class DynamicEntity
{
    private Dictionary<string, object?> _data = new();

    /// <summary>
    /// 文档ID
    /// </summary>
    [Id]
    public string? Id { get; set; }

    /// <summary>
    /// 动态数据存储
    /// </summary>
    public Dictionary<string, object?> Data
    {
        get => _data;
        set => _data = value ?? new Dictionary<string, object?>();
    }

    /// <summary>
    /// 索引器，便于动态访问字段
    /// </summary>
    public object? this[string key]
    {
        get => _data.TryGetValue(key, out var value) ? value : null;
        set => _data[key] = value;
    }

    /// <summary>
    /// 添加或更新字段
    /// </summary>
    public DynamicEntity Set(string key, object? value)
    {
        _data[key] = value;
        return this;
    }

    /// <summary>
    /// 获取字段值
    /// </summary>
    public object? Get(string key)
    {
        return _data.TryGetValue(key, out var value) ? value : null;
    }

    /// <summary>
    /// 检查是否包含字段
    /// </summary>
    public bool Contains(string key)
    {
        return _data.ContainsKey(key);
    }

    /// <summary>
    /// 获取所有字段名
    /// </summary>
    public IEnumerable<string> GetFieldNames()
    {
        return _data.Keys;
    }
}