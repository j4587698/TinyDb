namespace SimpleDb.Bson;

/// <summary>
/// BSON 数据类型枚举
/// </summary>
public enum BsonType : byte
{
    /// <summary>
    /// 结束标记
    /// </summary>
    End = 0x00,

    /// <summary>
    /// 双精度浮点数
    /// </summary>
    Double = 0x01,

    /// <summary>
    /// 字符串
    /// </summary>
    String = 0x02,

    /// <summary>
    /// 文档对象
    /// </summary>
    Document = 0x03,

    /// <summary>
    /// 数组
    /// </summary>
    Array = 0x04,

    /// <summary>
    /// 二进制数据
    /// </summary>
    Binary = 0x05,

    /// <summary>
    /// 未定义（已废弃）
    /// </summary>
    Undefined = 0x06,

    /// <summary>
    /// ObjectId
    /// </summary>
    ObjectId = 0x07,

    /// <summary>
    /// 布尔值
    /// </summary>
    Boolean = 0x08,

    /// <summary>
    /// UTC 日期时间
    /// </summary>
    DateTime = 0x09,

    /// <summary>
    /// 空值
    /// </summary>
    Null = 0x0A,

    /// <summary>
    /// 正则表达式
    /// </summary>
    RegularExpression = 0x0B,

    /// <summary>
    /// JavaScript 代码
    /// </summary>
    JavaScript = 0x0D,

    /// <summary>
    /// 符号（已废弃）
    /// </summary>
    Symbol = 0x0E,

    /// <summary>
    /// JavaScript 代码（带作用域）
    /// </summary>
    JavaScriptWithScope = 0x0F,

    /// <summary>
    /// 32位整数
    /// </summary>
    Int32 = 0x10,

    /// <summary>
    /// 时间戳
    /// </summary>
    Timestamp = 0x11,

    /// <summary>
    /// 64位整数
    /// </summary>
    Int64 = 0x12,

    /// <summary>
    /// 128位十进制数
    /// </summary>
    Decimal128 = 0x13,

    /// <summary>
    /// 最小键
    /// </summary>
    MinKey = 0xFF,

    /// <summary>
    /// 最大键
    /// </summary>
    MaxKey = 0x7F
}