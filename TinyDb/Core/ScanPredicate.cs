using System.Linq.Expressions;
using System.Text;

namespace TinyDb.Core;

/// <summary>
/// 扫描谓词，用于底层引擎的原地过滤。
/// </summary>
/// <remarks>
/// <see cref="FieldNameBytes"/> 是解析层算出的 BSON 存储字段名；
/// <see cref="AlternateFieldNameBytes"/> 仅用于兼容手工构造、未走序列化器的 PascalCase 文档。
/// </remarks>
public readonly struct ScanPredicate
{
    public readonly byte[] FieldNameBytes;
    public readonly byte[]? AlternateFieldNameBytes;
    public readonly object? TargetValue;
    public readonly byte[]? TargetStringUtf8Bytes;
    public readonly ExpressionType Operator;

    public ScanPredicate(byte[] fieldNameBytes, byte[]? alternateFieldNameBytes, object? targetValue, ExpressionType op)
    {
        FieldNameBytes = fieldNameBytes;
        AlternateFieldNameBytes = alternateFieldNameBytes;
        TargetValue = targetValue;
        Operator = op;

        TargetStringUtf8Bytes = targetValue is string s ? Encoding.UTF8.GetBytes(s) : null;
    }

    public bool IsEmpty => FieldNameBytes == null || FieldNameBytes.Length == 0;
}
