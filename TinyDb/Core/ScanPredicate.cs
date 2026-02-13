using System.Linq.Expressions;
using System.Text;

namespace TinyDb.Core;

/// <summary>
/// 扫描谓词，用于底层引擎的原地过滤。
/// </summary>
public readonly struct ScanPredicate
{
    public readonly byte[] FieldNameBytes;
    public readonly byte[]? AlternateFieldNameBytes;
    public readonly byte[]? SecondAlternateFieldNameBytes;
    public readonly object? TargetValue;
    public readonly byte[]? TargetStringUtf8Bytes;
    public readonly ExpressionType Operator;

    public ScanPredicate(byte[] fieldNameBytes, byte[]? alternateFieldNameBytes, byte[]? secondAlternateFieldNameBytes, object? targetValue, ExpressionType op)
    {
        FieldNameBytes = fieldNameBytes;
        AlternateFieldNameBytes = alternateFieldNameBytes;
        SecondAlternateFieldNameBytes = secondAlternateFieldNameBytes;
        TargetValue = targetValue;
        Operator = op;

        TargetStringUtf8Bytes = targetValue is string s ? Encoding.UTF8.GetBytes(s) : null;
    }

    public bool IsEmpty => FieldNameBytes == null || FieldNameBytes.Length == 0;
}
