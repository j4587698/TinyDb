namespace TinyDb.Index;

/// <summary>
/// 索引验证结果
/// </summary>
public sealed class IndexValidationResult
{
    public int TotalIndexes { get; set; }
    public int ValidIndexes { get; set; }
    public int InvalidIndexes { get; set; }
    public List<string> Errors { get; set; } = new();

    public bool IsValid => InvalidIndexes == 0;

    public override string ToString()
    {
        return $"IndexValidation: {ValidIndexes}/{TotalIndexes} valid, {InvalidIndexes} invalid";
    }
}
