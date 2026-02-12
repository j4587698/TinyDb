namespace TinyDb.Metadata;

public sealed class CSharpEntityGenerationOptions
{
    public string? Namespace { get; set; }

    public string? ClassName { get; set; }

    public bool FileScopedNamespace { get; set; } = true;

    public bool UseCSharpAliases { get; set; } = true;

    public bool EmitNullableAnnotations { get; set; } = true;

    public bool EmitEntityAttribute { get; set; } = true;

    public bool EmitMetadataAttributes { get; set; } = true;

    public bool EmitForeignKeyAttributes { get; set; } = true;
}
