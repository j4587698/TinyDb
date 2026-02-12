namespace TinyDb.Metadata;

public static class CSharpEntityGeneratorApi
{
    public static string GenerateCSharpEntity(this MetadataManager metadataManager, string tableName, CSharpEntityGenerationOptions? options = null)
    {
        if (metadataManager == null) throw new ArgumentNullException(nameof(metadataManager));
        if (string.IsNullOrWhiteSpace(tableName)) throw new ArgumentException("tableName is required.", nameof(tableName));

        var schema = metadataManager.GetMetadata(tableName);
        if (schema == null) throw new InvalidOperationException($"Schema not found for table '{tableName}'.");

        return CSharpEntityGenerator.Generate(schema, options);
    }

    public static IReadOnlyDictionary<string, string> GenerateAllCSharpEntities(this MetadataManager metadataManager, CSharpEntityGenerationOptions? options = null)
    {
        if (metadataManager == null) throw new ArgumentNullException(nameof(metadataManager));
        options ??= new CSharpEntityGenerationOptions();

        var result = new Dictionary<string, string>(StringComparer.Ordinal);

        foreach (var tableName in metadataManager.GetAllTableNames())
        {
            var schema = metadataManager.GetMetadata(tableName);
            if (schema == null) continue;

            var code = CSharpEntityGenerator.Generate(schema, options);
            result[tableName] = code;
        }

        return result;
    }
}
