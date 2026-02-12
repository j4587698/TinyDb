namespace TinyDb.Metadata;

public static class SchemaDdlApi
{
    public static string ExportDdl(this MetadataManager metadataManager, string tableName)
    {
        if (metadataManager == null) throw new ArgumentNullException(nameof(metadataManager));
        if (string.IsNullOrWhiteSpace(tableName)) throw new ArgumentException("tableName is required.", nameof(tableName));

        var schema = metadataManager.GetMetadata(tableName);
        if (schema == null) throw new InvalidOperationException($"Schema not found for table '{tableName}'.");

        return SchemaDdl.Export(schema);
    }

    public static string ExportAllDdl(this MetadataManager metadataManager)
    {
        if (metadataManager == null) throw new ArgumentNullException(nameof(metadataManager));

        var docs = metadataManager
            .GetAllTableNames()
            .Select(metadataManager.GetMetadata)
            .Where(d => d != null)!
            .Cast<MetadataDocument>();

        return SchemaDdl.Export(docs);
    }
}
