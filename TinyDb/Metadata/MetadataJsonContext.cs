using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace TinyDb.Metadata;

/// <summary>
/// System.Text.Json 源生成上下文，确保在 AOT 场景下正确序列化元数据。
/// </summary>
[JsonSourceGenerationOptions(WriteIndented = false)]
[JsonSerializable(typeof(List<PropertyMetadata>))]
internal partial class MetadataJsonContext : JsonSerializerContext
{
}
