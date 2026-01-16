using System.Collections.Generic;
using System.Collections.Immutable;

namespace TinyDb.Bson;

/// <summary>
/// BSON 文档构建器，用于高效创建 BsonDocument 实例
/// </summary>
public sealed class BsonDocumentBuilder
{
    private readonly ImmutableDictionary<string, BsonValue>.Builder _builder;

    public BsonDocumentBuilder()
    {
        _builder = ImmutableDictionary.CreateBuilder<string, BsonValue>();
    }

    public BsonDocumentBuilder(BsonDocument existing)
    {
        _builder = existing.ToBuilder();
    }

    public BsonDocumentBuilder Set(string key, BsonValue value)
    {
        _builder[key] = value;
        return this;
    }

    public BsonDocumentBuilder Remove(string key)
    {
        _builder.Remove(key);
        return this;
    }

    public BsonDocument Build()
    {
        return new BsonDocument(_builder);
    }
}
