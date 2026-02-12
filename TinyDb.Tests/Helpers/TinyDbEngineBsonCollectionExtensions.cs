using TinyDb.Bson;
using TinyDb.Collections;
using TinyDb.Metadata;

namespace TinyDb.Core;

internal static class TinyDbEngineBsonCollectionExtensions
{
    public static void EnsureBsonSchema(this TinyDbEngine engine, string name)
    {
        if (engine == null) throw new ArgumentNullException(nameof(engine));
        if (string.IsNullOrWhiteSpace(name)) throw new ArgumentException("name is required.", nameof(name));
        if (name.StartsWith("__", StringComparison.Ordinal)) return;

        if (engine.MetadataManager.GetMetadata(name) != null) return;

        engine.MetadataManager.SaveMetadata(new MetadataDocument
        {
            TableName = name,
            DisplayName = name,
            Columns = new BsonArray()
                .AddValue(new BsonDocument()
                    .Set("n", "_id")
                    .Set("pn", "Id")
                    .Set("t", typeof(ObjectId).FullName ?? "TinyDb.Bson.ObjectId")
                    .Set("pk", true)
                    .Set("r", true)
                    .Set("o", 0))
        });
    }

    public static ITinyCollection<BsonDocument> GetBsonCollection(this TinyDbEngine engine, string name)
    {
        if (engine == null) throw new ArgumentNullException(nameof(engine));
        if (string.IsNullOrWhiteSpace(name)) throw new ArgumentException("name is required.", nameof(name));

        if (name.StartsWith("__", StringComparison.Ordinal))
        {
            return engine.GetCollection<BsonDocument>(name);
        }

        engine.EnsureBsonSchema(name);

        return engine.GetCollection<BsonDocument>(name);
    }
}
