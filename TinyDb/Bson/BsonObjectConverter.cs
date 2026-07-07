namespace TinyDb.Bson;

internal static class BsonObjectConverter
{
    public static BsonValue ToBsonValue(object? value)
    {
        return value switch
        {
            null => BsonNull.Value,
            string str => str,
            int i => i,
            long l => l,
            double d => d,
            float f => (double)f,
            bool b => b,
            DateTime dt => dt,
            ObjectId oid => oid,
            Dictionary<string, object?> dict => BsonDocument.FromDictionary(dict),
            List<object?> list => BsonArray.FromList(list),
            _ => throw new NotSupportedException($"Type {value.GetType()} is not supported")
        };
    }

    public static object? FromBsonValue(BsonValue value)
    {
        return value switch
        {
            BsonNull => null,
            BsonString str => str.Value,
            BsonInt32 i => i.Value,
            BsonInt64 l => l.Value,
            BsonDouble d => d.Value,
            BsonBoolean b => b.Value,
            BsonDateTime dt => dt.Value,
            BsonObjectId oid => oid.Value,
            BsonDocument doc => doc.ToDictionary(),
            BsonArray array => array.ToList(),
            _ => value.RawValue
        };
    }
}
