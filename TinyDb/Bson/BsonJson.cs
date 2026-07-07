namespace TinyDb.Bson;

internal static class BsonJson
{
    public static string ToJsonString(BsonValue value)
    {
        return value switch
        {
            BsonString str => $"\"{str.Value.Replace("\"", "\\\"")}\"",
            BsonBoolean boolean => boolean.Value.ToString().ToLowerInvariant(),
            BsonNull => "null",
            BsonDocument doc => doc.ToString(),
            BsonArray array => array.ToString(),
            BsonObjectId oid => $"{{ \"$oid\": \"{oid.Value}\" }}",
            BsonDateTime dt => $"{{ \"$date\": \"{dt.Value:yyyy-MM-ddTHH:mm:ss.fffZ}\" }}",
            _ => value.ToString()
        };
    }
}
