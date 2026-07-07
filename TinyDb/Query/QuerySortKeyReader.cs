using System.Buffers.Binary;
using System.Diagnostics.CodeAnalysis;
using System.Text;
using TinyDb.Bson;
using TinyDb.Serialization;

namespace TinyDb.Query;

internal static class QuerySortKeyReader
{
    public static SortFieldBytes[] CreateSortFields(IReadOnlyList<QuerySortField> sort)
    {
        var sortFields = new SortFieldBytes[sort.Count];
        for (var i = 0; i < sort.Count; i++)
        {
            sortFields[i] = SortFieldBytes.Create(sort[i].FieldName);
        }

        return sortFields;
    }

    public static SortKey[] MaterializeKeysFromDocument(BsonDocument doc, IReadOnlyList<QuerySortField> sort)
    {
        var keys = new SortKey[sort.Count];
        for (var i = 0; i < sort.Count; i++)
        {
            keys[i] = SortKey.FromBsonValue(TryGetSortValue(doc, sort[i].FieldName));
        }

        return keys;
    }

    public static SortKey[] MaterializeKeysFromSlice(ReadOnlySpan<byte> slice, SortFieldBytes[] sortFields, IReadOnlyList<QuerySortField> sort)
    {
        var keys = new SortKey[sort.Count];
        for (var i = 0; i < sort.Count; i++)
        {
            keys[i] = TryReadKeyRef(slice, sortFields[i], out var keyRef)
                ? SortKey.Materialize(keyRef)
                : SortKey.Null;
        }

        return keys;
    }

    public static bool TryReadKeyRef(ReadOnlySpan<byte> document, in SortFieldBytes field, out SortKeyRef key)
    {
        if (!TryLocateFieldWithAlternates(document, field, out var valueOffset, out var type))
        {
            key = SortKeyRef.Null;
            return true;
        }

        return SortKeyRef.TryRead(document, valueOffset, type, out key);
    }

    public static bool TryReadBsonValue(ReadOnlySpan<byte> document, in SortFieldBytes field, [NotNullWhen(true)] out BsonValue? value)
    {
        value = null;

        if (!TryLocateFieldWithAlternates(document, field, out var valueOffset, out var type))
        {
            return false;
        }

        try
        {
            value = type switch
            {
                BsonType.Int32 => new BsonInt32(BinaryPrimitives.ReadInt32LittleEndian(document.Slice(valueOffset, 4))),
                BsonType.Int64 => new BsonInt64(BinaryPrimitives.ReadInt64LittleEndian(document.Slice(valueOffset, 8))),
                BsonType.Double => new BsonDouble(BinaryPrimitives.ReadDoubleLittleEndian(document.Slice(valueOffset, 8))),
                BsonType.Boolean => new BsonBoolean(document[valueOffset] != 0),
                BsonType.String => ReadString(document, valueOffset),
                BsonType.ObjectId => new BsonObjectId(new ObjectId(document.Slice(valueOffset, 12))),
                BsonType.Null => BsonNull.Value,
                _ => null
            };

            return value != null;
        }
        catch (Exception ex) when (ex is ArgumentException or ArgumentOutOfRangeException or IndexOutOfRangeException or OverflowException)
        {
            value = null;
            return false;
        }
    }

    public static BsonString? ReadString(ReadOnlySpan<byte> document, int valueOffset)
    {
        if (valueOffset < 0 || valueOffset + 4 > document.Length) return null;

        var len = BinaryPrimitives.ReadInt32LittleEndian(document.Slice(valueOffset, 4));
        if (len <= 0) return null;

        var bytesLen = len - 1;
        var start = valueOffset + 4;
        if (start < 0 || start + bytesLen > document.Length) return null;

        var value = Encoding.UTF8.GetString(document.Slice(start, bytesLen));
        return new BsonString(value);
    }

    public static BsonValue? TryGetSortValue(BsonDocument doc, string fieldName)
    {
        if (doc.TryGetValue(fieldName, out var value) && value != null) return value;

        if (fieldName.Length > 0 && fieldName[0] != '_')
        {
            var alt = char.ToUpperInvariant(fieldName[0]) + fieldName.Substring(1);
            if (doc.TryGetValue(alt, out var altValue) && altValue != null) return altValue;
        }

        if (string.Equals(fieldName, "_id", StringComparison.Ordinal))
        {
            if (doc.TryGetValue("id", out var idValue) && idValue != null) return idValue;
            if (doc.TryGetValue("Id", out var pascalIdValue) && pascalIdValue != null) return pascalIdValue;
        }

        return null;
    }

    public static int CompareSliceToRow(
        ReadOnlySpan<byte> slice,
        SortFieldBytes[] sortFields,
        in TopKRow row,
        long candidateSequence,
        IReadOnlyList<QuerySortField> sort)
    {
        for (var i = 0; i < sort.Count; i++)
        {
            var ok = TryReadKeyRef(slice, sortFields[i], out var keyRef);
            var cmp = ok ? SortKey.Compare(keyRef, row.Keys[i]) : SortKey.Compare(SortKeyRef.Null, row.Keys[i]);
            if (cmp != 0)
            {
                if (sort[i].Descending) cmp = -cmp;
                return cmp;
            }
        }

        return candidateSequence.CompareTo(row.Sequence);
    }

    public static int CompareDocumentToRow(BsonDocument doc, in TopKRow row, long candidateSequence, IReadOnlyList<QuerySortField> sort)
    {
        for (var i = 0; i < sort.Count; i++)
        {
            var cmp = SortKey.Compare(SortKey.FromBsonValue(TryGetSortValue(doc, sort[i].FieldName)), row.Keys[i]);
            if (cmp != 0)
            {
                if (sort[i].Descending) cmp = -cmp;
                return cmp;
            }
        }

        return candidateSequence.CompareTo(row.Sequence);
    }

    public static int CompareRows(in TopKRow a, in TopKRow b, IReadOnlyList<QuerySortField> sort)
    {
        for (var i = 0; i < sort.Count; i++)
        {
            var cmp = SortKey.Compare(a.Keys[i], b.Keys[i]);
            if (cmp != 0)
            {
                if (sort[i].Descending) cmp = -cmp;
                return cmp;
            }
        }

        return a.Sequence.CompareTo(b.Sequence);
    }

    private static bool TryLocateFieldWithAlternates(ReadOnlySpan<byte> document, in SortFieldBytes field, out int valueOffset, out BsonType type)
    {
        if (BsonScanner.TryLocateField(document, field.Primary, out valueOffset, out type)) return true;
        if (field.Alternate != null && BsonScanner.TryLocateField(document, field.Alternate, out valueOffset, out type)) return true;
        if (field.SecondAlternate != null && BsonScanner.TryLocateField(document, field.SecondAlternate, out valueOffset, out type)) return true;

        valueOffset = 0;
        type = BsonType.Null;
        return false;
    }
}
