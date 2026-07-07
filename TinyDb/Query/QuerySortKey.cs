using System.Buffers.Binary;
using System.Text;
using TinyDb.Bson;

namespace TinyDb.Query;

internal readonly struct SortFieldBytes
{
    public static SortFieldBytes Id { get; } = Create("_id");

    public byte[] Primary { get; }
    public byte[]? Alternate { get; }
    public byte[]? SecondAlternate { get; }

    private SortFieldBytes(byte[] primary, byte[]? alternate, byte[]? secondAlternate)
    {
        Primary = primary;
        Alternate = alternate;
        SecondAlternate = secondAlternate;
    }

    public static SortFieldBytes Create(string fieldName)
    {
        if (string.Equals(fieldName, "_id", StringComparison.Ordinal))
        {
            return new SortFieldBytes(
                Encoding.UTF8.GetBytes("_id"),
                Encoding.UTF8.GetBytes("id"),
                Encoding.UTF8.GetBytes("Id"));
        }

        var primary = Encoding.UTF8.GetBytes(fieldName);
        byte[]? alt = null;
        if (fieldName.Length > 0 && fieldName[0] != '_')
        {
            var altName = char.ToUpperInvariant(fieldName[0]) + fieldName.Substring(1);
            if (!string.Equals(altName, fieldName, StringComparison.Ordinal))
            {
                alt = Encoding.UTF8.GetBytes(altName);
            }
        }

        return new SortFieldBytes(primary, alt, null);
    }
}

internal readonly struct SortKey
{
    public static SortKey Null => new(BsonNull.Value);

    public BsonType Type { get; }
    private BsonValue Value { get; }

    private SortKey(BsonValue value)
    {
        Value = value;
        Type = value.BsonType;
    }

    public static SortKey Materialize(in SortKeyRef key)
    {
        return new SortKey(key.ToBsonValue());
    }

    public static SortKey FromBsonValue(BsonValue? value)
    {
        return value == null || value.IsNull ? Null : new SortKey(value);
    }

    public static int Compare(in SortKeyRef a, in SortKey b)
    {
        return BsonValueComparer.Compare(a.ToBsonValue(), b.Value);
    }

    public static int Compare(in SortKey a, in SortKey b)
    {
        return BsonValueComparer.Compare(a.Value, b.Value);
    }
}

internal readonly ref struct SortKeyRef
{
    public static SortKeyRef Null => new(BsonType.Null, 0, 0, default, default);

    public BsonType Type { get; }
    public double Double { get; }
    public long Int64 { get; }
    public Decimal128 Decimal128 { get; }
    public ReadOnlySpan<byte> Bytes { get; }

    internal SortKeyRef(BsonType type, double @double, long int64, Decimal128 decimal128, ReadOnlySpan<byte> bytes)
    {
        Type = type;
        Double = @double;
        Int64 = int64;
        Decimal128 = decimal128;
        Bytes = bytes;
    }

    public static bool TryRead(ReadOnlySpan<byte> document, int valueOffset, BsonType type, out SortKeyRef key)
    {
        key = Null;
        try
        {
            switch (type)
            {
                case BsonType.Null:
                    key = Null;
                    return true;
                case BsonType.Int32:
                    key = new SortKeyRef(type, 0, BinaryPrimitives.ReadInt32LittleEndian(document.Slice(valueOffset, 4)), default, default);
                    return true;
                case BsonType.Int64:
                    key = new SortKeyRef(type, 0, BinaryPrimitives.ReadInt64LittleEndian(document.Slice(valueOffset, 8)), default, default);
                    return true;
                case BsonType.Double:
                    key = new SortKeyRef(type, BinaryPrimitives.ReadDoubleLittleEndian(document.Slice(valueOffset, 8)), 0, default, default);
                    return true;
                case BsonType.Decimal128:
                {
                    var lo = BinaryPrimitives.ReadUInt64LittleEndian(document.Slice(valueOffset, 8));
                    var hi = BinaryPrimitives.ReadUInt64LittleEndian(document.Slice(valueOffset + 8, 8));
                    key = new SortKeyRef(type, 0, 0, new Decimal128(lo, hi), default);
                    return true;
                }
                case BsonType.Boolean:
                    key = new SortKeyRef(type, 0, document[valueOffset] != 0 ? 1 : 0, default, default);
                    return true;
                case BsonType.DateTime:
                {
                    var storedDateTime = BinaryPrimitives.ReadInt64LittleEndian(document.Slice(valueOffset, 8));
                    var dateTime = BsonDateTime.DecodeStoredValue(storedDateTime);
                    key = new SortKeyRef(type, 0, BsonDateTime.GetComparableTicks(dateTime), default, default);
                    return true;
                }
                case BsonType.ObjectId:
                    key = new SortKeyRef(type, 0, 0, default, document.Slice(valueOffset, 12));
                    return true;
                case BsonType.String:
                {
                    var len = BinaryPrimitives.ReadInt32LittleEndian(document.Slice(valueOffset, 4));
                    if (len <= 0) return false;
                    var bytesLen = len - 1;
                    var start = valueOffset + 4;
                    if (start < 0 || start + bytesLen > document.Length) return false;
                    key = new SortKeyRef(type, 0, 0, default, document.Slice(start, bytesLen));
                    return true;
                }
                default:
                    return false;
            }
        }
        catch (Exception ex) when (ex is ArgumentException or ArgumentOutOfRangeException or IndexOutOfRangeException or OverflowException)
        {
            key = Null;
            return false;
        }
    }

    public BsonValue ToBsonValue()
    {
        return Type switch
        {
            BsonType.Null => BsonNull.Value,
            BsonType.Int32 => new BsonInt32(checked((int)Int64)),
            BsonType.Int64 => new BsonInt64(Int64),
            BsonType.Double => new BsonDouble(Double),
            BsonType.Decimal128 => new BsonDecimal128(Decimal128),
            BsonType.Boolean => new BsonBoolean(Int64 != 0),
            BsonType.DateTime => new BsonDateTime(new DateTime(Int64, DateTimeKind.Utc)),
            BsonType.ObjectId => new BsonObjectId(new ObjectId(Bytes)),
            BsonType.String => new BsonString(Encoding.UTF8.GetString(Bytes)),
            _ => BsonNull.Value
        };
    }
}
