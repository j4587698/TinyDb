using System;
using System.Collections.Concurrent;
using TinyDb.Bson;

namespace TinyDb.Core;

internal readonly struct DocumentLocation : IEquatable<DocumentLocation>
{
    public DocumentLocation(uint pageId, ushort entryIndex)
    {
        PageId = pageId;
        EntryIndex = entryIndex;
    }

    public uint PageId { get; }
    public ushort EntryIndex { get; }

    public bool IsEmpty => PageId == 0 && EntryIndex == 0;

    public static DocumentLocation Empty => new(0, 0);

    // 序列化为 Int64 以便存储在 BTree 中
    // Format: [00000000 00000000] [PageId (32)] [EntryIndex (16)]
    public long ToInt64()
    {
        return ((long)PageId << 16) | (long)EntryIndex;
    }

    public static DocumentLocation FromInt64(long value)
    {
        uint pageId = (uint)((value >> 16) & 0xFFFFFFFF);
        ushort entryIndex = (ushort)(value & 0xFFFF);
        return new DocumentLocation(pageId, entryIndex);
    }

    public bool Equals(DocumentLocation other)
    {
        return PageId == other.PageId && EntryIndex == other.EntryIndex;
    }

    public override bool Equals(object? obj)
    {
        return obj is DocumentLocation other && Equals(other);
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(PageId, EntryIndex);
    }
}

internal readonly struct PageDocumentEntry
{
    public PageDocumentEntry(BsonDocument document, byte[] rawBytes, bool isLargeDocument = false, uint largeDocumentIndexPageId = 0, int largeDocumentSize = 0)
    {
        Document = document;
        RawBytes = rawBytes;
        IsLargeDocument = isLargeDocument;
        LargeDocumentIndexPageId = largeDocumentIndexPageId;
        LargeDocumentSize = largeDocumentSize;
    }
    public BsonDocument Document { get; }
    public byte[] RawBytes { get; }
    public bool IsLargeDocument { get; }
    public uint LargeDocumentIndexPageId { get; }
    public int LargeDocumentSize { get; }
    public BsonValue Id => Document.TryGetValue("_id", out var id) ? id : BsonNull.Value;
}