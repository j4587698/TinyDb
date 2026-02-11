using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using TinyDb.Bson;
using TinyDb.Serialization;
using TinyDb.Storage;
using TinyDb.Utils;

namespace TinyDb.Index;

/// <summary>
/// 磁盘 B+ 树节点
/// </summary>
public sealed class DiskBTreeNode : IDisposable
{
    private readonly Page _page;
    private readonly PageManager _pm;
    private bool _isDirty;
    private bool _disposed;

    public bool IsLeaf { get; private set; }
    public uint ParentId { get; private set; }
    public uint NextSiblingId { get; private set; }
    public uint PrevSiblingId { get; private set; }
    public long TreeEntryCount { get; set; }

    public List<IndexKey> Keys { get; private set; }
    public List<uint> ChildrenIds { get; private set; } 
    public List<BsonValue> Values { get; private set; }

    public uint PageId => _page.PageID;
    public int KeyCount => Keys.Count;

    public DiskBTreeNode(Page page, PageManager pm)
    {
        _page = page ?? throw new ArgumentNullException(nameof(page));
        _pm = pm ?? throw new ArgumentNullException(nameof(pm));
        
        // 锁定页面，防止被 LRU 置换
        _page.Pin();

        Keys = new List<IndexKey>();
        ChildrenIds = new List<uint>();
        Values = new List<BsonValue>();

        if (_page.PageType == PageType.Index && _page.Header.ItemCount > 0)
        {
            LoadFromPage();
        }
        else
        {
            IsLeaf = true;
            _isDirty = true;
        }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        
        // 释放页面锁定
        _page.Unpin();
    }
    public void InitAsRoot()
    {
        IsLeaf = true;
        ParentId = 0;
        TreeEntryCount = 0;
        Keys.Clear();
        ChildrenIds.Clear();
        Values.Clear();
        MarkDirty();
    }

    public void SetLeaf(bool isLeaf)
    {
        if (IsLeaf != isLeaf)
        {
            IsLeaf = isLeaf;
            MarkDirty();
        }
    }

    public void SetNext(uint nextId)
    {
        if (NextSiblingId != nextId)
        {
            NextSiblingId = nextId;
            MarkDirty();
        }
    }

    public void SetPrev(uint prevId)
    {
        if (PrevSiblingId != prevId)
        {
            PrevSiblingId = prevId;
            MarkDirty();
        }
    }
    
    public void SetParent(uint parentId)
    {
        if (ParentId != parentId)
        {
            ParentId = parentId;
            MarkDirty();
        }
    }

    public void SetTreeEntryCount(long count)
    {
        if (TreeEntryCount != count)
        {
            TreeEntryCount = count;
            MarkDirty();
        }
    }

    public void MarkDirty()
    {
        _isDirty = true;
    }

    public void Save(PageManager pm)
    {
        if (!_isDirty) return;

        static void WriteByte(IBufferWriter<byte> writer, byte value)
        {
            var span = writer.GetSpan(1);
            span[0] = value;
            writer.Advance(1);
        }

        static void WriteBoolean(IBufferWriter<byte> writer, bool value) => WriteByte(writer, value ? (byte)1 : (byte)0);

        static void WriteInt32(IBufferWriter<byte> writer, int value)
        {
            var span = writer.GetSpan(4);
            BinaryPrimitives.WriteInt32LittleEndian(span, value);
            writer.Advance(4);
        }

        static void WriteUInt32(IBufferWriter<byte> writer, uint value)
        {
            var span = writer.GetSpan(4);
            BinaryPrimitives.WriteUInt32LittleEndian(span, value);
            writer.Advance(4);
        }

        static void WriteInt64(IBufferWriter<byte> writer, long value)
        {
            var span = writer.GetSpan(8);
            BinaryPrimitives.WriteInt64LittleEndian(span, value);
            writer.Advance(8);
        }

        using var buffer = new PooledBufferWriter(Math.Max(CalculateSize(), 256));
        using var bsonWriter = new BsonWriter(buffer);

        WriteBoolean(buffer, IsLeaf);
        WriteInt32(buffer, Keys.Count);
        WriteUInt32(buffer, ParentId);
        WriteUInt32(buffer, NextSiblingId);
        WriteUInt32(buffer, PrevSiblingId);
        WriteInt64(buffer, TreeEntryCount);

        foreach (var key in Keys)
        {
            var keyValues = key.ValuesSpan;
            WriteInt32(buffer, keyValues.Length);
            for (int i = 0; i < keyValues.Length; i++)
            {
                var val = keyValues[i];
                WriteByte(buffer, (byte)val.BsonType);
                bsonWriter.WriteValue(val);
            }
        }

        if (IsLeaf)
        {
            foreach (var val in Values)
            {
                WriteByte(buffer, (byte)val.BsonType);
                bsonWriter.WriteValue(val);
            }
        }
        else
        {
            foreach (var childId in ChildrenIds)
            {
                WriteUInt32(buffer, childId);
            }
        }

        var fullDataSpan = buffer.WrittenSpan;
        int capacity = _page.DataCapacity - 4;

        if (fullDataSpan.Length <= capacity)
        {
            if (_page.Header.NextPageID != 0)
            {
                FreeChain(pm, _page.Header.NextPageID);
                _page.SetLinks(_page.Header.PrevPageID, 0);
            }

            _page.SetContent(fullDataSpan);
            _page.CachedParsedData = this;
            pm.SavePage(_page);
        }
        else
        {
            int offset = 0;
            int remaining = fullDataSpan.Length;

            uint nextPageId = _page.Header.NextPageID;
            if (nextPageId == 0)
            {
                var overflowPage = pm.NewPage(PageType.Index);
                nextPageId = overflowPage.PageID;
                _page.SetLinks(_page.Header.PrevPageID, nextPageId);
                pm.SavePage(overflowPage);
            }

            _page.SetContent(fullDataSpan.Slice(offset, capacity));
            _page.CachedParsedData = this;
            pm.SavePage(_page);

            offset += capacity;
            remaining -= capacity;
            var currentPageId = nextPageId;

            while (remaining > 0)
            {
                var page = pm.GetPage(currentPageId);
                if (page.PageType != PageType.Index) page.UpdatePageType(PageType.Index);

                int chunkLen = Math.Min(remaining, page.DataCapacity - 4);
                var subChunkSpan = fullDataSpan.Slice(offset, chunkLen);

                uint nextOverflowId = page.Header.NextPageID;
                if (remaining > chunkLen)
                {
                    if (nextOverflowId == 0)
                    {
                        var newPage = pm.NewPage(PageType.Index);
                        nextOverflowId = newPage.PageID;
                        page.SetLinks(page.Header.PrevPageID, nextOverflowId);
                    }
                }
                else
                {
                    if (nextOverflowId != 0)
                    {
                        FreeChain(pm, nextOverflowId);
                        page.SetLinks(page.Header.PrevPageID, 0);
                    }
                }

                page.SetContent(subChunkSpan);
                pm.SavePage(page);

                offset += chunkLen;
                remaining -= chunkLen;
                currentPageId = nextOverflowId;
            }
        }

        _isDirty = false;
    }

    private void FreeChain(PageManager pm, uint startPageId)
    {
        uint current = startPageId;
        while (current != 0)
        {
            var page = pm.GetPage(current);
            uint next = page.Header.NextPageID;
            pm.FreePage(current);
            current = next;
        }
    }

    public int CalculateSize()
    {
        int size = 25; // Fixed header fields: IsLeaf(1)+Count(4)+Parent(4)+Next(4)+Prev(4)+EntryCount(8)

        // Keys
        foreach (var key in Keys)
        {
            size += 4; // Values count in key
            var keyValues = key.ValuesSpan;
            for (int i = 0; i < keyValues.Length; i++)
            {
                size += 1; // Type byte
                size += GetBsonValueSize(keyValues[i]);
            }
        }

        if (IsLeaf)
        {
            foreach (var val in Values)
            {
                size += 1; // Type byte
                size += GetBsonValueSize(val);
            }
        }
        else
        {
            size += ChildrenIds.Count * 4;
        }

        return size;
    }

    private int GetBsonValueSize(BsonValue val)
    {
        if (val == null || val.IsNull) return 0;
        
        switch (val.BsonType)
        {
            case BsonType.Double: return 8;
            case BsonType.String: 
                // Estimate: Length * 2 (conservative) + length prefix (4)
                return ((BsonString)val).Value.Length * 2 + 4;
            case BsonType.Int32: return 4;
            case BsonType.Int64: return 8;
            case BsonType.Boolean: return 1;
            case BsonType.DateTime: return 8;
            case BsonType.ObjectId: return 12;
            case BsonType.Decimal128: return 16;
            default: return 20; // Default for others
        }
    }

    private void LoadFromPage()
    {
        int totalLength = 0;

        uint currentPageId = _page.PageID;
        while (true)
        {
            var page = currentPageId == _page.PageID ? _page : _pm.GetPage(currentPageId);
            var span = page.ValidDataSpan;
            if (span.Length < 4) break;

            int len = BinaryPrimitives.ReadInt32LittleEndian(span);
            if (len <= 0 || len > span.Length - 4) break;

            totalLength += len;
            currentPageId = page.Header.NextPageID;
            if (currentPageId == 0) break;
        }

        if (totalLength <= 0) return;

        var rented = ArrayPool<byte>.Shared.Rent(totalLength);
        try
        {
            int offset = 0;
            currentPageId = _page.PageID;

            while (true)
            {
                var page = currentPageId == _page.PageID ? _page : _pm.GetPage(currentPageId);
                var span = page.ValidDataSpan;
                if (span.Length < 4) break;

                int len = BinaryPrimitives.ReadInt32LittleEndian(span);
                if (len <= 0 || len > span.Length - 4) break;

                span.Slice(4, len).CopyTo(rented.AsSpan(offset, len));
                offset += len;

                currentPageId = page.Header.NextPageID;
                if (currentPageId == 0) break;
            }

            var data = rented.AsSpan(0, offset);

            if (!TryParseNodeData(data, hasTreeEntryCount: true) &&
                !TryParseNodeData(data, hasTreeEntryCount: false))
            {
                throw new InvalidDataException("Invalid index node data.");
            }

            _isDirty = false;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }

    private bool TryParseNodeData(ReadOnlySpan<byte> data, bool hasTreeEntryCount)
    {
        const int maxKeyValueCount = 32;

        var reader = new BsonSpanReader(data);

        try
        {
            IsLeaf = reader.ReadBoolean();
            int keyCount = reader.ReadInt32();
            if (keyCount < 0) throw new InvalidDataException($"Invalid key count: {keyCount}");

            ParentId = unchecked((uint)reader.ReadInt32());
            NextSiblingId = unchecked((uint)reader.ReadInt32());
            PrevSiblingId = unchecked((uint)reader.ReadInt32());
            TreeEntryCount = hasTreeEntryCount ? reader.ReadInt64() : 0;

            Keys.Clear();
            if (Keys.Capacity < keyCount) Keys.Capacity = keyCount;

            for (int i = 0; i < keyCount; i++)
            {
                int valCount = reader.ReadInt32();
                if ((uint)valCount > maxKeyValueCount) throw new InvalidDataException($"Invalid key value count: {valCount}");

                var keyVals = new BsonValue[valCount];
                for (int j = 0; j < valCount; j++)
                {
                    var type = (BsonType)reader.ReadByte();
                    keyVals[j] = reader.ReadValue(type);
                }

                Keys.Add(new IndexKey(keyVals));
            }

            if (IsLeaf)
            {
                Values.Clear();
                if (Values.Capacity < keyCount) Values.Capacity = keyCount;

                for (int i = 0; i < keyCount; i++)
                {
                    var type = (BsonType)reader.ReadByte();
                    Values.Add(reader.ReadValue(type));
                }
            }
            else
            {
                ChildrenIds.Clear();
                if (ChildrenIds.Capacity < keyCount + 1) ChildrenIds.Capacity = keyCount + 1;

                for (int i = 0; i < keyCount + 1; i++)
                {
                    ChildrenIds.Add(unchecked((uint)reader.ReadInt32()));
                }
            }

            if (reader.Remaining != 0) throw new InvalidDataException("Index node data has extra bytes.");
            return true;
        }
        catch (Exception ex) when (
            ex is EndOfStreamException ||
            ex is InvalidDataException ||
            ex is InvalidOperationException ||
            ex is NotSupportedException)
        {
            return false;
        }
    }
    
    public bool IsFull(int pageSize)
    {
        // Use _page.DataCapacity if available as it represents the true space on the page
        int limit = _page.DataCapacity;
        return CalculateSize() >= limit;
    }
}
