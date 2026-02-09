using System;
using System.Collections.Generic;
using System.IO;
using TinyDb.Bson;
using TinyDb.Serialization;
using TinyDb.Storage;

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

        using var ms = BsonSerializer.GetRecyclableStream();
        using var writer = new BinaryWriter(ms, System.Text.Encoding.UTF8, true);
        using var bsonWriter = new BsonWriter(ms, true);

        writer.Write(IsLeaf);
        writer.Write(Keys.Count);
        writer.Write(ParentId);
        writer.Write(NextSiblingId);
        writer.Write(PrevSiblingId);
        writer.Write(TreeEntryCount);

        foreach (var key in Keys)
        {
            writer.Write(key.Values.Count);
            foreach(var val in key.Values)
            {
                writer.Write((byte)val.BsonType);
                writer.Flush(); // Ensure binary writer buffer is flushed before switching to bson writer
                bsonWriter.WriteValue(val);
            }
        }

        if (IsLeaf)
        {
            foreach (var val in Values)
            {
                writer.Write((byte)val.BsonType);
                writer.Flush();
                bsonWriter.WriteValue(val);
            }
        }
        else
        {
            foreach (var childId in ChildrenIds)
            {
                writer.Write(childId);
            }
        }
        writer.Flush(); // Flush final data

        // Use GetBuffer + SetContent(Span) optimization
        // WARNING: GetBuffer returns a buffer that might be larger than Length. We must use Slice(0, Length).
        var buffer = ms.GetBuffer();
                var fullDataSpan = new ReadOnlySpan<byte>(buffer, 0, (int)ms.Length);
                
                // Multi-page logic
                int capacity = _page.DataCapacity - 4;
                
                if (fullDataSpan.Length <= capacity)
                {
                    if (_page.Header.NextPageID != 0)
                    {
                        FreeChain(_pm, _page.Header.NextPageID);
                        _page.SetLinks(_page.Header.PrevPageID, 0); 
                    }
                                _page.SetContent(fullDataSpan);
                                // Restore cache because SetContent cleared it, and we are the valid representation
                                _page.CachedParsedData = this;
                                _pm.SavePage(_page);
                            }
                            else
                            {
                                // For large nodes exceeding page size (rare for index pages if _maxKeys is tuned right)
                                // But we must handle it. We can't pass Span to overflow pages easily without copy if we iterate.
                                // Actually SetContent takes Span, so we can Slice fullDataSpan!
                                
                                int offset = 0;
                                int remaining = fullDataSpan.Length;
                                
                                var chunkSpan = fullDataSpan.Slice(offset, capacity);
                                
                                uint nextPageId = _page.Header.NextPageID;
                                if (nextPageId == 0)
                                {
                                    var overflowPage = _pm.NewPage(PageType.Index);
                                    nextPageId = overflowPage.PageID;
                                    _page.SetLinks(_page.Header.PrevPageID, nextPageId);
                                    _pm.SavePage(overflowPage);
                                }
                                
                                _page.SetContent(chunkSpan);
                                // Restore cache
                                _page.CachedParsedData = this;
                                _pm.SavePage(_page);
                                
                                offset += capacity;
                                remaining -= capacity;
                                var currentPageId = nextPageId;
                                
                                while (remaining > 0)
                                {
                                    var page = _pm.GetPage(currentPageId);
                                    if (page.PageType != PageType.Index) page.UpdatePageType(PageType.Index);
                                    
                                    int chunkLen = Math.Min(remaining, page.DataCapacity - 4);
                                    var subChunkSpan = fullDataSpan.Slice(offset, chunkLen);
                                    
                                    uint nextOverflowId = page.Header.NextPageID;
                                    if (remaining > chunkLen)
                                    {
                                        if (nextOverflowId == 0)
                                        {
                                            var newPage = _pm.NewPage(PageType.Index);
                                            nextOverflowId = newPage.PageID;
                                            page.SetLinks(page.Header.PrevPageID, nextOverflowId);
                                        }
                                    }
                                    else
                                    {
                                        if (nextOverflowId != 0)
                                        {
                                            FreeChain(_pm, nextOverflowId);
                                            page.SetLinks(page.Header.PrevPageID, 0);
                                        }
                                    }
                                    
                                    page.SetContent(subChunkSpan);
                                    _pm.SavePage(page);
                                    
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
            foreach(var val in key.Values)
            {
                size += 1; // Type byte
                size += GetBsonValueSize(val);
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
        using var ms = new MemoryStream();
        
        var span = _page.ValidDataSpan;
        if (span.Length >= 4)
        {
            int len = BitConverter.ToInt32(span.Slice(0, 4));
            var chunk = span.Slice(4, len).ToArray();
            ms.Write(chunk, 0, chunk.Length);
        }
        
        uint nextId = _page.Header.NextPageID;
        while (nextId != 0)
        {
            var page = _pm.GetPage(nextId);
            var pSpan = page.ValidDataSpan;
            if (pSpan.Length >= 4)
            {
                int pLen = BitConverter.ToInt32(pSpan.Slice(0, 4));
                var pChunk = pSpan.Slice(4, pLen).ToArray();
                ms.Write(pChunk, 0, pChunk.Length);
            }
            nextId = page.Header.NextPageID;
        }
        
        ms.Position = 0;
        if (ms.Length == 0) return;

        using var reader = new BinaryReader(ms, System.Text.Encoding.UTF8, true);
        using var bsonReader = new BsonReader(ms, true);

        IsLeaf = reader.ReadBoolean();
        int keyCount = reader.ReadInt32();
        ParentId = reader.ReadUInt32();
        NextSiblingId = reader.ReadUInt32();
        PrevSiblingId = reader.ReadUInt32();
        
        if (ms.Length - ms.Position >= 8)
            TreeEntryCount = reader.ReadInt64();
        else
            TreeEntryCount = 0;

        Keys.Clear();
        for (int i = 0; i < keyCount; i++)
        {
            int valCount = reader.ReadInt32();
            var keyVals = new BsonValue[valCount];
            for(int j=0; j<valCount; j++)
            {
                var type = (BsonType)reader.ReadByte();
                keyVals[j] = bsonReader.ReadValue(type);
            }
            Keys.Add(new IndexKey(keyVals));
        }

        if (IsLeaf)
        {
            Values.Clear();
            for (int i = 0; i < keyCount; i++)
            {
                var type = (BsonType)reader.ReadByte();
                Values.Add(bsonReader.ReadValue(type));
            }
        }
        else
        {
            ChildrenIds.Clear();
            for (int i = 0; i < keyCount + 1; i++)
            {
                ChildrenIds.Add(reader.ReadUInt32());
            }
        }
        
        _isDirty = false;
    }
    
    public bool IsFull(int pageSize)
    {
        // Use _page.DataCapacity if available as it represents the true space on the page
        int limit = _page.DataCapacity;
        return CalculateSize() >= limit;
    }
}
