using System;
using System.Collections.Generic;
using System.Text;
using TinyDb.Bson;
using TinyDb.Storage;
using TinyDb.Serialization;
using TinyDb.Query;

namespace TinyDb.Core;

internal sealed class DataPageAccess
{
    private readonly PageManager _pm;
    private readonly LargeDocumentStorage _lds;
    private readonly WriteAheadLog _wal;

    private const int InternalReserved = 0;
    private static readonly byte[] IsLargeDocumentFieldNameBytes = Encoding.UTF8.GetBytes("_isLargeDocument");

    public DataPageAccess(PageManager pm, LargeDocumentStorage lds, WriteAheadLog wal)
    {
        _pm = pm;
        _lds = lds;
        _wal = wal;
    }

    public static int GetEntrySize(int len) => len + 4;
    public int GetMaxDocumentSize() => (int)_pm.PageSize - 300;

    public IEnumerable<BsonDocument> ScanDocumentsFromPage(Page p)
    {
        int count = p.Header.ItemCount;
        var mem = p.Memory;
        int offset = Page.DataStartOffset + InternalReserved;
        int endOffset = p.PageSize - p.Header.FreeBytes;

        for (int i = 0; i < count; i++)
        {
            if (offset + 4 > endOffset) break;
            int len = BitConverter.ToInt32(mem.Span.Slice(offset, 4));
            offset += 4;

            if (offset + len > endOffset) break;

            var slice = mem.Slice(offset, len);
            offset += len;

            BsonDocument? doc = null;
            try
            {
                doc = BsonSerializer.DeserializeDocument(slice);
            }
            catch { }

            if (doc != null)
            {
                yield return doc;
            }
        }
    }

    public List<PageDocumentEntry> ReadDocumentsFromPage(Page p)
    {
        if (p.CachedParsedData is List<PageDocumentEntry> cached)
        {
            return new List<PageDocumentEntry>(cached);
        }

        var res = new List<PageDocumentEntry>();
        int count = p.Header.ItemCount;
        var span = p.ValidDataSpan;
        int offset = InternalReserved;
        
        for (int i = 0; i < count; i++)
        {
            if (offset + 4 > span.Length) break;
            int len = BitConverter.ToInt32(span.Slice(offset, 4));
            offset += 4;
            
            if (offset + len > span.Length) break;
            var bytes = span.Slice(offset, len).ToArray();
            offset += len;
            
            try
            {
                var doc = BsonSerializer.DeserializeDocument(bytes);
                bool isL = doc.TryGetValue("_isLargeDocument", out var v) && v.ToBoolean(null);
                uint lId = isL ? (uint)doc["_largeDocumentIndex"].ToInt64(null) : 0;
                int lS = isL ? (int)doc["_largeDocumentSize"].ToInt64(null) : 0;
                res.Add(new PageDocumentEntry(doc, bytes, isL, lId, lS));
            }
            catch { }
        }

        p.CachedParsedData = res;
        return new List<PageDocumentEntry>(res);
    }

    public PageDocumentEntry? ReadDocumentAt(Page p, int index)
    {
        int count = p.Header.ItemCount;
        if (index >= count) return null;

        var span = p.ValidDataSpan;
        int offset = InternalReserved;

        // Skip previous documents
        for (int i = 0; i < index; i++)
        {
            if (offset + 4 > span.Length) return null;
            int len = BitConverter.ToInt32(span.Slice(offset, 4));
            offset += 4 + len;
        }

        // Read target document
        if (offset + 4 > span.Length) return null;
        int targetLen = BitConverter.ToInt32(span.Slice(offset, 4));
        offset += 4;
        if (offset + targetLen > span.Length) return null;

        var bytes = span.Slice(offset, targetLen).ToArray();
        try
        {
            var doc = BsonSerializer.DeserializeDocument(bytes);
            bool isL = doc.TryGetValue("_isLargeDocument", out var v) && v.ToBoolean(null);
            uint lId = isL ? (uint)doc["_largeDocumentIndex"].ToInt64(null) : 0;
            int lS = isL ? (int)doc["_largeDocumentSize"].ToInt64(null) : 0;
            return new PageDocumentEntry(doc, bytes, isL, lId, lS);
        }
        catch { return null; }
    }

    public BsonDocument? ReadDocumentAt(Page p, int index, HashSet<string>? fields)
    {
        int count = p.Header.ItemCount;
        if (index >= count) return null;

        var span = p.ValidDataSpan;
        int offset = InternalReserved;

        // Skip previous documents
        for (int i = 0; i < index; i++)
        {
            if (offset + 4 > span.Length) return null;
            int len = BitConverter.ToInt32(span.Slice(offset, 4));
            offset += 4 + len;
        }

        // Read target document
        if (offset + 4 > span.Length) return null;
        int targetLen = BitConverter.ToInt32(span.Slice(offset, 4));
        offset += 4;
        if (offset + targetLen > span.Length) return null;

        var bytes = span.Slice(offset, targetLen).ToArray();
        try
        {
            // Always ensure system fields are loaded to check for large document
            if (fields != null)
            {
                if (!fields.Contains("_isLargeDocument")) fields.Add("_isLargeDocument");
                if (!fields.Contains("_largeDocumentIndex")) fields.Add("_largeDocumentIndex");
                if (!fields.Contains("_largeDocumentSize")) fields.Add("_largeDocumentSize");
            }

            var doc = fields != null
                ? BsonSerializer.DeserializeDocument(bytes, fields)
                : BsonSerializer.DeserializeDocument(bytes);

            bool isL = doc.TryGetValue("_isLargeDocument", out var v) && v.ToBoolean(null);
            if (isL)
            {
                uint lId = (uint)doc["_largeDocumentIndex"].ToInt64(null);
                var largeBytes = _lds.ReadLargeDocument(lId);
                return fields != null
                   ? BsonSerializer.DeserializeDocument(largeBytes, fields)
                   : BsonSerializer.DeserializeDocument(largeBytes);
            }

            return doc;
        }
        catch { return null; }
    }

    public void AppendDocumentToPage(Page p, ReadOnlySpan<byte> bytes)
    {
        p.Append(bytes);
    }

    public IEnumerable<ReadOnlyMemory<byte>> ScanRawDocumentsFromPage(Page p, ScanPredicate[]? predicates = null)
    {
        foreach (var result in ScanRawDocumentsFromPageWithPredicateInfo(p, predicates))
        {
            yield return result.Slice;
        }
    }

    public IEnumerable<RawScanResult> ScanRawDocumentsFromPageWithPredicateInfo(Page p, ScanPredicate[]? predicates = null)
    {
        int count = p.Header.ItemCount;
        var mem = p.Memory;
        int offset = Page.DataStartOffset + InternalReserved;
        int endOffset = p.PageSize - p.Header.FreeBytes;

        for (int i = 0; i < count; i++)
        {
            if (offset + 4 > endOffset) break;
            int len = BitConverter.ToInt32(mem.Span.Slice(offset, 4));
            offset += 4;

            if (offset + len > endOffset) break;

            var slice = mem.Slice(offset, len);
            offset += len;

            if (predicates == null || predicates.Length == 0)
            {
                yield return new RawScanResult(slice, requiresPostFilter: true);
                continue;
            }

            if (!TryMatchPredicates(slice.Span, predicates, out bool definitiveMatch))
            {
                continue;
            }

            yield return new RawScanResult(slice, requiresPostFilter: !definitiveMatch);
        }
    }

    private static bool TryMatchPredicates(ReadOnlySpan<byte> document, ScanPredicate[] predicates, out bool definitiveMatch)
    {
        definitiveMatch = false;

        if (predicates.Length == 0)
        {
            definitiveMatch = true;
            return true;
        }

        if (predicates.Length > 64)
        {
            return TryMatchPredicatesSlow(document, predicates, out definitiveMatch);
        }

        ulong remainingMask = 0;
        for (int i = 0; i < predicates.Length; i++)
        {
            if (!predicates[i].IsEmpty) remainingMask |= 1UL << i;
        }

        if (remainingMask == 0)
        {
            definitiveMatch = true;
            return true;
        }

        if (document.Length < 5) return true;

        bool hasUnknown = false;
        bool isLargeDocument = false;
        bool parseComplete = false;

        int offset = 4;

        while (offset < document.Length)
        {
            var type = (BsonType)document[offset];
            offset++;

            if (type == BsonType.End)
            {
                parseComplete = true;
                break;
            }

            int nameEnd = offset;
            while (nameEnd < document.Length && document[nameEnd] != 0) nameEnd++;
            if (nameEnd >= document.Length) break;

            var nameSpan = document.Slice(offset, nameEnd - offset);
            offset = nameEnd + 1;

            int valueOffset = offset;

            if (!isLargeDocument && type == BsonType.Boolean && nameSpan.SequenceEqual(IsLargeDocumentFieldNameBytes))
            {
                if (valueOffset < document.Length && document[valueOffset] != 0) isLargeDocument = true;
            }

            if (remainingMask != 0)
            {
                for (int i = 0; i < predicates.Length; i++)
                {
                    ulong bit = 1UL << i;
                    if ((remainingMask & bit) == 0) continue;

                    var pred = predicates[i];
                    if (pred.IsEmpty)
                    {
                        remainingMask &= ~bit;
                        continue;
                    }

                    if (!nameSpan.SequenceEqual(pred.FieldNameBytes) &&
                        !(pred.AlternateFieldNameBytes is { Length: > 0 } alt1 && nameSpan.SequenceEqual(alt1)) &&
                        !(pred.SecondAlternateFieldNameBytes is { Length: > 0 } alt2 && nameSpan.SequenceEqual(alt2)))
                    {
                        continue;
                    }

                    if (BinaryPredicateEvaluator.TryEvaluate(document, valueOffset, type, pred.Operator, pred.TargetValue, pred.TargetStringUtf8Bytes, out bool evalResult))
                    {
                        if (!evalResult) return false;
                    }
                    else
                    {
                        hasUnknown = true;
                    }

                    remainingMask &= ~bit;
                }

                if (remainingMask == 0) break;
            }

            if (!BsonScanner.TrySkipValue(type, document, ref offset))
            {
                hasUnknown = true;
                break;
            }
        }

        if (remainingMask == 0)
        {
            definitiveMatch = !hasUnknown && !isLargeDocument;
            return true;
        }

        if (isLargeDocument)
        {
            definitiveMatch = false;
            return true;
        }

        if (!parseComplete)
        {
            definitiveMatch = false;
            return true;
        }

        if (remainingMask != 0)
        {
            // Missing fields are treated as null for non-large documents (matches ExpressionEvaluator.Compare null semantics).
            for (int i = 0; i < predicates.Length; i++)
            {
                ulong bit = 1UL << i;
                if ((remainingMask & bit) == 0) continue;

                var pred = predicates[i];
                if (pred.IsEmpty)
                {
                    remainingMask &= ~bit;
                    continue;
                }

                if (BinaryPredicateEvaluator.TryEvaluate(document, 0, BsonType.Null, pred.Operator, pred.TargetValue, pred.TargetStringUtf8Bytes, out bool evalResult))
                {
                    if (!evalResult) return false;
                }
                else
                {
                    hasUnknown = true;
                }

                remainingMask &= ~bit;
            }
        }

        definitiveMatch = !hasUnknown && remainingMask == 0;
        return true;
    }

    private static bool TryMatchPredicatesSlow(ReadOnlySpan<byte> document, ScanPredicate[] predicates, out bool definitiveMatch)
    {
        definitiveMatch = false;
        if (document.Length < 5) return true;

        foreach (var pred in predicates)
        {
            if (pred.IsEmpty) continue;

            bool found = BsonScanner.TryLocateField(document, pred.FieldNameBytes, out int valueOffset, out var type);
            if (!found && pred.AlternateFieldNameBytes is { Length: > 0 } alt1)
            {
                found = BsonScanner.TryLocateField(document, alt1, out valueOffset, out type);
            }
            if (!found && pred.SecondAlternateFieldNameBytes is { Length: > 0 } alt2)
            {
                found = BsonScanner.TryLocateField(document, alt2, out valueOffset, out type);
            }

            if (!found) continue;

            if (BinaryPredicateEvaluator.TryEvaluate(document, valueOffset, type, pred.Operator, pred.TargetValue, pred.TargetStringUtf8Bytes, out bool evalResult))
            {
                if (!evalResult) return false;
            }
        }

        return true;
    }

    public bool CanFitInPage(Page p, List<PageDocumentEntry> docs)
    {
        long total = 0;
        foreach (var d in docs) total += 4 + d.RawBytes.Length; // 4 bytes len + content
        return total <= p.DataCapacity;
    }

    public (Page Page, bool IsNew) GetWritableDataPageLocked(DataPageState st, int req)
    {
        if (st.PageId != 0)
        {
            var p = _pm.GetPage(st.PageId);
            if (p.Header.PageType == PageType.Data && p.Header.FreeBytes >= req) return (p, false);
        }
        var n = _pm.NewPage(PageType.Data);
        n.ResetBytes(InternalReserved);
        st.PageId = n.PageID;
        return (n, true);
    }

    public void RewritePageWithDocuments(string col, CollectionState st, Page p, List<PageDocumentEntry> docs, Action<BsonValue, uint, ushort> updIdx)
    {
        // Preserve Links: Read -> Reset -> Set
        uint prev = p.Header.PrevPageID;
        uint next = p.Header.NextPageID;
        p.ResetBytes(InternalReserved);
        p.SetLinks(prev, next);
        
        for (ushort i = 0; i < docs.Count; i++)
        {
            AppendDocumentToPage(p, docs[i].RawBytes);
            var id = docs[i].Id;
            if (id != null) updIdx(id, p.PageID, i);
        }
        _pm.SavePage(p, false);
    }

    public void PersistPage(Page p)
    {
        _pm.SavePage(p, false);
    }
}
