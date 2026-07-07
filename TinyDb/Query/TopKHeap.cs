using TinyDb.Bson;

namespace TinyDb.Query;

internal readonly struct TopKRow
{
    public BsonValue Id { get; }
    public SortKey[] Keys { get; }
    public long Sequence { get; }
    public BsonDocument? TransactionDocument { get; }

    public TopKRow(BsonValue id, SortKey[] keys, long sequence, BsonDocument? transactionDocument)
    {
        Id = id;
        Keys = keys;
        Sequence = sequence;
        TransactionDocument = transactionDocument;
    }
}

internal static class TopKHeap
{
    public static void AddOrReplaceWorst(List<TopKRow> heap, TopKRow row, int capacity, IReadOnlyList<QuerySortField> sort)
    {
        if (heap.Count < capacity)
        {
            heap.Add(row);
            HeapifyUp(heap, heap.Count - 1, sort);
            return;
        }

        if (QuerySortKeyReader.CompareRows(row, heap[0], sort) < 0)
        {
            heap[0] = row;
            HeapifyDown(heap, 0, sort);
        }
    }

    public static void SortBestFirst(List<TopKRow> heap, IReadOnlyList<QuerySortField> sort)
    {
        heap.Sort((a, b) => QuerySortKeyReader.CompareRows(a, b, sort));
    }

    private static void HeapifyUp(List<TopKRow> heap, int index, IReadOnlyList<QuerySortField> sort)
    {
        while (index > 0)
        {
            var parent = (index - 1) / 2;
            if (QuerySortKeyReader.CompareRows(heap[index], heap[parent], sort) <= 0) break;

            (heap[parent], heap[index]) = (heap[index], heap[parent]);
            index = parent;
        }
    }

    private static void HeapifyDown(List<TopKRow> heap, int index, IReadOnlyList<QuerySortField> sort)
    {
        var count = heap.Count;
        while (true)
        {
            var left = (index * 2) + 1;
            if (left >= count) break;

            var right = left + 1;
            var largest = left;
            if (right < count && QuerySortKeyReader.CompareRows(heap[right], heap[left], sort) > 0)
            {
                largest = right;
            }

            if (QuerySortKeyReader.CompareRows(heap[largest], heap[index], sort) <= 0) break;

            (heap[largest], heap[index]) = (heap[index], heap[largest]);
            index = largest;
        }
    }
}
