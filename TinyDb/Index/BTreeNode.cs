using TinyDb.Bson;

namespace TinyDb.Index;

/// <summary>
/// Disk-backed BTreeIndex node wrapper.
/// </summary>
public sealed class BTreeNode
{
    private readonly DiskBTreeNode _diskNode;
    private readonly DiskBTree _diskTree;

    public bool IsDiskMode => true;

    public BTreeNode(DiskBTreeNode diskNode, DiskBTree diskTree)
    {
        _diskNode = diskNode ?? throw new ArgumentNullException(nameof(diskNode));
        _diskTree = diskTree ?? throw new ArgumentNullException(nameof(diskTree));
    }

    public bool IsLeaf => _diskNode.IsLeaf;

    public int KeyCount => _diskNode.KeyCount;

    public int ChildCount => IsLeaf ? 0 : _diskNode.ChildrenIds.Count;

    public int DocumentCount => IsLeaf ? _diskNode.Values.Count : 0; // Approximate for disk mode

    public int MaxKeys => _diskTree.MaxKeys;
    public int MinKeys => Math.Max(1, MaxKeys / 2);

    public IndexKey GetKey(int index) => _diskNode.Keys[index];

    public BTreeNode GetChild(int index)
    {
        if (IsLeaf) throw new InvalidOperationException("Leaf node has no children");
        var childId = _diskNode.ChildrenIds[index];
        var childNode = _diskTree.LoadNode(childId);
        return new BTreeNode(childNode, _diskTree);
    }

    public int FindKeyPosition(IndexKey key)
    {
        int count = KeyCount;
        for (int i = 0; i < count; i++)
        {
            if (key.CompareTo(GetKey(i)) < 0) return i;
            if (key.CompareTo(GetKey(i)) == 0) return i;
        }
        return count;
    }

    public IReadOnlyList<BsonValue> GetDocumentIds(int index)
    {
         return new List<BsonValue> { _diskNode.Values[index] };
    }

    public void SetKey(int index, IndexKey key) => throw new NotSupportedException();
    public void SetChild(int index, BTreeNode child) => throw new NotSupportedException();
    public void RemoveKeyAt(int index) => throw new NotSupportedException();
    public void RemoveChildAt(int index) => throw new NotSupportedException();

    public BTreeNode? Parent { get; set; }
    public BTreeNode? NextSibling { get; set; }
    public BTreeNode? PreviousSibling { get; set; }

    public BTreeNodeStatistics GetStatistics()
    {
        return new BTreeNodeStatistics
        {
            IsLeaf = IsLeaf,
            KeyCount = KeyCount,
            ChildCount = ChildCount,
            DocumentCount = DocumentCount,
            MaxKeys = MaxKeys,
            MinKeys = MinKeys,
            IsFull = KeyCount >= MaxKeys,
            NeedsMerge = KeyCount < MinKeys
        };
    }

    public override string ToString()
    {
        var type = IsLeaf ? "Leaf" : "Internal";
        return $"BTreeNode({type}): {KeyCount} keys";
    }
}

public sealed class SplitResult
{
    public BTreeNode NewNode { get; init; } = null!;
    public IndexKey PromotedKey { get; init; } = null!;
}

public sealed class BTreeNodeStatistics
{
    public bool IsLeaf { get; init; }
    public int KeyCount { get; init; }
    public int ChildCount { get; init; }
    public int DocumentCount { get; init; }
    public int MaxKeys { get; init; }
    public int MinKeys { get; init; }
    public bool IsFull { get; init; }
    public bool NeedsMerge { get; init; }
}
