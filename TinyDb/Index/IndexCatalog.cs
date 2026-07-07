using System.Diagnostics.CodeAnalysis;

namespace TinyDb.Index;

internal readonly record struct PersistedIndexDefinition(
    string Name,
    string[] Fields,
    bool IsUnique,
    bool IsSparse,
    uint RootPageId,
    int MaxKeys);

internal sealed class IndexCatalog : IDisposable
{
    private readonly Dictionary<string, BTreeIndex> _indexes = new(StringComparer.Ordinal);

    public int Count => _indexes.Count;

    public IReadOnlyCollection<BTreeIndex> Indexes => _indexes.Values;

    public IReadOnlyList<string> Names => _indexes.Keys.ToArray();

    public bool Contains(string name)
    {
        return _indexes.ContainsKey(name);
    }

    public bool TryGet(string name, [NotNullWhen(true)] out BTreeIndex? index)
    {
        return _indexes.TryGetValue(name, out index);
    }

    public void Add(BTreeIndex index)
    {
        _indexes.Add(index.Name, index);
    }

    public bool TryAddLoaded(BTreeIndex index)
    {
        return _indexes.TryAdd(index.Name, index);
    }

    public bool Remove(string name, [NotNullWhen(true)] out BTreeIndex? index)
    {
        return _indexes.Remove(name, out index);
    }

    public void ClearIndexes()
    {
        foreach (var index in _indexes.Values)
        {
            index.Clear();
        }
    }

    public IReadOnlyList<PersistedIndexDefinition> CreateDefinitionSnapshot()
    {
        return _indexes.Values
            .OrderBy(index => index.Name, StringComparer.Ordinal)
            .Select(index => new PersistedIndexDefinition(
                index.Name,
                index.Fields.ToArray(),
                index.IsUnique,
                index.IsSparse,
                index.RootPageId,
                index.MaxKeys))
            .ToArray();
    }

    public IndexValidationResult ValidateAll()
    {
        var result = new IndexValidationResult { TotalIndexes = _indexes.Count };

        foreach (var index in _indexes.Values)
        {
            if (!index.Validate())
            {
                result.InvalidIndexes++;
                result.Errors.Add($"Index '{index.Name}' failed validation.");
                continue;
            }

            result.ValidIndexes++;
        }

        return result;
    }

    public void Dispose()
    {
        foreach (var index in _indexes.Values)
        {
            index.Dispose();
        }

        _indexes.Clear();
    }
}
