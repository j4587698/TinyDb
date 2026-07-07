namespace TinyDb.Index;

internal static class IndexSelectionPolicy
{
    public static BTreeIndex? SelectBest(IEnumerable<BTreeIndex> indexes, string[] queryFields)
    {
        return indexes
            .OrderByDescending(index => CalculateMatchScore(index, queryFields))
            .FirstOrDefault();
    }

    private static int CalculateMatchScore(BTreeIndex index, string[] queryFields)
    {
        var score = 0;
        var indexFields = index.Fields;

        for (var i = 0; i < Math.Min(indexFields.Count, queryFields.Length); i++)
        {
            if (!string.Equals(indexFields[i], queryFields[i], StringComparison.OrdinalIgnoreCase))
            {
                break;
            }

            score++;
        }

        return score > 0 && index.IsUnique ? score + 10 : score;
    }
}
