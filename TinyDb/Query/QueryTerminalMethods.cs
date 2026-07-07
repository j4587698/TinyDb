namespace TinyDb.Query;

internal static class QueryTerminalMethods
{
    private static readonly HashSet<string> Names = new(StringComparer.Ordinal)
    {
        "Count",
        "LongCount",
        "Any",
        "All",
        "First",
        "FirstOrDefault",
        "Single",
        "SingleOrDefault",
        "Last",
        "LastOrDefault",
        "ElementAt",
        "ElementAtOrDefault"
    };

    public static bool Contains(string name) => Names.Contains(name);
}
