using System;

namespace TinyDb.SourceGenerator;

internal static class SourceGeneratorFieldName
{
    public static string ToCamelCase(string name)
    {
        if (string.IsNullOrEmpty(name)) return name;

        var firstLetter = 0;
        while (firstLetter < name.Length && name[firstLetter] == '_')
        {
            firstLetter++;
        }

        if (firstLetter >= name.Length || char.IsLower(name[firstLetter]))
        {
            return name;
        }

        var chars = name.ToCharArray();
        for (var i = firstLetter; i < chars.Length && char.IsUpper(chars[i]); i++)
        {
            if (i > firstLetter && i + 1 < chars.Length && char.IsLower(chars[i + 1]))
            {
                break;
            }

            chars[i] = char.ToLowerInvariant(chars[i]);
        }

        return new string(chars);
    }
}
