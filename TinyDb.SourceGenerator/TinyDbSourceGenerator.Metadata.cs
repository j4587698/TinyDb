using System;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Text;
using System.Collections.Immutable;
using System.Text;

namespace TinyDb.SourceGenerator;

public partial class TinyDbSourceGenerator
{

    private static int GetEnumConstantValue(TypedConstant constant)
    {
        return constant.Value is int value ? value : 0;
    }

    private static string? GetConstructorString(AttributeData? attribute, int index)
    {
        if (attribute == null || attribute.ConstructorArguments.Length <= index) return null;
        return attribute.ConstructorArguments[index].Value?.ToString();
    }

    private static string? GetNamedString(AttributeData? attribute, string name)
    {
        if (attribute == null) return null;

        var argument = attribute.NamedArguments.FirstOrDefault(arg => arg.Key == name);
        return argument.Value.Value?.ToString();
    }

    private static int GetNamedInt(AttributeData? attribute, string name, int defaultValue = 0)
    {
        if (attribute == null) return defaultValue;

        var argument = attribute.NamedArguments.FirstOrDefault(arg => arg.Key == name);
        return argument.Value.Value is int value ? value : defaultValue;
    }

    private static bool GetNamedBool(AttributeData? attribute, string name, bool defaultValue = false)
    {
        if (attribute == null) return defaultValue;

        var argument = attribute.NamedArguments.FirstOrDefault(arg => arg.Key == name);
        return argument.Value.Value is bool value ? value : defaultValue;
    }

    private static string GetEntityMetadataDisplayName(INamedTypeSymbol? classSymbol, string fallback)
    {
        if (classSymbol == null) return fallback;

        foreach (var attribute in classSymbol.GetAttributes().Where(attr => attr.AttributeClass?.Name == "EntityMetadataAttribute"))
        {
            var displayName = GetConstructorString(attribute, 0) ?? GetNamedString(attribute, "DisplayName");
            if (!string.IsNullOrEmpty(displayName)) return displayName!;
        }

        return fallback;
    }

    private static string? GetEntityMetadataDescription(INamedTypeSymbol? classSymbol)
    {
        if (classSymbol == null) return null;

        foreach (var attribute in classSymbol.GetAttributes().Where(attr => attr.AttributeClass?.Name == "EntityMetadataAttribute"))
        {
            var description = GetNamedString(attribute, "Description");
            if (!string.IsNullOrEmpty(description)) return description;
        }

        return null;
    }

    private static AttributeData? GetPropertyMetadataAttribute(ISymbol symbol)
    {
        return symbol.GetAttributes()
            .FirstOrDefault(attr => attr.AttributeClass?.Name == "PropertyMetadataAttribute");
    }
}
