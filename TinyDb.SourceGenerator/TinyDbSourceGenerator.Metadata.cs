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

    private static AttributeData? GetFirstAttribute(ImmutableArray<AttributeData> attributes)
    {
        return attributes.IsDefaultOrEmpty ? null : attributes[0];
    }

    private static AttributeData? FindAttribute(ImmutableArray<AttributeData> attributes, string name)
    {
        if (attributes.IsDefaultOrEmpty) return null;

        foreach (var attribute in attributes)
        {
            if (attribute.AttributeClass?.Name == name)
            {
                return attribute;
            }
        }

        return null;
    }

    private static AttributeData? FindAttribute(ImmutableArray<AttributeData> attributes, string name, string alternateName)
    {
        if (attributes.IsDefaultOrEmpty) return null;

        foreach (var attribute in attributes)
        {
            var attributeName = attribute.AttributeClass?.Name;
            if (attributeName == name || attributeName == alternateName)
            {
                return attribute;
            }
        }

        return null;
    }

    private static bool HasAttribute(ImmutableArray<AttributeData> attributes, string name)
    {
        return FindAttribute(attributes, name) != null;
    }

    private static bool HasAttribute(ImmutableArray<AttributeData> attributes, string name, string alternateName)
    {
        return FindAttribute(attributes, name, alternateName) != null;
    }

    private static bool TryGetNamedArgument(AttributeData attribute, string name, out TypedConstant value)
    {
        foreach (var argument in attribute.NamedArguments)
        {
            if (argument.Key == name)
            {
                value = argument.Value;
                return true;
            }
        }

        value = default;
        return false;
    }

    private static string? GetConstructorString(AttributeData? attribute, int index)
    {
        if (attribute == null || attribute.ConstructorArguments.Length <= index) return null;
        return attribute.ConstructorArguments[index].Value?.ToString();
    }

    private static string? GetNamedString(AttributeData? attribute, string name)
    {
        if (attribute == null) return null;

        return TryGetNamedArgument(attribute, name, out var argument)
            ? argument.Value?.ToString()
            : null;
    }

    private static int GetNamedInt(AttributeData? attribute, string name, int defaultValue = 0)
    {
        if (attribute == null) return defaultValue;

        return TryGetNamedArgument(attribute, name, out var argument) && argument.Value is int value
            ? value
            : defaultValue;
    }

    private static bool GetNamedBool(AttributeData? attribute, string name, bool defaultValue = false)
    {
        if (attribute == null) return defaultValue;

        return TryGetNamedArgument(attribute, name, out var argument) && argument.Value is bool value
            ? value
            : defaultValue;
    }

    private static string GetEntityMetadataDisplayName(INamedTypeSymbol? classSymbol, string fallback)
    {
        if (classSymbol == null) return fallback;

        foreach (var attribute in classSymbol.GetAttributes())
        {
            if (attribute.AttributeClass?.Name != "EntityMetadataAttribute") continue;

            var displayName = GetConstructorString(attribute, 0) ?? GetNamedString(attribute, "DisplayName");
            if (!string.IsNullOrEmpty(displayName)) return displayName!;
        }

        return fallback;
    }

    private static string? GetEntityMetadataDescription(INamedTypeSymbol? classSymbol)
    {
        if (classSymbol == null) return null;

        foreach (var attribute in classSymbol.GetAttributes())
        {
            if (attribute.AttributeClass?.Name != "EntityMetadataAttribute") continue;

            var description = GetNamedString(attribute, "Description");
            if (!string.IsNullOrEmpty(description)) return description;
        }

        return null;
    }

    private static AttributeData? GetPropertyMetadataAttribute(ISymbol symbol)
    {
        return FindAttribute(symbol.GetAttributes(), "PropertyMetadataAttribute");
    }
}
