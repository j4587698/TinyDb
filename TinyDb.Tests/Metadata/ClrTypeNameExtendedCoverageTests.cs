using System;
using System.Collections.Generic;
using System.Reflection;
using TinyDb.Metadata;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Metadata;

public sealed class ClrTypeNameExtendedCoverageTests
{
    private sealed class NestedHolder
    {
        public sealed class InnerType;
    }

    [Test]
    public async Task GetStableName_ShouldCoverGenericParameterArrayAndNestedTypes()
    {
        var genericParam = typeof(List<>).GetGenericArguments()[0];
        var genericName = ClrTypeName.GetStableName(genericParam);
        await Assert.That(genericName).IsEqualTo("T");

        var arrayName = ClrTypeName.GetStableName(typeof(Dictionary<string, int>[]));
        await Assert.That(arrayName).Contains("System.Collections.Generic.Dictionary");
        await Assert.That(arrayName).Contains("[]");

        var nestedName = ClrTypeName.GetStableName(typeof(NestedHolder.InnerType));
        await Assert.That(nestedName).Contains("NestedHolder+InnerType");
    }

    [Test]
    public async Task Normalize_ShouldCoverGlobalPrefixAssemblyQualifiedAndFallback()
    {
        var assemblyQualified = "global::System.String, mscorlib";
        var normalized = ClrTypeName.Normalize(assemblyQualified);
        await Assert.That(normalized).IsEqualTo("System.String");

        var nested = ClrTypeName.Normalize(typeof(NestedHolder.InnerType).FullName!);
        await Assert.That(nested).Contains("NestedHolder.InnerType");

        // Malformed reflection-like generic name should hit graceful fallback path.
        var malformed = ClrTypeName.Normalize("System.Collections.Generic.List`1[[");
        await Assert.That(malformed).Contains("List<>");
    }

    [Test]
    public async Task NormalizeForComparison_And_ToCSharpTypeSyntax_ShouldCoverNullableAndAliases()
    {
        var n1 = ClrTypeName.NormalizeForComparison("System.Nullable<System.Int32>");
        await Assert.That(n1).IsEqualTo("System.Int32");

        var n2 = ClrTypeName.NormalizeForComparison("int?");
        await Assert.That(n2).IsEqualTo("int");

        var t1 = ClrTypeName.ToCSharpTypeSyntax("System.Nullable<System.Int64>", useAliases: true, out var nullable1);
        await Assert.That(t1).IsEqualTo("long");
        await Assert.That(nullable1).IsTrue();

        var t2 = ClrTypeName.ToCSharpTypeSyntax("System.String?", useAliases: true, out var nullable2);
        await Assert.That(t2).IsEqualTo("string");
        await Assert.That(nullable2).IsTrue();

        var t3 = ClrTypeName.ToCSharpTypeSyntax("System.Byte[]", useAliases: true, out var nullable3);
        await Assert.That(t3).IsEqualTo("byte[]");
        await Assert.That(nullable3).IsFalse();

        var t4 = ClrTypeName.ToCSharpTypeSyntax("TinyDb.CustomType", useAliases: false, out _);
        await Assert.That(t4).IsEqualTo("TinyDb.CustomType");
    }

    [Test]
    public async Task ToCSharpTypeSyntax_ShouldCoverAliasSwitchAndReflectionGenericArrayParsing()
    {
        var aliasCases = new (string Input, string Expected)[]
        {
            ("System.Boolean", "bool"),
            ("System.Byte", "byte"),
            ("System.SByte", "sbyte"),
            ("System.Char", "char"),
            ("System.Int16", "short"),
            ("System.UInt16", "ushort"),
            ("System.UInt32", "uint"),
            ("System.UInt64", "ulong"),
            ("System.Single", "float"),
            ("System.Double", "double"),
            ("System.Decimal", "decimal"),
            ("System.Object", "object")
        };

        foreach (var (input, expected) in aliasCases)
        {
            var alias = ClrTypeName.ToCSharpTypeSyntax(input, useAliases: true, out _);
            await Assert.That(alias).IsEqualTo(expected);
        }

        var dictReflectionName = typeof(Dictionary<string, int>).FullName!;
        var dictNormalized = ClrTypeName.Normalize(dictReflectionName);
        await Assert.That(dictNormalized).Contains("System.Collections.Generic.Dictionary<System.String,System.Int32>");

        var listArrayReflectionName = typeof(List<int>[]).FullName!;
        var listArrayNormalized = ClrTypeName.Normalize(listArrayReflectionName);
        await Assert.That(listArrayNormalized).Contains("[]");
    }

    [Test]
    public async Task PrivateAssemblyQualifiedParser_ShouldCoverBracketDepthBranches()
    {
        var parseAssemblyQualifiedArgument = typeof(ClrTypeName).GetMethod(
            "ParseAssemblyQualifiedArgument",
            BindingFlags.Static | BindingFlags.NonPublic)
            ?? throw new MissingMethodException(typeof(ClrTypeName).FullName, "ParseAssemblyQualifiedArgument");

        var argsBreakOnRightBracket = new object?[] { "]", 0 };
        var parsedBreak = (string)parseAssemblyQualifiedArgument.Invoke(null, argsBreakOnRightBracket)!;
        await Assert.That(parsedBreak).IsEqualTo(string.Empty);

        var argsNestedBracket = new object?[] { "[System.String], mscorlib]", 0 };
        var parsedNested = (string)parseAssemblyQualifiedArgument.Invoke(null, argsNestedBracket)!;
        await Assert.That(parsedNested).IsNotNull();

        // Keep this malformed reflection-generic input to exercise Normalize fallback behavior.
        var malformedInput = "System.Collections.Generic.List`[[";
        var malformed = ClrTypeName.Normalize(malformedInput);
        await Assert.That(malformed).IsEqualTo(malformedInput);
    }
}
