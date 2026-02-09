using System;
using System.IO;
using System.Reflection;
using System.Reflection.Metadata;
using System.Reflection.PortableExecutable;
using TinyDb.Query;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Query;

public class QueryableToEnumerableRewriterAdditionalCoverageTests
{
    private static string GetAssemblyPath(Assembly assembly)
    {
        var name = assembly.GetName().Name ?? throw new InvalidOperationException("Assembly name is missing.");
        var path = Path.Combine(AppContext.BaseDirectory, $"{name}.dll");
        if (!File.Exists(path))
        {
            throw new FileNotFoundException($"Assembly file not found: {path}", path);
        }

        return path;
    }

    private static bool ContainsType(Assembly assembly, string fullTypeName)
    {
        using var stream = File.OpenRead(GetAssemblyPath(assembly));
        using var peReader = new PEReader(stream);
        var reader = peReader.GetMetadataReader();

        foreach (var handle in reader.TypeDefinitions)
        {
            var type = reader.GetTypeDefinition(handle);
            var ns = reader.GetString(type.Namespace);
            var name = reader.GetString(type.Name);
            var current = string.IsNullOrEmpty(ns) ? name : $"{ns}.{name}";
            if (current == fullTypeName)
            {
                return true;
            }
        }

        return false;
    }

    private static bool ContainsMethod(Assembly assembly, string fullTypeName, string methodName)
    {
        using var stream = File.OpenRead(GetAssemblyPath(assembly));
        using var peReader = new PEReader(stream);
        var reader = peReader.GetMetadataReader();

        foreach (var handle in reader.TypeDefinitions)
        {
            var type = reader.GetTypeDefinition(handle);
            var ns = reader.GetString(type.Namespace);
            var name = reader.GetString(type.Name);
            var current = string.IsNullOrEmpty(ns) ? name : $"{ns}.{name}";
            if (current != fullTypeName)
            {
                continue;
            }

            foreach (var methodHandle in type.GetMethods())
            {
                var method = reader.GetMethodDefinition(methodHandle);
                if (reader.GetString(method.Name) == methodName)
                {
                    return true;
                }
            }

            return false;
        }

        return false;
    }

    [Test]
    public async Task ExecuteInMemory_ShouldBeRemoved_InAotOnlyMode()
    {
        var asm = typeof(ExpressionParser).Assembly;

        await Assert.That(ContainsType(asm, "TinyDb.Query.QueryPipeline")).IsTrue();
        await Assert.That(ContainsMethod(asm, "TinyDb.Query.QueryPipeline", "ExecuteInMemory")).IsFalse();
    }

    [Test]
    public async Task QueryableToEnumerableRewriter_ShouldBeRemoved_InAotOnlyMode()
    {
        var asm = typeof(ExpressionParser).Assembly;
        await Assert.That(ContainsType(asm, "TinyDb.Query.QueryableToEnumerableRewriter")).IsFalse();
    }

    [Test]
    public async Task QueryableToEnumerableRewriter_Visit_ShouldNotExist_InAotOnlyMode()
    {
        var asm = typeof(ExpressionParser).Assembly;
        await Assert.That(ContainsType(asm, "TinyDb.Query.QueryableToEnumerableRewriter")).IsFalse();
    }
}
