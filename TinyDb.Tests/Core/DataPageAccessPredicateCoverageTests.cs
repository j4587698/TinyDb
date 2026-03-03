using System;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using TinyDb.Bson;
using TinyDb.Core;
using TinyDb.Query;
using TinyDb.Serialization;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Core;

public sealed class DataPageAccessPredicateCoverageTests
{
    private delegate bool TryMatchPredicatesDelegate(ReadOnlySpan<byte> document, ScanPredicate[] predicates, out bool definitiveMatch);

    private static readonly TryMatchPredicatesDelegate TryMatchPredicates = CreateDelegate("TryMatchPredicates");
    private static readonly TryMatchPredicatesDelegate TryMatchPredicatesSlow = CreateDelegate("TryMatchPredicatesSlow");

    [Test]
    public async Task TryMatchPredicates_BaseBranches_ShouldReturnExpectedFlags()
    {
        var doc = BsonSerializer.SerializeDocument(new BsonDocument().Set("a", 1));

        var okEmpty = TryMatchPredicates(doc, Array.Empty<ScanPredicate>(), out var definitiveEmpty);
        await Assert.That(okEmpty).IsTrue();
        await Assert.That(definitiveEmpty).IsTrue();

        var onlyEmptyPredicates = new[]
        {
            new ScanPredicate(Array.Empty<byte>(), null, null, null, ExpressionType.Equal),
            new ScanPredicate(Array.Empty<byte>(), null, null, null, ExpressionType.NotEqual)
        };

        var okOnlyEmpty = TryMatchPredicates(doc, onlyEmptyPredicates, out var definitiveOnlyEmpty);
        await Assert.That(okOnlyEmpty).IsTrue();
        await Assert.That(definitiveOnlyEmpty).IsTrue();
    }

    [Test]
    public async Task TryMatchPredicates_ParseAndNullSemanticsBranches_ShouldBeCovered()
    {
        var largeDoc = BsonSerializer.SerializeDocument(
            new BsonDocument()
                .Set("_isLargeDocument", true)
                .Set("a", 1));

        var missingEqOne = new[]
        {
            CreatePredicate("missing", 1, ExpressionType.Equal)
        };

        var largeResult = TryMatchPredicates(largeDoc, missingEqOne, out var largeDefinitive);
        await Assert.That(largeResult).IsTrue();
        await Assert.That(largeDefinitive).IsFalse();

        var malformed = new byte[]
        {
            12, 0, 0, 0,
            (byte)BsonType.String,
            (byte)'a', 0,
            5, 0, 0, 0,
            (byte)'x'
        };

        var malformedPredicates = new[]
        {
            CreatePredicate("missing", null, ExpressionType.Equal)
        };

        var malformedResult = TryMatchPredicates(malformed, malformedPredicates, out var malformedDefinitive);
        await Assert.That(malformedResult).IsTrue();
        await Assert.That(malformedDefinitive).IsFalse();

        var normalDoc = BsonSerializer.SerializeDocument(new BsonDocument().Set("a", 1));
        var missingEqNull = new[]
        {
            CreatePredicate("missing", null, ExpressionType.Equal)
        };
        var missingNeNull = new[]
        {
            CreatePredicate("missing", null, ExpressionType.NotEqual)
        };

        var nullEqResult = TryMatchPredicates(normalDoc, missingEqNull, out var nullEqDefinitive);
        await Assert.That(nullEqResult).IsTrue();
        await Assert.That(nullEqDefinitive).IsTrue();

        var nullNeResult = TryMatchPredicates(normalDoc, missingNeNull, out var nullNeDefinitive);
        await Assert.That(nullNeResult).IsFalse();
        await Assert.That(nullNeDefinitive).IsFalse();
    }

    [Test]
    public async Task TryMatchPredicates_SlowPathBranches_ShouldBeCovered()
    {
        var doc = BsonSerializer.SerializeDocument(new BsonDocument().Set("Name", "Alice"));

        var slowPredicates = new ScanPredicate[65];
        slowPredicates[0] = new ScanPredicate(Array.Empty<byte>(), null, null, null, ExpressionType.Equal);
        slowPredicates[1] = CreatePredicate("name", "Alice", ExpressionType.Equal, "Name");
        slowPredicates[2] = CreatePredicate("notFound", "Alice", ExpressionType.Equal, "stillNotFound", "Name");
        for (int i = 3; i < slowPredicates.Length; i++)
        {
            slowPredicates[i] = new ScanPredicate(Array.Empty<byte>(), null, null, null, ExpressionType.Equal);
        }

        var slowResult = TryMatchPredicates(doc, slowPredicates, out var slowDefinitive);
        await Assert.That(slowResult).IsTrue();
        await Assert.That(slowDefinitive).IsFalse();

        var shortDoc = new byte[] { 1, 2, 3, 4 };
        var shortResult = TryMatchPredicatesSlow(shortDoc, new[] { CreatePredicate("a", 1, ExpressionType.Equal) }, out var shortDefinitive);
        await Assert.That(shortResult).IsTrue();
        await Assert.That(shortDefinitive).IsFalse();

        var slowFalsePredicates = new ScanPredicate[65];
        slowFalsePredicates[0] = CreatePredicate("Name", "Bob", ExpressionType.Equal);
        for (int i = 1; i < slowFalsePredicates.Length; i++)
        {
            slowFalsePredicates[i] = new ScanPredicate(Array.Empty<byte>(), null, null, null, ExpressionType.Equal);
        }

        var slowFalseResult = TryMatchPredicates(doc, slowFalsePredicates, out var slowFalseDefinitive);
        await Assert.That(slowFalseResult).IsFalse();
        await Assert.That(slowFalseDefinitive).IsFalse();
    }

    private static ScanPredicate CreatePredicate(string field, object? value, ExpressionType op, string? alt1 = null, string? alt2 = null)
    {
        var fieldBytes = Encoding.UTF8.GetBytes(field);
        var alt1Bytes = alt1 != null ? Encoding.UTF8.GetBytes(alt1) : null;
        var alt2Bytes = alt2 != null ? Encoding.UTF8.GetBytes(alt2) : null;
        return new ScanPredicate(fieldBytes, alt1Bytes, alt2Bytes, value, op);
    }

    private static TryMatchPredicatesDelegate CreateDelegate(string methodName)
    {
        var method = typeof(DataPageAccess).GetMethod(methodName, BindingFlags.NonPublic | BindingFlags.Static)
            ?? throw new MissingMethodException(typeof(DataPageAccess).FullName, methodName);
        return (TryMatchPredicatesDelegate)method.CreateDelegate(typeof(TryMatchPredicatesDelegate));
    }
}
