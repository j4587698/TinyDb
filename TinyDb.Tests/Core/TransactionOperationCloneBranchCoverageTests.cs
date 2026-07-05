using System;
using TinyDb.Bson;
using TinyDb.Core;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Core;

public class TransactionOperationCloneBranchCoverageTests
{
    [Test]
    public async Task Clone_WhenDocumentsNull_ShouldKeepNull()
    {
        var op = new TransactionOperation(TransactionOperationType.Insert, "c", documentId: new BsonInt32(1));
        var clone = op.Clone();

        await Assert.That((object?)clone.OriginalDocument).IsNull();
        await Assert.That((object?)clone.NewDocument).IsNull();
    }

    [Test]
    public async Task Clone_WhenDocumentsProvided_ShouldPreserveImmutableDocuments()
    {
        var original = new BsonDocument().Set("x", 1);
        var updated = new BsonDocument().Set("x", 2);

        var op = new TransactionOperation(
            TransactionOperationType.Update,
            "c",
            documentId: new BsonInt32(1),
            originalDocument: original,
            newDocument: updated);

        var clone = op.Clone();

        await Assert.That((object?)clone.OriginalDocument).IsNotNull();
        await Assert.That((object?)clone.NewDocument).IsNotNull();
        await Assert.That(clone.OriginalDocument!.Equals(original)).IsTrue();
        await Assert.That(clone.NewDocument!.Equals(updated)).IsTrue();

        var changedOriginal = clone.OriginalDocument.Set("y", 3);
        var changedUpdated = clone.NewDocument.Set("y", 4);

        await Assert.That(changedOriginal.ContainsKey("y")).IsTrue();
        await Assert.That(changedUpdated.ContainsKey("y")).IsTrue();
        await Assert.That(original.ContainsKey("y")).IsFalse();
        await Assert.That(updated.ContainsKey("y")).IsFalse();
        await Assert.That(clone.OriginalDocument.ContainsKey("y")).IsFalse();
        await Assert.That(clone.NewDocument.ContainsKey("y")).IsFalse();
    }
}
