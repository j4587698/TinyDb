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

        await Assert.That(clone.OriginalDocument).IsNull();
        await Assert.That(clone.NewDocument).IsNull();
    }

    [Test]
    public async Task Clone_WhenDocumentsProvided_ShouldCloneDocuments()
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

        await Assert.That(clone.OriginalDocument).IsNotNull();
        await Assert.That(clone.NewDocument).IsNotNull();
        await Assert.That(ReferenceEquals(clone.OriginalDocument, original)).IsFalse();
        await Assert.That(ReferenceEquals(clone.NewDocument, updated)).IsFalse();
    }
}

