using TinyDb.Bson;
using TinyDb.Core;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Core;

public class CollectionStateLockTests
{
    [Test]
    public async Task EnterDocumentLockAsync_ShouldAllowSameExecutionContextToReenterSynchronously()
    {
        var state = new CollectionState();
        var id = new BsonInt32(1);

        using var asyncScope = await state.EnterDocumentLockAsync(id);
        await Task.Yield();
        using var syncScope = state.EnterDocumentLock(id);

        await Assert.That(syncScope).IsNotNull();
    }
}
