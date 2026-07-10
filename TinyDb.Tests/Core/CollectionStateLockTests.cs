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

    [Test]
    public async Task PageMutationLockRetention_ShouldHoldPageLockUntilRetentionScopeIsDisposed()
    {
        var state = new CollectionState();
        using var retention = CollectionState.RetainPageMutationLocksForCurrentContext();

        using (state.EnterPageMutationLock(42))
        {
        }

        var acquired = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var competingThread = new Thread(() =>
        {
            using var competingScope = state.EnterPageMutationLock(42);
            acquired.TrySetResult();
        })
        {
            IsBackground = true
        };

        using (ExecutionContext.SuppressFlow())
        {
            competingThread.Start();
        }

        var acquiredWhileRetained = await Task.WhenAny(acquired.Task, Task.Delay(TimeSpan.FromMilliseconds(200))) == acquired.Task;
        await Assert.That(acquiredWhileRetained).IsFalse();

        retention.Dispose();

        await acquired.Task.WaitAsync(TimeSpan.FromSeconds(2));
        await Assert.That(competingThread.Join(TimeSpan.FromSeconds(2))).IsTrue();
    }
}
