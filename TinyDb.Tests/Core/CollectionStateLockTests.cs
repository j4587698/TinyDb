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

        // 用同步事件而不是 TaskCompletionSource：TCS 的续体要排到线程池，
        // CI 上全量测试并行时线程池饱和会让续体延迟数秒，产生假超时。
        using var attempted = new ManualResetEventSlim(false);
        using var acquired = new ManualResetEventSlim(false);
        var competingThread = new Thread(() =>
        {
            attempted.Set();
            using var competingScope = state.EnterPageMutationLock(42);
            acquired.Set();
        })
        {
            IsBackground = true,
            Name = "page-mutation-lock-competitor"
        };

        using (ExecutionContext.SuppressFlow())
        {
            competingThread.Start();
        }

        // 先确认竞争线程确实开始尝试，避免"线程还没跑起来就断言未获取"的时序假象。
        await Assert.That(attempted.Wait(TimeSpan.FromSeconds(10))).IsTrue();
        await Assert.That(acquired.Wait(TimeSpan.FromMilliseconds(200))).IsFalse();

        retention.Dispose();

        // 释放是同步的，正常会立刻唤醒等待者；给足宽限以容忍 CI 的 CPU 抢占。
        await Assert.That(acquired.Wait(TimeSpan.FromSeconds(10))).IsTrue();
        await Assert.That(competingThread.Join(TimeSpan.FromSeconds(10))).IsTrue();
    }
}
