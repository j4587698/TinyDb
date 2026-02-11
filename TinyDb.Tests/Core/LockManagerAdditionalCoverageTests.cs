using System;
using System.Collections.Generic;
using System.Reflection;
using TinyDb.Core;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Core;

public class LockManagerAdditionalCoverageTests
{
    private static void DetectAndResolveDeadlocks(LockManager manager)
    {
        var method = typeof(LockManager).GetMethod("DetectAndResolveDeadlocks", BindingFlags.NonPublic | BindingFlags.Instance);
        if (method == null) throw new InvalidOperationException("DetectAndResolveDeadlocks method not found");
        method.Invoke(manager, null);
    }

    [Test]
    public async Task RequestLock_SameTransaction_SameLockType_ShouldBeGranted()
    {
        using var manager = new LockManager();
        var tx = Guid.NewGuid();

        var first = manager.RequestLock(tx, "res", LockType.Read, TimeSpan.FromSeconds(1));
        await Assert.That(first.IsGranted).IsTrue();

        var second = manager.RequestLock(tx, "res", LockType.Read, TimeSpan.FromSeconds(1));
        await Assert.That(second.IsGranted).IsTrue();
    }

    [Test]
    public async Task RequestLock_SameTransaction_WriteToRead_ShouldNotUpgrade()
    {
        using var manager = new LockManager();
        var tx = Guid.NewGuid();

        var write = manager.RequestLock(tx, "res", LockType.Write, TimeSpan.FromSeconds(1));
        await Assert.That(write.IsGranted).IsTrue();

        var downgrade = manager.RequestLock(tx, "res", LockType.Read, TimeSpan.FromSeconds(1));
        await Assert.That(downgrade.IsGranted).IsFalse();
    }

    [Test]
    public async Task RequestLock_SameTransaction_ReadToIntentWrite_ShouldNotUpgrade()
    {
        using var manager = new LockManager();
        var tx = Guid.NewGuid();

        var read = manager.RequestLock(tx, "res", LockType.Read, TimeSpan.FromSeconds(1));
        await Assert.That(read.IsGranted).IsTrue();

        var notUpgradable = manager.RequestLock(tx, "res", LockType.IntentWrite, TimeSpan.FromSeconds(1));
        await Assert.That(notUpgradable.IsGranted).IsFalse();
    }

    [Test]
    public async Task RequestLock_SameTransaction_IntentWriteOrUpdate_ToWrite_ShouldUpgrade()
    {
        using var manager = new LockManager();
        var tx1 = Guid.NewGuid();
        var tx2 = Guid.NewGuid();

        var intentWrite = manager.RequestLock(tx1, "res_intent", LockType.IntentWrite, TimeSpan.FromSeconds(1));
        await Assert.That(intentWrite.IsGranted).IsTrue();

        var upgradedFromIntent = manager.RequestLock(tx1, "res_intent", LockType.Write, TimeSpan.FromSeconds(1));
        await Assert.That(upgradedFromIntent.IsGranted).IsTrue();

        var update = manager.RequestLock(tx2, "res_update", LockType.Update, TimeSpan.FromSeconds(1));
        await Assert.That(update.IsGranted).IsTrue();

        var upgradedFromUpdate = manager.RequestLock(tx2, "res_update", LockType.Write, TimeSpan.FromSeconds(1));
        await Assert.That(upgradedFromUpdate.IsGranted).IsTrue();
    }

    [Test]
    public async Task RequestLock_DifferentTransactions_IntentWrite_ShouldNotConflictWithIntentWrite()
    {
        using var manager = new LockManager();
        var tx1 = Guid.NewGuid();
        var tx2 = Guid.NewGuid();

        var first = manager.RequestLock(tx1, "res", LockType.IntentWrite, TimeSpan.FromSeconds(1));
        await Assert.That(first.IsGranted).IsTrue();

        var second = manager.RequestLock(tx2, "res", LockType.IntentWrite, TimeSpan.FromSeconds(1));
        await Assert.That(second.IsGranted).IsTrue();
    }

    [Test]
    public async Task RequestLock_DifferentTransactions_ReadAndIntentWrite_ShouldConflictBothDirections()
    {
        using var manager = new LockManager();

        var tx1 = Guid.NewGuid();
        var tx2 = Guid.NewGuid();
        var intentWriteHolder = manager.RequestLock(tx1, "res1", LockType.IntentWrite, TimeSpan.FromSeconds(1));
        await Assert.That(intentWriteHolder.IsGranted).IsTrue();

        var readWaiter = manager.RequestLock(tx2, "res1", LockType.Read, TimeSpan.FromSeconds(1));
        await Assert.That(readWaiter.IsGranted).IsFalse();

        var tx3 = Guid.NewGuid();
        var tx4 = Guid.NewGuid();
        var readHolder = manager.RequestLock(tx3, "res2", LockType.Read, TimeSpan.FromSeconds(1));
        await Assert.That(readHolder.IsGranted).IsTrue();

        var intentWriteWaiter = manager.RequestLock(tx4, "res2", LockType.IntentWrite, TimeSpan.FromSeconds(1));
        await Assert.That(intentWriteWaiter.IsGranted).IsFalse();
    }

    [Test]
    public async Task RequestLock_DifferentTransactions_ReadAndUpdate_ShouldConflictBothDirections()
    {
        using var manager = new LockManager();

        var tx1 = Guid.NewGuid();
        var tx2 = Guid.NewGuid();
        var updateHolder = manager.RequestLock(tx1, "res1", LockType.Update, TimeSpan.FromSeconds(1));
        await Assert.That(updateHolder.IsGranted).IsTrue();

        var readWaiter = manager.RequestLock(tx2, "res1", LockType.Read, TimeSpan.FromSeconds(1));
        await Assert.That(readWaiter.IsGranted).IsFalse();

        var tx3 = Guid.NewGuid();
        var tx4 = Guid.NewGuid();
        var readHolder = manager.RequestLock(tx3, "res2", LockType.Read, TimeSpan.FromSeconds(1));
        await Assert.That(readHolder.IsGranted).IsTrue();

        var updateWaiter = manager.RequestLock(tx4, "res2", LockType.Update, TimeSpan.FromSeconds(1));
        await Assert.That(updateWaiter.IsGranted).IsFalse();
    }

    [Test]
    public async Task RequestLock_WhenNoCycle_ShouldNotThrow()
    {
        using var manager = new LockManager(TimeSpan.FromSeconds(5));
        var txA = Guid.NewGuid();
        var txB = Guid.NewGuid();

        var aOnR1 = manager.RequestLock(txA, "r1", LockType.Write, TimeSpan.FromSeconds(5));
        await Assert.That(aOnR1.IsGranted).IsTrue();

        var bWaitingOnR1 = manager.RequestLock(txB, "r1", LockType.Write, TimeSpan.FromSeconds(5));
        await Assert.That(bWaitingOnR1.IsGranted).IsFalse();

        var aOnR2 = manager.RequestLock(txA, "r2", LockType.Read, TimeSpan.FromSeconds(5));
        await Assert.That(aOnR2.IsGranted).IsTrue();
    }

    [Test]
    public async Task DetectAndResolveDeadlocks_AcyclicGraph_ShouldNotMarkVictims()
    {
        using var manager = new LockManager(TimeSpan.FromSeconds(10));
        var txA = Guid.NewGuid();
        var txB = Guid.NewGuid();

        var aOnR1 = manager.RequestLock(txA, "r1", LockType.Read, TimeSpan.FromSeconds(10));
        await Assert.That(aOnR1.IsGranted).IsTrue();

        var bOnR2 = manager.RequestLock(txB, "r2", LockType.Read, TimeSpan.FromSeconds(10));
        await Assert.That(bOnR2.IsGranted).IsTrue();

        var aWaitingOnR2 = manager.RequestLock(txA, "r2", LockType.IntentWrite, TimeSpan.FromSeconds(10));
        await Assert.That(aWaitingOnR2.IsGranted).IsFalse();

        DetectAndResolveDeadlocks(manager);

        await Assert.That(aWaitingOnR2.IsDeadlockVictim).IsFalse();
    }

    [Test]
    public async Task DetectAndResolveDeadlocks_Cycle_ShouldMarkPendingRequestsAsVictims()
    {
        using var manager = new LockManager(TimeSpan.FromSeconds(10));
        var txA = Guid.NewGuid();
        var txB = Guid.NewGuid();

        var aOnR1 = manager.RequestLock(txA, "r1", LockType.Read, TimeSpan.FromSeconds(10));
        await Assert.That(aOnR1.IsGranted).IsTrue();

        var bOnR2 = manager.RequestLock(txB, "r2", LockType.Read, TimeSpan.FromSeconds(10));
        await Assert.That(bOnR2.IsGranted).IsTrue();

        var aWaitingOnR2 = manager.RequestLock(txA, "r2", LockType.IntentWrite, TimeSpan.FromSeconds(10));
        await Assert.That(aWaitingOnR2.IsGranted).IsFalse();

        var bWaitingOnR1 = manager.RequestLock(txB, "r1", LockType.IntentWrite, TimeSpan.FromSeconds(10));
        await Assert.That(bWaitingOnR1.IsGranted).IsFalse();

        DetectAndResolveDeadlocks(manager);

        await Assert.That(aWaitingOnR2.IsDeadlockVictim || bWaitingOnR1.IsDeadlockVictim).IsTrue();
    }

    [Test]
    public async Task DeadlockDetectionTask_WhenDetectionThrows_ShouldBeSwallowed()
    {
        var manager = new LockManager();
        try
        {
            var tx = Guid.NewGuid();
            manager.RequestLock(tx, "boom", LockType.Read, TimeSpan.FromSeconds(1));

            var lockBucketsField = typeof(LockManager).GetField("_lockBuckets", BindingFlags.NonPublic | BindingFlags.Instance);
            if (lockBucketsField == null) throw new InvalidOperationException("_lockBuckets field not found");
            var lockBuckets = (System.Collections.Concurrent.ConcurrentDictionary<string, LockBucket>)lockBucketsField.GetValue(manager)!;

            var bucket = lockBuckets["boom"];
            var pendingField = typeof(LockBucket).GetField("<PendingRequests>k__BackingField", BindingFlags.NonPublic | BindingFlags.Instance);
            if (pendingField == null) throw new InvalidOperationException("PendingRequests backing field not found");
            pendingField.SetValue(bucket, null);

            await Task.Delay(TimeSpan.FromSeconds(11));
        }
        finally
        {
            manager.Dispose();
        }
    }

    [Test]
    public async Task Dispose_WhenInternalStateCorrupted_ShouldSwallowException()
    {
        var manager = new LockManager();

        var lockBucketsField = typeof(LockManager).GetField("_lockBuckets", BindingFlags.NonPublic | BindingFlags.Instance);
        if (lockBucketsField == null) throw new InvalidOperationException("_lockBuckets field not found");
        lockBucketsField.SetValue(manager, null);

        manager.Dispose();
    }

    [Test]
    public async Task CheckDeadlockRecursive_WhenVisitedAlreadyContainsNode_ShouldReturnFalse()
    {
        using var manager = new LockManager();

        var method = typeof(LockManager).GetMethod("CheckDeadlockRecursive", BindingFlags.NonPublic | BindingFlags.Instance);
        await Assert.That(method).IsNotNull();

        var tx = Guid.NewGuid();
        var visited = new HashSet<Guid> { tx };

        var result = (bool)method!.Invoke(manager, new object[] { tx, tx, "missing", LockType.Read, visited })!;
        await Assert.That(result).IsFalse();
    }

    [Test]
    public async Task CheckDeadlockRecursive_WhenResourceBucketMissing_ShouldReturnFalse()
    {
        using var manager = new LockManager();

        var method = typeof(LockManager).GetMethod("CheckDeadlockRecursive", BindingFlags.NonPublic | BindingFlags.Instance);
        await Assert.That(method).IsNotNull();

        var tx = Guid.NewGuid();
        var visited = new HashSet<Guid>();

        var result = (bool)method!.Invoke(manager, new object[] { tx, tx, "missing", LockType.Read, visited })!;
        await Assert.That(result).IsFalse();
    }
}
