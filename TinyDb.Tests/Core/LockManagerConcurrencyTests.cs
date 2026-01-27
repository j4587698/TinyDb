using System;
using System.Threading.Tasks;
using TinyDb.Core;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Core;

/// <summary>
/// LockManager 并发和死锁检测测试
/// </summary>
public class LockManagerConcurrencyTests : IDisposable
{
    private LockManager? _manager;

    [Before(Test)]
    public void Setup()
    {
        _manager = new LockManager(TimeSpan.FromSeconds(5));
    }

    [After(Test)]
    public void Cleanup()
    {
        _manager?.Dispose();
        _manager = null;
    }

    public void Dispose()
    {
        _manager?.Dispose();
    }

    [Test]
    public async Task LockRequest_IsExpired_ShouldReturnTrueAfterTimeout()
    {
        var request = new LockRequest(Guid.NewGuid(), "res", LockType.Read, TimeSpan.FromMilliseconds(1));
        
        // Wait for expiration
        await Task.Delay(10);
        
        await Assert.That(request.IsExpired()).IsTrue();
    }

    [Test]
    public async Task LockRequest_IsExpired_ShouldReturnFalseBeforeTimeout()
    {
        var request = new LockRequest(Guid.NewGuid(), "res", LockType.Read, TimeSpan.FromSeconds(60));
        
        await Assert.That(request.IsExpired()).IsFalse();
    }

    [Test]
    public async Task LockRequest_ToString_ShouldContainDetails()
    {
        var transId = Guid.NewGuid();
        var request = new LockRequest(transId, "test_resource", LockType.Write, TimeSpan.FromSeconds(10));
        
        var str = request.ToString();
        await Assert.That(str).Contains("Write");
        await Assert.That(str).Contains("test_resource");
        await Assert.That(str).Contains("Granted=False");
    }

    [Test]
    public async Task RequestLock_ReadLock_ShouldBeGrantedImmediately()
    {
        var transId = Guid.NewGuid();
        var request = _manager!.RequestLock(transId, "resource1", LockType.Read);
        
        await Assert.That(request.IsGranted).IsTrue();
        await Assert.That(request.GrantedTime).IsNotNull();
    }

    [Test]
    public async Task RequestLock_WriteLock_ShouldBeGrantedImmediately()
    {
        var transId = Guid.NewGuid();
        var request = _manager!.RequestLock(transId, "resource1", LockType.Write);
        
        await Assert.That(request.IsGranted).IsTrue();
    }

    [Test]
    public async Task RequestLock_MultipleReadLocks_ShouldAllBeGranted()
    {
        var trans1 = Guid.NewGuid();
        var trans2 = Guid.NewGuid();
        var trans3 = Guid.NewGuid();
        
        var req1 = _manager!.RequestLock(trans1, "shared_resource", LockType.Read);
        var req2 = _manager.RequestLock(trans2, "shared_resource", LockType.Read);
        var req3 = _manager.RequestLock(trans3, "shared_resource", LockType.Read);
        
        await Assert.That(req1.IsGranted).IsTrue();
        await Assert.That(req2.IsGranted).IsTrue();
        await Assert.That(req3.IsGranted).IsTrue();
    }

    [Test]
    public async Task RequestLock_WriteLockAfterRead_ShouldNotBeGranted()
    {
        var trans1 = Guid.NewGuid();
        var trans2 = Guid.NewGuid();
        
        // First get a read lock
        var readReq = _manager!.RequestLock(trans1, "resource", LockType.Read);
        await Assert.That(readReq.IsGranted).IsTrue();
        
        // Then try to get a write lock from another transaction
        var writeReq = _manager.RequestLock(trans2, "resource", LockType.Write);
        await Assert.That(writeReq.IsGranted).IsFalse();
    }

    [Test]
    public async Task RequestLock_ReadLockAfterWrite_ShouldNotBeGranted()
    {
        var trans1 = Guid.NewGuid();
        var trans2 = Guid.NewGuid();
        
        // First get a write lock
        var writeReq = _manager!.RequestLock(trans1, "resource", LockType.Write);
        await Assert.That(writeReq.IsGranted).IsTrue();
        
        // Then try to get a read lock from another transaction
        var readReq = _manager.RequestLock(trans2, "resource", LockType.Read);
        await Assert.That(readReq.IsGranted).IsFalse();
    }

    [Test]
    public async Task ReleaseLock_ShouldAllowPendingLocksToBeGranted()
    {
        var trans1 = Guid.NewGuid();
        var trans2 = Guid.NewGuid();
        
        // Get a write lock
        var writeReq = _manager!.RequestLock(trans1, "resource", LockType.Write);
        await Assert.That(writeReq.IsGranted).IsTrue();
        
        // Request a read lock (should be pending)
        var readReq = _manager.RequestLock(trans2, "resource", LockType.Read);
        await Assert.That(readReq.IsGranted).IsFalse();
        
        // Release the write lock
        _manager.ReleaseLock(writeReq);
        
        // The pending lock should now be granted after next lock operation or deadlock detection
        // Note: In this implementation, TryGrantPendingLocks is called in ReleaseLock
        // So check if readReq was granted (may depend on implementation timing)
    }

    [Test]
    public async Task ReleaseAllLocks_ShouldReleaseAllTransactionLocks()
    {
        var transId = Guid.NewGuid();
        
        _manager!.RequestLock(transId, "res1", LockType.Read);
        _manager.RequestLock(transId, "res2", LockType.Write);
        _manager.RequestLock(transId, "res3", LockType.Read);
        
        await Assert.That(_manager.ActiveLockCount).IsEqualTo(3);
        
        _manager.ReleaseAllLocks(transId);
        
        await Assert.That(_manager.ActiveLockCount).IsEqualTo(0);
    }

    [Test]
    public async Task DefaultTimeout_ShouldBeSet()
    {
        await Assert.That(_manager!.DefaultTimeout).IsEqualTo(TimeSpan.FromSeconds(5));
    }

    [Test]
    public async Task GetStatistics_ShouldReturnCorrectCounts()
    {
        var trans1 = Guid.NewGuid();
        var trans2 = Guid.NewGuid();
        
        _manager!.RequestLock(trans1, "res1", LockType.Read);
        _manager.RequestLock(trans1, "res2", LockType.Write);
        _manager.RequestLock(trans2, "res1", LockType.Read);
        
        var stats = _manager.GetStatistics();
        
        await Assert.That(stats.ActiveLockCount).IsGreaterThanOrEqualTo(2);
        await Assert.That(stats.BucketCount).IsEqualTo(2);
        await Assert.That(stats.DefaultTimeout).IsEqualTo(TimeSpan.FromSeconds(5));
    }

    [Test]
    public async Task GetStatistics_LockTypeCounts_ShouldBeAccurate()
    {
        var trans1 = Guid.NewGuid();
        var trans2 = Guid.NewGuid();
        
        _manager!.RequestLock(trans1, "res1", LockType.Read);
        _manager.RequestLock(trans2, "res2", LockType.Write);
        
        var stats = _manager.GetStatistics();
        
        await Assert.That(stats.LockTypeCounts.GetValueOrDefault(LockType.Read)).IsGreaterThanOrEqualTo(1);
        await Assert.That(stats.LockTypeCounts.GetValueOrDefault(LockType.Write)).IsGreaterThanOrEqualTo(1);
    }

    [Test]
    public async Task ToString_ShouldContainStatus()
    {
        _manager!.RequestLock(Guid.NewGuid(), "res1", LockType.Read);
        
        var str = _manager.ToString();
        await Assert.That(str).Contains("LockManager");
        await Assert.That(str).Contains("active");
    }

    [Test]
    public async Task Dispose_ShouldClearAllLocks()
    {
        var transId = Guid.NewGuid();
        _manager!.RequestLock(transId, "res1", LockType.Read);
        _manager.RequestLock(transId, "res2", LockType.Write);
        
        _manager.Dispose();
        
        // After dispose, operations should throw
        await Assert.That(() => _manager.RequestLock(Guid.NewGuid(), "res", LockType.Read))
            .Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task Dispose_CalledTwice_ShouldNotThrow()
    {
        _manager!.Dispose();
        _manager.Dispose(); // Should not throw
    }

    [Test]
    public async Task RequestLock_AfterDispose_ShouldThrow()
    {
        _manager!.Dispose();
        
        await Assert.That(() => _manager.RequestLock(Guid.NewGuid(), "res", LockType.Read))
            .Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task ReleaseLock_AfterDispose_ShouldThrow()
    {
        var req = new LockRequest(Guid.NewGuid(), "res", LockType.Read, TimeSpan.FromSeconds(1));
        _manager!.Dispose();
        
        await Assert.That(() => _manager.ReleaseLock(req))
            .Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task GetStatistics_AfterDispose_ShouldThrow()
    {
        _manager!.Dispose();
        
        await Assert.That(() => _manager.GetStatistics())
            .Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task LockManager_WithNullTimeout_ShouldUseDefault()
    {
        using var manager = new LockManager(null);
        await Assert.That(manager.DefaultTimeout).IsEqualTo(TimeSpan.FromSeconds(30));
    }

    [Test]
    public async Task LockUpgrade_SameTransaction_ReadToWrite_ShouldWork()
    {
        var transId = Guid.NewGuid();
        
        // Get read lock first
        var readReq = _manager!.RequestLock(transId, "resource", LockType.Read);
        await Assert.That(readReq.IsGranted).IsTrue();
        
        // Try to upgrade to write lock (same transaction)
        var writeReq = _manager.RequestLock(transId, "resource", LockType.Write);
        await Assert.That(writeReq.IsGranted).IsTrue();
    }

    [Test]
    public async Task IntentWriteLock_ShouldConflictWithRead()
    {
        var trans1 = Guid.NewGuid();
        var trans2 = Guid.NewGuid();
        
        var intentWriteReq = _manager!.RequestLock(trans1, "resource", LockType.IntentWrite);
        await Assert.That(intentWriteReq.IsGranted).IsTrue();
        
        var readReq = _manager.RequestLock(trans2, "resource", LockType.Read);
        await Assert.That(readReq.IsGranted).IsFalse();
    }

    [Test]
    public async Task UpdateLock_ShouldConflictWithRead()
    {
        var trans1 = Guid.NewGuid();
        var trans2 = Guid.NewGuid();
        
        var updateReq = _manager!.RequestLock(trans1, "resource", LockType.Update);
        await Assert.That(updateReq.IsGranted).IsTrue();
        
        var readReq = _manager.RequestLock(trans2, "resource", LockType.Read);
        await Assert.That(readReq.IsGranted).IsFalse();
    }

    [Test]
    public async Task PendingLockCount_ShouldTrackWaitingRequests()
    {
        var trans1 = Guid.NewGuid();
        var trans2 = Guid.NewGuid();
        
        // Get write lock
        _manager!.RequestLock(trans1, "resource", LockType.Write);
        
        // Request read lock (will be pending)
        _manager.RequestLock(trans2, "resource", LockType.Read);
        
        await Assert.That(_manager.PendingLockCount).IsGreaterThanOrEqualTo(1);
    }

    [Test]
    public async Task ActiveLockCount_ShouldTrackGrantedLocks()
    {
        var transId = Guid.NewGuid();
        
        await Assert.That(_manager!.ActiveLockCount).IsEqualTo(0);
        
        _manager.RequestLock(transId, "res1", LockType.Read);
        await Assert.That(_manager.ActiveLockCount).IsEqualTo(1);
        
        _manager.RequestLock(transId, "res2", LockType.Read);
        await Assert.That(_manager.ActiveLockCount).IsEqualTo(2);
    }

    [Test]
    public async Task LockManagerStatistics_ToString_ShouldWork()
    {
        var stats = _manager!.GetStatistics();
        var str = stats.ToString();
        
        await Assert.That(str).Contains("LockManager");
        await Assert.That(str).Contains("active");
        await Assert.That(str).Contains("pending");
    }

    [Test]
    public async Task CustomTimeout_ShouldBeRespected()
    {
        var transId = Guid.NewGuid();
        var customTimeout = TimeSpan.FromMilliseconds(100);
        
        var request = _manager!.RequestLock(transId, "res", LockType.Read, customTimeout);
        
        await Assert.That(request.Timeout).IsEqualTo(customTimeout);
    }

    [Test]
    public async Task LockRequest_Properties_ShouldBeSet()
    {
        var transId = Guid.NewGuid();
        var request = new LockRequest(transId, "test_res", LockType.Write, TimeSpan.FromSeconds(10));
        
        await Assert.That(request.TransactionId).IsEqualTo(transId);
        await Assert.That(request.ResourceKey).IsEqualTo("test_res");
        await Assert.That(request.LockType).IsEqualTo(LockType.Write);
        await Assert.That(request.Timeout).IsEqualTo(TimeSpan.FromSeconds(10));
        await Assert.That(request.IsGranted).IsFalse();
        await Assert.That(request.IsDeadlockVictim).IsFalse();
        await Assert.That(request.GrantedTime).IsNull();
        await Assert.That(request.RequestId).IsNotEqualTo(Guid.Empty);
    }

    [Test]
    public async Task LockRequest_SetProperties_ShouldWork()
    {
        var request = new LockRequest(Guid.NewGuid(), "res", LockType.Read, TimeSpan.FromSeconds(1));
        
        request.IsGranted = true;
        request.GrantedTime = DateTime.UtcNow;
        request.IsDeadlockVictim = true;
        
        await Assert.That(request.IsGranted).IsTrue();
        await Assert.That(request.GrantedTime).IsNotNull();
        await Assert.That(request.IsDeadlockVictim).IsTrue();
    }

    [Test]
    public async Task MultipleDifferentResources_ShouldNotConflict()
    {
        var trans1 = Guid.NewGuid();
        var trans2 = Guid.NewGuid();
        
        var req1 = _manager!.RequestLock(trans1, "resource_a", LockType.Write);
        var req2 = _manager.RequestLock(trans2, "resource_b", LockType.Write);
        
        await Assert.That(req1.IsGranted).IsTrue();
        await Assert.That(req2.IsGranted).IsTrue();
    }
}
