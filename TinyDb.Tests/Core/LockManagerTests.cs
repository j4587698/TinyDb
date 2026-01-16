using TinyDb.Core;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Core;

public class LockManagerTests
{
    [Test]
    public async Task RequestLock_NoConflict_ShouldGrantImmediately()
    {
        using var manager = new LockManager();
        var transId = Guid.NewGuid();
        
        var request = manager.RequestLock(transId, "res1", LockType.Read);
        
        await Assert.That(request.IsGranted).IsTrue();
    }

    [Test]
    public async Task RequestLock_ReadShared_ShouldGrantMultiple()
    {
        using var manager = new LockManager();
        var trans1 = Guid.NewGuid();
        var trans2 = Guid.NewGuid();
        
        var req1 = manager.RequestLock(trans1, "res1", LockType.Read);
        var req2 = manager.RequestLock(trans2, "res1", LockType.Read);
        
        await Assert.That(req1.IsGranted).IsTrue();
        await Assert.That(req2.IsGranted).IsTrue();
    }

    [Test]
    public async Task RequestLock_Write_ShouldBlockRead()
    {
        using var manager = new LockManager();
        var trans1 = Guid.NewGuid();
        var trans2 = Guid.NewGuid();
        
        // T1 holds Write lock
        var req1 = manager.RequestLock(trans1, "res1", LockType.Write);
        await Assert.That(req1.IsGranted).IsTrue();
        
        // T2 requests Read lock -> Should Block
        var req2 = manager.RequestLock(trans2, "res1", LockType.Read);
        await Assert.That(req2.IsGranted).IsFalse();
        
        // Release T1
        manager.ReleaseLock(req1);
        
        // T2 should now be granted (Wait a bit as TryGrantPendingLocks is synchronous but called during ReleaseLock)
        // Since ReleaseLock calls TryGrantPendingLocks synchronously, it should be granted now.
        await Assert.That(req2.IsGranted).IsTrue();
    }

    [Test]
    public async Task RequestLock_Read_ShouldBlockWrite()
    {
        using var manager = new LockManager();
        var trans1 = Guid.NewGuid();
        var trans2 = Guid.NewGuid();
        
        // T1 holds Read lock
        var req1 = manager.RequestLock(trans1, "res1", LockType.Read);
        await Assert.That(req1.IsGranted).IsTrue();
        
        // T2 requests Write lock -> Should Block
        var req2 = manager.RequestLock(trans2, "res1", LockType.Write);
        await Assert.That(req2.IsGranted).IsFalse();
        
        // Release T1
        manager.ReleaseLock(req1);
        
        // T2 should now be granted
        await Assert.That(req2.IsGranted).IsTrue();
    }

    [Test]
    public async Task ReleaseAllLocks_ShouldWork()
    {
        using var manager = new LockManager();
        var t1 = Guid.NewGuid();
        manager.RequestLock(t1, "R1", LockType.Read);
        manager.RequestLock(t1, "R2", LockType.Write);
        
        await Assert.That(manager.GetStatistics().ActiveLockCount).IsEqualTo(2);
        
        manager.ReleaseAllLocks(t1);
        await Assert.That(manager.GetStatistics().ActiveLockCount).IsEqualTo(0);
    }

    [Test]
    public async Task LockConflicts_ShouldBeDetected()
    {
        using var manager = new LockManager();
        var t1 = Guid.NewGuid();
        var t2 = Guid.NewGuid();
        
        var req1 = manager.RequestLock(t1, "R1", LockType.Write);
        
        // T2 requests Read on R1 - should be pending
        var req2 = manager.RequestLock(t2, "R1", LockType.Read, TimeSpan.FromMilliseconds(100));
        await Assert.That(req2.IsGranted).IsFalse();
        
        manager.ReleaseLock(req1);
    }
}
