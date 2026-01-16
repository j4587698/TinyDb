using System;
using System.Threading;
using System.Threading.Tasks;
using TinyDb.Core;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Core;

[NotInParallel]
public class LockManagerExtendedTests
{
    [Test]
    public async Task LockManager_TryAcquire_Timeout_Behavior()
    {
        var lm = new LockManager();
        var resource = "res1";
        var owner1 = Guid.NewGuid();
        var owner2 = Guid.NewGuid();

        // Owner1 takes exclusive lock
        var req1 = lm.RequestLock(owner1, resource, LockType.Write, TimeSpan.FromSeconds(10));
        await Assert.That(req1.IsGranted).IsTrue();

        // Owner2 tries to take exclusive lock with short timeout
        // RequestLock returns immediately, IsGranted should be false
        var req2 = lm.RequestLock(owner2, resource, LockType.Write, TimeSpan.FromMilliseconds(100));
        await Assert.That(req2.IsGranted).IsFalse();
        
        // Wait for timeout
        await Task.Delay(200);
        
        // Owner1 releases lock. This triggers processing of pending requests.
        // req2 should be expired, so it should NOT be granted.
        lm.ReleaseLock(req1);
        
        await Assert.That(req2.IsGranted).IsFalse();
    }

    [Test]
    public async Task LockManager_Upgrade_ReadToWrite_Success()
    {
        var lm = new LockManager();
        var resource = "res1";
        var owner = Guid.NewGuid();

        var req1 = lm.RequestLock(owner, resource, LockType.Read, TimeSpan.FromSeconds(1));
        await Assert.That(req1.IsGranted).IsTrue();
        
        // Upgrade to exclusive - should succeed because same owner and no other readers
        var req2 = lm.RequestLock(owner, resource, LockType.Write, TimeSpan.FromSeconds(1));
        await Assert.That(req2.IsGranted).IsTrue();
    }
    
    [Test]
    public async Task LockManager_Upgrade_ReadToWrite_Fail_Shared()
    {
        var lm = new LockManager();
        var resource = "res1";
        var owner1 = Guid.NewGuid();
        var owner2 = Guid.NewGuid();

        // Owner1 takes read lock
        var req1 = lm.RequestLock(owner1, resource, LockType.Read, TimeSpan.FromSeconds(10));
        await Assert.That(req1.IsGranted).IsTrue();
        
        // Owner2 takes read lock (shared)
        var req2 = lm.RequestLock(owner2, resource, LockType.Read, TimeSpan.FromSeconds(10));
        await Assert.That(req2.IsGranted).IsTrue();
        
        // Owner1 tries to upgrade to Write - should fail/wait because Owner2 holds read lock
        var reqUpgrade = lm.RequestLock(owner1, resource, LockType.Write, TimeSpan.FromMilliseconds(100));
        await Assert.That(reqUpgrade.IsGranted).IsFalse();
    }
}
