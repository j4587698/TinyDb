using TinyDb.Core;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Core;

public class LockManagerEdgeCasesTests
{
    [Test]
    public async Task RequestLock_With_Invalid_Arguments_Should_Throw()
    {
        using var manager = new LockManager();
        var transId = Guid.NewGuid();
        
        await Assert.That(() => manager.RequestLock(Guid.Empty, "res", LockType.Read)).Throws<ArgumentException>();
        await Assert.That(() => manager.RequestLock(transId, null!, LockType.Read)).Throws<ArgumentException>();
        await Assert.That(() => manager.RequestLock(transId, "", LockType.Read)).Throws<ArgumentException>();
    }

    [Test]
    public async Task ReleaseLock_With_Null_Should_Throw()
    {
        using var manager = new LockManager();
        await Assert.That(() => manager.ReleaseLock(null!)).Throws<ArgumentNullException>();
    }

    [Test]
    public async Task ReleaseLock_With_NonExistent_Resource_Should_NoOp()
    {
        using var manager = new LockManager();
        // Create a dummy request not tracked by manager
        var req = new LockRequest(Guid.NewGuid(), "nonexistent", LockType.Read, TimeSpan.Zero);
        
        // Should not throw
        manager.ReleaseLock(req);
    }

    [Test]
    public async Task ReleaseAllLocks_With_Empty_Guid_Should_NoOp()
    {
        using var manager = new LockManager();
        manager.ReleaseAllLocks(Guid.Empty);
    }

    [Test]
    public async Task GetStatistics_Should_Return_Valid_Stats()
    {
        using var manager = new LockManager();
        var transId = Guid.NewGuid();
        manager.RequestLock(transId, "res1", LockType.Read);
        
        var stats = manager.GetStatistics();
        await Assert.That(stats.ActiveLockCount).IsEqualTo(1);
        await Assert.That(stats.ToString()).IsNotNull();
    }
}
