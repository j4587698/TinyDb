using System;
using TinyDb.Core;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Core;

/// <summary>
/// Coverage tests for LockRequest class
/// </summary>
public class LockRequestCoverageTests
{
    #region Constructor Tests

    [Test]
    public async Task Constructor_ShouldInitializeAllProperties()
    {
        var transactionId = Guid.NewGuid();
        var resourceKey = "test_resource";
        var lockType = LockType.Read;
        var timeout = TimeSpan.FromSeconds(30);

        var request = new LockRequest(transactionId, resourceKey, lockType, timeout);

        await Assert.That(request.TransactionId).IsEqualTo(transactionId);
        await Assert.That(request.ResourceKey).IsEqualTo(resourceKey);
        await Assert.That(request.LockType).IsEqualTo(lockType);
        await Assert.That(request.Timeout).IsEqualTo(timeout);
        await Assert.That(request.RequestId).IsNotEqualTo(Guid.Empty);
        await Assert.That(request.IsGranted).IsFalse();
        await Assert.That(request.IsDeadlockVictim).IsFalse();
        await Assert.That(request.GrantedTime).IsNull();
    }

    [Test]
    public async Task Constructor_WithWriteLockType_ShouldSetCorrectly()
    {
        var request = new LockRequest(Guid.NewGuid(), "resource", LockType.Write, TimeSpan.FromSeconds(10));
        await Assert.That(request.LockType).IsEqualTo(LockType.Write);
    }

    [Test]
    public async Task Constructor_WithIntentWriteLockType_ShouldSetCorrectly()
    {
        var request = new LockRequest(Guid.NewGuid(), "resource", LockType.IntentWrite, TimeSpan.FromSeconds(10));
        await Assert.That(request.LockType).IsEqualTo(LockType.IntentWrite);
    }

    [Test]
    public async Task Constructor_WithUpdateLockType_ShouldSetCorrectly()
    {
        var request = new LockRequest(Guid.NewGuid(), "resource", LockType.Update, TimeSpan.FromSeconds(10));
        await Assert.That(request.LockType).IsEqualTo(LockType.Update);
    }

    [Test]
    public async Task Constructor_RequestTime_ShouldBeRecentUtcNow()
    {
        var before = DateTime.UtcNow;
        var request = new LockRequest(Guid.NewGuid(), "resource", LockType.Read, TimeSpan.FromSeconds(10));
        var after = DateTime.UtcNow;

        await Assert.That(request.RequestTime).IsGreaterThanOrEqualTo(before);
        await Assert.That(request.RequestTime).IsLessThanOrEqualTo(after);
    }

    [Test]
    public async Task Constructor_MultipleRequests_ShouldHaveUniqueRequestIds()
    {
        var transactionId = Guid.NewGuid();
        var request1 = new LockRequest(transactionId, "resource", LockType.Read, TimeSpan.FromSeconds(10));
        var request2 = new LockRequest(transactionId, "resource", LockType.Read, TimeSpan.FromSeconds(10));

        await Assert.That(request1.RequestId).IsNotEqualTo(request2.RequestId);
    }

    #endregion

    #region Property Setter Tests

    [Test]
    public async Task IsGranted_CanBeSetToTrue()
    {
        var request = new LockRequest(Guid.NewGuid(), "resource", LockType.Read, TimeSpan.FromSeconds(10));
        request.IsGranted = true;
        await Assert.That(request.IsGranted).IsTrue();
    }

    [Test]
    public async Task IsGranted_CanBeSetToFalseAfterTrue()
    {
        var request = new LockRequest(Guid.NewGuid(), "resource", LockType.Read, TimeSpan.FromSeconds(10));
        request.IsGranted = true;
        request.IsGranted = false;
        await Assert.That(request.IsGranted).IsFalse();
    }

    [Test]
    public async Task IsDeadlockVictim_CanBeSetToTrue()
    {
        var request = new LockRequest(Guid.NewGuid(), "resource", LockType.Read, TimeSpan.FromSeconds(10));
        request.IsDeadlockVictim = true;
        await Assert.That(request.IsDeadlockVictim).IsTrue();
    }

    [Test]
    public async Task IsDeadlockVictim_CanBeSetToFalseAfterTrue()
    {
        var request = new LockRequest(Guid.NewGuid(), "resource", LockType.Read, TimeSpan.FromSeconds(10));
        request.IsDeadlockVictim = true;
        request.IsDeadlockVictim = false;
        await Assert.That(request.IsDeadlockVictim).IsFalse();
    }

    [Test]
    public async Task GrantedTime_CanBeSet()
    {
        var request = new LockRequest(Guid.NewGuid(), "resource", LockType.Read, TimeSpan.FromSeconds(10));
        var grantedTime = DateTime.UtcNow;
        request.GrantedTime = grantedTime;
        await Assert.That(request.GrantedTime).IsEqualTo(grantedTime);
    }

    [Test]
    public async Task GrantedTime_CanBeSetToNull()
    {
        var request = new LockRequest(Guid.NewGuid(), "resource", LockType.Read, TimeSpan.FromSeconds(10));
        request.GrantedTime = DateTime.UtcNow;
        request.GrantedTime = null;
        await Assert.That(request.GrantedTime).IsNull();
    }

    #endregion

    #region IsExpired Tests

    [Test]
    public async Task IsExpired_WhenNotExpired_ShouldReturnFalse()
    {
        var request = new LockRequest(Guid.NewGuid(), "resource", LockType.Read, TimeSpan.FromMinutes(5));
        await Assert.That(request.IsExpired()).IsFalse();
    }

    [Test]
    public async Task IsExpired_WhenExpired_ShouldReturnTrue()
    {
        // Create a request with very short timeout
        var request = new LockRequest(Guid.NewGuid(), "resource", LockType.Read, TimeSpan.FromMilliseconds(1));
        
        // Wait for it to expire
        await Task.Delay(10);
        
        await Assert.That(request.IsExpired()).IsTrue();
    }

    [Test]
    public async Task IsExpired_WithZeroTimeout_ShouldReturnTrueImmediately()
    {
        var request = new LockRequest(Guid.NewGuid(), "resource", LockType.Read, TimeSpan.Zero);
        await Assert.That(request.IsExpired()).IsTrue();
    }

    [Test]
    public async Task IsExpired_WithLongTimeout_ShouldReturnFalse()
    {
        var request = new LockRequest(Guid.NewGuid(), "resource", LockType.Read, TimeSpan.FromHours(1));
        await Assert.That(request.IsExpired()).IsFalse();
    }

    #endregion

    #region ToString Tests

    [Test]
    public async Task ToString_ShouldContainTransactionId()
    {
        var transactionId = Guid.NewGuid();
        var request = new LockRequest(transactionId, "resource", LockType.Read, TimeSpan.FromSeconds(10));
        var result = request.ToString();
        
        // Transaction ID is formatted with :N format (no hyphens)
        await Assert.That(result).Contains(transactionId.ToString("N"));
    }

    [Test]
    public async Task ToString_ShouldContainLockType()
    {
        var request = new LockRequest(Guid.NewGuid(), "resource", LockType.Read, TimeSpan.FromSeconds(10));
        var result = request.ToString();
        await Assert.That(result).Contains("Read");
    }

    [Test]
    public async Task ToString_ShouldContainResourceKey()
    {
        var request = new LockRequest(Guid.NewGuid(), "my_resource_key", LockType.Read, TimeSpan.FromSeconds(10));
        var result = request.ToString();
        await Assert.That(result).Contains("my_resource_key");
    }

    [Test]
    public async Task ToString_ShouldContainGrantedStatus_WhenNotGranted()
    {
        var request = new LockRequest(Guid.NewGuid(), "resource", LockType.Read, TimeSpan.FromSeconds(10));
        var result = request.ToString();
        await Assert.That(result).Contains("Granted=False");
    }

    [Test]
    public async Task ToString_ShouldContainGrantedStatus_WhenGranted()
    {
        var request = new LockRequest(Guid.NewGuid(), "resource", LockType.Read, TimeSpan.FromSeconds(10));
        request.IsGranted = true;
        var result = request.ToString();
        await Assert.That(result).Contains("Granted=True");
    }

    [Test]
    public async Task ToString_ShouldContainAge()
    {
        var request = new LockRequest(Guid.NewGuid(), "resource", LockType.Read, TimeSpan.FromSeconds(10));
        var result = request.ToString();
        // Age is formatted as "Age=X.Xs"
        await Assert.That(result).Contains("Age=");
        await Assert.That(result).Contains("s");
    }

    [Test]
    public async Task ToString_WithWriteLockType_ShouldContainWrite()
    {
        var request = new LockRequest(Guid.NewGuid(), "resource", LockType.Write, TimeSpan.FromSeconds(10));
        var result = request.ToString();
        await Assert.That(result).Contains("Write");
    }

    [Test]
    public async Task ToString_WithIntentWriteLockType_ShouldContainIntentWrite()
    {
        var request = new LockRequest(Guid.NewGuid(), "resource", LockType.IntentWrite, TimeSpan.FromSeconds(10));
        var result = request.ToString();
        await Assert.That(result).Contains("IntentWrite");
    }

    [Test]
    public async Task ToString_WithUpdateLockType_ShouldContainUpdate()
    {
        var request = new LockRequest(Guid.NewGuid(), "resource", LockType.Update, TimeSpan.FromSeconds(10));
        var result = request.ToString();
        await Assert.That(result).Contains("Update");
    }

    #endregion

    #region Edge Cases

    [Test]
    public async Task Constructor_WithEmptyResourceKey_ShouldWork()
    {
        var request = new LockRequest(Guid.NewGuid(), "", LockType.Read, TimeSpan.FromSeconds(10));
        await Assert.That(request.ResourceKey).IsEqualTo("");
    }

    [Test]
    public async Task Constructor_WithNegativeTimeout_ShouldWork()
    {
        // Negative timeout is allowed but IsExpired will always be true
        var request = new LockRequest(Guid.NewGuid(), "resource", LockType.Read, TimeSpan.FromSeconds(-1));
        await Assert.That(request.IsExpired()).IsTrue();
    }

    [Test]
    public async Task Constructor_WithEmptyGuid_ShouldWork()
    {
        var request = new LockRequest(Guid.Empty, "resource", LockType.Read, TimeSpan.FromSeconds(10));
        await Assert.That(request.TransactionId).IsEqualTo(Guid.Empty);
    }

    [Test]
    public async Task Constructor_WithMaxValueTimeout_ShouldWork()
    {
        var request = new LockRequest(Guid.NewGuid(), "resource", LockType.Read, TimeSpan.MaxValue);
        await Assert.That(request.Timeout).IsEqualTo(TimeSpan.MaxValue);
        await Assert.That(request.IsExpired()).IsFalse();
    }

    [Test]
    public async Task Constructor_WithLongResourceKey_ShouldWork()
    {
        var longKey = new string('a', 10000);
        var request = new LockRequest(Guid.NewGuid(), longKey, LockType.Read, TimeSpan.FromSeconds(10));
        await Assert.That(request.ResourceKey).IsEqualTo(longKey);
    }

    #endregion

    #region LockType Enum Tests

    [Test]
    public async Task LockType_Read_HasCorrectValue()
    {
        await Assert.That((int)LockType.Read).IsEqualTo(0);
    }

    [Test]
    public async Task LockType_Write_HasCorrectValue()
    {
        await Assert.That((int)LockType.Write).IsEqualTo(1);
    }

    [Test]
    public async Task LockType_IntentWrite_HasCorrectValue()
    {
        await Assert.That((int)LockType.IntentWrite).IsEqualTo(2);
    }

    [Test]
    public async Task LockType_Update_HasCorrectValue()
    {
        await Assert.That((int)LockType.Update).IsEqualTo(3);
    }

    #endregion
}
