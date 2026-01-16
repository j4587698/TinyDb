using System.Collections.Concurrent;

namespace TinyDb.Core;

/// <summary>
/// 锁类型
/// </summary>
public enum LockType
{
    /// <summary>
    /// 读锁（共享锁）
    /// </summary>
    Read,

    /// <summary>
    /// 写锁（排他锁）
    /// </summary>
    Write,

    /// <summary>
    /// 意向写锁
    /// </summary>
    IntentWrite,

    /// <summary>
    /// 更新锁
    /// </summary>
    Update
}

/// <summary>
/// 锁请求
/// </summary>
public sealed class LockRequest
{
    /// <summary>
    /// 请求ID
    /// </summary>
    public Guid RequestId { get; }

    /// <summary>
    /// 事务ID
    /// </summary>
    public Guid TransactionId { get; }

    /// <summary>
    /// 资源键
    /// </summary>
    public string ResourceKey { get; }

    /// <summary>
    /// 锁类型
    /// </summary>
    public LockType LockType { get; }

    /// <summary>
    /// 请求时间
    /// </summary>
    public DateTime RequestTime { get; }

    /// <summary>
    /// 超时时间
    /// </summary>
    public TimeSpan Timeout { get; }

    /// <summary>
    /// 是否已授予
    /// </summary>
    public bool IsGranted { get; set; }

    /// <summary>
    /// 是否为死锁受害者
    /// </summary>
    public bool IsDeadlockVictim { get; set; }

    /// <summary>
    /// 授予时间
    /// </summary>
    public DateTime? GrantedTime { get; set; }

    /// <summary>
    /// 初始化锁请求
    /// </summary>
    /// <param name="transactionId">事务ID</param>
    /// <param name="resourceKey">资源键</param>
    /// <param name="lockType">锁类型</param>
    /// <param name="timeout">超时时间</param>
    public LockRequest(Guid transactionId, string resourceKey, LockType lockType, TimeSpan timeout)
    {
        RequestId = Guid.NewGuid();
        TransactionId = transactionId;
        ResourceKey = resourceKey;
        LockType = lockType;
        Timeout = timeout;
        RequestTime = DateTime.UtcNow;
        IsGranted = false;
    }

    /// <summary>
    /// 检查是否超时
    /// </summary>
    /// <returns>是否超时</returns>
    public bool IsExpired()
    {
        return DateTime.UtcNow - RequestTime > Timeout;
    }

    /// <summary>
    /// 重写 ToString 方法
    /// </summary>
    public override string ToString()
    {
        return $"LockRequest[{TransactionId:N}]: {LockType} on {ResourceKey}, " +
               $"Granted={IsGranted}, Age={(DateTime.UtcNow - RequestTime).TotalSeconds:F1}s";
    }
}

/// <summary>
/// 锁管理器
/// </summary>
public sealed class LockManager : IDisposable
{
    private readonly ConcurrentDictionary<string, LockBucket> _lockBuckets;
    private readonly ConcurrentDictionary<Guid, List<LockRequest>> _transactionLocks;
    private readonly object _globalLock = new();
    private readonly TimeSpan _defaultTimeout;
    private bool _disposed;

    /// <summary>
    /// 默认超时时间
    /// </summary>
    public TimeSpan DefaultTimeout => _defaultTimeout;

    /// <summary>
    /// 活跃锁数量
    /// </summary>
    public int ActiveLockCount
    {
        get
        {
            return _lockBuckets.Values.Sum(bucket => bucket.ActiveLocks.Count);
        }
    }

    /// <summary>
    /// 等待锁数量
    /// </summary>
    public int PendingLockCount
    {
        get
        {
            return _lockBuckets.Values.Sum(bucket => bucket.PendingRequests.Count);
        }
    }

    /// <summary>
    /// 初始化锁管理器
    /// </summary>
    /// <param name="defaultTimeout">默认超时时间</param>
    public LockManager(TimeSpan? defaultTimeout = null)
    {
        _defaultTimeout = defaultTimeout ?? TimeSpan.FromSeconds(30);
        _lockBuckets = new ConcurrentDictionary<string, LockBucket>();
        _transactionLocks = new ConcurrentDictionary<Guid, List<LockRequest>>();

        // 启动死锁检测任务
        _ = StartDeadlockDetectionTask();
    }

    /// <summary>
    /// 请求锁
    /// </summary>
    /// <param name="transactionId">事务ID</param>
    /// <param name="resourceKey">资源键</param>
    /// <param name="lockType">锁类型</param>
    /// <param name="timeout">超时时间</param>
    /// <returns>锁请求</returns>
    public LockRequest RequestLock(Guid transactionId, string resourceKey, LockType lockType, TimeSpan? timeout = null)
    {
        ThrowIfDisposed();
        if (transactionId == Guid.Empty) throw new ArgumentException("Transaction ID cannot be empty", nameof(transactionId));
        if (string.IsNullOrEmpty(resourceKey)) throw new ArgumentException("Resource key cannot be null or empty", nameof(resourceKey));

        var request = new LockRequest(transactionId, resourceKey, lockType, timeout ?? _defaultTimeout);
        var bucket = _lockBuckets.GetOrAdd(resourceKey, _ => new LockBucket());

        lock (bucket)
        {
            // 检查死锁
            if (WouldCauseDeadlock(transactionId, resourceKey, lockType))
            {
                throw new InvalidOperationException($"Deadlock detected for transaction {transactionId:N}");
            }

            // 检查是否可以立即授予
            if (CanGrantLock(bucket, request))
            {
                GrantLock(bucket, request);
            }
            else
            {
                bucket.PendingRequests.Enqueue(request);
            }
        }

        // 记录事务锁
        _transactionLocks.GetOrAdd(transactionId, _ => new List<LockRequest>()).Add(request);

        return request;
    }

    /// <summary>
    /// 释放锁
    /// </summary>
    /// <param name="request">锁请求</param>
    public void ReleaseLock(LockRequest request)
    {
        ThrowIfDisposed();
        if (request == null) throw new ArgumentNullException(nameof(request));

        if (!_lockBuckets.TryGetValue(request.ResourceKey, out var bucket))
        {
            return; // 锁不存在
        }

        lock (bucket)
        {
            // 从活跃锁中移除
            bucket.ActiveLocks.TryRemove(request.TransactionId, out _);

            // 尝试授予等待的锁
            TryGrantPendingLocks(bucket);
        }

        // 从事务锁记录中移除
        if (_transactionLocks.TryGetValue(request.TransactionId, out var transactionLocks))
        {
            transactionLocks.Remove(request);
        }
    }

    /// <summary>
    /// 释放事务的所有锁
    /// </summary>
    /// <param name="transactionId">事务ID</param>
    public void ReleaseAllLocks(Guid transactionId)
    {
        ThrowIfDisposed();
        if (transactionId == Guid.Empty) return;

        if (_transactionLocks.TryRemove(transactionId, out var locks))
        {
            foreach (var request in locks)
            {
                ReleaseLock(request);
            }
        }
    }

    /// <summary>
    /// 检查是否可以授予锁
    /// </summary>
    /// <param name="bucket">锁桶</param>
    /// <param name="request">锁请求</param>
    /// <returns>是否可以授予</returns>
    private static bool CanGrantLock(LockBucket bucket, LockRequest request)
    {
        foreach (var activeLock in bucket.ActiveLocks.Values)
        {
            if (activeLock.TransactionId == request.TransactionId)
            {
                // 同一事务的锁升级检查
                if (!CanUpgradeLock(activeLock.LockType, request.LockType))
                {
                    return false;
                }
            }
            else
            {
                // 不同事务的锁冲突检查
                if (AreLocksConflicting(activeLock.LockType, request.LockType))
                {
                    return false;
                }
            }
        }

        return true;
    }

    /// <summary>
    /// 检查锁是否可以升级
    /// </summary>
    /// <param name="currentType">当前锁类型</param>
    /// <param name="requestedType">请求的锁类型</param>
    /// <returns>是否可以升级</returns>
    private static bool CanUpgradeLock(LockType currentType, LockType requestedType)
    {
        // 读锁可以升级为写锁
        if (currentType == LockType.Read && requestedType == LockType.Write)
            return true;

        // 相同类型不需要升级
        if (currentType == requestedType)
            return true;

        // 写锁不能降级为读锁
        if (currentType == LockType.Write && requestedType == LockType.Read)
            return false;

        // 其他情况需要具体判断
        return currentType switch
        {
            LockType.IntentWrite => requestedType == LockType.Write,
            LockType.Update => requestedType == LockType.Write,
            _ => false
        };
    }

    /// <summary>
    /// 检查锁是否冲突
    /// </summary>
    /// <param name="type1">锁类型1</param>
    /// <param name="type2">锁类型2</param>
    /// <returns>是否冲突</returns>
    private static bool AreLocksConflicting(LockType type1, LockType type2)
    {
        // 写锁与任何锁都冲突
        if (type1 == LockType.Write || type2 == LockType.Write)
            return true;

        // 读锁与读锁不冲突
        if (type1 == LockType.Read && type2 == LockType.Read)
            return false;

        // 意向写锁与读锁冲突
        if ((type1 == LockType.IntentWrite && type2 == LockType.Read) ||
            (type1 == LockType.Read && type2 == LockType.IntentWrite))
            return true;

        // 更新锁与读锁冲突
        if ((type1 == LockType.Update && type2 == LockType.Read) ||
            (type1 == LockType.Read && type2 == LockType.Update))
            return true;

        return false;
    }

    /// <summary>
    /// 授予锁
    /// </summary>
    /// <param name="bucket">锁桶</param>
    /// <param name="request">锁请求</param>
    private static void GrantLock(LockBucket bucket, LockRequest request)
    {
        request.IsGranted = true;
        request.GrantedTime = DateTime.UtcNow;
        bucket.ActiveLocks[request.TransactionId] = request;
    }

    /// <summary>
    /// 尝试授予等待的锁
    /// </summary>
    /// <param name="bucket">锁桶</param>
    private void TryGrantPendingLocks(LockBucket bucket)
    {
        var toRemove = new List<LockRequest>();

        while (bucket.PendingRequests.TryPeek(out var request))
        {
            // 检查是否超时
            if (request.IsExpired())
            {
                bucket.PendingRequests.TryDequeue(out _);
                toRemove.Add(request);
                continue;
            }

            // 检查是否可以授予
            if (CanGrantLock(bucket, request))
            {
                bucket.PendingRequests.TryDequeue(out _);
                GrantLock(bucket, request);
            }
            else
            {
                break; // FIFO顺序，后面的请求也无法授予
            }
        }

        // 清理超时的请求
        foreach (var expiredRequest in toRemove)
        {
            if (_transactionLocks.TryGetValue(expiredRequest.TransactionId, out var transactionLocks))
            {
                transactionLocks.Remove(expiredRequest);
            }
        }
    }

    /// <summary>
    /// 检查是否会导致死锁
    /// </summary>
    /// <param name="transactionId">事务ID</param>
    /// <param name="resourceKey">资源键</param>
    /// <param name="lockType">锁类型</param>
    /// <returns>是否会导致死锁</returns>
    private bool WouldCauseDeadlock(Guid transactionId, string resourceKey, LockType lockType)
    {
        // 简化的死锁检测：检查事务是否持有其他资源的锁
        if (!_transactionLocks.TryGetValue(transactionId, out var heldLocks))
            return false;

        foreach (var heldLock in heldLocks)
        {
            if (heldLock.ResourceKey != resourceKey && heldLock.IsGranted)
            {
                // 检查其他事务是否在等待当前事务持有的资源
                if (_lockBuckets.TryGetValue(heldLock.ResourceKey, out var bucket))
                {
                    foreach (var waitingRequest in bucket.PendingRequests)
                    {
                        if (waitingRequest.TransactionId != transactionId &&
                            AreLocksConflicting(waitingRequest.LockType, lockType))
                        {
                            return true; // 可能的死锁
                        }
                    }
                }
            }
        }

        return false;
    }

    /// <summary>
    /// 启动死锁检测任务
    /// </summary>
    /// <returns>任务</returns>
    private async Task StartDeadlockDetectionTask()
    {
        while (!_disposed)
        {
            try
            {
                await Task.Delay(TimeSpan.FromSeconds(10)); // 每10秒检查一次
                DetectAndResolveDeadlocks();
            }
            catch
            {
            }
        }
    }

    /// <summary>
    /// 检测并解决死锁
    /// (使用等待图 Wait-For Graph 算法)
    /// </summary>
    private void DetectAndResolveDeadlocks()
    {
        // 1. 构建等待图
        var graph = new Dictionary<Guid, HashSet<Guid>>();
        
        foreach (var bucket in _lockBuckets.Values)
        {
            lock (bucket)
            {
                // 获取持有锁的事务
                var holders = bucket.ActiveLocks.Keys.ToList();
                
                // 获取等待锁的事务
                foreach (var request in bucket.PendingRequests)
                {
                    if (!graph.ContainsKey(request.TransactionId))
                    {
                        graph[request.TransactionId] = new HashSet<Guid>();
                    }
                    
                    // 添加依赖边: 等待者 -> 持有者
                    foreach (var holder in holders)
                    {
                        if (holder != request.TransactionId)
                        {
                            graph[request.TransactionId].Add(holder);
                        }
                    }
                }
            }
        }

        // 2. 检测循环 (使用DFS)
        var visited = new HashSet<Guid>();
        var recursionStack = new HashSet<Guid>();
        var deadlockTransactions = new HashSet<Guid>();

        foreach (var transactionId in graph.Keys)
        {
            if (HasCycle(transactionId, graph, visited, recursionStack))
            {
                // 发现死锁，选择一个受害者 (这里简单选择当前节点)
                // 实际策略可以更复杂：选择最年轻的事务，或者持有锁最少的事务
                deadlockTransactions.Add(transactionId);
            }
        }

        // 3. 解决死锁 (中止受害者事务的锁请求)
        foreach (var victimId in deadlockTransactions)
        {
            // 找到受害者的所有待处理请求并标记为失败
            // 注意：这只是清理 LockManager 的状态，实际的 Transaction 对象需要在上层处理异常
            
            // 我们不能直接在这里抛出异常到用户线程，只能取消请求
            // 用户线程在等待锁时会收到超时或被取消的信号
            
            // 在此简单实现中，我们移除受害者的请求
             if (_transactionLocks.TryGetValue(victimId, out var requests))
             {
                 foreach(var req in requests)
                 {
                     if (!req.IsGranted)
                     {
                         req.IsDeadlockVictim = true;
                     }
                 }
             }
        }
        
        // 兼容现有逻辑：清理已超时的请求
        var expiredRequests = new List<LockRequest>();
        foreach (var bucket in _lockBuckets.Values)
        {
            lock (bucket)
            {
                // 检查超时的请求
                while (bucket.PendingRequests.TryPeek(out var request) && request.IsExpired())
                {
                    bucket.PendingRequests.TryDequeue(out _);
                    expiredRequests.Add(request);
                }
            }
        }
        
        foreach (var expiredRequest in expiredRequests)
        {
            if (_transactionLocks.TryGetValue(expiredRequest.TransactionId, out var transactionLocks))
            {
                transactionLocks.Remove(expiredRequest);
            }
        }
    }

    private bool HasCycle(Guid current, Dictionary<Guid, HashSet<Guid>> graph, HashSet<Guid> visited, HashSet<Guid> recursionStack)
    {
        visited.Add(current);
        recursionStack.Add(current);

        if (graph.TryGetValue(current, out var neighbors))
        {
            foreach (var neighbor in neighbors)
            {
                if (!visited.Contains(neighbor))
                {
                    if (HasCycle(neighbor, graph, visited, recursionStack))
                        return true;
                }
                else if (recursionStack.Contains(neighbor))
                {
                    return true;
                }
            }
        }

        recursionStack.Remove(current);
        return false;
    }

    /// <summary>
    /// 获取锁统计信息
    /// </summary>
    /// <returns>统计信息</returns>
    public LockManagerStatistics GetStatistics()
    {
        ThrowIfDisposed();

        var lockCounts = new Dictionary<LockType, int>();
        var transactionLockCounts = new Dictionary<Guid, int>();

        foreach (var bucket in _lockBuckets.Values)
        {
            lock (bucket)
            {
                foreach (var activeLock in bucket.ActiveLocks.Values)
                {
                    lockCounts[activeLock.LockType] = lockCounts.GetValueOrDefault(activeLock.LockType) + 1;
                    transactionLockCounts[activeLock.TransactionId] = transactionLockCounts.GetValueOrDefault(activeLock.TransactionId) + 1;
                }
            }
        }

        return new LockManagerStatistics
        {
            ActiveLockCount = ActiveLockCount,
            PendingLockCount = PendingLockCount,
            LockTypeCounts = lockCounts,
            TransactionLockCounts = transactionLockCounts,
            BucketCount = _lockBuckets.Count,
            DefaultTimeout = _defaultTimeout
        };
    }

    /// <summary>
    /// 检查是否已释放
    /// </summary>
    private void ThrowIfDisposed()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(LockManager));
    }

    /// <summary>
    /// 释放资源
    /// </summary>
    public void Dispose()
    {
        if (!_disposed)
        {
            try
            {
                // 清理所有锁
                foreach (var bucket in _lockBuckets.Values)
                {
                    lock (bucket)
                    {
                        bucket.ActiveLocks.Clear();
                        bucket.PendingRequests.Clear();
                    }
                }

                _lockBuckets.Clear();
                _transactionLocks.Clear();
            }
            catch
            {
            }
            finally
            {
                _disposed = true;
            }
        }
    }

    /// <summary>
    /// 重写 ToString 方法
    /// </summary>
    public override string ToString()
    {
        return $"LockManager: {ActiveLockCount} active, {PendingLockCount} pending, {_lockBuckets.Count} buckets";
    }
}

/// <summary>
/// 锁桶
/// </summary>
internal sealed class LockBucket
{
    /// <summary>
    /// 活跃锁
    /// </summary>
    public ConcurrentDictionary<Guid, LockRequest> ActiveLocks { get; }

    /// <summary>
    /// 等待请求
    /// </summary>
    public Queue<LockRequest> PendingRequests { get; }

    /// <summary>
    /// 初始化锁桶
    /// </summary>
    public LockBucket()
    {
        ActiveLocks = new ConcurrentDictionary<Guid, LockRequest>();
        PendingRequests = new Queue<LockRequest>();
    }
}

/// <summary>
/// 锁管理器统计信息
/// </summary>
public sealed class LockManagerStatistics
{
    public int ActiveLockCount { get; init; }
    public int PendingLockCount { get; init; }
    public Dictionary<LockType, int> LockTypeCounts { get; init; } = new();
    public Dictionary<Guid, int> TransactionLockCounts { get; init; } = new();
    public int BucketCount { get; init; }
    public TimeSpan DefaultTimeout { get; init; }

    public override string ToString()
    {
        return $"LockManager: {ActiveLockCount} active, {PendingLockCount} pending, {BucketCount} buckets";
    }
}