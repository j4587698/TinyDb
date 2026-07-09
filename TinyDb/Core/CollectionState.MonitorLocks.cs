using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace TinyDb.Core;

internal sealed class CollectionCommitGate
{
    private readonly SemaphoreSlim _turnstile = new(1, 1);
    private readonly SemaphoreSlim _readerMutex = new(1, 1);
    private readonly SemaphoreSlim _roomEmpty = new(1, 1);
    private int _sharedCount;

    [ThreadStatic]
    private static Dictionary<CollectionCommitGate, int>? s_exclusiveDepthByGate;

    public IDisposable EnterShared()
    {
        if (IsCurrentThreadExclusiveOwner())
        {
            return NoopScope.Instance;
        }

        _turnstile.Wait();
        _turnstile.Release();

        _readerMutex.Wait();
        var enteredShared = false;
        try
        {
            _sharedCount++;
            enteredShared = true;
            if (_sharedCount == 1)
            {
                _roomEmpty.Wait();
            }

            return new SharedScope(this);
        }
        catch
        {
            if (enteredShared)
            {
                _sharedCount--;
            }

            throw;
        }
        finally
        {
            _readerMutex.Release();
        }
    }

    public async Task<IDisposable> EnterSharedAsync(CancellationToken cancellationToken = default)
    {
        if (IsCurrentThreadExclusiveOwner())
        {
            return NoopScope.Instance;
        }

        await _turnstile.WaitAsync(cancellationToken).ConfigureAwait(false);
        _turnstile.Release();

        await _readerMutex.WaitAsync(cancellationToken).ConfigureAwait(false);
        var enteredShared = false;
        try
        {
            _sharedCount++;
            enteredShared = true;
            if (_sharedCount == 1)
            {
                await _roomEmpty.WaitAsync(cancellationToken).ConfigureAwait(false);
            }

            return new SharedScope(this);
        }
        catch
        {
            if (enteredShared)
            {
                _sharedCount--;
            }

            throw;
        }
        finally
        {
            _readerMutex.Release();
        }
    }

    public IDisposable EnterExclusive()
    {
        if (IsCurrentThreadExclusiveOwner())
        {
            EnterThreadExclusiveScope();
            return new ExclusiveScope(this, releasesGate: false, releasesThreadOwnership: true);
        }

        _turnstile.Wait();
        var enteredTurnstile = true;
        try
        {
            _roomEmpty.Wait();
            EnterThreadExclusiveScope();
            return new ExclusiveScope(this, releasesGate: true, releasesThreadOwnership: true);
        }
        catch
        {
            if (enteredTurnstile)
            {
                _turnstile.Release();
            }

            throw;
        }
    }

    public async Task<IDisposable> EnterExclusiveAsync(CancellationToken cancellationToken = default)
    {
        if (IsCurrentThreadExclusiveOwner())
        {
            EnterThreadExclusiveScope();
            return new ExclusiveScope(this, releasesGate: false, releasesThreadOwnership: true);
        }

        await _turnstile.WaitAsync(cancellationToken).ConfigureAwait(false);
        var enteredTurnstile = true;
        try
        {
            await _roomEmpty.WaitAsync(cancellationToken).ConfigureAwait(false);
            return new ExclusiveScope(this, releasesGate: true, releasesThreadOwnership: false);
        }
        catch
        {
            if (enteredTurnstile)
            {
                _turnstile.Release();
            }

            throw;
        }
    }

    private void ExitShared()
    {
        _readerMutex.Wait();
        try
        {
            _sharedCount--;
            if (_sharedCount == 0)
            {
                _roomEmpty.Release();
            }
        }
        finally
        {
            _readerMutex.Release();
        }
    }

    private bool IsCurrentThreadExclusiveOwner()
    {
        return s_exclusiveDepthByGate != null &&
               s_exclusiveDepthByGate.TryGetValue(this, out var depth) &&
               depth > 0;
    }

    private void EnterThreadExclusiveScope()
    {
        s_exclusiveDepthByGate ??= new Dictionary<CollectionCommitGate, int>();
        s_exclusiveDepthByGate.TryGetValue(this, out var depth);
        s_exclusiveDepthByGate[this] = depth + 1;
    }

    private int ExitThreadExclusiveScope()
    {
        if (s_exclusiveDepthByGate == null ||
            !s_exclusiveDepthByGate.TryGetValue(this, out var depth) ||
            depth <= 0)
        {
            return 0;
        }

        depth--;
        if (depth == 0)
        {
            s_exclusiveDepthByGate.Remove(this);
            if (s_exclusiveDepthByGate.Count == 0)
            {
                s_exclusiveDepthByGate = null;
            }
        }
        else
        {
            s_exclusiveDepthByGate[this] = depth;
        }

        return depth;
    }

    private void ExitExclusive(bool releasesGate, bool releasesThreadOwnership)
    {
        if (releasesThreadOwnership && ExitThreadExclusiveScope() > 0)
        {
            return;
        }

        if (!releasesGate)
        {
            return;
        }

        _roomEmpty.Release();
        _turnstile.Release();
    }

    private sealed class NoopScope : IDisposable
    {
        public static readonly NoopScope Instance = new();

        private NoopScope()
        {
        }

        public void Dispose()
        {
        }
    }

    private sealed class SharedScope : IDisposable
    {
        private readonly CollectionCommitGate _gate;
        private bool _disposed;

        public SharedScope(CollectionCommitGate gate)
        {
            _gate = gate;
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;
            _gate.ExitShared();
        }
    }

    private sealed class ExclusiveScope : IDisposable
    {
        private readonly CollectionCommitGate _gate;
        private readonly bool _releasesGate;
        private readonly bool _releasesThreadOwnership;
        private bool _disposed;

        public ExclusiveScope(CollectionCommitGate gate, bool releasesGate, bool releasesThreadOwnership)
        {
            _gate = gate;
            _releasesGate = releasesGate;
            _releasesThreadOwnership = releasesThreadOwnership;
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;
            _gate.ExitExclusive(_releasesGate, _releasesThreadOwnership);
        }
    }
}

internal sealed partial class CollectionState
{
    internal sealed class CollectionCommitGateScope : IDisposable
    {
        private readonly IDisposable[] _scopes;
        private bool _disposed;

        public CollectionCommitGateScope(CollectionCommitGate[] gates, bool exclusive)
        {
            if (gates == null) throw new ArgumentNullException(nameof(gates));

            var scopes = new IDisposable[gates.Length];
            var entered = 0;
            try
            {
                for (var i = 0; i < gates.Length; i++)
                {
                    scopes[i] = exclusive ? gates[i].EnterExclusive() : gates[i].EnterShared();
                    entered++;
                }
            }
            catch
            {
                for (var i = entered - 1; i >= 0; i--)
                {
                    scopes[i]?.Dispose();
                }

                throw;
            }

            _scopes = scopes;
        }

        private CollectionCommitGateScope(IDisposable[] scopes)
        {
            _scopes = scopes;
        }

        public static async Task<CollectionCommitGateScope> EnterAsync(
            CollectionCommitGate[] gates,
            bool exclusive,
            CancellationToken cancellationToken)
        {
            if (gates == null) throw new ArgumentNullException(nameof(gates));

            var scopes = new IDisposable[gates.Length];
            var entered = 0;
            try
            {
                for (var i = 0; i < gates.Length; i++)
                {
                    scopes[i] = exclusive
                        ? await gates[i].EnterExclusiveAsync(cancellationToken).ConfigureAwait(false)
                        : await gates[i].EnterSharedAsync(cancellationToken).ConfigureAwait(false);
                    entered++;
                }

                return new CollectionCommitGateScope(scopes);
            }
            catch
            {
                for (var i = entered - 1; i >= 0; i--)
                {
                    scopes[i]?.Dispose();
                }

                throw;
            }
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;

            for (var i = _scopes.Length - 1; i >= 0; i--)
            {
                _scopes[i].Dispose();
            }
        }
    }
}
