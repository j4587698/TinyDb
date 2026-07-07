using System;
using System.Threading;
using System.Threading.Tasks;

namespace TinyDb.Core;

internal sealed partial class CollectionState
{
    private sealed class SingleKeyedDocumentLockScope : IDisposable
    {
        private readonly CollectionState _state;
        private readonly KeyedDocumentLock _lock;
        private readonly DocumentLockOwner _owner;
        private bool _entered;
        private bool _disposed;

        public SingleKeyedDocumentLockScope(CollectionState state, KeyedDocumentLock documentLock)
        {
            _state = state ?? throw new ArgumentNullException(nameof(state));
            _lock = documentLock ?? throw new ArgumentNullException(nameof(documentLock));
            try
            {
                _owner = EnterDocumentSemaphore(_lock);
                _entered = true;
            }
            catch
            {
                Dispose();
                throw;
            }
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;

            if (_entered)
            {
                ExitDocumentSemaphore(_lock, _owner);
                _entered = false;
            }

            _state.ReleaseDocumentLockReference(_lock);
        }
    }

    private sealed class KeyedDocumentLockScope : IDisposable
    {
        private readonly CollectionState _state;
        private readonly KeyedDocumentLock[] _locks;
        private DocumentLockOwner? _owner;
        private int _entered;
        private bool _disposed;

        public KeyedDocumentLockScope(CollectionState state, KeyedDocumentLock[] locks)
        {
            _state = state ?? throw new ArgumentNullException(nameof(state));
            _locks = locks ?? throw new ArgumentNullException(nameof(locks));
            try
            {
                for (var i = 0; i < _locks.Length; i++)
                {
                    _owner = EnterDocumentSemaphore(_locks[i]);
                    _entered++;
                }
            }
            catch
            {
                Dispose();
                throw;
            }
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;

            for (var i = _entered - 1; i >= 0; i--)
            {
                ExitDocumentSemaphore(_locks[i], _owner!);
            }

            _entered = 0;

            for (var i = _locks.Length - 1; i >= 0; i--)
            {
                _state.ReleaseDocumentLockReference(_locks[i]);
            }
        }
    }

    private sealed class AsyncKeyedDocumentLockScope : IDisposable
    {
        private readonly CollectionState _state;
        private readonly KeyedDocumentLock[] _locks;
        private readonly DocumentLockOwner _owner;
        private int _entered;
        private bool _disposed;

        private AsyncKeyedDocumentLockScope(CollectionState state, KeyedDocumentLock[] locks, DocumentLockOwner owner)
        {
            _state = state ?? throw new ArgumentNullException(nameof(state));
            _locks = locks ?? throw new ArgumentNullException(nameof(locks));
            _owner = owner ?? throw new ArgumentNullException(nameof(owner));
        }

        public static async ValueTask<AsyncKeyedDocumentLockScope> EnterAsync(
            CollectionState state,
            KeyedDocumentLock[] locks,
            DocumentLockOwner owner,
            CancellationToken cancellationToken)
        {
            var scope = new AsyncKeyedDocumentLockScope(state, locks, owner);
            try
            {
                for (var i = 0; i < scope._locks.Length; i++)
                {
                    await EnterDocumentSemaphoreAsync(scope._locks[i], owner, cancellationToken).ConfigureAwait(false);
                    scope._entered++;
                }

                return scope;
            }
            catch
            {
                scope.Dispose();
                throw;
            }
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;

            for (var i = _entered - 1; i >= 0; i--)
            {
                ExitDocumentSemaphore(_locks[i], _owner);
            }

            _entered = 0;

            for (var i = _locks.Length - 1; i >= 0; i--)
            {
                _state.ReleaseDocumentLockReference(_locks[i]);
            }
        }
    }

    private sealed class SingleAsyncKeyedDocumentLockScope : IDisposable
    {
        private readonly CollectionState _state;
        private readonly KeyedDocumentLock _lock;
        private readonly DocumentLockOwner _owner;
        private bool _entered;
        private bool _disposed;

        private SingleAsyncKeyedDocumentLockScope(CollectionState state, KeyedDocumentLock documentLock, DocumentLockOwner owner)
        {
            _state = state ?? throw new ArgumentNullException(nameof(state));
            _lock = documentLock ?? throw new ArgumentNullException(nameof(documentLock));
            _owner = owner ?? throw new ArgumentNullException(nameof(owner));
        }

        public static async ValueTask<IDisposable> EnterAsync(
            CollectionState state,
            KeyedDocumentLock documentLock,
            DocumentLockOwner owner,
            CancellationToken cancellationToken)
        {
            var scope = new SingleAsyncKeyedDocumentLockScope(state, documentLock, owner);
            try
            {
                await EnterDocumentSemaphoreAsync(documentLock, owner, cancellationToken).ConfigureAwait(false);
                scope._entered = true;
                return scope;
            }
            catch
            {
                scope.Dispose();
                throw;
            }
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;

            if (_entered)
            {
                ExitDocumentSemaphore(_lock, _owner);
                _entered = false;
            }

            _state.ReleaseDocumentLockReference(_lock);
        }
    }
}
