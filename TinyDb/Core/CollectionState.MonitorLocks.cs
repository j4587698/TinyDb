using System;
using System.Threading;

namespace TinyDb.Core;

internal sealed partial class CollectionState
{
    internal sealed class MonitorLockScope : IDisposable
    {
        private readonly object[] _locks;
        private int _entered;
        private bool _disposed;

        public MonitorLockScope(object[] locks)
        {
            _locks = locks ?? throw new ArgumentNullException(nameof(locks));
            try
            {
                for (var i = 0; i < _locks.Length; i++)
                {
                    var lockTaken = false;
                    Monitor.Enter(_locks[i], ref lockTaken);
                    if (lockTaken)
                    {
                        _entered++;
                    }
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
                Monitor.Exit(_locks[i]);
            }

            _entered = 0;
        }
    }

}
