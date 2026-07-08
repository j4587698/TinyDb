using System;
using System.Buffers;
using System.Buffers.Binary;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using TinyDb.Core;
using TinyDb.Utils;

namespace TinyDb.Storage;

public sealed partial class WriteAheadLog
{

    internal WalTransactionScope BeginTransaction(Guid transactionId) => BeginTransaction(transactionId, flushOnCommit: true);


    internal WalTransactionScope BeginTransaction(Guid transactionId, bool flushOnCommit)
    {
        if (!IsEnabled)
        {
            return new WalTransactionScope(this, transactionId, transactionContext: null, ownsContext: false, flushOnCommit);
        }

        try
        {
            var transactionContext = new TransactionContext(transactionId);
            _currentTransactionContext.Value = transactionContext;
            RunWithWriteLock(_ =>
            {
                WriteEntry(EntryTypeTransactionBegin, 0, CreateTransactionControlData(transactionId));
                SetHasPendingEntries(true);
            });
            return new WalTransactionScope(this, transactionId, transactionContext, ownsContext: true, flushOnCommit);
        }
        catch
        {
            _currentTransactionContext.Value?.Clear();
            _currentTransactionContext.Value = null;
            throw;
        }
    }


    internal Task WriteTransactionBeginAsync(Guid transactionId, CancellationToken cancellationToken = default)
    {
        if (!IsEnabled)
        {
            return Task.CompletedTask;
        }

        return RunWithWriteLockAsync(async _ =>
        {
            await WriteEntryAsync(
                EntryTypeTransactionBegin,
                0,
                CreateTransactionControlData(transactionId),
                cancellationToken).ConfigureAwait(false);
            SetHasPendingEntries(true);
        }, cancellationToken);
    }


    internal WalTransactionScope EnterTransactionContext(Guid transactionId, bool flushOnCommit)
    {
        if (!IsEnabled)
        {
            return new WalTransactionScope(this, transactionId, transactionContext: null, ownsContext: false, flushOnCommit);
        }

        var transactionContext = new TransactionContext(transactionId);
        _currentTransactionContext.Value = transactionContext;
        return new WalTransactionScope(this, transactionId, transactionContext, ownsContext: true, flushOnCommit);
    }

    internal sealed class WalTransactionScope : IDisposable
    {
        private readonly WriteAheadLog _wal;
        private readonly Guid _transactionId;
        private readonly TransactionContext? _transactionContext;
        private readonly bool _ownsContext;
        private readonly bool _flushOnCommit;
        private bool _completed;
        private bool _disposed;

        internal WalTransactionScope(
            WriteAheadLog wal,
            Guid transactionId,
            TransactionContext? transactionContext,
            bool ownsContext,
            bool flushOnCommit)
        {
            _wal = wal;
            _transactionId = transactionId;
            _transactionContext = transactionContext;
            _ownsContext = ownsContext;
            _flushOnCommit = flushOnCommit;
        }

        public void Commit()
        {
            if (_completed) return;
            if (_wal.IsEnabled)
            {
                if (!_wal.TryGetCurrentTransactionContext(out var transactionContext) ||
                    transactionContext.TransactionId != _transactionId)
                {
                    throw new InvalidOperationException("WAL transaction scope mismatch.");
                }

                _wal.RunWithWriteLock(_ =>
                {
                    _wal.WriteDeferredTransactionPages(_transactionId);
                    _wal.WriteEntry(EntryTypeTransactionCommit, 0, CreateTransactionControlData(_transactionId));
                    _wal.SetHasPendingEntries(true);
                    if (_flushOnCommit)
                    {
                        _wal.FlushLogCore();
                    }
                });
            }

            _completed = true;
        }

        public async Task CommitAsync(CancellationToken cancellationToken = default)
        {
            if (_completed) return;
            if (_wal.IsEnabled)
            {
                if (!_wal.TryGetCurrentTransactionContext(out var transactionContext) ||
                    transactionContext.TransactionId != _transactionId)
                {
                    throw new InvalidOperationException("WAL transaction scope mismatch.");
                }

                await _wal.RunWithWriteLockAsync(async _ =>
                {
                    await _wal.WriteDeferredTransactionPagesAsync(_transactionId, cancellationToken).ConfigureAwait(false);
                    await _wal.WriteEntryAsync(
                        EntryTypeTransactionCommit,
                        0,
                        CreateTransactionControlData(_transactionId),
                        cancellationToken).ConfigureAwait(false);
                    _wal.SetHasPendingEntries(true);
                    if (_flushOnCommit)
                    {
                        _wal.FlushLogCore();
                    }
                }, cancellationToken).ConfigureAwait(false);
            }

            _completed = true;
        }

        public void Rollback(Action<uint, byte[]> restore)
        {
            Rollback(restore, discardNewPage: null);
        }

        internal void Rollback(Action<uint, byte[]> restore, Action<uint>? discardNewPage)
        {
            if (_completed) return;
            if (restore == null) throw new ArgumentNullException(nameof(restore));

            if (_wal.IsEnabled)
            {
                if (!_wal.TryGetCurrentTransactionContext(out var transactionContext) ||
                    transactionContext.TransactionId != _transactionId)
                {
                    throw new InvalidOperationException("WAL transaction scope mismatch.");
                }

                _wal.RestoreTransactionBeforeImages(_transactionId, restore, discardNewPage);
            }

            _completed = true;
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;

            if (!_ownsContext) return;

            _transactionContext?.Clear();
            if (ReferenceEquals(_wal._currentTransactionContext.Value, _transactionContext))
            {
                _wal._currentTransactionContext.Value = null;
            }
        }
    }

    private void WriteDeferredTransactionPages(Guid transactionId)
    {
        if (!TryGetCurrentTransactionContext(out var transactionContext))
        {
            return;
        }

        if (transactionContext.TransactionId != transactionId)
        {
            throw new InvalidOperationException("WAL transaction page buffer mismatch.");
        }

        foreach (var record in transactionContext.Pages.GetPagesPendingWalWriteInFirstTouchOrder())
        {
            var data = PrepareTransactionPageRecord(transactionId, record.AfterImage, record.BeforeImage, out var lsn);
            WriteEntry(EntryTypeTransactionPage, record.PageId, data);
            DeferredTransactionPageLogged?.Invoke(record.PageId, lsn);
        }
    }


    private async Task WriteDeferredTransactionPagesAsync(Guid transactionId, CancellationToken cancellationToken)
    {
        if (!TryGetCurrentTransactionContext(out var transactionContext))
        {
            return;
        }

        if (transactionContext.TransactionId != transactionId)
        {
            throw new InvalidOperationException("WAL transaction page buffer mismatch.");
        }

        foreach (var record in transactionContext.Pages.GetPagesPendingWalWriteInFirstTouchOrder())
        {
            var data = PrepareTransactionPageRecord(transactionId, record.AfterImage, record.BeforeImage, out var lsn);
            await WriteEntryAsync(EntryTypeTransactionPage, record.PageId, data, cancellationToken).ConfigureAwait(false);
            DeferredTransactionPageLogged?.Invoke(record.PageId, lsn);
        }
    }


    private void RestoreTransactionBeforeImages(Guid transactionId, Action<uint, byte[]> restore, Action<uint>? discardNewPage)
    {
        if (!TryGetCurrentTransactionContext(out var transactionContext))
        {
            return;
        }

        if (transactionContext.TransactionId != transactionId)
        {
            throw new InvalidOperationException("WAL transaction page buffer mismatch.");
        }

        foreach (var record in transactionContext.Pages.GetPagesInReverseFirstTouchOrder())
        {
            if (record.BeforeImage != null)
            {
                restore(record.PageId, record.BeforeImage);
            }
            else
            {
                discardNewPage?.Invoke(record.PageId);
            }
        }
    }

}
