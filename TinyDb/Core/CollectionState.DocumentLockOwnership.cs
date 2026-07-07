using System;
using System.Threading;
using System.Threading.Tasks;

namespace TinyDb.Core;

internal sealed partial class CollectionState
{
    private static DocumentLockOwner EnterDocumentSemaphore(KeyedDocumentLock documentLock)
    {
        var owner = GetOrCreateDocumentLockOwner();
        var ownerThreadId = Environment.CurrentManagedThreadId;
        var ownerTaskId = Task.CurrentId;

        lock (documentLock.ReferenceSyncRoot)
        {
            if (IsOwnedByCurrentExecution(documentLock, owner, ownerThreadId, ownerTaskId))
            {
                documentLock.OwnerDepth++;
                return owner;
            }
        }

        documentLock.Semaphore.Wait();

        lock (documentLock.ReferenceSyncRoot)
        {
            SetDocumentLockOwner(documentLock, owner, ownerThreadId, ownerTaskId);
        }

        return owner;
    }

    private static async ValueTask EnterDocumentSemaphoreAsync(
        KeyedDocumentLock documentLock,
        DocumentLockOwner owner,
        CancellationToken cancellationToken)
    {
        var ownerThreadId = Environment.CurrentManagedThreadId;
        var ownerTaskId = Task.CurrentId;

        lock (documentLock.ReferenceSyncRoot)
        {
            if (IsOwnedByCurrentExecution(documentLock, owner, ownerThreadId, ownerTaskId))
            {
                documentLock.OwnerDepth++;
                return;
            }
        }

        await documentLock.Semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);

        ownerThreadId = Environment.CurrentManagedThreadId;
        ownerTaskId = Task.CurrentId;
        lock (documentLock.ReferenceSyncRoot)
        {
            SetDocumentLockOwner(documentLock, owner, ownerThreadId, ownerTaskId);
        }
    }

    private static void ExitDocumentSemaphore(KeyedDocumentLock documentLock, DocumentLockOwner scopeOwner)
    {
        var owner = CurrentDocumentLockOwner.Value;
        var ownerThreadId = Environment.CurrentManagedThreadId;
        var ownerTaskId = Task.CurrentId;

        lock (documentLock.ReferenceSyncRoot)
        {
            if (!IsOwnedByCurrentExecution(documentLock, owner, ownerThreadId, ownerTaskId) &&
                !ReferenceEquals(documentLock.Owner, scopeOwner))
            {
                throw new SynchronizationLockException("Document lock is not owned by the current execution context.");
            }

            if (documentLock.OwnerDepth > 1)
            {
                documentLock.OwnerDepth--;
                return;
            }

            documentLock.Owner = null;
            documentLock.OwnerThreadId = 0;
            documentLock.OwnerTaskId = null;
            documentLock.OwnerDepth = 0;
        }

        documentLock.Semaphore.Release();
    }

    private static DocumentLockOwner GetOrCreateDocumentLockOwner()
    {
        var owner = CurrentDocumentLockOwner.Value;
        var currentTaskId = Task.CurrentId;
        if (owner != null && ShouldReuseDocumentLockOwner(owner, currentTaskId)) return owner;

        owner = new DocumentLockOwner(currentTaskId);
        CurrentDocumentLockOwner.Value = owner;
        return owner;
    }

    private static bool ShouldReuseDocumentLockOwner(DocumentLockOwner owner, int? currentTaskId)
    {
        return currentTaskId == null || owner.CreatorTaskId == currentTaskId;
    }

    private static DocumentLockOwner? CreateDocumentLockOwnerIfNeeded(
        KeyedDocumentLock[]? locks,
        KeyedDocumentLock? singleLock)
    {
        return singleLock != null || (locks != null && locks.Length > 0)
            ? GetOrCreateDocumentLockOwner()
            : null;
    }

    private static void SetDocumentLockOwner(
        KeyedDocumentLock documentLock,
        DocumentLockOwner owner,
        int ownerThreadId,
        int? ownerTaskId)
    {
        documentLock.Owner = owner;
        documentLock.OwnerThreadId = ownerThreadId;
        documentLock.OwnerTaskId = ownerTaskId;
        documentLock.OwnerDepth = 1;
    }

    private static bool IsOwnedByCurrentExecution(
        KeyedDocumentLock documentLock,
        DocumentLockOwner? owner,
        int ownerThreadId,
        int? ownerTaskId)
    {
        return documentLock.OwnerDepth > 0 &&
               ((owner != null && ReferenceEquals(documentLock.Owner, owner)) ||
                (documentLock.Owner == null &&
                 documentLock.OwnerThreadId == ownerThreadId &&
                 documentLock.OwnerTaskId == ownerTaskId));
    }

}
