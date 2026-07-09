using System;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using TinyDb.Bson;
using TinyDb.Security;
using TinyDb.Collections;
using TinyDb.Index;
using TinyDb.Query;
using TinyDb.Serialization;
using TinyDb.Storage;
using TinyDb.Attributes;
using TinyDb.Utils;

namespace TinyDb.Core;

public sealed partial class TinyDbEngine
{
    internal CollectionState GetCollectionState(string col)
    {
        if (_collectionStates.TryGetValue(col, out var existing))
        {
            return existing;
        }

        lock (_collectionStateInitLock)
        {
            if (_collectionStates.TryGetValue(col, out existing))
            {
                return existing;
            }

            var state = CreateEmptyCollectionState();
            BuildDocumentLocationCache(col, state);
            state.MarkCacheInitialized();
            _collectionStates[col] = state;
            return state;
        }
    }

    private static CollectionState CreateEmptyCollectionState()
    {
        return new CollectionState { Index = new MemoryDocumentIndex() };
    }

    internal IDisposable EnterCollectionCommitGates(IEnumerable<string> collectionNames)
    {
        return EnterCollectionGates(collectionNames, exclusive: true);
    }

    internal Task<IDisposable> EnterCollectionCommitGatesAsync(
        IEnumerable<string> collectionNames,
        CancellationToken cancellationToken = default)
    {
        return EnterCollectionGatesAsync(collectionNames, exclusive: true, cancellationToken);
    }

    internal IDisposable EnterCollectionWriteGates(IEnumerable<string> collectionNames)
    {
        return EnterCollectionGates(collectionNames, exclusive: false);
    }

    internal Task<IDisposable> EnterCollectionWriteGatesAsync(
        IEnumerable<string> collectionNames,
        CancellationToken cancellationToken = default)
    {
        return EnterCollectionGatesAsync(collectionNames, exclusive: false, cancellationToken);
    }

    private IDisposable EnterCollectionGates(IEnumerable<string> collectionNames, bool exclusive)
    {
        if (collectionNames == null) throw new ArgumentNullException(nameof(collectionNames));

        var gates = GetCollectionCommitGates(collectionNames);
        return new CollectionState.CollectionCommitGateScope(gates, exclusive);
    }

    private Task<IDisposable> EnterCollectionGatesAsync(
        IEnumerable<string> collectionNames,
        bool exclusive,
        CancellationToken cancellationToken)
    {
        if (collectionNames == null) throw new ArgumentNullException(nameof(collectionNames));

        var gates = GetCollectionCommitGates(collectionNames);
        return EnterCollectionGatesAsyncCore(gates, exclusive, cancellationToken);
    }

    private CollectionCommitGate[] GetCollectionCommitGates(IEnumerable<string> collectionNames)
    {
        var gates = collectionNames
            .Where(static name => !string.IsNullOrWhiteSpace(name))
            .Distinct(StringComparer.Ordinal)
            .OrderBy(static name => name, StringComparer.Ordinal)
            .Select(GetCollectionState)
            .Select(static state => state.CommitGate)
            .ToArray();

        return gates;
    }

    private static async Task<IDisposable> EnterCollectionGatesAsyncCore(
        CollectionCommitGate[] gates,
        bool exclusive,
        CancellationToken cancellationToken)
    {
        return await CollectionState.CollectionCommitGateScope.EnterAsync(gates, exclusive, cancellationToken).ConfigureAwait(false);
    }

    internal IDisposable EnterCollectionDocumentLocks(IEnumerable<CollectionDocumentLockKey> lockKeys)
    {
        if (lockKeys == null) throw new ArgumentNullException(nameof(lockKeys));

        var groupedKeys = lockKeys
            .Where(static key => !string.IsNullOrWhiteSpace(key.CollectionName) &&
                                 key.DocumentId != null &&
                                 !key.DocumentId.IsNull)
            .GroupBy(static key => key.CollectionName, StringComparer.Ordinal)
            .OrderBy(static group => group.Key, StringComparer.Ordinal)
            .ToArray();

        var scopes = new List<IDisposable>(groupedKeys.Length);
        try
        {
            foreach (var group in groupedKeys)
            {
                scopes.Add(GetCollectionState(group.Key).EnterDocumentLocks(group.Select(static key => key.DocumentId)));
            }

            return new DisposableListScope(scopes.ToArray());
        }
        catch
        {
            for (var i = scopes.Count - 1; i >= 0; i--)
            {
                scopes[i].Dispose();
            }

            throw;
        }
    }

    private sealed class DisposableListScope : IDisposable
    {
        private readonly IDisposable[] _scopes;
        private bool _disposed;

        public DisposableListScope(IDisposable[] scopes)
        {
            _scopes = scopes ?? throw new ArgumentNullException(nameof(scopes));
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

    private sealed class PrefetchedDocumentLockScope : IDisposable
    {
        private readonly IDisposable _documentLocks;
        private readonly Page[] _pinnedPages;
        private bool _disposed;

        public PrefetchedDocumentLockScope(IDisposable documentLocks, Page[] pinnedPages)
        {
            _documentLocks = documentLocks ?? throw new ArgumentNullException(nameof(documentLocks));
            _pinnedPages = pinnedPages ?? throw new ArgumentNullException(nameof(pinnedPages));
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;

            _documentLocks.Dispose();
            foreach (var page in _pinnedPages)
            {
                page.Unpin();
            }
        }
    }

    private sealed class PrefetchedSingleDocumentLockScope : IDisposable
    {
        private readonly IDisposable _documentLock;
        private readonly Page? _pinnedPage;
        private bool _disposed;

        public PrefetchedSingleDocumentLockScope(IDisposable documentLock, Page? pinnedPage)
        {
            _documentLock = documentLock ?? throw new ArgumentNullException(nameof(documentLock));
            _pinnedPage = pinnedPage;
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;

            _documentLock.Dispose();
            _pinnedPage?.Unpin();
        }
    }

    private async Task<IDisposable> EnterDocumentLocksWithPrefetchedPagesAsync(
        CollectionState st,
        IEnumerable<BsonValue> ids,
        CancellationToken cancellationToken)
    {
        if (st == null) throw new ArgumentNullException(nameof(st));
        if (ids == null) throw new ArgumentNullException(nameof(ids));

        var documentIds = CollectDistinctDocumentIds(ids, static id => id);

        return await EnterDocumentLocksWithPrefetchedPagesAsync(st, documentIds, cancellationToken).ConfigureAwait(false);
    }

    private async Task<IDisposable> EnterDocumentLocksWithPrefetchedPagesAsync<T>(
        CollectionState st,
        IReadOnlyList<T> items,
        Func<T, BsonValue> idSelector,
        CancellationToken cancellationToken)
    {
        if (st == null) throw new ArgumentNullException(nameof(st));
        if (items == null) throw new ArgumentNullException(nameof(items));
        if (idSelector == null) throw new ArgumentNullException(nameof(idSelector));

        var documentIds = CollectDistinctDocumentIds(items, idSelector);

        return await EnterDocumentLocksWithPrefetchedPagesAsync(st, documentIds, cancellationToken).ConfigureAwait(false);
    }

    private async Task<IDisposable> EnterDocumentLockWithPrefetchedPageAsync(
        CollectionState st,
        BsonValue documentId,
        CancellationToken cancellationToken)
    {
        if (st == null) throw new ArgumentNullException(nameof(st));
        if (documentId == null)
        {
            return await st.EnterDocumentLocksAsync(Array.Empty<BsonValue>(), cancellationToken).ConfigureAwait(false);
        }

        if (documentId.IsNull)
        {
            return await st.EnterDocumentLockAsync(documentId, cancellationToken).ConfigureAwait(false);
        }

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            Page? pinnedPage = null;
            IDisposable? documentLock = null;
            var transferOwnership = false;

            try
            {
                var prefetchedPageId = GetCurrentDocumentPageId(st, documentId);
                if (prefetchedPageId != 0)
                {
                    pinnedPage = await _pageManager.GetPagePinnedAsync(
                        prefetchedPageId,
                        cancellationToken: cancellationToken).ConfigureAwait(false);
                }

                documentLock = await st.EnterDocumentLockAsync(documentId, cancellationToken).ConfigureAwait(false);

                var currentPageId = GetCurrentDocumentPageId(st, documentId);
                if (currentPageId == 0 || currentPageId == prefetchedPageId)
                {
                    transferOwnership = true;
                    return new PrefetchedSingleDocumentLockScope(documentLock, pinnedPage);
                }
            }
            finally
            {
                if (!transferOwnership)
                {
                    documentLock?.Dispose();
                    pinnedPage?.Unpin();
                }
            }
        }
    }

    private async Task<IDisposable> EnterDocumentLocksWithPrefetchedPagesAsync(
        CollectionState st,
        BsonValue[] documentIds,
        CancellationToken cancellationToken)
    {
        if (st == null) throw new ArgumentNullException(nameof(st));
        if (documentIds == null) throw new ArgumentNullException(nameof(documentIds));

        if (documentIds.Length == 0)
        {
            return await st.EnterDocumentLocksAsync(documentIds, cancellationToken).ConfigureAwait(false);
        }

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var pinnedPages = new Dictionary<uint, Page>();
            IDisposable? documentLocks = null;
            var transferOwnership = false;

            try
            {
                foreach (var pageId in GetCurrentDocumentPageIds(st, documentIds))
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    if (pinnedPages.ContainsKey(pageId))
                    {
                        continue;
                    }

                    var page = await _pageManager.GetPagePinnedAsync(pageId, cancellationToken: cancellationToken).ConfigureAwait(false);
                    pinnedPages.Add(pageId, page);
                }

                documentLocks = await st.EnterDocumentLocksAsync(documentIds, cancellationToken).ConfigureAwait(false);

                var currentPageIds = GetCurrentDocumentPageIds(st, documentIds);
                if (currentPageIds.All(pinnedPages.ContainsKey))
                {
                    transferOwnership = true;
                    return new PrefetchedDocumentLockScope(documentLocks, ToPageArray(pinnedPages.Values));
                }
            }
            finally
            {
                if (!transferOwnership)
                {
                    documentLocks?.Dispose();
                    foreach (var page in pinnedPages.Values)
                    {
                        page.Unpin();
                    }
                }
            }
        }
    }

    private static BsonValue[] CollectDistinctDocumentIds<T>(IEnumerable<T> items, Func<T, BsonValue> idSelector)
    {
        List<BsonValue>? ids = null;
        HashSet<BsonValue>? seen = null;
        BsonValue? singleId = null;

        foreach (var item in items)
        {
            var id = idSelector(item);
            if (id == null || id.IsNull)
            {
                continue;
            }

            if (singleId == null)
            {
                singleId = id;
                continue;
            }

            if (seen == null)
            {
                seen = new HashSet<BsonValue>(BsonValueComparer.EqualityComparer) { singleId };
                ids = items is IReadOnlyCollection<T> collection
                    ? new List<BsonValue>(collection.Count)
                    : new List<BsonValue>();
                ids.Add(singleId);
            }

            if (seen.Add(id))
            {
                ids!.Add(id);
            }
        }

        if (ids == null)
        {
            return singleId == null ? Array.Empty<BsonValue>() : new[] { singleId };
        }

        return ids.ToArray();
    }

    private static uint GetCurrentDocumentPageId(CollectionState st, BsonValue id)
    {
        return st.Index.TryGet(id, out var location) ? location.PageId : 0;
    }

    private static uint[] GetCurrentDocumentPageIds(CollectionState st, IReadOnlyList<BsonValue> ids)
    {
        List<uint>? pageIds = null;
        HashSet<uint>? seen = null;
        uint singlePageId = 0;

        foreach (var id in ids)
        {
            if (st.Index.TryGet(id, out var location) && location.PageId != 0)
            {
                if (singlePageId == 0)
                {
                    singlePageId = location.PageId;
                    continue;
                }

                if (seen == null)
                {
                    seen = new HashSet<uint> { singlePageId };
                    pageIds = new List<uint>(ids.Count) { singlePageId };
                }

                if (seen.Add(location.PageId))
                {
                    pageIds!.Add(location.PageId);
                }
            }
        }

        if (pageIds == null)
        {
            return singlePageId == 0 ? Array.Empty<uint>() : new[] { singlePageId };
        }

        pageIds.Sort();
        return pageIds.ToArray();
    }

    private static Page[] ToPageArray(Dictionary<uint, Page>.ValueCollection pages)
    {
        var result = new Page[pages.Count];
        var index = 0;
        foreach (var page in pages)
        {
            result[index++] = page;
        }

        return result;
    }
}
