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
    internal bool TryGetSecurityMetadata(out DatabaseSecurityMetadata m) => _header.TryGetSecurityMetadata(out m);

    internal bool IsEncrypted => _encryptionContext != null;

    internal bool VerifyEncryptionPassword(string password)
    {
        if (_encryptionContext == null)
        {
            throw new InvalidOperationException("Database is not encrypted.");
        }

        return _encryptionContext.VerifyPassword(password);
    }

    internal void SetSecurityMetadata(DatabaseSecurityMetadata m)
    {
        EnsureWritable();
        lock (_lock)
        {
            _header.SetSecurityMetadata(m);
            WriteHeader();
        }
    }

    internal void ClearSecurityMetadata()
    {
        EnsureWritable();
        lock (_lock)
        {
            _header.ClearSecurityMetadata();
            WriteHeader();
        }
    }

    internal void RewrapEncryptionPassword(string newPassword)
    {
        EnsureWritable();
        if (_encryptionContext == null)
        {
            return;
        }

        if (_encryptionContext.Metadata.CredentialKind != EncryptionCredentialKind.Password)
        {
            throw new InvalidOperationException("Password changes are only supported for password-encrypted databases.");
        }

        lock (_lock)
        {
            _encryptionContext.RewrapWithPassword(newPassword);
            var p = _pageManager.GetPage(1);
            EncryptionMetadataStore.WriteToPage(p, _encryptionContext.Metadata);
            _pageManager.SavePage(p, true);
        }
    }

    internal void RegisterCollection(string n) => _collectionMetaStore.RegisterCollection(n, _options.WriteConcern == WriteConcern.Synced);

    internal void CopyCollectionsTo(TinyDbEngine target, IReadOnlyList<string>? collectionNames = null)
    {
        if (target == null) throw new ArgumentNullException(nameof(target));

        const int CopyBatchSize = 1000;
        var names = collectionNames ?? GetCollectionNames(includeSystemCollections: true)
            .Distinct(StringComparer.Ordinal)
            .OrderBy(static name => name, StringComparer.Ordinal)
            .ToArray();

        foreach (var collectionName in names)
        {
            target.RegisterCollection(collectionName);

            var sourceIndexManager = GetIndexManager(collectionName);
            var targetIndexManager = target.GetIndexManager(collectionName);
            foreach (var stat in sourceIndexManager.GetAllStatistics())
            {
                targetIndexManager.CreateIndex(stat.Name, stat.Fields, stat.IsUnique, stat.IsSparse);
            }

            var documentBatch = new List<BsonDocument>(CopyBatchSize);
            foreach (var document in StreamCollectionDocumentsForCopy(collectionName))
            {
                documentBatch.Add(document);
                if (documentBatch.Count < CopyBatchSize)
                {
                    continue;
                }

                target.InsertDocuments(collectionName, documentBatch);
                documentBatch.Clear();
            }

            if (documentBatch.Count > 0)
            {
                target.InsertDocuments(collectionName, documentBatch);
            }
        }
    }

    private IEnumerable<BsonDocument> StreamCollectionDocumentsForCopy(string collectionName)
    {
        var state = GetCollectionState(collectionName);
        foreach (var result in StreamRawScanResultPages(collectionName, state, null))
        {
            var document = DeserializeDocumentOrThrow(result.Slice);
            if (document.TryGetValue("_collection", out var collectionValue) &&
                collectionValue.ToString() != collectionName)
            {
                continue;
            }

            yield return ResolveLargeDocument(document);
        }
    }

    internal BsonValue AllocateIdentityId(string collectionName, string idFieldName, Type idType)
    {
        if (string.IsNullOrWhiteSpace(collectionName)) throw new ArgumentException("Collection name cannot be empty.", nameof(collectionName));
        if (string.IsNullOrWhiteSpace(idFieldName)) throw new ArgumentException("ID field name cannot be empty.", nameof(idFieldName));

        bool isInt32 = idType == typeof(int);
        bool isInt64 = idType == typeof(long);
        if (!isInt32 && !isInt64)
        {
            throw new NotSupportedException($"Identity ID type '{idType.FullName}' is not supported.");
        }

        var metadataKey = isInt32 ? $"__identity_int32_{idFieldName}" : $"__identity_int64_{idFieldName}";
        var cacheKey = $"{collectionName}\0{metadataKey}";

        lock (_identitySequenceLock)
        {
            if (!_identitySequences.TryGetValue(cacheKey, out var sequence))
            {
                var current = Math.Max(ReadPersistedIdentityValue(collectionName, metadataKey), ScanMaxIdentityValue(collectionName));
                sequence = new IdentitySequenceState(collectionName, metadataKey, current);
                _identitySequences[cacheKey] = sequence;
            }

            var next = checked(sequence.Current + 1);
            var maxValue = isInt32 ? int.MaxValue : long.MaxValue;
            if (next > maxValue)
            {
                throw new InvalidOperationException($"Identity sequence for '{collectionName}.{idFieldName}' exceeded {idType.Name}.MaxValue.");
            }

            if (next > sequence.ReservedUntil)
            {
                var reservedUntil = ReserveIdentityRangeEnd(next, maxValue);
                PersistIdentityValue(collectionName, metadataKey, reservedUntil);
                sequence.ReservedUntil = reservedUntil;
            }

            sequence.Current = next;
            sequence.HasUnflushedExactValue = true;
            return isInt32 ? new BsonInt32((int)next) : new BsonInt64(next);
        }
    }

    private static long ReserveIdentityRangeEnd(long next, long maxValue)
    {
        var remaining = maxValue - next;
        if (remaining < IdentitySequenceReservationSize - 1)
        {
            return maxValue;
        }

        return next + IdentitySequenceReservationSize - 1;
    }

    private long ReadPersistedIdentityValue(string collectionName, string metadataKey)
    {
        var metadata = _collectionMetaStore.GetMetadata(collectionName, includeInternal: true);
        return TryGetInt64(metadata, metadataKey, out var value) ? value : 0;
    }

    private void PersistIdentityValue(string collectionName, string metadataKey, long value)
    {
        var metadata = _collectionMetaStore.GetMetadata(collectionName, includeInternal: true);
        metadata = metadata.Set(metadataKey, new BsonInt64(value));
        _collectionMetaStore.UpdateMetadata(collectionName, metadata, _options.WriteConcern == WriteConcern.Synced);
    }

    private void FlushIdentitySequenceExactValues()
    {
        lock (_identitySequenceLock)
        {
            foreach (var sequence in _identitySequences.Values)
            {
                if (!sequence.HasUnflushedExactValue)
                {
                    continue;
                }

                if (sequence.Current >= sequence.ReservedUntil)
                {
                    sequence.HasUnflushedExactValue = false;
                    continue;
                }

                PersistIdentityValue(sequence.CollectionName, sequence.MetadataKey, sequence.Current);
                sequence.ReservedUntil = sequence.Current;
                sequence.HasUnflushedExactValue = false;
            }
        }
    }

    private long ScanMaxIdentityValue(string collectionName)
    {
        var state = GetCollectionState(collectionName);
        long max = 0;
        foreach (var document in ReadAllDocumentsSnapshotFromPageSnapshots(collectionName, state))
        {
            if (document.TryGetValue("_id", out var id) && TryConvertIdentityValue(id, out var value) && value > max)
            {
                max = value;
            }
        }

        return max;
    }

    private static bool TryConvertIdentityValue(BsonValue? value, out long result)
    {
        result = 0;
        switch (value)
        {
            case BsonInt32 int32:
                result = int32.Value;
                return result > 0;
            case BsonInt64 int64:
                result = int64.Value;
                return result > 0;
            default:
                return false;
        }
    }

    private void EnsureDatabaseSecurity()
    {
        if (IsEncrypted)
        {
            return;
        }

        var p = _options.Password;
        var hasSecurityMetadata = TryGetSecurityMetadata(out _);
        if (string.IsNullOrEmpty(p))
        {
            if (hasSecurityMetadata) throw new UnauthorizedAccessException();
            return;
        }
        if (p.Length < 8) throw new ArgumentException();
        if (hasSecurityMetadata)
        {
            if (!DatabaseSecurity.AuthenticateDatabase(this, p)) throw new UnauthorizedAccessException();
            return;
        }
        if (!_options.ReadOnly)
        {
            DatabaseSecurity.CreateSecureDatabase(this, p);
        }
    }
}
