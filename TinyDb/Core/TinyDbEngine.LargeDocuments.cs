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
    private static BsonDocument CreateLargeDocumentIndexDocument(BsonValue id, string col, uint pId, int s)
    {
        var dict = new Dictionary<string, BsonValue>(5)
        {
            { "_id", id },
            { "_collection", col },
            { "_largeDocumentIndex", (long)pId },
            { "_largeDocumentSize", s },
            { "_isLargeDocument", true }
        };
        return new BsonDocument(dict);
    }

    private static bool BsonValuesEqual(BsonValue? left, BsonValue? right) => BsonValueComparer.ValueEquals(left, right);

    private uint StoreLargeDocumentOrThrow(ReadOnlySpan<byte> documentBytes, string collectionName)
    {
        try
        {
            var pageId = _largeDocumentStorage.StoreLargeDocument(documentBytes, collectionName);
            WriteHeader();
            return pageId;
        }
        catch (Exception ex)
        {
            try
            {
                WriteHeader();
            }
            catch (Exception headerException)
            {
                throw new AggregateException(
                    "Failed to store large document and persist page allocation state.",
                    ex,
                    headerException);
            }

            throw;
        }
    }

    private void CleanupLargeDocumentAfterFailedInsert(uint largeDocumentIndexPageId, Exception originalException)
    {
        try
        {
            DeleteLargeDocumentOrThrow(largeDocumentIndexPageId);
        }
        catch (Exception cleanupException)
        {
            var aggregate = new AggregateException(
                $"Failed to clean up large document pages after insert failure at index page {largeDocumentIndexPageId}.",
                originalException,
                cleanupException);
            MarkCorrupted(aggregate);
            throw aggregate;
        }
    }

    private void CleanupLargeDocumentAfterFailedUpdate(uint largeDocumentIndexPageId, Exception originalException)
    {
        try
        {
            DeleteLargeDocumentOrThrow(largeDocumentIndexPageId);
        }
        catch (Exception cleanupException)
        {
            var aggregate = new AggregateException(
                $"Failed to clean up large document pages after update failure at index page {largeDocumentIndexPageId}.",
                originalException,
                cleanupException);
            MarkCorrupted(aggregate);
            throw aggregate;
        }
    }

    private void DeleteLargeDocumentOrThrow(uint largeDocumentIndexPageId)
    {
        try
        {
            _largeDocumentStorage.DeleteLargeDocument(largeDocumentIndexPageId);
        }
        finally
        {
            WriteHeader();
        }
    }
}
