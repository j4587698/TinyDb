using System;
using System.IO;
using System.Reflection;
using TinyDb.Core;
using TinyDb.Storage;
using TinyDb.Tests.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Storage;

[NotInParallel]
[SkipInAot]
public sealed class LargeDocumentStorageCoverageEdgeTests
{
    [Test]
    public async Task PrivateLogMethod_ShouldInvokeLoggerThroughSafeLog()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"lds_log_cov_{Guid.NewGuid():N}.db");
        try
        {
            TinyDbLogLevel? level = null;
            string? message = null;
            Exception? captured = null;

            using var stream = new DiskStream(dbPath);
            using var pageManager = new PageManager(
                stream,
                4096,
                logger: (l, m, ex) =>
                {
                    level = l;
                    message = m;
                    captured = ex;
                });
            var storage = new LargeDocumentStorage(pageManager, 4096, (l, m, ex) =>
            {
                level = l;
                message = m;
                captured = ex;
            });

            var logMethod = typeof(LargeDocumentStorage).GetMethod("Log", BindingFlags.Instance | BindingFlags.NonPublic)
                ?? throw new MissingMethodException(typeof(LargeDocumentStorage).FullName, "Log");

            var expected = new InvalidOperationException("lds-log");
            logMethod.Invoke(storage, new object?[] { TinyDbLogLevel.Error, "large-doc-log", expected });

            await Assert.That(level).IsEqualTo(TinyDbLogLevel.Error);
            await Assert.That(message).IsEqualTo("large-doc-log");
            await Assert.That(object.ReferenceEquals(captured, expected)).IsTrue();
        }
        finally
        {
            try { if (File.Exists(dbPath)) File.Delete(dbPath); } catch { }
        }
    }
}
