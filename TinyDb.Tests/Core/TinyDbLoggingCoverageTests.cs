using System;
using TinyDb.Core;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Core;

public sealed class TinyDbLoggingCoverageTests
{
    [Test]
    public async Task SafeLog_WhenLoggerThrowsNonOom_ShouldSwallowException()
    {
        var callCount = 0;

        void ThrowingLogger(TinyDbLogLevel level, string message, Exception? ex)
        {
            callCount++;
            throw new InvalidOperationException("log callback failed");
        }

        await Assert.That(() => TinyDbLogging.SafeLog(ThrowingLogger, TinyDbLogLevel.Warning, "x")).ThrowsNothing();
        await Assert.That(callCount).IsEqualTo(1);
    }
}
