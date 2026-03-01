using System;

namespace TinyDb.Core;

/// <summary>
/// TinyDb 日志级别。
/// </summary>
public enum TinyDbLogLevel
{
    Debug = 0,
    Information = 1,
    Warning = 2,
    Error = 3,
    Critical = 4
}

internal static class TinyDbLogging
{
    public static readonly Action<TinyDbLogLevel, string, Exception?> NoopLogger = static (_, _, _) => { };

    public static void SafeLog(Action<TinyDbLogLevel, string, Exception?>? logger, TinyDbLogLevel level, string message, Exception? ex = null)
    {
        if (logger == null) return;

        try
        {
            logger(level, message, ex);
        }
        catch (Exception logEx) when (logEx is not OutOfMemoryException)
        {
            // 日志回调属于辅助路径，不应影响主流程。
        }
    }
}
