using System;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text.Json;
using TinyDb.Bson;

namespace TinyDb.Serialization;

/// <summary>
/// 提供在BSON值与CLR值之间转换的基础设施，避免运行时反射构造动态代码。
/// </summary>
public static partial class BsonConversion
{
    private const int MaxConversionDepth = 128;

    [ThreadStatic]
    private static int _conversionDepth;

    [ThreadStatic]
    private static HashSet<object>? _serializingObjects;

    internal static bool HasActiveSerializationTracking => _serializingObjects != null;

}
