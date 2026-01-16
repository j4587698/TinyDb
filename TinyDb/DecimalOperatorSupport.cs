using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace TinyDb;

/// <summary>
/// 为 Decimal 类型提供运算符支持的辅助类（主要用于 AOT 环境）。
/// </summary>
internal static class DecimalOperatorSupport
{
    private static bool _initialized;

#pragma warning disable CA2255
    [ModuleInitializer]
    internal static void Initialize()
    {
        Ensure();
    }
#pragma warning restore CA2255

    [DynamicDependency(DynamicallyAccessedMemberTypes.PublicMethods, typeof(decimal))]
    public static void Ensure()
    {
        if (_initialized) return;

        var zero = decimal.Zero;
        var one = decimal.One;

        _ = GreaterThanInternal(one, zero);
        _ = GreaterThanOrEqualInternal(one, zero);
        _ = LessThanInternal(zero, one);
        _ = LessThanOrEqualInternal(one, one);
        _ = EqualityInternal(one, one);
        _ = InequalityInternal(one, zero);

        _initialized = true;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static bool GreaterThanInternal(decimal left, decimal right) => left > right;

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static bool GreaterThanOrEqualInternal(decimal left, decimal right) => left >= right;

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static bool LessThanInternal(decimal left, decimal right) => left < right;

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static bool LessThanOrEqualInternal(decimal left, decimal right) => left <= right;

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static bool EqualityInternal(decimal left, decimal right) => left == right;

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static bool InequalityInternal(decimal left, decimal right) => left != right;
}
