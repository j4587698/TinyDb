using System.Runtime.CompilerServices;
using TUnit.Core;

namespace TinyDb.Tests.Utils;

/// <summary>
/// Skip attribute for tests that require dynamic code generation (not available in AOT).
/// When running in AOT mode, tests with this attribute will be skipped with the provided reason.
/// </summary>
public class SkipInAotAttribute : SkipAttribute
{
    public SkipInAotAttribute() : base("This test requires dynamic code generation which is not available in AOT mode.")
    {
    }

    public SkipInAotAttribute(string reason) : base(reason)
    {
    }

    public override Task<bool> ShouldSkip(TestRegisteredContext context)
    {
        return Task.FromResult(!RuntimeFeature.IsDynamicCodeSupported);
    }
}

/// <summary>
/// Attribute to indicate a test requires dynamic code generation (not available in AOT).
/// The test should manually check RuntimeFeature.IsDynamicCodeSupported and return early if false.
/// </summary>
[AttributeUsage(AttributeTargets.Method | AttributeTargets.Class, AllowMultiple = false)]
[Obsolete("Use [SkipInAot] attribute instead for proper test skipping in AOT mode.")]
public class RequiresDynamicCodeAttribute : Attribute
{
}

/// <summary>
/// Helper methods for AOT compatibility checks
/// </summary>
public static class AotHelper
{
    /// <summary>
    /// Returns true if dynamic code generation is supported (JIT mode)
    /// </summary>
    public static bool IsDynamicCodeSupported => RuntimeFeature.IsDynamicCodeSupported;

    /// <summary>
    /// Returns true if running in AOT mode (no dynamic code)
    /// </summary>
    public static bool IsAotMode => !RuntimeFeature.IsDynamicCodeSupported;
}
