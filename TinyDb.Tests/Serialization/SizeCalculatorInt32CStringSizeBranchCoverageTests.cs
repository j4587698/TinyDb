using System;
using System.Reflection;
using TinyDb.Serialization;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Serialization;

public class SizeCalculatorInt32CStringSizeBranchCoverageTests
{
    [Test]
    public async Task CalculateInt32CStringSize_ShouldThrow_ForNegative_AndReturnDigitsPlusNull()
    {
        var method = typeof(SizeCalculator).GetMethod("CalculateInt32CStringSize", BindingFlags.NonPublic | BindingFlags.Static);
        await Assert.That(method).IsNotNull();

        await Assert.That(() => InvokeInt32(method!, -1)).Throws<ArgumentOutOfRangeException>();
        await Assert.That(InvokeInt32(method!, 0)).IsEqualTo(2);   // "0\0"
        await Assert.That(InvokeInt32(method!, 10)).IsEqualTo(3);  // "10\0"
    }

    private static int InvokeInt32(MethodInfo method, int value)
    {
        try
        {
            return (int)method.Invoke(null, new object[] { value })!;
        }
        catch (TargetInvocationException tie) when (tie.InnerException != null)
        {
            throw tie.InnerException;
        }
    }
}

