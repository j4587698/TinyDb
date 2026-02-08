using System.Reflection;
using TinyDb.Serialization;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Serialization;

public class SizeCalculatorNullStringBranchCoverageTests
{
    [Test]
    public async Task PrivateStringSizeHelpers_WhenNull_ShouldReturnZero()
    {
        var calcString = typeof(SizeCalculator).GetMethod("CalculateStringSize", BindingFlags.NonPublic | BindingFlags.Static);
        var calcCString = typeof(SizeCalculator).GetMethod("CalculateCStringSize", BindingFlags.NonPublic | BindingFlags.Static);

        await Assert.That(calcString).IsNotNull();
        await Assert.That(calcCString).IsNotNull();

        var s1 = (int)calcString!.Invoke(null, new object[] { null! })!;
        var s2 = (int)calcCString!.Invoke(null, new object[] { null! })!;

        await Assert.That(s1).IsEqualTo(0);
        await Assert.That(s2).IsEqualTo(0);
    }
}

