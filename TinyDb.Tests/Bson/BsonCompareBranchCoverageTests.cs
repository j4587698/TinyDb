using System;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Bson;

public class BsonCompareBranchCoverageTests
{
    [Test]
    public async Task CompareTo_BsonJavaScript_ShouldCoverTypeBranch()
    {
        var js = new BsonJavaScript("return 1;");
        await Assert.That(js.CompareTo((BsonValue)new BsonJavaScript("return 2;"))).IsNotEqualTo(0);
        await Assert.That(js.CompareTo((BsonValue)new BsonInt32(1))).IsNotEqualTo(0);
    }

    [Test]
    public async Task CompareTo_BsonJavaScriptWithScope_ShouldCoverTypeBranch()
    {
        var scope = new BsonDocument().Set("x", 1);
        var js = new BsonJavaScriptWithScope("return x;", scope);
        await Assert.That(js.CompareTo((BsonValue)new BsonJavaScriptWithScope("return x;", scope))).IsEqualTo(0);
        await Assert.That(js.CompareTo((BsonValue)new BsonInt32(1))).IsNotEqualTo(0);
    }

    [Test]
    public async Task CompareTo_BsonSymbol_ShouldCoverTypeBranch()
    {
        var sym = new BsonSymbol("abc");
        await Assert.That(sym.CompareTo((BsonValue)new BsonSymbol("xyz"))).IsNotEqualTo(0);
        await Assert.That(sym.CompareTo((BsonValue)new BsonInt32(1))).IsNotEqualTo(0);
    }

    [Test]
    public async Task Equals_BsonTimestamp_ShouldCoverTypeAndValueBranches()
    {
        var ts1 = new BsonTimestamp(1, 2);
        var ts2 = new BsonTimestamp(1, 3);

        await Assert.That(ts1.Equals((BsonValue)ts1)).IsTrue();
        await Assert.That(ts1.Equals((BsonValue)ts2)).IsFalse();
        await Assert.That(ts1.Equals(new BsonInt32(1))).IsFalse();
    }
}

