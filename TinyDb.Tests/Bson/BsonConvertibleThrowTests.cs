using System;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Bson;

public class BsonConvertibleThrowTests
{
    [Test]
    public async Task BsonJavaScript_Convert_ShouldThrow()
    {
        var js = new BsonJavaScript("code");
        
        await Assert.That(js.ToBoolean(null)).IsTrue();
        
        await Assert.That(() => js.ToDouble(null)).Throws<InvalidCastException>();
        await Assert.That(() => js.ToDecimal(null)).Throws<InvalidCastException>();
        await Assert.That(() => js.ToInt32(null)).Throws<InvalidCastException>();
        await Assert.That(() => js.ToInt64(null)).Throws<InvalidCastException>();
        await Assert.That(() => js.ToDateTime(null)).Throws<InvalidCastException>();
    }

    [Test]
    public async Task BsonSymbol_Convert_ShouldThrow()
    {
        var sym = new BsonSymbol("sym");
        
        await Assert.That(sym.ToBoolean(null)).IsTrue();
        
        await Assert.That(() => sym.ToDouble(null)).Throws<InvalidCastException>();
        await Assert.That(() => sym.ToDecimal(null)).Throws<InvalidCastException>();
        await Assert.That(() => sym.ToInt32(null)).Throws<InvalidCastException>();
    }

    [Test]
    public async Task BsonJavaScriptWithScope_Convert_ShouldThrow()
    {
        var js = new BsonJavaScriptWithScope("code", new BsonDocument());
        
        await Assert.That(js.ToBoolean(null)).IsTrue();
        
        await Assert.That(() => js.ToDouble(null)).Throws<InvalidCastException>();
        await Assert.That(() => js.ToDecimal(null)).Throws<InvalidCastException>();
        await Assert.That(() => js.ToInt32(null)).Throws<InvalidCastException>();
        await Assert.That(() => js.ToDateTime(null)).Throws<InvalidCastException>();
        
        await Assert.That(() => js.ToType(typeof(int), null)).Throws<InvalidCastException>();
        await Assert.That(js.ToType(typeof(string), null)).IsEqualTo("code");
    }
}