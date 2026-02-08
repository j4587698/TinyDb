using System;
using TinyDb.Attributes;
using TUnit.Assertions;
using TUnit.Core;

namespace TinyDb.Tests.Attributes;

public class AttributeNullArgumentCoverageTests
{
    [Test]
    public async Task ForeignKeyAttribute_NullCollectionName_ShouldThrow()
    {
        await Assert.That(() => new ForeignKeyAttribute(null!))
            .Throws<ArgumentNullException>();
    }

    [Test]
    public async Task CompositeIndexAttribute_NullFields_ShouldThrow()
    {
        await Assert.That(() => new CompositeIndexAttribute((string[])null!))
            .Throws<ArgumentNullException>();
    }

    [Test]
    public async Task CompositeIndexAttribute_NullName_ShouldThrow()
    {
        await Assert.That(() => new CompositeIndexAttribute(null!, "a"))
            .Throws<ArgumentNullException>();
        await Assert.That(() => new CompositeIndexAttribute(null!, false, "a"))
            .Throws<ArgumentNullException>();
    }

    [Test]
    public async Task CompositeIndexAttribute_NullFields_WithName_ShouldThrow()
    {
        await Assert.That(() => new CompositeIndexAttribute("idx", (string[])null!))
            .Throws<ArgumentNullException>();
        await Assert.That(() => new CompositeIndexAttribute("idx", true, (string[])null!))
            .Throws<ArgumentNullException>();
    }
}

