using TinyDb.Query;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Query;

public sealed class QueryPushdownInfoCoverageTests
{
    [Test]
    public async Task BoolProperties_ShouldReflectCountFlags()
    {
        var pushed = new QueryPushdownInfo
        {
            WherePushedCount = 1,
            OrderPushedCount = 2,
            SkipPushedCount = 1,
            TakePushedCount = 1
        };

        var notPushed = new QueryPushdownInfo
        {
            WherePushedCount = 0,
            OrderPushedCount = 0,
            SkipPushedCount = 0,
            TakePushedCount = 0
        };

        await Assert.That(pushed.WherePushed).IsTrue();
        await Assert.That(pushed.SkipPushed).IsTrue();
        await Assert.That(notPushed.WherePushed).IsFalse();
        await Assert.That(notPushed.SkipPushed).IsFalse();
    }

    [Test]
    public async Task QuerySortField_NullArguments_ShouldThrow()
    {
        await Assert.That(() => new QuerySortField(null!, typeof(int), descending: false)).Throws<ArgumentNullException>();
        await Assert.That(() => new QuerySortField("a", null!, descending: false)).Throws<ArgumentNullException>();
    }

    [Test]
    public async Task QuerySortField_Properties_ShouldExposeCtorValues()
    {
        var field = new QuerySortField("score", typeof(long), descending: true);

        await Assert.That(field.FieldName).IsEqualTo("score");
        await Assert.That(field.MemberType).IsEqualTo(typeof(long));
        await Assert.That(field.Descending).IsTrue();
    }
}
