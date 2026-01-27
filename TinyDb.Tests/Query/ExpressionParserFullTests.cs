using System.Linq.Expressions;
using TinyDb.Query;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Query;

public class ExpressionParserFullTests
{
    private readonly ExpressionParser _parser = new();

    public class TestObj
    {
        public bool Flag { get; set; }
        public int Num { get; set; }
        public string Text { get; set; } = "";
    }

    [Test]
    public async Task Parse_Unary_Not_Should_Work()
    {
        Expression<Func<TestObj, bool>> expr = x => !x.Flag;
        var q = _parser.Parse(expr);
        await Assert.That(q).IsNotNull();
    }

    [Test]
    public async Task Parse_MethodCall_Equals_Should_Work()
    {
        Expression<Func<TestObj, bool>> expr = x => x.Text.Equals("A");
        var q = _parser.Parse(expr);
        await Assert.That(q).IsNotNull();
    }

    [Test]
    public async Task Parse_Conditional_Should_Throw()
    {
        Expression<Func<TestObj, bool>> expr = x => x.Flag ? true : false;
        await Assert.That(() => _parser.Parse(expr)).Throws<NotSupportedException>();
    }

    [Test]
    public async Task Parse_Unsupported_NodeType_Should_Throw()
    {
        // Array index access might be supported or throw NotSupportedException
        Expression<Func<int[], bool>> expr = x => x[0] == 1;
        
        // If it's not supported, it throws NotSupportedException
        // The parser supports Binary, Member, Constant, Parameter, Unary, MethodCall.
        // Index access is IndexExpression? Or MethodCall (get_Item)?
        // Arrays use IndexExpression (if supported by language version) or Binary? 
        // Array access is usually NodeType.Index or NodeType.ArrayIndex.
        // Neither is in the switch.
        
        await Assert.That(() => _parser.Parse(expr)).Throws<NotSupportedException>();
    }
}
