using System;
using System.Linq.Expressions;
using TinyDb.Query;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Query;

public class ExpressionParserErrorTests
{
    private readonly ExpressionParser _parser = new();

    public class TestDoc
    {
        public int Id { get; set; }
    }

    [Test]
    public async Task Parse_Unsupported_NodeType_ShouldThrow()
    {
        // 1. Conditional (ternary)
        // x => x.Id > 0 ? true : false
        Expression<Func<TestDoc, bool>> conditional = x => x.Id > 0 ? true : false;
        await Assert.That(() => _parser.Parse(conditional)).Throws<NotSupportedException>();

        // 2. New
        // x => new TestDoc() != null
        Expression<Func<TestDoc, bool>> @new = x => new TestDoc() != null;
        // Wait, if it doesn't depend on x, it is PRE-EVALUATED!
        // So I must include parameter x in the New expression if possible, or use one that cannot be evaluated.
        // But New usually can be evaluated if it has no param.
        
        // Use a node that HAS a parameter but is not handled.
        // ListInit? x => new List<int> { x.Id }.Count > 0
        Expression<Func<TestDoc, bool>> listInit = x => new System.Collections.Generic.List<int> { x.Id }.Count > 0;
        await Assert.That(() => _parser.Parse(listInit)).Throws<NotSupportedException>();
    }

    [Test]
    public async Task Parse_Unsupported_Binary_ShouldThrow()
    {
        // Power? Not a standard C# operator for ints.
        // LeftShift? 
        Expression<Func<TestDoc, bool>> shift = x => (x.Id << 1) == 2;
        await Assert.That(() => _parser.Parse(shift)).Throws<NotSupportedException>();
    }
}
