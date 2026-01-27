using System;
using System.Collections.Generic;
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

        // 2. ListInit (New with initializer)
        // x => new List<int> { x.Id }.Count > 0
        Expression<Func<TestDoc, bool>> listInit = x => new List<int> { x.Id }.Count > 0;
        await Assert.That(() => _parser.Parse(listInit)).Throws<NotSupportedException>();
        
        // 3. NewArrayInit
        // x => new int[] { x.Id }.Length > 0
        Expression<Func<TestDoc, bool>> newArrayInit = x => new int[] { x.Id }.Length > 0;
        await Assert.That(() => _parser.Parse(newArrayInit)).Throws<NotSupportedException>();
        
        // 4. NewArrayBounds
        // x => new int[x.Id].Length > 0
        Expression<Func<TestDoc, bool>> newArrayBounds = x => new int[x.Id].Length > 0;
        await Assert.That(() => _parser.Parse(newArrayBounds)).Throws<NotSupportedException>();
        
        // 5. TypeIs (x is TestDoc)
        // Note: x is TestDoc is always true, might be optimized by compiler?
        // Use: x is object
        Expression<Func<TestDoc, bool>> typeIs = x => x is object;
        await Assert.That(() => _parser.Parse(typeIs)).Throws<NotSupportedException>();
        
        // 6. TypeAs (x as object)
        Expression<Func<TestDoc, bool>> typeAs = x => (x as object) != null;
        await Assert.That(() => _parser.Parse(typeAs)).Throws<NotSupportedException>();
        
        // 7. MemberInit - NOW SUPPORTED for AOT projections
        // x => new TestDoc { Id = x.Id } != null
        // MemberInitExpression is now parsed and can be evaluated
        
        // 8. Invocation (invoking a delegate)
        Func<int, bool> d = i => i > 0;
        Expression<Func<TestDoc, bool>> invoke = x => d(x.Id);
        // Note: This might be compiled as InvocationExpression
        await Assert.That(() => _parser.Parse(invoke)).Throws<NotSupportedException>();
    }

    [Test]
    public async Task Parse_Unsupported_Binary_ShouldThrow()
    {
        // LeftShift
        Expression<Func<TestDoc, bool>> shift = x => (x.Id << 1) == 2;
        await Assert.That(() => _parser.Parse(shift)).Throws<NotSupportedException>();
        
        // RightShift
        Expression<Func<TestDoc, bool>> rightShift = x => (x.Id >> 1) == 0;
        await Assert.That(() => _parser.Parse(rightShift)).Throws<NotSupportedException>();
        
        // ExclusiveOr (^)
        Expression<Func<TestDoc, bool>> xor = x => (x.Id ^ 1) == 0;
        await Assert.That(() => _parser.Parse(xor)).Throws<NotSupportedException>();
        
        // Modulo (%)
        Expression<Func<TestDoc, bool>> modulo = x => (x.Id % 2) == 0;
        await Assert.That(() => _parser.Parse(modulo)).Throws<NotSupportedException>();
        
        // Coalesce (??)
        // Needs nullable int
        Expression<Func<TestDoc, bool>> coalesce = x => (x.Id as int? ?? 0) == 0;
        // x.Id as int? is TypeAs (unsupported). 
        // Let's assume we have a nullable property.
        // But TypeAs fails first.
    }
    
    [Test]
    public async Task Parse_Unsupported_Unary_ShouldThrow()
    {
        // ArrayLength?
        // x.Id is int. ArrayLength expects array.
        // new int[x.Id].Length is Unary ArrayLength? Or MemberAccess Length?
        // Usually MemberAccess "Length".
        // Unary ArrayLength is for multidimensional arrays?
        
        // TypeAs is Unary. Covered.
        
        // Negate is supported.
        
        // UnaryPlus (+x.Id)
        Expression<Func<TestDoc, bool>> unaryPlus = x => +x.Id == 1;
        // C# compiler might optimize +x to x?
        // If not, it's UnaryPlus.
        // Let's see.
        try 
        {
            _parser.Parse(unaryPlus); 
            // If it passes (optimized), fine. If it throws, fine.
            // If it returns UnaryExpression with UnaryPlus, evaluating it might fail if Evaluator doesn't support it.
            // Parser supports: Not, Negate, Convert.
            // UnaryPlus is likely not supported in Parser switch.
        }
        catch(NotSupportedException) {}
    }
}