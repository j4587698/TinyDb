using System;
using System.Linq;
using System.Reflection;
using TinyDb.Query;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using Expression = System.Linq.Expressions.Expression;

namespace TinyDb.Tests.Query;

public sealed class ExpressionParserCoverage100Tests
{
    private sealed class InstanceFieldContainer
    {
        public int Value;
    }

    private sealed class NullToString
    {
        public override string ToString() => null!;
    }

    [Test]
    public async Task ParseExpression_WhenConstantEvaluationThrows_ShouldFallbackToNormalParsing()
    {
        var field = typeof(InstanceFieldContainer).GetField(nameof(InstanceFieldContainer.Value))!;
        var expr = Expression.Field(Expression.Constant(null, typeof(InstanceFieldContainer)), field);

        var parser = new ExpressionParser();
        var parsed = parser.ParseExpression(expr);

        await Assert.That(parsed).IsTypeOf<MemberExpression>();
    }

    [Test]
    public async Task TryEvaluateMember_DateTimePropertiesAndDefaults_ShouldBeCovered()
    {
        var dt = new DateTime(2024, 2, 3, 4, 5, 6, DateTimeKind.Utc);
        var parser = new ExpressionParser();

        QueryExpression Parse(Expression e) => parser.ParseExpression(e);

        await Assert.That(((ConstantExpression)Parse(Expression.Property(Expression.Constant(dt), nameof(DateTime.Year)))).Value).IsEqualTo(dt.Year);
        await Assert.That(((ConstantExpression)Parse(Expression.Property(Expression.Constant(dt), nameof(DateTime.Month)))).Value).IsEqualTo(dt.Month);
        await Assert.That(((ConstantExpression)Parse(Expression.Property(Expression.Constant(dt), nameof(DateTime.Day)))).Value).IsEqualTo(dt.Day);
        await Assert.That(((ConstantExpression)Parse(Expression.Property(Expression.Constant(dt), nameof(DateTime.Hour)))).Value).IsEqualTo(dt.Hour);
        await Assert.That(((ConstantExpression)Parse(Expression.Property(Expression.Constant(dt), nameof(DateTime.Minute)))).Value).IsEqualTo(dt.Minute);
        await Assert.That(((ConstantExpression)Parse(Expression.Property(Expression.Constant(dt), nameof(DateTime.Second)))).Value).IsEqualTo(dt.Second);
        await Assert.That(((ConstantExpression)Parse(Expression.Property(Expression.Constant(dt), nameof(DateTime.Date)))).Value).IsEqualTo(dt.Date);
        await Assert.That(((ConstantExpression)Parse(Expression.Property(Expression.Constant(dt), nameof(DateTime.DayOfWeek)))).Value).IsEqualTo((int)dt.DayOfWeek);

        await Assert.That(Parse(Expression.Property(Expression.Constant(dt), nameof(DateTime.Ticks)))).IsTypeOf<MemberExpression>();
    }

    [Test]
    public async Task TryEvaluateMember_StaticDateTimeAndEnvironmentNewLine_ShouldBeCovered()
    {
        var parser = new ExpressionParser();

        await Assert.That(parser.ParseExpression(Expression.Property(null, typeof(DateTime), nameof(DateTime.Now)))).IsTypeOf<ConstantExpression>();
        await Assert.That(parser.ParseExpression(Expression.Property(null, typeof(DateTime), nameof(DateTime.UtcNow)))).IsTypeOf<ConstantExpression>();
        await Assert.That(parser.ParseExpression(Expression.Property(null, typeof(DateTime), nameof(DateTime.Today)))).IsTypeOf<ConstantExpression>();
        await Assert.That(parser.ParseExpression(Expression.Property(null, typeof(Environment), nameof(Environment.NewLine)))).IsTypeOf<ConstantExpression>();

        await Assert.That(() => parser.ParseExpression(Expression.Property(null, typeof(Environment), nameof(Environment.MachineName))))
            .Throws<NotSupportedException>();
    }

    [Test]
    public async Task TryEvaluateMethodCall_StaticCalls_ShouldBeCovered()
    {
        var parser = new ExpressionParser();

        var newGuid = typeof(Guid).GetMethod(nameof(Guid.NewGuid), Type.EmptyTypes)!;
        var guidExpr = Expression.Call(newGuid);
        await Assert.That(parser.ParseExpression(guidExpr)).IsTypeOf<ConstantExpression>();

        var isNullOrEmpty = typeof(string).GetMethod(nameof(string.IsNullOrEmpty), new[] { typeof(string) })!;
        var isNullOrEmptyExpr = Expression.Call(isNullOrEmpty, Expression.Constant(""));
        await Assert.That(((ConstantExpression)parser.ParseExpression(isNullOrEmptyExpr)).Value).IsEqualTo(true);

        var isNullOrWhiteSpace = typeof(string).GetMethod(nameof(string.IsNullOrWhiteSpace), new[] { typeof(string) })!;
        var isNullOrWhiteSpaceExpr = Expression.Call(isNullOrWhiteSpace, Expression.Constant(" \t"));
        await Assert.That(((ConstantExpression)parser.ParseExpression(isNullOrWhiteSpaceExpr)).Value).IsEqualTo(true);

        var concat = typeof(string).GetMethod(nameof(string.Concat), new[] { typeof(string), typeof(string) })!;
        var concatExpr = Expression.Call(concat, Expression.Constant("a"), Expression.Constant("b"));
        await Assert.That(parser.ParseExpression(concatExpr)).IsTypeOf<FunctionExpression>();
    }

    [Test]
    public async Task TryEvaluateMethodCall_StringInstanceMethods_ShouldBeCovered()
    {
        var parser = new ExpressionParser();
        var s = Expression.Constant("Abc");

        await Assert.That(((ConstantExpression)parser.ParseExpression(Expression.Call(s, nameof(string.Contains), null, Expression.Constant("b")))).Value).IsEqualTo(true);
        await Assert.That(((ConstantExpression)parser.ParseExpression(Expression.Call(s, nameof(string.StartsWith), null, Expression.Constant("A")))).Value).IsEqualTo(true);
        await Assert.That(((ConstantExpression)parser.ParseExpression(Expression.Call(s, nameof(string.EndsWith), null, Expression.Constant("c")))).Value).IsEqualTo(true);
        await Assert.That(((ConstantExpression)parser.ParseExpression(Expression.Call(s, nameof(string.ToLower), null))).Value).IsEqualTo("abc");
        await Assert.That(((ConstantExpression)parser.ParseExpression(Expression.Call(s, nameof(string.ToUpper), null))).Value).IsEqualTo("ABC");
        await Assert.That(((ConstantExpression)parser.ParseExpression(Expression.Call(Expression.Constant("  a  "), nameof(string.Trim), null))).Value).IsEqualTo("a");
        await Assert.That(((ConstantExpression)parser.ParseExpression(Expression.Call(s, nameof(string.Substring), null, Expression.Constant(1)))).Value).IsEqualTo("bc");
        await Assert.That(((ConstantExpression)parser.ParseExpression(Expression.Call(s, nameof(string.Substring), null, Expression.Constant(0), Expression.Constant(2)))).Value).IsEqualTo("Ab");
        await Assert.That(((ConstantExpression)parser.ParseExpression(Expression.Call(s, nameof(string.Replace), null, Expression.Constant("A"), Expression.Constant("Z")))).Value).IsEqualTo("Zbc");

        await Assert.That(parser.ParseExpression(Expression.Call(s, nameof(string.PadLeft), null, Expression.Constant(5)))).IsTypeOf<FunctionExpression>();
    }

    [Test]
    public async Task TryEvaluateMethodCall_DateTimeInstanceMethods_ShouldBeCovered()
    {
        var parser = new ExpressionParser();
        var dt = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var target = Expression.Constant(dt);

        await Assert.That(((ConstantExpression)parser.ParseExpression(Expression.Call(target, nameof(DateTime.AddDays), null, Expression.Constant(1d)))).Value).IsEqualTo(dt.AddDays(1));
        await Assert.That(((ConstantExpression)parser.ParseExpression(Expression.Call(target, nameof(DateTime.AddHours), null, Expression.Constant(2d)))).Value).IsEqualTo(dt.AddHours(2));
        await Assert.That(((ConstantExpression)parser.ParseExpression(Expression.Call(target, nameof(DateTime.AddMinutes), null, Expression.Constant(3d)))).Value).IsEqualTo(dt.AddMinutes(3));
        await Assert.That(((ConstantExpression)parser.ParseExpression(Expression.Call(target, nameof(DateTime.AddSeconds), null, Expression.Constant(4d)))).Value).IsEqualTo(dt.AddSeconds(4));
        await Assert.That(((ConstantExpression)parser.ParseExpression(Expression.Call(target, nameof(DateTime.AddYears), null, Expression.Constant(5)))).Value).IsEqualTo(dt.AddYears(5));
        await Assert.That(((ConstantExpression)parser.ParseExpression(Expression.Call(target, nameof(DateTime.AddMonths), null, Expression.Constant(6)))).Value).IsEqualTo(dt.AddMonths(6));

        await Assert.That(parser.ParseExpression(Expression.Call(target, nameof(DateTime.AddTicks), null, Expression.Constant(1L)))).IsTypeOf<FunctionExpression>();
    }

    [Test]
    public async Task TryEvaluateMethodCall_WhenTargetOrArgumentNotEvaluatable_ShouldReturnNull()
    {
        var method = typeof(ExpressionParser).GetMethod("TryEvaluateMethodCall", BindingFlags.NonPublic | BindingFlags.Static);
        await Assert.That(method).IsNotNull();

        // target not evaluatable
        var p = Expression.Parameter(typeof(string), "s");
        var callOnParam = Expression.Call(p, nameof(string.ToLowerInvariant), null);
        var result1 = method!.Invoke(null, new object[] { callOnParam });
        await Assert.That(result1).IsNull();

        // argument not evaluatable
        var arg = Expression.Parameter(typeof(string), "arg");
        var callWithParamArg = Expression.Call(Expression.Constant("abc"), nameof(string.Contains), null, arg);
        var result2 = method!.Invoke(null, new object[] { callWithParamArg });
        await Assert.That(result2).IsNull();

        // constant null argument is allowed but patterns won't match => null
        var callWithNullArg = Expression.Call(Expression.Constant("abc"), nameof(string.Contains), null, Expression.Constant(null, typeof(string)));
        var result3 = method!.Invoke(null, new object[] { callWithNullArg });
        await Assert.That(result3).IsNull();

        // fall-through return null
        var getHashCode = typeof(object).GetMethod(nameof(object.GetHashCode), Type.EmptyTypes)!;
        var callOther = Expression.Call(Expression.Constant(new object()), getHashCode);
        var result4 = method!.Invoke(null, new object[] { callOther });
        await Assert.That(result4).IsNull();
    }

    [Test]
    public async Task TryEvaluateMethodCall_ToStringOnNullTarget_ShouldReturnNull()
    {
        var method = typeof(ExpressionParser).GetMethod("TryEvaluateMethodCall", BindingFlags.NonPublic | BindingFlags.Static);
        await Assert.That(method).IsNotNull();

        var toString = typeof(object).GetMethod(nameof(object.ToString), Type.EmptyTypes)!;
        var call = Expression.Call(Expression.Constant(null, typeof(object)), toString);

        var result = method!.Invoke(null, new object[] { call });
        await Assert.That(result).IsNull();
    }

    [Test]
    public async Task TryEvaluateBinary_AndAlsoOrElse_ShortCircuitBranches_ShouldBeCovered()
    {
        var method = typeof(ExpressionParser).GetMethod("TryEvaluateBinary", BindingFlags.NonPublic | BindingFlags.Static);
        await Assert.That(method).IsNotNull();

        object? Invoke(System.Linq.Expressions.BinaryExpression expr) => method!.Invoke(null, new object[] { expr });

        var p = Expression.Parameter(typeof(bool), "b");
        await Assert.That(Invoke(Expression.AndAlso(p, Expression.Constant(true)))).IsNull();
        await Assert.That(Invoke(Expression.OrElse(p, Expression.Constant(true)))).IsNull();

        await Assert.That(Invoke(Expression.AndAlso(Expression.Constant(false), p))).IsEqualTo(false);
        await Assert.That(Invoke(Expression.OrElse(Expression.Constant(true), p))).IsEqualTo(true);

        await Assert.That(Invoke(Expression.AndAlso(Expression.Constant(true), Expression.Constant(false)))).IsEqualTo(false);
        await Assert.That(Invoke(Expression.OrElse(Expression.Constant(false), Expression.Constant(true)))).IsEqualTo(true);

        await Assert.That(Invoke(Expression.AndAlso(Expression.Constant(true), p))).IsNull();
        await Assert.That(Invoke(Expression.OrElse(Expression.Constant(false), p))).IsNull();

        await Assert.That(Invoke(Expression.NotEqual(Expression.Constant(1), Expression.Constant(1)))).IsEqualTo(false);
        await Assert.That(Invoke(Expression.GreaterThan(Expression.Constant(2), Expression.Constant(1)))).IsEqualTo(true);
        await Assert.That(Invoke(Expression.GreaterThanOrEqual(Expression.Constant(1), Expression.Constant(1)))).IsEqualTo(true);
        await Assert.That(Invoke(Expression.LessThan(Expression.Constant(1), Expression.Constant(2)))).IsEqualTo(true);
        await Assert.That(Invoke(Expression.LessThanOrEqual(Expression.Constant(1), Expression.Constant(1)))).IsEqualTo(true);

        await Assert.That(Invoke(Expression.Modulo(Expression.Constant(5), Expression.Constant(2)))).IsNull();
    }

    [Test]
    public async Task EvaluateArithmeticOperators_AllTypeCombinations_ShouldBeCovered()
    {
        var add = typeof(ExpressionParser).GetMethod("EvaluateAdd", BindingFlags.NonPublic | BindingFlags.Static)!;
        var sub = typeof(ExpressionParser).GetMethod("EvaluateSubtract", BindingFlags.NonPublic | BindingFlags.Static)!;
        var mul = typeof(ExpressionParser).GetMethod("EvaluateMultiply", BindingFlags.NonPublic | BindingFlags.Static)!;
        var div = typeof(ExpressionParser).GetMethod("EvaluateDivide", BindingFlags.NonPublic | BindingFlags.Static)!;

        await Assert.That(add.Invoke(null, new object[] { 1, 2 })).IsEqualTo(3);
        await Assert.That(add.Invoke(null, new object[] { 1L, 2L })).IsEqualTo(3L);
        await Assert.That(add.Invoke(null, new object[] { 1d, 2d })).IsEqualTo(3d);
        await Assert.That(add.Invoke(null, new object[] { 1m, 2m })).IsEqualTo(3m);
        await Assert.That(add.Invoke(null, new object[] { 1, 2L })).IsEqualTo(3L);
        await Assert.That(add.Invoke(null, new object[] { 1L, 2 })).IsEqualTo(3L);
        await Assert.That(add.Invoke(null, new object[] { new DateTime(2024, 1, 1), TimeSpan.FromDays(1) })).IsEqualTo(new DateTime(2024, 1, 2));
        await Assert.That(add.Invoke(null, new object[] { TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(2) })).IsEqualTo(TimeSpan.FromSeconds(3));
        await Assert.That(add.Invoke(null, new object[] { new object(), new object() })).IsNull();

        await Assert.That(sub.Invoke(null, new object[] { 3, 2 })).IsEqualTo(1);
        await Assert.That(sub.Invoke(null, new object[] { 3L, 2L })).IsEqualTo(1L);
        await Assert.That(sub.Invoke(null, new object[] { 3d, 2d })).IsEqualTo(1d);
        await Assert.That(sub.Invoke(null, new object[] { 3m, 2m })).IsEqualTo(1m);
        await Assert.That(sub.Invoke(null, new object[] { 3, 2L })).IsEqualTo(1L);
        await Assert.That(sub.Invoke(null, new object[] { 3L, 2 })).IsEqualTo(1L);
        await Assert.That(sub.Invoke(null, new object[] { new DateTime(2024, 1, 2), TimeSpan.FromDays(1) })).IsEqualTo(new DateTime(2024, 1, 1));
        await Assert.That(sub.Invoke(null, new object[] { new DateTime(2024, 1, 2), new DateTime(2024, 1, 1) })).IsEqualTo(TimeSpan.FromDays(1));
        await Assert.That(sub.Invoke(null, new object[] { TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(1) })).IsEqualTo(TimeSpan.FromSeconds(1));
        await Assert.That(sub.Invoke(null, new object[] { new object(), new object() })).IsNull();

        await Assert.That(mul.Invoke(null, new object[] { 2, 3 })).IsEqualTo(6);
        await Assert.That(mul.Invoke(null, new object[] { 2L, 3L })).IsEqualTo(6L);
        await Assert.That(mul.Invoke(null, new object[] { 2d, 3d })).IsEqualTo(6d);
        await Assert.That(mul.Invoke(null, new object[] { 2m, 3m })).IsEqualTo(6m);
        await Assert.That(mul.Invoke(null, new object[] { 2, 3L })).IsEqualTo(6L);
        await Assert.That(mul.Invoke(null, new object[] { 2L, 3 })).IsEqualTo(6L);
        await Assert.That(mul.Invoke(null, new object[] { TimeSpan.FromSeconds(2), 2d })).IsEqualTo(TimeSpan.FromSeconds(4));
        await Assert.That(mul.Invoke(null, new object[] { 2d, TimeSpan.FromSeconds(2) })).IsEqualTo(TimeSpan.FromSeconds(4));
        await Assert.That(mul.Invoke(null, new object[] { TimeSpan.FromSeconds(2), 2 })).IsEqualTo(TimeSpan.FromSeconds(4));
        await Assert.That(mul.Invoke(null, new object[] { 2, TimeSpan.FromSeconds(2) })).IsEqualTo(TimeSpan.FromSeconds(4));
        await Assert.That(mul.Invoke(null, new object[] { new object(), new object() })).IsNull();

        await Assert.That(div.Invoke(null, new object[] { 10, 2 })).IsEqualTo(5);
        await Assert.That(div.Invoke(null, new object[] { 10L, 2L })).IsEqualTo(5L);
        await Assert.That(div.Invoke(null, new object[] { 10d, 2d })).IsEqualTo(5d);
        await Assert.That(div.Invoke(null, new object[] { 10m, 2m })).IsEqualTo(5m);
        await Assert.That(div.Invoke(null, new object[] { 10, 2L })).IsEqualTo(5L);
        await Assert.That(div.Invoke(null, new object[] { 10L, 2 })).IsEqualTo(5L);
        await Assert.That(div.Invoke(null, new object[] { TimeSpan.FromSeconds(10), 2d })).IsEqualTo(TimeSpan.FromSeconds(5));
        await Assert.That(div.Invoke(null, new object[] { TimeSpan.FromSeconds(10), 2 })).IsEqualTo(TimeSpan.FromSeconds(5));

        await Assert.That(div.Invoke(null, new object[] { 10, 0 })).IsNull();
        await Assert.That(div.Invoke(null, new object[] { 10L, 0L })).IsNull();
        await Assert.That(div.Invoke(null, new object[] { 10d, 0d })).IsNull();
        await Assert.That(div.Invoke(null, new object[] { 10m, 0m })).IsNull();
        await Assert.That(div.Invoke(null, new object[] { 10, 0L })).IsNull();
        await Assert.That(div.Invoke(null, new object[] { 10L, 0 })).IsNull();
        await Assert.That(div.Invoke(null, new object[] { TimeSpan.FromSeconds(10), 0d })).IsNull();
        await Assert.That(div.Invoke(null, new object[] { TimeSpan.FromSeconds(10), 0 })).IsNull();
    }

    [Test]
    public async Task TryCompare_AllTypeCombinations_ShouldBeCovered()
    {
        var method = typeof(ExpressionParser).GetMethod("TryCompare", BindingFlags.NonPublic | BindingFlags.Static)!;
        object Invoke(object left, object right)
        {
            var args = new object?[] { left, right, 0 };
            return method.Invoke(null, args)!;
        }

        bool TryCompare(object left, object right, out int result)
        {
            var args = new object?[] { left, right, 0 };
            var ok = (bool)method.Invoke(null, args)!;
            result = (int)args[2]!;
            return ok;
        }

        await Assert.That(TryCompare(1, 2, out _)).IsTrue();
        await Assert.That(TryCompare(1, 2L, out _)).IsTrue();
        await Assert.That(TryCompare(2L, 1, out _)).IsTrue();
        await Assert.That(TryCompare(1L, 2L, out _)).IsTrue();

        await Assert.That(TryCompare(1d, 2d, out _)).IsTrue();
        await Assert.That(TryCompare(1d, 2, out _)).IsTrue();
        await Assert.That(TryCompare(1, 2d, out _)).IsTrue();
        await Assert.That(TryCompare(1d, 2L, out _)).IsTrue();
        await Assert.That(TryCompare(2L, 1d, out _)).IsTrue();

        await Assert.That(TryCompare(1m, 2m, out _)).IsTrue();
        await Assert.That(TryCompare(1m, 2, out _)).IsTrue();
        await Assert.That(TryCompare(1, 2m, out _)).IsTrue();
        await Assert.That(TryCompare(1m, 2L, out _)).IsTrue();
        await Assert.That(TryCompare(2L, 1m, out _)).IsTrue();

        await Assert.That(TryCompare("a", "b", out _)).IsTrue();
        await Assert.That(TryCompare(new DateTime(2024, 1, 1), new DateTime(2024, 1, 2), out _)).IsTrue();
        await Assert.That(TryCompare(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(2), out _)).IsTrue();

        await Assert.That((bool)Invoke(Guid.Empty, Guid.Empty)).IsFalse();
    }

    [Test]
    public async Task TryEvaluateConvert_And_TryEvaluateConditional_NullBranches_ShouldBeCovered()
    {
        var convert = typeof(ExpressionParser).GetMethod("TryEvaluateConvert", BindingFlags.NonPublic | BindingFlags.Static)!;
        var conditional = typeof(ExpressionParser).GetMethod("TryEvaluateConditional", BindingFlags.NonPublic | BindingFlags.Static)!;

        var p = Expression.Parameter(typeof(int), "x");
        var unary = Expression.Convert(p, typeof(long));
        await Assert.That(convert.Invoke(null, new object[] { unary })).IsNull();

        var b = Expression.Parameter(typeof(bool), "b");
        var cond = Expression.Condition(b, Expression.Constant(1), Expression.Constant(2));
        await Assert.That(conditional.Invoke(null, new object[] { cond })).IsNull();
    }

    [Test]
    public async Task ParseMemberExpression_StaticDateTimeCases_ShouldBeCovered()
    {
        var parser = new ExpressionParser();
        var parseMember = typeof(ExpressionParser).GetMethod("ParseMemberExpression", BindingFlags.NonPublic | BindingFlags.Instance)!;

        var now = Expression.Property(null, typeof(DateTime), nameof(DateTime.Now));
        await Assert.That(parseMember.Invoke(parser, new object[] { now })).IsTypeOf<ConstantExpression>();

        var utcNow = Expression.Property(null, typeof(DateTime), nameof(DateTime.UtcNow));
        await Assert.That(parseMember.Invoke(parser, new object[] { utcNow })).IsTypeOf<ConstantExpression>();

        var today = Expression.Property(null, typeof(DateTime), nameof(DateTime.Today));
        await Assert.That(parseMember.Invoke(parser, new object[] { today })).IsTypeOf<ConstantExpression>();

        var machineName = Expression.Property(null, typeof(Environment), nameof(Environment.MachineName));

        static object? InvokeUnwrap(MethodInfo method, object instance, object[] args)
        {
            try
            {
                return method.Invoke(instance, args);
            }
            catch (TargetInvocationException tie) when (tie.InnerException != null)
            {
                throw tie.InnerException;
            }
        }

        await Assert.That(() => InvokeUnwrap(parseMember, parser, new object[] { machineName }))
            .Throws<NotSupportedException>();
    }

    [Test]
    public async Task ParseMemberExpression_ConstantContainerFieldAndKnownProperty_ShouldBeCovered()
    {
        var parser = new ExpressionParser();
        var parseMember = typeof(ExpressionParser).GetMethod("ParseMemberExpression", BindingFlags.NonPublic | BindingFlags.Instance)!;

        var container = new InstanceFieldContainer { Value = 123 };
        var field = typeof(InstanceFieldContainer).GetField(nameof(InstanceFieldContainer.Value))!;
        var fieldExpr = Expression.Field(Expression.Constant(container), field);
        await Assert.That(parseMember.Invoke(parser, new object[] { fieldExpr })).IsTypeOf<ConstantExpression>();

        var lengthProp = typeof(string).GetProperty(nameof(string.Length))!;
        var lengthExpr = Expression.Property(Expression.Constant("abc"), lengthProp);
        var parsed = (ConstantExpression)parseMember.Invoke(parser, new object[] { lengthExpr })!;
        await Assert.That(parsed.Value).IsEqualTo(3);
    }

    [Test]
    public async Task EvaluateKnownProperty_StringAndDateTime_ShouldBeCovered()
    {
        var method = typeof(ExpressionParser).GetMethod("EvaluateKnownProperty", BindingFlags.NonPublic | BindingFlags.Static)!;

        var lengthProp = typeof(string).GetProperty(nameof(string.Length))!;
        await Assert.That(method.Invoke(null, new object?[] { "abc", lengthProp })).IsEqualTo(3);

        var dt = new DateTime(2024, 2, 3, 4, 5, 6, DateTimeKind.Utc);
        var properties = new[]
        {
            nameof(DateTime.Year),
            nameof(DateTime.Month),
            nameof(DateTime.Day),
            nameof(DateTime.Hour),
            nameof(DateTime.Minute),
            nameof(DateTime.Second),
            nameof(DateTime.Date),
            nameof(DateTime.DayOfWeek)
        };

        foreach (var name in properties)
        {
            var prop = typeof(DateTime).GetProperty(name)!;
            await Assert.That(method.Invoke(null, new object?[] { dt, prop })).IsNotNull();
        }

        var ticks = typeof(DateTime).GetProperty(nameof(DateTime.Ticks))!;
        await Assert.That(method.Invoke(null, new object?[] { dt, ticks })).IsNull();
    }

    [Test]
    public async Task ParseMethodCallExpression_ToStringOptimization_ShouldBeCovered()
    {
        var parser = new ExpressionParser();
        var toString = typeof(NullToString).GetMethod(nameof(ToString), Type.EmptyTypes)!;
        var expr = Expression.Call(Expression.Constant(new NullToString()), toString);

        var parsed = (ConstantExpression)parser.ParseExpression(expr);
        await Assert.That(parsed.Value).IsNull();
    }
}
