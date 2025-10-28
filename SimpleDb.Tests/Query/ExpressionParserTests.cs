using System.Linq.Expressions;
using SimpleDb.Query;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace SimpleDb.Tests.Query;

public class ExpressionParserTests
{
    // 测试实体类
    public class TestPerson
    {
        public string? Name { get; set; }
        public int Age { get; set; }
        public bool IsActive { get; set; }
        public decimal Salary { get; set; }
        public DateTime BirthDate { get; set; }
    }

    [Test]
    public async Task Constructor_Should_Initialize_Correctly()
    {
        // Act
        var parser = new ExpressionParser();

        // Assert
        await Assert.That(parser).IsNotNull();
    }

    [Test]
    public async Task Parse_Null_Expression_Should_Return_Null()
    {
        // Arrange
        var parser = new ExpressionParser();

        // Act
        var result = parser.Parse<TestPerson>(null!);

        // Assert
        await Assert.That(result).IsNull();
    }

    [Test]
    public async Task Parse_Simple_Property_Equality_Should_Work()
    {
        // Arrange
        var parser = new ExpressionParser();
        Expression<Func<TestPerson, bool>> expression = p => p.Name == "Alice";

        // Act
        var result = parser.Parse(expression);

        // Assert
        await Assert.That(result).IsNotNull();
    }

    [Test]
    public async Task Parse_Numeric_Comparison_Should_Work()
    {
        // Arrange
        var parser = new ExpressionParser();
        Expression<Func<TestPerson, bool>> expression = p => p.Age > 30;

        // Act
        var result = parser.Parse(expression);

        // Assert
        await Assert.That(result).IsNotNull();
    }

    [Test]
    public async Task Parse_Boolean_Property_Should_Work()
    {
        // Arrange
        var parser = new ExpressionParser();
        Expression<Func<TestPerson, bool>> expression = p => p.IsActive;

        // Act
        var result = parser.Parse(expression);

        // Assert
        await Assert.That(result).IsNotNull();
    }

    [Test]
    public async Task Parse_Negated_Boolean_Should_Work()
    {
        // Arrange
        var parser = new ExpressionParser();
        Expression<Func<TestPerson, bool>> expression = p => !p.IsActive;

        // Act
        var result = parser.Parse(expression);

        // Assert
        await Assert.That(result).IsNotNull();
    }

    [Test]
    public async Task Parse_Decimal_Comparison_Should_Work()
    {
        // Arrange
        var parser = new ExpressionParser();
        Expression<Func<TestPerson, bool>> expression = p => p.Salary >= 50000.00m;

        // Act
        var result = parser.Parse(expression);

        // Assert
        await Assert.That(result).IsNotNull();
    }

    [Test]
    public async Task Parse_DateTime_Comparison_Should_Work()
    {
        // Arrange
        var parser = new ExpressionParser();
        var cutoffDate = new DateTime(2000, 1, 1);
        Expression<Func<TestPerson, bool>> expression = p => p.BirthDate > cutoffDate;

        // Act
        var result = parser.Parse(expression);

        // Assert
        await Assert.That(result).IsNotNull();
    }

    [Test]
    public async Task Parse_And_Operation_Should_Work()
    {
        // Arrange
        var parser = new ExpressionParser();
        Expression<Func<TestPerson, bool>> expression = p => p.IsActive && p.Age > 25;

        // Act
        var result = parser.Parse(expression);

        // Assert
        await Assert.That(result).IsNotNull();
    }

    [Test]
    public async Task Parse_Or_Operation_Should_Work()
    {
        // Arrange
        var parser = new ExpressionParser();
        Expression<Func<TestPerson, bool>> expression = p => p.IsActive || p.Age > 65;

        // Act
        var result = parser.Parse(expression);

        // Assert
        await Assert.That(result).IsNotNull();
    }

    [Test]
    public async Task Parse_Complex_Logical_Operation_Should_Work()
    {
        // Arrange
        var parser = new ExpressionParser();
        Expression<Func<TestPerson, bool>> expression = p => p.IsActive && (p.Age > 25 || p.Salary > 100000);

        // Act
        var result = parser.Parse(expression);

        // Assert
        await Assert.That(result).IsNotNull();
    }

    [Test]
    public async Task Parse_Not_Equal_Operation_Should_Work()
    {
        // Arrange
        var parser = new ExpressionParser();
        Expression<Func<TestPerson, bool>> expression = p => p.Name != "Admin";

        // Act
        var result = parser.Parse(expression);

        // Assert
        await Assert.That(result).IsNotNull();
    }

    [Test]
    public async Task Parse_Less_Than_Operation_Should_Work()
    {
        // Arrange
        var parser = new ExpressionParser();
        Expression<Func<TestPerson, bool>> expression = p => p.Age < 18;

        // Act
        var result = parser.Parse(expression);

        // Assert
        await Assert.That(result).IsNotNull();
    }

    [Test]
    public async Task Parse_Less_Than_Or_Equal_Operation_Should_Work()
    {
        // Arrange
        var parser = new ExpressionParser();
        Expression<Func<TestPerson, bool>> expression = p => p.Age <= 65;

        // Act
        var result = parser.Parse(expression);

        // Assert
        await Assert.That(result).IsNotNull();
    }

    [Test]
    public async Task Parse_Greater_Than_Or_Equal_Operation_Should_Work()
    {
        // Arrange
        var parser = new ExpressionParser();
        Expression<Func<TestPerson, bool>> expression = p => p.Age >= 18;

        // Act
        var result = parser.Parse(expression);

        // Assert
        await Assert.That(result).IsNotNull();
    }

    [Test]
    public async Task Parse_String_Method_Call_Should_Work()
    {
        // Arrange
        var parser = new ExpressionParser();
        Expression<Func<TestPerson, bool>> expression = p => p.Name!.Contains("test");

        // Act
        var result = parser.Parse(expression);

        // Assert
        await Assert.That(result).IsNotNull();
    }

    [Test]
    public async Task Parse_String_StartsWith_Should_Work()
    {
        // Arrange
        var parser = new ExpressionParser();
        Expression<Func<TestPerson, bool>> expression = p => p.Name!.StartsWith("A");

        // Act
        var result = parser.Parse(expression);

        // Assert
        await Assert.That(result).IsNotNull();
    }

    [Test]
    public async Task Parse_String_EndsWith_Should_Work()
    {
        // Arrange
        var parser = new ExpressionParser();
        Expression<Func<TestPerson, bool>> expression = p => p.Name!.EndsWith("son");

        // Act
        var result = parser.Parse(expression);

        // Assert
        await Assert.That(result).IsNotNull();
    }

    [Test]
    public async Task Parse_Null_Check_Should_Work()
    {
        // Arrange
        var parser = new ExpressionParser();
        Expression<Func<TestPerson, bool>> expression = p => p.Name != null;

        // Act
        var result = parser.Parse(expression);

        // Assert
        await Assert.That(result).IsNotNull();
    }

    [Test]
    public async Task Parse_Multiple_Conditions_Should_Work()
    {
        // Arrange
        var parser = new ExpressionParser();
        Expression<Func<TestPerson, bool>> expression = p =>
            p.Name != null &&
            p.IsActive &&
            p.Age >= 18 &&
            p.Age <= 65 &&
            p.Salary > 30000;

        // Act
        var result = parser.Parse(expression);

        // Assert
        await Assert.That(result).IsNotNull();
    }

    [Test]
    public async Task Parse_Nested_Conditions_Should_Work()
    {
        // Arrange
        var parser = new ExpressionParser();
        Expression<Func<TestPerson, bool>> expression = p =>
            (p.IsActive && p.Age > 30) ||
            (p.Salary > 100000 && p.Name != null);

        // Act
        var result = parser.Parse(expression);

        // Assert
        await Assert.That(result).IsNotNull();
    }

    [Test]
    public async Task Parse_Constant_Expression_Should_Work()
    {
        // Arrange
        var parser = new ExpressionParser();
        Expression<Func<TestPerson, bool>> expression = p => true; // Constant true

        // Act
        var result = parser.Parse(expression);

        // Assert
        await Assert.That(result).IsNotNull();
    }

    [Test]
    public async Task Parse_Constant_False_Should_Work()
    {
        // Arrange
        var parser = new ExpressionParser();
        Expression<Func<TestPerson, bool>> expression = p => false; // Constant false

        // Act
        var result = parser.Parse(expression);

        // Assert
        await Assert.That(result).IsNotNull();
    }

    [Test]
    public async Task Parse_Complex_String_Operations_Should_Work()
    {
        // Arrange
        var parser = new ExpressionParser();
        Expression<Func<TestPerson, bool>> expression = p =>
            p.Name != null &&
            (p.Name.Contains("John") || p.Name.StartsWith("Jane")) &&
            p.Name.EndsWith("Doe");

        // Act
        var result = parser.Parse(expression);

        // Assert
        await Assert.That(result).IsNotNull();
    }

    [Test]
    public async Task Parse_Mixed_Type_Comparisons_Should_Work()
    {
        // Arrange
        var parser = new ExpressionParser();
        var referenceDate = DateTime.Now.AddYears(-30);
        Expression<Func<TestPerson, bool>> expression = p =>
            p.Age > 25 &&
            p.Salary >= 50000 &&
            p.BirthDate < referenceDate;

        // Act
        var result = parser.Parse(expression);

        // Assert
        await Assert.That(result).IsNotNull();
    }
}
