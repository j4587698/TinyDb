using System;
using TinyDb.Bson;
using TinyDb.Query;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Query;

public sealed class ExpressionEvaluatorToDoubleCoverageTests
{
    [Test]
    public async Task ToDouble_ShouldCoverBranches()
    {
        var entity = new object();

        var decimalValue = new FunctionExpression("Pow", null, new QueryExpression[]
        {
            new ConstantExpression(12.5m),
            new ConstantExpression(1)
        });
        await Assert.That(ExpressionEvaluator.EvaluateValue(decimalValue, entity)).IsEqualTo(12.5d);

        var bsonDouble = new BinaryExpression(System.Linq.Expressions.ExpressionType.Add, new ConstantExpression(new BsonDouble(1.25d)), new ConstantExpression(1.0d));
        await Assert.That(ExpressionEvaluator.EvaluateValue(bsonDouble, entity)).IsEqualTo(2.25d);

        var bsonInt32 = new BinaryExpression(System.Linq.Expressions.ExpressionType.Add, new ConstantExpression(new BsonInt32(3)), new ConstantExpression(0.5d));
        await Assert.That(ExpressionEvaluator.EvaluateValue(bsonInt32, entity)).IsEqualTo(3.5d);

        var bsonInt64 = new BinaryExpression(System.Linq.Expressions.ExpressionType.Add, new ConstantExpression(new BsonInt64(4)), new ConstantExpression(0.5d));
        await Assert.That(ExpressionEvaluator.EvaluateValue(bsonInt64, entity)).IsEqualTo(4.5d);

        var invalid = new BinaryExpression(System.Linq.Expressions.ExpressionType.Add, new ConstantExpression(new object()), new ConstantExpression(1.0d));
        await Assert.That(() => ExpressionEvaluator.EvaluateValue(invalid, entity)).Throws<InvalidOperationException>();

        var bsonDateTime = new BinaryExpression(System.Linq.Expressions.ExpressionType.Add, new ConstantExpression(new BsonDateTime(DateTime.UtcNow)), new ConstantExpression(1.0d));
        await Assert.That(() => ExpressionEvaluator.EvaluateValue(bsonDateTime, entity)).Throws<InvalidOperationException>();

        var bsonObjectId = new BinaryExpression(System.Linq.Expressions.ExpressionType.Add, new ConstantExpression(new BsonObjectId(ObjectId.NewObjectId())), new ConstantExpression(1.0d));
        await Assert.That(() => ExpressionEvaluator.EvaluateValue(bsonObjectId, entity)).Throws<InvalidOperationException>();
    }
}
