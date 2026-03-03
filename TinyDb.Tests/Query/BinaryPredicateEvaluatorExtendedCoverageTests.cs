using System;
using System.Buffers.Binary;
using System.Linq.Expressions;
using System.Text;
using TinyDb.Bson;
using TinyDb.Query;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Query;

public sealed class BinaryPredicateEvaluatorExtendedCoverageTests
{
    [Test]
    public async Task TryEvaluate_PrimitiveNumericAndDateBranches_ShouldWork()
    {
        var int64Bytes = new byte[8];
        BinaryPrimitives.WriteInt64LittleEndian(int64Bytes, 123L);
        var tsOk = BinaryPredicateEvaluator.TryEvaluate(int64Bytes, 0, BsonType.Timestamp, ExpressionType.Equal, 123L, null, out var tsResult);
        await Assert.That(tsOk).IsTrue();
        await Assert.That(tsResult).IsTrue();

        var dtBytes = new byte[8];
        var targetDate = new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Utc);
        var ms = (long)(targetDate - new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc)).TotalMilliseconds;
        BinaryPrimitives.WriteInt64LittleEndian(dtBytes, ms);
        var dtOk = BinaryPredicateEvaluator.TryEvaluate(dtBytes, 0, BsonType.DateTime, ExpressionType.Equal, targetDate, null, out var dtResult);
        await Assert.That(dtOk).IsTrue();
        await Assert.That(dtResult).IsTrue();

        var dbl = BitConverter.GetBytes(7.5d);
        var dblOk = BinaryPredicateEvaluator.TryEvaluate(dbl, 0, BsonType.Double, ExpressionType.GreaterThan, 7.0d, null, out var dblResult);
        await Assert.That(dblOk).IsTrue();
        await Assert.That(dblResult).IsTrue();

        var invalidOffset = BinaryPredicateEvaluator.TryEvaluate(dbl, -1, BsonType.Double, ExpressionType.Equal, 7.5d, null, out _);
        await Assert.That(invalidOffset).IsFalse();
    }

    [Test]
    public async Task TryEvaluate_ObjectIdBooleanStringAndNullBranches_ShouldWork()
    {
        var oid = ObjectId.NewObjectId();
        var oidBytes = oid.ToByteArray().ToArray();

        var eqOk = BinaryPredicateEvaluator.TryEvaluate(oidBytes, 0, BsonType.ObjectId, ExpressionType.Equal, oid, null, out var eqResult);
        await Assert.That(eqOk).IsTrue();
        await Assert.That(eqResult).IsTrue();

        var neOk = BinaryPredicateEvaluator.TryEvaluate(oidBytes, 0, BsonType.ObjectId, ExpressionType.NotEqual, ObjectId.NewObjectId(), null, out var neResult);
        await Assert.That(neOk).IsTrue();
        await Assert.That(neResult).IsTrue();

        var boolBytes = new byte[] { 1 };
        var boolEq = BinaryPredicateEvaluator.TryEvaluate(boolBytes, 0, BsonType.Boolean, ExpressionType.Equal, true, null, out var boolEqResult);
        await Assert.That(boolEq).IsTrue();
        await Assert.That(boolEqResult).IsTrue();

        var boolUnsupported = BinaryPredicateEvaluator.TryEvaluate(boolBytes, 0, BsonType.Boolean, ExpressionType.GreaterThan, true, null, out _);
        await Assert.That(boolUnsupported).IsFalse();

        var sourceText = "hello";
        var sourceUtf8 = Encoding.UTF8.GetBytes(sourceText);
        var bsonStringBytes = new byte[4 + sourceUtf8.Length + 1];
        BinaryPrimitives.WriteInt32LittleEndian(bsonStringBytes.AsSpan(0, 4), sourceUtf8.Length + 1);
        sourceUtf8.CopyTo(bsonStringBytes.AsSpan(4));
        bsonStringBytes[^1] = 0;

        var strOk = BinaryPredicateEvaluator.TryEvaluate(
            bsonStringBytes, 0, BsonType.String, ExpressionType.Equal, "ignored", sourceUtf8, out var strResult);
        await Assert.That(strOk).IsTrue();
        await Assert.That(strResult).IsTrue();

        var strNotEq = BinaryPredicateEvaluator.TryEvaluate(
            bsonStringBytes, 0, BsonType.String, ExpressionType.NotEqual, "world", null, out var strNotEqResult);
        await Assert.That(strNotEq).IsTrue();
        await Assert.That(strNotEqResult).IsTrue();

        var strInvalidOperator = BinaryPredicateEvaluator.TryEvaluate(
            bsonStringBytes, 0, BsonType.String, ExpressionType.GreaterThan, "hello", null, out _);
        await Assert.That(strInvalidOperator).IsFalse();

        var nullEq = BinaryPredicateEvaluator.TryEvaluate(Array.Empty<byte>(), 0, BsonType.Null, ExpressionType.Equal, null, null, out var nullEqResult);
        await Assert.That(nullEq).IsTrue();
        await Assert.That(nullEqResult).IsTrue();

        var nullLt = BinaryPredicateEvaluator.TryEvaluate(Array.Empty<byte>(), 0, BsonType.Null, ExpressionType.LessThan, "x", null, out var nullLtResult);
        await Assert.That(nullLt).IsTrue();
        await Assert.That(nullLtResult).IsTrue();

        var defaultType = BinaryPredicateEvaluator.TryEvaluate(Array.Empty<byte>(), 0, BsonType.Array, ExpressionType.Equal, null, null, out _);
        await Assert.That(defaultType).IsFalse();
    }

    [Test]
    public async Task TryEvaluate_ConversionFailureAndCatchBranches_ShouldReturnFalse()
    {
        var intBytes = new byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(intBytes, 10);

        var badTarget = BinaryPredicateEvaluator.TryEvaluate(intBytes, 0, BsonType.Int32, ExpressionType.Equal, "not-an-int", null, out _);
        await Assert.That(badTarget).IsFalse();

        var dtBadTarget = BinaryPredicateEvaluator.TryEvaluate(new byte[8], 0, BsonType.DateTime, ExpressionType.Equal, new object(), null, out _);
        await Assert.That(dtBadTarget).IsFalse();

        var oidBadTarget = BinaryPredicateEvaluator.TryEvaluate(new byte[12], 0, BsonType.ObjectId, ExpressionType.Equal, new byte[3], null, out _);
        await Assert.That(oidBadTarget).IsFalse();

        // Trigger Decimal128.ToDecimal overflow path and enter TryEvaluate catch block.
        var overflowDec = Decimal128.MaxValue;
        var overflowBytes = new byte[16];
        BinaryPrimitives.WriteUInt64LittleEndian(overflowBytes.AsSpan(0, 8), overflowDec.LowBits);
        BinaryPrimitives.WriteUInt64LittleEndian(overflowBytes.AsSpan(8, 8), overflowDec.HighBits);
        var overflowResult = BinaryPredicateEvaluator.TryEvaluate(overflowBytes, 0, BsonType.Decimal128, ExpressionType.Equal, 1m, null, out _);
        await Assert.That(overflowResult).IsFalse();

        var evaluateFalse = BinaryPredicateEvaluator.Evaluate(intBytes, 0, BsonType.Int32, ExpressionType.Equal, "bad-target");
        await Assert.That(evaluateFalse).IsFalse();
    }

    [Test]
    public async Task TryEvaluate_ShouldCoverRemainingOperatorAndConverterBranches()
    {
        var int64Bytes = new byte[8];
        BinaryPrimitives.WriteInt64LittleEndian(int64Bytes, 50L);
        var int64Ok = BinaryPredicateEvaluator.TryEvaluate(int64Bytes, 0, BsonType.Int64, ExpressionType.LessThan, 100L, null, out var int64Result);
        await Assert.That(int64Ok).IsTrue();
        await Assert.That(int64Result).IsTrue();

        var unsupportedNumericOp = BinaryPredicateEvaluator.TryEvaluate(int64Bytes, 0, BsonType.Int64, ExpressionType.Add, 50L, null, out var unsupportedResult);
        await Assert.That(unsupportedNumericOp).IsTrue();
        await Assert.That(unsupportedResult).IsFalse();

        var oid = ObjectId.NewObjectId();
        var oidBytes = oid.ToByteArray().ToArray();
        var asByteArrayTarget = BinaryPredicateEvaluator.TryEvaluate(oidBytes, 0, BsonType.ObjectId, ExpressionType.GreaterThanOrEqual, oidBytes, null, out var geResult);
        await Assert.That(asByteArrayTarget).IsTrue();
        await Assert.That(geResult).IsTrue();

        var asStringTarget = BinaryPredicateEvaluator.TryEvaluate(oidBytes, 0, BsonType.ObjectId, ExpressionType.LessThanOrEqual, oid.ToString(), null, out var leResult);
        await Assert.That(asStringTarget).IsTrue();
        await Assert.That(leResult).IsTrue();

        var objUnsupported = BinaryPredicateEvaluator.TryEvaluate(oidBytes, 0, BsonType.ObjectId, ExpressionType.Add, oid, null, out var objUnsupportedResult);
        await Assert.That(objUnsupported).IsTrue();
        await Assert.That(objUnsupportedResult).IsFalse();

        var nullGt = BinaryPredicateEvaluator.TryEvaluate(Array.Empty<byte>(), 0, BsonType.Null, ExpressionType.GreaterThan, null, null, out var nullGtResult);
        await Assert.That(nullGt).IsTrue();
        await Assert.That(nullGtResult).IsFalse();

        var nullGe = BinaryPredicateEvaluator.TryEvaluate(Array.Empty<byte>(), 0, BsonType.Null, ExpressionType.GreaterThanOrEqual, null, null, out var nullGeResult);
        await Assert.That(nullGe).IsTrue();
        await Assert.That(nullGeResult).IsTrue();

        var nullLe = BinaryPredicateEvaluator.TryEvaluate(Array.Empty<byte>(), 0, BsonType.Null, ExpressionType.LessThanOrEqual, "x", null, out var nullLeResult);
        await Assert.That(nullLe).IsTrue();
        await Assert.That(nullLeResult).IsTrue();

        var nullUnsupported = BinaryPredicateEvaluator.TryEvaluate(Array.Empty<byte>(), 0, BsonType.Null, ExpressionType.Add, null, null, out var nullUnsupportedResult);
        await Assert.That(nullUnsupported).IsTrue();
        await Assert.That(nullUnsupportedResult).IsFalse();

        var badInt64Convert = BinaryPredicateEvaluator.TryEvaluate(int64Bytes, 0, BsonType.Int64, ExpressionType.Equal, new object(), null, out _);
        await Assert.That(badInt64Convert).IsFalse();

        var badDoubleConvert = BinaryPredicateEvaluator.TryEvaluate(BitConverter.GetBytes(1.25d), 0, BsonType.Double, ExpressionType.Equal, new object(), null, out _);
        await Assert.That(badDoubleConvert).IsFalse();

        var badDecimalConvert = BinaryPredicateEvaluator.TryEvaluate(new byte[16], 0, BsonType.Decimal128, ExpressionType.Equal, new object(), null, out _);
        await Assert.That(badDecimalConvert).IsFalse();

        var badBoolConvert = BinaryPredicateEvaluator.TryEvaluate(new byte[] { 1 }, 0, BsonType.Boolean, ExpressionType.Equal, new object(), null, out _);
        await Assert.That(badBoolConvert).IsFalse();

        var dateFromString = BinaryPredicateEvaluator.TryEvaluate(new byte[8], 0, BsonType.DateTime, ExpressionType.Equal, "2020-01-01T00:00:00Z", null, out _);
        await Assert.That(dateFromString).IsTrue();
    }
}
