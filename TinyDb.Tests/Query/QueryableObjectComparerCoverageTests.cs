using System;
using System.Globalization;
using System.Linq;
using TinyDb.Bson;
using TinyDb.Query;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Query;

public sealed class QueryableObjectComparerCoverageTests
{
    private sealed class KeyItem
    {
        public object Key { get; init; } = null!;
    }

    [Test]
    public async Task ObjectComparer_CompareAndToDouble_ShouldCoverBranches()
    {
        static KeyItem Item(object key) => new() { Key = key };

        static List<KeyItem> OrderByKey(params KeyItem[] items)
        {
            var query = items.AsQueryable().OrderBy(x => x.Key);
            var result = QueryPipeline.ExecuteAotForTests<KeyItem>(query.Expression, items, extractedPredicate: null);
            return ((IEnumerable<KeyItem>)result!).ToList();
        }

        static double ToNumber(object value) => value switch
        {
            BsonDouble bd => bd.Value,
            BsonInt32 bi => bi.Value,
            BsonInt64 bl => bl.Value,
            _ => Convert.ToDouble(value, CultureInfo.InvariantCulture)
        };

        var bsonOrdered = OrderByKey(Item(new BsonInt32(2)), Item(new BsonInt32(1)));
        await Assert.That(((BsonValue)bsonOrdered[0].Key).ToInt32(null)).IsEqualTo(1);
        await Assert.That(((BsonValue)bsonOrdered[1].Key).ToInt32(null)).IsEqualTo(2);

        var bsonDoubleVsDoubleOrdered = OrderByKey(Item(2.0d), Item(new BsonDouble(1.0d)));
        await Assert.That(ToNumber(bsonDoubleVsDoubleOrdered[0].Key)).IsEqualTo(1.0d);
        await Assert.That(ToNumber(bsonDoubleVsDoubleOrdered[1].Key)).IsEqualTo(2.0d);

        var numericOrdered = OrderByKey(Item(3), Item(1L), Item((short)2));
        await Assert.That(ToNumber(numericOrdered[0].Key)).IsEqualTo(1.0d);
        await Assert.That(ToNumber(numericOrdered[1].Key)).IsEqualTo(2.0d);
        await Assert.That(ToNumber(numericOrdered[2].Key)).IsEqualTo(3.0d);

        var stringOrdered = OrderByKey(Item("b"), Item("a"));
        await Assert.That((string)stringOrdered[0].Key).IsEqualTo("a");
        await Assert.That((string)stringOrdered[1].Key).IsEqualTo("b");

        var d1 = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var d2 = d1.AddDays(1);
        var dateOrdered = OrderByKey(Item(d2), Item(d1));
        await Assert.That((DateTime)dateOrdered[0].Key).IsEqualTo(d1);
        await Assert.That((DateTime)dateOrdered[1].Key).IsEqualTo(d2);

        var g1 = Guid.Empty;
        var g2 = Guid.NewGuid();
        var guidOrdered = OrderByKey(Item(g2), Item(g1));
        await Assert.That((Guid)guidOrdered[0].Key).IsEqualTo(g1);
        await Assert.That((Guid)guidOrdered[1].Key).IsEqualTo(g2);

        var mixedOrdered = OrderByKey(Item("x"), Item(Guid.Empty));
        await Assert.That(mixedOrdered.Count).IsEqualTo(2);
        await Assert.That(mixedOrdered[0].Key).IsEqualTo(Guid.Empty);
    }
}
