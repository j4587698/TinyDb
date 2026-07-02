using TinyDb.Serialization;
using TinyDb.Bson;
using TinyDb.Attributes;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Serialization;

public class AotFallbackTests
{
    // A class NOT registered with Source Generator
    public class UnregisteredEntity
    {
        public string Name { get; set; } = "";
        public int Value { get; set; }
        public Dictionary<string, int> Scores { get; set; } = new();
        public List<string> Tags { get; set; } = new();
    }

    [Test]
    public async Task UnregisteredEntity_ShouldThrow_InAotMode()
    {
        var entity = new UnregisteredEntity
        {
            Name = "Fallback Test",
            Value = 123,
            Scores = new Dictionary<string, int> { { "math", 90 }, { "science", 85 } },
            Tags = new List<string> { "tag1", "tag2" }
        };

        await Assert.That(() => AotBsonMapper.ToDocument(entity))
            .Throws<InvalidOperationException>();

        var doc = new BsonDocument().Set("name", "x");
        await Assert.That(() => AotBsonMapper.FromDocument<UnregisteredEntity>(doc))
            .Throws<InvalidOperationException>();
    }

    [Test]
    public async Task CircularReference_Should_Handle_Gracefully_ForRegisteredEntity()
    {
        var parent = new CircularEntity { Name = "Parent" };
        var child = new CircularEntity { Name = "Child", Parent = parent };
        parent.Child = child;

        // Act
        var doc = AotBsonMapper.ToDocument(parent);
        
        // Assert - Should not stack overflow
        await Assert.That(doc.ContainsKey("child")).IsTrue();
        var childDoc = (BsonDocument)doc["child"];
        await Assert.That(childDoc.ContainsKey("parent")).IsTrue();
        // Circular reference is handled by returning a doc without the back-reference (empty since no _id)
        await Assert.That(((BsonDocument)childDoc["parent"]).Count).IsEqualTo(0);
    }

    [Test]
    public async Task EmbeddedPocoCollections_ShouldRoundTripWithoutEntityAttributeOnItem()
    {
        var order = new EmbeddedOrder
        {
            Id = 42,
            PrimaryItem = new EmbeddedOrderItem
            {
                Sku = "primary",
                Quantity = 3,
                Detail = new EmbeddedOrderItemDetail { Lot = "A1" }
            },
            OptionalItem = new EmbeddedOrderItem
            {
                Sku = "optional",
                Quantity = 6,
                Detail = new EmbeddedOrderItemDetail { Lot = "LO" }
            },
            Items =
            [
                new EmbeddedOrderItem
                {
                    Sku = "first",
                    Quantity = 1,
                    Detail = new EmbeddedOrderItemDetail { Lot = "L1" }
                },
                new EmbeddedOrderItem
                {
                    Sku = "second",
                    Quantity = 2,
                    Detail = new EmbeddedOrderItemDetail { Lot = "L2" }
                }
            ],
            NullableItems =
            [
                null,
                new EmbeddedOrderItem
                {
                    Sku = "nullable-list",
                    Quantity = 7,
                    Detail = new EmbeddedOrderItemDetail { Lot = "LN" }
                }
            ],
            ItemArray =
            [
                new EmbeddedOrderItem
                {
                    Sku = "array",
                    Quantity = 4,
                    Detail = new EmbeddedOrderItemDetail { Lot = "LA" }
                }
            ],
            ItemsByCode = new Dictionary<string, EmbeddedOrderItem>
            {
                ["gift"] = new EmbeddedOrderItem
                {
                    Sku = "gift",
                    Quantity = 5,
                    Detail = new EmbeddedOrderItemDetail { Lot = "LG" }
                }
            },
            NullableItemsByCode = new Dictionary<string, EmbeddedOrderItem?>
            {
                ["none"] = null,
                ["hit"] = new EmbeddedOrderItem
                {
                    Sku = "nullable-dict",
                    Quantity = 8,
                    Detail = new EmbeddedOrderItemDetail { Lot = "LD" }
                }
            }
        };

        var document = AotBsonMapper.ToDocument(order);
        var roundTrip = AotBsonMapper.FromDocument<EmbeddedOrder>(document);

        await Assert.That(roundTrip.PrimaryItem.Sku).IsEqualTo("primary");
        await Assert.That(roundTrip.PrimaryItem.Detail.Lot).IsEqualTo("A1");
        await Assert.That(roundTrip.OptionalItem!.Sku).IsEqualTo("optional");
        await Assert.That(roundTrip.OptionalItem.Detail.Lot).IsEqualTo("LO");
        await Assert.That(roundTrip.Items).Count().IsEqualTo(2);
        await Assert.That(roundTrip.Items[1].Detail.Lot).IsEqualTo("L2");
        await Assert.That(roundTrip.NullableItems).Count().IsEqualTo(2);
        await Assert.That(roundTrip.NullableItems[0]).IsNull();
        await Assert.That(roundTrip.NullableItems[1]!.Detail.Lot).IsEqualTo("LN");
        await Assert.That(roundTrip.ItemArray).Count().IsEqualTo(1);
        await Assert.That(roundTrip.ItemArray[0].Sku).IsEqualTo("array");
        await Assert.That(roundTrip.ItemsByCode["gift"].Detail.Lot).IsEqualTo("LG");
        await Assert.That(roundTrip.NullableItemsByCode["none"]).IsNull();
        await Assert.That(roundTrip.NullableItemsByCode["hit"]!.Detail.Lot).IsEqualTo("LD");
    }

    [Test]
    public async Task EmbeddedEntityCollection_ShouldUseEntityAdapterWithoutRequiringInlineDuplicate()
    {
        var catalog = new CatalogWithEntityItems
        {
            Id = 7,
            Items =
            [
                new CatalogEntityItem { Id = 100, Name = "registered" }
            ]
        };

        var document = AotBsonMapper.ToDocument(catalog);
        var roundTrip = AotBsonMapper.FromDocument<CatalogWithEntityItems>(document);

        await Assert.That(roundTrip.Items).Count().IsEqualTo(1);
        await Assert.That(roundTrip.Items[0].Id).IsEqualTo(100);
        await Assert.That(roundTrip.Items[0].Name).IsEqualTo("registered");
    }

    [Entity]
    public class CircularEntity
    {
        public string Name { get; set; } = "";
        public CircularEntity? Parent { get; set; }
        public CircularEntity? Child { get; set; }
    }

    [Entity]
    public class EmbeddedOrder
    {
        public int Id { get; set; }
        public EmbeddedOrderItem PrimaryItem { get; set; } = new();
        public EmbeddedOrderItem? OptionalItem { get; set; }
        public List<EmbeddedOrderItem> Items { get; set; } = new();
        public List<EmbeddedOrderItem?> NullableItems { get; set; } = new();
        public EmbeddedOrderItem[] ItemArray { get; set; } = [];
        public Dictionary<string, EmbeddedOrderItem> ItemsByCode { get; set; } = new();
        public Dictionary<string, EmbeddedOrderItem?> NullableItemsByCode { get; set; } = new();
    }

    public class EmbeddedOrderItem
    {
        public string Sku { get; set; } = "";
        public int Quantity { get; set; }
        public EmbeddedOrderItemDetail Detail { get; set; } = new();
    }

    public class EmbeddedOrderItemDetail
    {
        public string Lot { get; set; } = "";
    }

    [Entity]
    public class CatalogWithEntityItems
    {
        public int Id { get; set; }
        public List<CatalogEntityItem> Items { get; set; } = new();
    }

    [Entity]
    public class CatalogEntityItem
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
    }
}
