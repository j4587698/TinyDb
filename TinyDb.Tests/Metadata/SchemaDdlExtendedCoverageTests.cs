using System;
using System.Reflection;
using TinyDb.Bson;
using TinyDb.Metadata;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Metadata;

public sealed class SchemaDdlExtendedCoverageTests
{
    [Test]
    public async Task SchemaDdl_Export_ShouldCoverQuoteEscapesForeignKeyAndDefaultValueKinds()
    {
        var dt = new DateTime(2025, 1, 2, 3, 4, 5, DateTimeKind.Utc);

        var schema = new MetadataDocument
        {
            TableName = "orders",
            TypeName = "MyApp.Models.Order",
            DisplayName = "Order \"Table\"",
            Description = "line1\r\nline2\t\\slash",
            Columns = new BsonArray()
                .AddValue(new BsonDocument()
                    .Set("n", "_id")
                    .Set("t", "System.Int32")
                    .Set("r", true)
                    .Set("o", 1))
                .AddValue(new BsonDocument()
                    .Set("n", "fkField")
                    .Set("t", "System.String")
                    .Set("fk", "users(_id)")
                    .Set("desc", "desc\"with\"quote")
                    .Set("o", 2))
                .AddValue(new BsonDocument().Set("n", "b").Set("t", "System.Boolean").Set("dv", new BsonBoolean(true)).Set("o", 3))
                .AddValue(new BsonDocument().Set("n", "i64").Set("t", "System.Int64").Set("dv", new BsonInt64(42L)).Set("o", 4))
                .AddValue(new BsonDocument().Set("n", "dbl").Set("t", "System.Double").Set("dv", new BsonDouble(1.5)).Set("o", 5))
                .AddValue(new BsonDocument().Set("n", "dec").Set("t", "System.Decimal").Set("dv", new BsonDecimal128(new Decimal128(12.34m))).Set("o", 6))
                .AddValue(new BsonDocument().Set("n", "str").Set("t", "System.String").Set("dv", new BsonString("a\\b\"c")).Set("o", 7))
                .AddValue(new BsonDocument().Set("n", "dt").Set("t", "System.DateTime").Set("dv", new BsonDateTime(dt)).Set("o", 8))
                .AddValue(new BsonDocument().Set("n", "arr").Set("t", "System.Object").Set("dv", new BsonArray().AddValue(1)).Set("o", 9))
                .AddValue(new BsonDocument().Set("n", "nullable").Set("t", "System.String").Set("dv", BsonNull.Value).Set("o", 10))
        };

        var ddl = SchemaDdl.Export(schema);

        await Assert.That(ddl).Contains("display \"Order \\\"Table\\\"\"");
        await Assert.That(ddl).Contains("desc \"line1\\r\\nline2\\t\\\\slash\"");
        await Assert.That(ddl).Contains("fk \"users(_id)\"");
        await Assert.That(ddl).Contains("dv true");
        await Assert.That(ddl).Contains("dv 42");
        await Assert.That(ddl).Contains("dv 1.5");
        await Assert.That(ddl).Contains("dv 12.34");
        await Assert.That(ddl).Contains("dv \"a\\\\b\\\"c\"");
        await Assert.That(ddl).Contains("dv datetime(\"2025-01-02T03:04:05.0000000Z\")");
        await Assert.That(ddl).Contains("dv \"[1]\"");
    }

    [Test]
    public async Task SchemaDdl_FormatDefaultValue_ShouldReturnNullLiteralForBsonNull()
    {
        var method = typeof(SchemaDdl).GetMethod("FormatDefaultValue", BindingFlags.NonPublic | BindingFlags.Static);
        await Assert.That(method).IsNotNull();

        var result = method!.Invoke(null, new object[] { BsonNull.Value }) as string;
        await Assert.That(result).IsEqualTo("null");
    }

    [Test]
    public async Task SchemaDdl_ExportMultiple_ShouldValidateArgumentsAndSeparateDocuments()
    {
        await Assert.That(() => SchemaDdl.Export((MetadataDocument)null!)).Throws<ArgumentNullException>();
        await Assert.That(() => SchemaDdl.Export((System.Collections.Generic.IEnumerable<MetadataDocument>)null!)).Throws<ArgumentNullException>();

        var a = new MetadataDocument { TableName = "a", Columns = new BsonArray() };
        var b = new MetadataDocument { TableName = "b", Columns = new BsonArray() };
        var all = SchemaDdl.Export(new[] { a, b });

        await Assert.That(all).Contains("create table \"a\"");
        await Assert.That(all).Contains("create table \"b\"");
    }
}
