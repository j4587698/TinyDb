using TinyDb.Bson;
using TinyDb.Metadata;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Metadata;

public class SchemaDdlAndCSharpGenerationTests
{
    [Test]
    public async Task SchemaDdl_Export_ShouldContainTableAndColumns()
    {
        var schema = new MetadataDocument
        {
            TableName = "Users",
            TypeName = "MyApp.Models.User",
            DisplayName = "User Table",
            Description = "Table description",
            Columns = new BsonArray()
                .AddValue(new BsonDocument()
                    .Set("n", "_id")
                    .Set("pn", "Id")
                    .Set("t", "System.Int32")
                    .Set("pk", true)
                    .Set("r", true)
                    .Set("o", 1))
                .AddValue(new BsonDocument()
                    .Set("n", "userName")
                    .Set("pn", "UserName")
                    .Set("t", "System.String")
                    .Set("r", true)
                    .Set("o", 2)
                    .Set("dn", "Username")
                    .Set("desc", "The user's name"))
                .AddValue(new BsonDocument()
                    .Set("n", "age")
                    .Set("t", "System.Int32")
                    .Set("r", false)
                    .Set("o", 3)
                    .Set("dv", BsonInt32.FromValue(0)))
        };

        var ddl = SchemaDdl.Export(schema);

        await Assert.That(ddl).Contains("create table \"Users\"");
        await Assert.That(ddl).Contains("type \"MyApp.Models.User\"");
        await Assert.That(ddl).Contains("\"_id\" \"System.Int32\" pk required pn \"Id\"");
        await Assert.That(ddl).Contains("\"userName\" \"System.String\" required pn \"UserName\" order 2 dn \"Username\" desc \"The user's name\"");
        await Assert.That(ddl).Contains("\"age\" \"System.Int32\" pn");
        await Assert.That(ddl).Contains("dv 0");
    }

    [Test]
    public async Task ClrTypeName_Normalize_ShouldSimplifyReflectionGenericNames()
    {
        var raw = typeof(List<string>).FullName!;
        var normalized = ClrTypeName.Normalize(raw);
        await Assert.That(normalized).IsEqualTo("System.Collections.Generic.List<System.String>");
    }

    [Test]
    public async Task CSharpEntityGenerator_Generate_ShouldProduceAttributesAndNullability()
    {
        var schema = new MetadataDocument
        {
            TableName = "Users",
            TypeName = "MyApp.Models.User",
            DisplayName = "User Table",
            Columns = new BsonArray()
                .AddValue(new BsonDocument()
                    .Set("n", "_id")
                    .Set("pn", "Id")
                    .Set("t", "System.Int32")
                    .Set("pk", true)
                    .Set("r", true)
                    .Set("o", 1))
                .AddValue(new BsonDocument()
                    .Set("n", "userName")
                    .Set("t", "System.String")
                    .Set("r", true)
                    .Set("o", 2)
                    .Set("dn", "Username"))
                .AddValue(new BsonDocument()
                    .Set("n", "age")
                    .Set("t", "System.Int32")
                    .Set("r", false)
                    .Set("o", 3))
        };

        var code = CSharpEntityGenerator.Generate(schema, new CSharpEntityGenerationOptions
        {
            Namespace = "MyApp.Entities"
        });

        await Assert.That(code).Contains("namespace MyApp.Entities;");
        await Assert.That(code).Contains("[Entity(\"Users\")]");
        await Assert.That(code).Contains("public partial class User");
        await Assert.That(code).Contains("[Id]");
        await Assert.That(code).Contains("public int Id { get; set; }");
        await Assert.That(code).Contains("public string UserName { get; set; } = string.Empty;");
        await Assert.That(code).Contains("public int? Age { get; set; }");
        await Assert.That(code).Contains("[PropertyMetadata(\"Username\"");
    }
}
