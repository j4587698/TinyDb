using System.Collections.Generic;
using System.Reflection;
using TinyDb.Bson;
using TinyDb.Metadata;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Metadata;

public sealed class CSharpEntityGeneratorExtendedCoverageTests
{
    [Test]
    public async Task Generate_ShouldCoverBlockNamespaceEscapesAndIdentifierBranches()
    {
        var schema = new MetadataDocument
        {
            TableName = "order-items",
            DisplayName = "Order \"Items\"",
            Description = "line1\r\nline2\t\\desc",
            Columns = new BsonArray()
                .AddValue(new BsonDocument()
                    .Set("n", "_id")
                    .Set("t", "System.Int32")
                    .Set("r", true)
                    .Set("o", 1))
                .AddValue(new BsonDocument()
                    .Set("n", "name")
                    .Set("pn", "1abc")
                    .Set("t", "System.Text.StringBuilder")
                    .Set("r", true)
                    .Set("o", 2)
                    .Set("dn", "Name\"Display")
                    .Set("desc", "Desc\\With\r\nTab\t")
                    .Set("fk", "orders(_id)"))
                .AddValue(new BsonDocument()
                    .Set("n", "name2")
                    .Set("pn", "1abc")
                    .Set("t", "System.String")
                    .Set("r", true)
                    .Set("o", 3))
                .AddValue(new BsonDocument()
                    .Set("n", "kw")
                    .Set("pn", "class")
                    .Set("t", "System.String")
                    .Set("r", false)
                    .Set("o", 4))
        };

        var code = CSharpEntityGenerator.Generate(schema, new CSharpEntityGenerationOptions
        {
            Namespace = "  My.App.Entities  ",
            FileScopedNamespace = false,
            ClassName = "class",
            EmitForeignKeyAttributes = true,
            EmitMetadataAttributes = true,
            EmitNullableAnnotations = true,
            UseCSharpAliases = true
        });

        await Assert.That(code).Contains("namespace My.App.Entities");
        await Assert.That(code).Contains("[Entity(\"order-items\")]");
        await Assert.That(code).Contains("[EntityMetadata(DisplayName = \"Order \\\"Items\\\"\", Description = \"line1\\r\\nline2\\t\\\\desc\")]");
        await Assert.That(code).Contains("public partial class Class");
        await Assert.That(code).Contains("[ForeignKey(\"orders(_id)\")]");
        await Assert.That(code).Contains("[PropertyMetadata(\"Name\\\"Display\", Description = \"Desc\\\\With\\r\\nTab\\t\", Order = 2, Required = true)]");
        await Assert.That(code).Contains("public System.Text.StringBuilder _1abc { get; set; } = default!;");
        await Assert.That(code).Contains("public string _1abc_2 { get; set; } = string.Empty;");
        await Assert.That(code).Contains("public string? @class { get; set; }");
        await Assert.That(code).Contains("}");
    }

    [Test]
    public async Task Generate_ShouldCoverNoEntityMetadataAndNoMetadataAttributesBranch()
    {
        var schema = new MetadataDocument
        {
            TableName = "users",
            DisplayName = "users",
            Columns = new BsonArray()
                .AddValue(new BsonDocument()
                    .Set("n", "nick-name")
                    .Set("t", "System.String")
                    .Set("r", true)
                    .Set("o", 1))
        };

        var code = CSharpEntityGenerator.Generate(schema, new CSharpEntityGenerationOptions
        {
            Namespace = "My.App",
            FileScopedNamespace = true,
            EmitMetadataAttributes = false,
            EmitForeignKeyAttributes = false,
            EmitEntityAttribute = false,
            ClassName = null
        });

        await Assert.That(code).Contains("namespace My.App;");
        await Assert.That(code).Contains("public partial class Users");
        await Assert.That(code).Contains("public string Nick_name { get; set; } = string.Empty;");
        await Assert.That(code).DoesNotContain("[Entity(");
        await Assert.That(code).DoesNotContain("[PropertyMetadata(");
        await Assert.That(code).DoesNotContain("[EntityMetadata");
    }

    [Test]
    public async Task PrivateIdentifierHelpers_ShouldCoverPascalTypeEscapeAndEnsureUniqueLoop()
    {
        var toPascalCase = typeof(CSharpEntityGenerator).GetMethod("ToPascalCase", BindingFlags.Static | BindingFlags.NonPublic)
            ?? throw new MissingMethodException(typeof(CSharpEntityGenerator).FullName, "ToPascalCase");
        var toValidIdentifier = typeof(CSharpEntityGenerator).GetMethod("ToValidIdentifier", BindingFlags.Static | BindingFlags.NonPublic)
            ?? throw new MissingMethodException(typeof(CSharpEntityGenerator).FullName, "ToValidIdentifier");
        var ensureUnique = typeof(CSharpEntityGenerator).GetMethod("EnsureUnique", BindingFlags.Static | BindingFlags.NonPublic)
            ?? throw new MissingMethodException(typeof(CSharpEntityGenerator).FullName, "EnsureUnique");

        var pascal = (string)toPascalCase.Invoke(null, new object[] { "ab-c d" })!;
        await Assert.That(pascal).IsEqualTo("AbCD");

        var escapedType = (string)toValidIdentifier.Invoke(null, new object[] { "class", true })!;
        await Assert.That(escapedType).IsEqualTo("class");

        var used = new HashSet<string>(StringComparer.Ordinal) { "dup", "dup_2" };
        var unique = (string)ensureUnique.Invoke(null, new object[] { "dup", used })!;
        await Assert.That(unique).IsEqualTo("dup_3");
    }
}
