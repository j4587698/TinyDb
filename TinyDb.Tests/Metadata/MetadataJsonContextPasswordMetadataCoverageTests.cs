using System.Collections.Generic;
using System.Text.Json;
using TinyDb.Metadata;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

using DbPasswordMetadata = TinyDb.Metadata.PasswordMetadata;
using DbPropertyMetadata = TinyDb.Metadata.PropertyMetadata;

namespace TinyDb.Tests.Metadata;

public class MetadataJsonContextPasswordMetadataCoverageTests
{
    [Test]
    public async Task MetadataJsonContext_Serialization_ShouldCoverPasswordMetadata()
    {
        var list = new List<DbPropertyMetadata>
        {
            new()
            {
                PropertyName = "P1",
                PropertyType = "System.String",
                DisplayName = "D1",
                Description = null,
                Order = 1,
                Required = true,
                Password = null,
                ForeignKeyCollection = null
            },
            new()
            {
                PropertyName = "Password1",
                PropertyType = "System.String",
                DisplayName = "Password One",
                Description = "Desc",
                Order = 2,
                Required = true,
                ForeignKeyCollection = "Users",
                Password = new DbPasswordMetadata
                {
                    IsPassword = true,
                    RequiredStrength = PasswordStrength.VeryStrong,
                    MinLength = 12,
                    MaxLength = 64,
                    RequireSpecialChar = false,
                    RequireNumber = false,
                    RequireUppercase = false,
                    RequireLowercase = false,
                    Hint = "hint",
                    ShowStrengthIndicator = false,
                    AllowToggle = false
                }
            },
            new()
            {
                PropertyName = "Password2",
                PropertyType = "System.String",
                DisplayName = "Password Two",
                Description = "",
                Order = 3,
                Required = false,
                ForeignKeyCollection = "",
                Password = new DbPasswordMetadata
                {
                    IsPassword = true,
                    RequiredStrength = PasswordStrength.Weak,
                    MinLength = 1,
                    MaxLength = 2,
                    RequireSpecialChar = true,
                    RequireNumber = true,
                    RequireUppercase = true,
                    RequireLowercase = true,
                    Hint = null,
                    ShowStrengthIndicator = true,
                    AllowToggle = true
                }
            }
        };

        var json = JsonSerializer.Serialize(list, MetadataJsonContext.Default.ListPropertyMetadata);
        var roundtrip = JsonSerializer.Deserialize(json, MetadataJsonContext.Default.ListPropertyMetadata);

        await Assert.That(roundtrip).IsNotNull();
        await Assert.That(roundtrip!.Count).IsEqualTo(3);

        await Assert.That(roundtrip[0].Password).IsNull();

        await Assert.That(roundtrip[1].Password).IsNotNull();
        await Assert.That(roundtrip[1].Password!.Hint).IsEqualTo("hint");
        await Assert.That(roundtrip[1].Password.RequiredStrength).IsEqualTo(PasswordStrength.VeryStrong);
        await Assert.That(roundtrip[1].ForeignKeyCollection).IsEqualTo("Users");

        await Assert.That(roundtrip[2].Password).IsNotNull();
        await Assert.That(roundtrip[2].Password!.Hint).IsNull();
        await Assert.That(roundtrip[2].Password.RequiredStrength).IsEqualTo(PasswordStrength.Weak);
    }

    [Test]
    public async Task MetadataJsonContext_DirectSerialization_ShouldCoverGeneratedTypeInfoCacheBranches()
    {
        var context = MetadataJsonContext.Default;

        var password = new DbPasswordMetadata
        {
            IsPassword = true,
            RequiredStrength = PasswordStrength.Strong,
            MinLength = 10,
            MaxLength = 20,
            RequireSpecialChar = true,
            RequireNumber = true,
            RequireUppercase = true,
            RequireLowercase = true,
            Hint = "x",
            ShowStrengthIndicator = true,
            AllowToggle = true
        };

        var passwordJson = JsonSerializer.Serialize(password, typeof(DbPasswordMetadata), context);
        _ = JsonSerializer.Deserialize(passwordJson, typeof(DbPasswordMetadata), context);

        // Call twice to hit both cache branches in generated context.
        _ = context.GetTypeInfo(typeof(DbPasswordMetadata));
        _ = context.GetTypeInfo(typeof(DbPasswordMetadata));

        var property = new DbPropertyMetadata
        {
            PropertyName = "P",
            PropertyType = "System.String",
            DisplayName = "D",
            Description = null,
            Order = 0,
            Required = false,
            Password = null,
            ForeignKeyCollection = null
        };

        var propJson = JsonSerializer.Serialize(property, typeof(DbPropertyMetadata), context);
        _ = JsonSerializer.Deserialize(propJson, typeof(DbPropertyMetadata), context);

        _ = context.GetTypeInfo(typeof(DbPropertyMetadata));
        _ = context.GetTypeInfo(typeof(DbPropertyMetadata));

        await Assert.That(passwordJson).Contains("\"IsPassword\"");
    }
}
