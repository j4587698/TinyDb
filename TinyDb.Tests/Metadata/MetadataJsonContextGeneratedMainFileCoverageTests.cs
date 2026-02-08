using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using TinyDb.Metadata;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using PropertyMetadata = TinyDb.Metadata.PropertyMetadata;

namespace TinyDb.Tests.Metadata;

public class MetadataJsonContextGeneratedMainFileCoverageTests
{
    [Test]
    public async Task GetTypeInfo_ShouldExerciseGeneratedResolver()
    {
        var ctx = MetadataJsonContext.Default;

        _ = ctx.GetTypeInfo(typeof(List<PropertyMetadata>));
        _ = ctx.GetTypeInfo(typeof(PropertyMetadata));
        _ = ctx.GetTypeInfo(typeof(string));
        _ = ctx.GetTypeInfo(typeof(bool));
        _ = ctx.GetTypeInfo(typeof(object));
        _ = ctx.GetTypeInfo(typeof(DateTime));

        var list = new List<PropertyMetadata>
        {
            new() { PropertyName = "P1", DisplayName = "D1", Required = true }
        };

        var json = JsonSerializer.Serialize(list, ctx.ListPropertyMetadata);
        await Assert.That(json).Contains("P1");
    }

    [Test]
    public async Task CustomContextInstance_ShouldWorkWithOptionsIfAvailable()
    {
        var ctxType = typeof(MetadataJsonContext);
        var ctor = ctxType.GetConstructors(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)
            .FirstOrDefault(c =>
            {
                var p = c.GetParameters();
                return p.Length == 1 && p[0].ParameterType == typeof(JsonSerializerOptions);
            });

        if (ctor != null)
        {
            var options = new JsonSerializerOptions { WriteIndented = true };
            var ctx = (MetadataJsonContext)ctor.Invoke(new object[] { options });

            await Assert.That(ctx.ListPropertyMetadata).IsNotNull();
            _ = ctx.GetTypeInfo(typeof(List<PropertyMetadata>));
        }
        else
        {
            await Assert.That(MetadataJsonContext.Default).IsNotNull();
        }
    }
}
