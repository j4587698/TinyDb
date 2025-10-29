using System.Diagnostics.CodeAnalysis;
using TinyDb.Attributes;
using TinyDb.IdGeneration;

namespace TinyDb.Tests.TestEntities;

[Entity("products_guid")]
[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)]
public partial class ProductWithGuidId
{
    [IdGeneration(IdGenerationStrategy.GuidV7)]
    public Guid Id { get; set; }
    public string Name { get; set; } = "";
    public decimal Price { get; set; }
    public bool InStock { get; set; }
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
}