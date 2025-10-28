using System.Diagnostics.CodeAnalysis;
using SimpleDb.Attributes;
using SimpleDb.IdGeneration;

namespace SimpleDb.Tests.TestEntities;

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