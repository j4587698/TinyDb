using System.Diagnostics.CodeAnalysis;
using TinyDb.Attributes;

namespace TinyDb.Tests.TestEntities;

[Entity("auto_products")]
[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)]
public partial class AutoProduct
{
    public long Id { get; set; }
    public string Name { get; set; } = "";
    public decimal Price { get; set; }
    public bool InStock { get; set; }
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
}