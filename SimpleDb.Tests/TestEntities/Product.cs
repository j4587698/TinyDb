using System.Diagnostics.CodeAnalysis;
using SimpleDb.Attributes;
using SimpleDb.Bson;

namespace SimpleDb.Tests.TestEntities;

[Entity("products")]
[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)]
public partial class Product
{
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();
    public string Name { get; set; } = "";
    public decimal Price { get; set; }
    public bool InStock { get; set; }
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
}