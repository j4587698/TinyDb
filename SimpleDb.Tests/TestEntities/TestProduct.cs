using SimpleDb.Attributes;
using SimpleDb.Bson;

namespace SimpleDb.Tests.TestEntities;

[Entity("Products")]
public class TestProduct
{
    public ObjectId? Id { get; set; }
    public string? Name { get; set; }
    public decimal Price { get; set; }
    public string? Category { get; set; }
    public bool InStock { get; set; }
    public DateTime CreatedAt { get; set; }
}