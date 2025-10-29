using System.Diagnostics.CodeAnalysis;
using TinyDb.Attributes;

namespace TinyDb.Tests.TestEntities;

[Entity("auto_orders")]
[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)]
public partial class AutoOrder
{
    public Guid Id { get; set; }
    public string OrderNumber { get; set; } = "";
    public decimal TotalAmount { get; set; }
    public DateTime OrderDate { get; set; } = DateTime.UtcNow;
}