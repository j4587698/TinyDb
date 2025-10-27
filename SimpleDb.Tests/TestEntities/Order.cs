using System.Diagnostics.CodeAnalysis;
using SimpleDb.Attributes;
using SimpleDb.Bson;

namespace SimpleDb.Tests.TestEntities;

[Entity("orders")]
[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)]
public partial class Order
{
    public ObjectId _id { get; set; } = ObjectId.NewObjectId();
    public string OrderNumber { get; set; } = "";
    public decimal TotalAmount { get; set; }
    public DateTime OrderDate { get; set; } = DateTime.UtcNow;
    public bool IsCompleted { get; set; }
}