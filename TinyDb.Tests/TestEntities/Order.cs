using System.Diagnostics.CodeAnalysis;
using TinyDb.Attributes;
using TinyDb.Bson;

namespace TinyDb.Tests.TestEntities;

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