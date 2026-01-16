using System;
using System.Diagnostics.CodeAnalysis;
using TinyDb.Attributes;

namespace TinyDb.Tests.TestEntities;

[Entity("auto_guid_orders")]
[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)]
public partial class AutoGuidOrder
{
    public Guid Id { get; set; }
    public string OrderNumber { get; set; } = "";
    public decimal Amount { get; set; }
}
