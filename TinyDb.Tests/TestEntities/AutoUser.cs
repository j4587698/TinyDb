using System.Diagnostics.CodeAnalysis;
using TinyDb.Attributes;

namespace TinyDb.Tests.TestEntities;

[Entity("auto_users")]
[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)]
public partial class AutoUser
{
    public int Id { get; set; }
    public string Name { get; set; } = "";
    public int Age { get; set; }
    public string Email { get; set; } = "";
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
}