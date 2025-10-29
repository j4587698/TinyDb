using System.Diagnostics.CodeAnalysis;
using TinyDb.Attributes;
using TinyDb.IdGeneration;

namespace TinyDb.Tests.TestEntities;

[Entity("users_int")]
[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)]
public partial class UserWithIntId
{
    [IdGeneration(IdGenerationStrategy.IdentityInt)]
    public int Id { get; set; }
    public string Name { get; set; } = "";
    public int Age { get; set; }
    public string Email { get; set; } = "";
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
}