using System.Diagnostics.CodeAnalysis;
using SimpleDb.Attributes;
using SimpleDb.IdGeneration;

namespace SimpleDb.Tests.TestEntities;

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