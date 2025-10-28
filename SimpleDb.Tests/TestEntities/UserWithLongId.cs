using System.Diagnostics.CodeAnalysis;
using SimpleDb.Attributes;
using SimpleDb.IdGeneration;

namespace SimpleDb.Tests.TestEntities;

[Entity("users_long")]
[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)]
public partial class UserWithLongId
{
    [IdGeneration(IdGenerationStrategy.IdentityLong, "users_long_seq")]
    public long Id { get; set; }
    public string Name { get; set; } = "";
    public int Age { get; set; }
    public string Email { get; set; } = "";
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
}