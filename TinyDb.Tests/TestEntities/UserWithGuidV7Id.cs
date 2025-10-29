using System.Diagnostics.CodeAnalysis;
using TinyDb.Attributes;
using TinyDb.IdGeneration;

namespace TinyDb.Tests.TestEntities;

[Entity("users_guidv7")]
[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)]
public partial class UserWithGuidV7Id
{
    [IdGeneration(IdGenerationStrategy.GuidV7)]
    public string Id { get; set; } = "";
    public string Name { get; set; } = "";
    public int Age { get; set; }
    public string Email { get; set; } = "";
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
}