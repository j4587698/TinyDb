using System.Diagnostics.CodeAnalysis;
using TinyDb.Attributes;
using TinyDb.Bson;

namespace TinyDb.Tests.TestEntities;

[Entity("users")]
[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)]
public partial class User
{
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();
    public string Name { get; set; } = "";
    public int Age { get; set; }
    public string Email { get; set; } = "";
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
}