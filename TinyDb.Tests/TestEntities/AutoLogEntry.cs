using System.Diagnostics.CodeAnalysis;
using TinyDb.Attributes;

namespace TinyDb.Tests.TestEntities;

[Entity("auto_logs")]
[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)]
public partial class AutoLogEntry
{
    public string Id { get; set; } = "";
    public string Message { get; set; } = "";
    public string Level { get; set; } = "INFO";
    public DateTime Timestamp { get; set; } = DateTime.UtcNow;
}