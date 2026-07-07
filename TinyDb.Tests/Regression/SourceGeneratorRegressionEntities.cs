using TinyDb.Attributes;

namespace TinyDb.Tests.Regression.Systematic.Models;

[Entity("SystematicEntities")]
public partial class SystematicEntity
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
}

[Entity("GenericArityOneEntities")]
public partial class GenericArityRegressionEntity<T>
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
}

[Entity("GenericArityTwoEntities")]
public partial class GenericArityRegressionEntity<TFirst, TSecond>
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
}
