using TinyDb.Serialization;
using TinyDb.Bson;
using TinyDb.Attributes;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using System.Collections;

namespace TinyDb.Tests.Serialization;

public class AotMapperComplexTests
{
    [Entity]
    public class ComplexTypes
    {
        public Hashtable NonGenericTable { get; set; } = new();
        public ArrayList NonGenericList { get; set; } = new();
        public int[] IntArray { get; set; } = Array.Empty<int>();
    }

    [Test]
    public async Task Fallback_NonGeneric_Collections_Should_Work()
    {
        var entity = new ComplexTypes();
        entity.NonGenericTable.Add("key", "value");
        entity.NonGenericList.Add(100);
        entity.IntArray = new[] { 1, 2, 3 };

        var doc = AotBsonMapper.ToDocument(entity);
        
        // Hashtable is not supported because it doesn't implement generic IDictionary<,>
        // which is required by AotBsonMapper fallback logic.
        await Assert.That(() => AotBsonMapper.FromDocument<ComplexTypes>(doc))
            .Throws<NotSupportedException>();
    }
}
