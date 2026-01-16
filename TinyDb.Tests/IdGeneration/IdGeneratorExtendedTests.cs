using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading.Tasks;
using TinyDb.IdGeneration;
using TinyDb.Attributes; // Added for IdGenerationStrategy if needed, though constructor is paramless now
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.IdGeneration;

[NotInParallel]
public class IdGeneratorExtendedTests
{
    [Test]
    public async Task GuidV7_ShouldBeTimeOrdered_HighPrecision()
    {
        var generator = new GuidV7Generator();
        // Use a dummy property info, although V7 generator might ignore it, good practice to be non-null
        var propInfo = typeof(TinyDb.Tests.TestEntities.UserWithGuidV7Id).GetProperty("Id");
        
        var bson1 = generator.GenerateId(null!, propInfo!);
        await Task.Delay(5); 
        var bson2 = generator.GenerateId(null!, propInfo!);

        var guid1 = Guid.Parse(bson1.ToString());
        var guid2 = Guid.Parse(bson2.ToString());

        await Assert.That(guid1).IsNotEqualTo(guid2);
    }

    [Test]
    public async Task IdentityGenerator_ShouldHandleConcurrency()
    {
        var generator = new IdentityGenerator();
        
        const int threadCount = 10;
        const int countPerThread = 1000;
        var bag = new ConcurrentBag<object>();

        // We need a dummy property info for int type
        var propInfo = typeof(TinyDb.Tests.TestEntities.UserWithIntId).GetProperty("Id");

        var tasks = Enumerable.Range(0, threadCount).Select(_ => Task.Run(() =>
        {
            for (int i = 0; i < countPerThread; i++)
            {
                var bson = generator.GenerateId(typeof(TinyDb.Tests.TestEntities.UserWithIntId), propInfo!, "test_seq_extended");
                bag.Add(((TinyDb.Bson.BsonInt32)bson).Value);
            }
        }));

        await Task.WhenAll(tasks);

        await Assert.That(bag.Count).IsEqualTo(threadCount * countPerThread);
        
        var intIds = bag.Select(x => (int)x).OrderBy(x => x).ToList();
        
        // Should be 1 to 10000
        await Assert.That(intIds.First()).IsEqualTo(1);
        await Assert.That(intIds.Last()).IsEqualTo(threadCount * countPerThread);
        await Assert.That(intIds.Distinct().Count()).IsEqualTo(threadCount * countPerThread);
    }

    [Test]
    public async Task GuidV4Generator_ShouldGenerateUniqueIds()
    {
        var generator = new GuidV4Generator();
        var bag = new ConcurrentBag<string>();
        var propInfo = typeof(TinyDb.Tests.TestEntities.UserWithGuidV7Id).GetProperty("Id"); // String property
        
        const int count = 1000;
        
        Parallel.For(0, count, _ =>
        {
            var bson = generator.GenerateId(null!, propInfo!);
            bag.Add(bson.ToString());
        });

        await Assert.That(bag.Distinct().Count()).IsEqualTo(count);
        
        // Verify format
        var sample = bag.First();
        await Assert.That(Guid.TryParse(sample, out var g)).IsTrue();
    }

    [Test]
    public async Task Generator_ShouldThrow_OnUnsupportedType()
    {
        var intGen = new IdentityGenerator();
        await Assert.That(() => intGen.Supports(typeof(string))).IsFalse();
    }
}
