using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using TinyDb.Core;
using TinyDb.Tests.TestEntities;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Core;

[NotInParallel]
public class StressTests
{
    private string _databasePath = null!;
    private TinyDbOptions _options = null!;

    [Before(Test)]
    public void Setup()
    {
        _databasePath = Path.Combine(Path.GetTempPath(), $"tinydb_stress_{Guid.NewGuid():N}.db");
        if (File.Exists(_databasePath))
            File.Delete(_databasePath);

        _options = new TinyDbOptions
        {
            DatabaseName = "StressDb",
            PageSize = 4096,
            CacheSize = 1024,
            EnableJournaling = true
        };
    }

    [After(Test)]
    public void Cleanup()
    {
        if (File.Exists(_databasePath))
            File.Delete(_databasePath);
    }

    [Test]
    public async Task HighConcurrency_Insert_StressTest()
    {
        const int threadCount = 10;
        const int itemsPerThread = 1000;
        const int totalItems = threadCount * itemsPerThread;

        using (var engine = new TinyDbEngine(_databasePath, _options))
        {
            var collection = engine.GetCollection<User>();
            var sw = Stopwatch.StartNew();

            var tasks = Enumerable.Range(0, threadCount).Select(t => Task.Run(() =>
            {
                for (int i = 0; i < itemsPerThread; i++)
                {
                    collection.Insert(new User 
                    { 
                        Name = $"User_{t}_{i}", 
                        Age = (t * 10) + (i % 100) 
                    });
                }
            })).ToArray();

            await Task.WhenAll(tasks);
            sw.Stop();

            Console.WriteLine($"Inserted {totalItems} items in {sw.ElapsedMilliseconds}ms ({(totalItems * 1000.0 / sw.ElapsedMilliseconds):F2} ops/s)");

            // Verify count
            var count = collection.Count();
            await Assert.That(count).IsEqualTo(totalItems);

            // Verify some samples
            var sample = collection.Find(u => u.Name == "User_5_500").FirstOrDefault();
            await Assert.That(sample).IsNotNull();
            await Assert.That(sample!.Age).IsEqualTo(50 + (500 % 100));
        }
    }

    [Test]
    public async Task Mixed_Load_StressTest()
    {
        const int iterations = 500;
        using (var engine = new TinyDbEngine(_databasePath, _options))
        {
            var collection = engine.GetCollection<Product>();
            
            // Initial data
            for(int i=0; i<100; i++)
                collection.Insert(new Product { Name = $"Initial_{i}", Price = i * 10 });

            var insertTask = Task.Run(() => {
                for(int i=0; i<iterations; i++)
                    collection.Insert(new Product { Name = $"New_{i}", Price = i });
            });

            var updateTask = Task.Run(() => {
                var random = new Random(42);
                for(int i=0; i<iterations; i++)
                {
                    var all = collection.FindAll().Take(10).ToList();
                    if(all.Any())
                    {
                        var p = all[random.Next(all.Count)];
                        p.Price += 1;
                        collection.Update(p);
                    }
                }
            });

            var deleteTask = Task.Run(() => {
                for(int i=0; i<iterations/2; i++)
                {
                    var first = collection.FindAll().FirstOrDefault();
                    if(first != null)
                        collection.Delete(first.Id);
                }
            });

            await Task.WhenAll(insertTask, updateTask, deleteTask);
            
            // Just ensure engine didn't crash and we can still query
            var finalCount = collection.Count();
            Console.WriteLine($"Final count after mixed load: {finalCount}");
            await Assert.That(finalCount).IsGreaterThan(0);
        }
    }
}
