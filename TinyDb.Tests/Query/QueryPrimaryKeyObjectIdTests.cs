using System;
using System.IO;
using System.Linq;
using TinyDb.Core;
using TinyDb.Tests.TestEntities;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Query;

[NotInParallel]
public class QueryPrimaryKeyObjectIdTests
{
    private string _testFile = null!;
    private TinyDbEngine _engine = null!;

    [Before(Test)]
    public void Setup()
    {
        _testFile = Path.Combine(Path.GetTempPath(), $"query_oid_pk_{Guid.NewGuid():N}.db");
        _engine = new TinyDbEngine(_testFile);
    }

    [After(Test)]
    public void Cleanup()
    {
        _engine?.Dispose();
        if (File.Exists(_testFile))
        {
            File.Delete(_testFile);
        }
    }

    [Test]
    public async Task Query_PrimaryKey_ObjectId_ShouldWork()
    {
        var col = _engine.GetCollection<User>();
        var user = new User { Name = "OidUser", Age = 30, Email = "oid@test.com" };
        col.Insert(user);

        var id = user.Id;
        var results = col.Find(u => u.Id == id).ToList();

        await Assert.That(results).Count().IsEqualTo(1);
        await Assert.That(results[0].Email).IsEqualTo("oid@test.com");
    }
}

