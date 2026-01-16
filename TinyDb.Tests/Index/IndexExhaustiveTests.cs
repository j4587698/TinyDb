using System;
using System.Collections.Generic;
using System.Linq;
using TinyDb.Bson;
using TinyDb.Index;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Index;

public class IndexExhaustiveTests
{
    [Test]
    public async Task BTreeIndex_FindRange_Comprehensive_ShouldWork()
    {
        using var index = new BTreeIndex("idx", new[] { "f" }, false, 4);
        for (int i = 1; i <= 20; i++)
        {
            index.Insert(new IndexKey(new BsonInt32(i)), new BsonInt32(i * 10));
        }

        // Test permutations of includeStart/includeEnd
        await Assert.That(index.FindRange(new IndexKey(5), new IndexKey(10), true, true)).Count().IsEqualTo(6);   // 5,6,7,8,9,10
        await Assert.That(index.FindRange(new IndexKey(5), new IndexKey(10), false, true)).Count().IsEqualTo(5);  // 6,7,8,9,10
        await Assert.That(index.FindRange(new IndexKey(5), new IndexKey(10), true, false)).Count().IsEqualTo(5);  // 5,6,7,8,9
        await Assert.That(index.FindRange(new IndexKey(5), new IndexKey(10), false, false)).Count().IsEqualTo(4); // 6,7,8,9
        
        // Test MinValue/MaxValue
        await Assert.That(index.FindRange(IndexKey.MinValue, new IndexKey(5), true, true)).Count().IsEqualTo(5);
        await Assert.That(index.FindRange(new IndexKey(15), IndexKey.MaxValue, true, true)).Count().IsEqualTo(6); // 15..20
        await Assert.That(index.FindRange(IndexKey.MinValue, IndexKey.MaxValue, true, true)).Count().IsEqualTo(20);
    }

    [Test]
    public async Task BTreeIndex_Delete_Comprehensive_ShouldWork()
    {
        using var index = new BTreeIndex("idx", new[] { "f" }, false, 4);
        for (int i = 1; i <= 10; i++)
        {
            index.Insert(new IndexKey(new BsonInt32(i)), new BsonInt32(i));
        }
        
        // Delete from start
        await Assert.That(index.Delete(new IndexKey(1), 1)).IsTrue();
        await Assert.That(index.EntryCount).IsEqualTo(9);
        
        // Delete from end
        await Assert.That(index.Delete(new IndexKey(10), 10)).IsTrue();
        await Assert.That(index.EntryCount).IsEqualTo(8);
        
        // Delete non-existent
        await Assert.That(index.Delete(new IndexKey(99), 99)).IsFalse();
        await Assert.That(index.Delete(new IndexKey(5), 99)).IsFalse(); // Key exists, DocId doesn't
    }

    [Test]
    public async Task BTreeIndex_Unique_Constraint_ShouldWork()
    {
        using var index = new BTreeIndex("idx", new[] { "f" }, true);
        await Assert.That(index.Insert(new IndexKey(1), 100)).IsTrue();
        await Assert.That(index.Insert(new IndexKey(1), 200)).IsFalse();
    }
}
