using TinyDb.Serialization;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using System.Collections;
using System.Collections.Generic;

namespace TinyDb.Tests.Serialization;

public class AotMapperErrorTests
{
    public class NoAddCollection : IEnumerable
    {
        public IEnumerator GetEnumerator() => throw new NotImplementedException();
    }

    [Test]
    public async Task Deserialize_To_Collection_Without_Add_Should_Throw()
    {
        var array = new BsonArray().AddValue(1);
        try
        {
            AotBsonMapper.ConvertValue(array, typeof(NoAddCollection));
            Assert.Fail("Should throw NotSupportedException");
        }
        catch (NotSupportedException ex)
        {
            await Assert.That(ex.Message).Contains("Add");
        }
    }

    public class ExplicitDict : IDictionary, IDictionary<string, int>
    {
        // Explicit impl
        void IDictionary.Add(object key, object value) { }
        public void Clear() { }
        public bool Contains(object key) => false;
        public IDictionaryEnumerator GetEnumerator() => null!;
        public bool IsFixedSize => false;
        public bool IsReadOnly => false;
        public ICollection Keys => null!;
        public void Remove(object key) { }
        public ICollection Values => null!;
        public object? this[object key] { get => null; set { } }
        public void CopyTo(Array array, int index) { }
        public int Count => 0;
        public bool IsSynchronized => false;
        public object SyncRoot => null!;
        IEnumerator IEnumerable.GetEnumerator() => null!;
        
        // Generic impl
        public int this[string key] { get => 0; set { } }
        ICollection<string> IDictionary<string, int>.Keys => null!;
        ICollection<int> IDictionary<string, int>.Values => null!;
        void IDictionary<string, int>.Add(string key, int value) { }
        bool IDictionary<string, int>.ContainsKey(string key) => false;
        bool IDictionary<string, int>.Remove(string key) => false;
        bool IDictionary<string, int>.TryGetValue(string key, out int value) { value = 0; return false; }
        void ICollection<KeyValuePair<string, int>>.Add(KeyValuePair<string, int> item) { }
        bool ICollection<KeyValuePair<string, int>>.Contains(KeyValuePair<string, int> item) => false;
        void ICollection<KeyValuePair<string, int>>.CopyTo(KeyValuePair<string, int>[] array, int arrayIndex) { }
        bool ICollection<KeyValuePair<string, int>>.Remove(KeyValuePair<string, int> item) => false;
        IEnumerator<KeyValuePair<string, int>> IEnumerable<KeyValuePair<string, int>>.GetEnumerator() => null!;

        // Hide public Add to trigger fallback error
        private void Add(string key, int value) { }
    }
    
    [Test]
    public async Task Deserialize_To_Dictionary_Without_Public_Add_Should_Throw()
    {
        var doc = new BsonDocument().Set("a", 1);
        try
        {
            AotBsonMapper.ConvertValue(doc, typeof(ExplicitDict));
            Assert.Fail("Should throw NotSupportedException");
        }
        catch (NotSupportedException ex)
        {
            // Fallback dictionary conversion looks for Add method
            await Assert.That(ex.Message).Contains("Add");
        }
    }
}
