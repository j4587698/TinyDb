using TinyDb.Attributes;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Attributes;

public class AttributeCoverageTests
{
    [Test]
    public async Task IndexAttribute_Constructors_And_Properties()
    {
        // Default constructor
        var attr1 = new IndexAttribute();
        await Assert.That(attr1.Unique).IsFalse();
        await Assert.That(attr1.SortDirection).IsEqualTo(IndexSortDirection.Ascending);
        await Assert.That(attr1.Name).IsNull();
        
        // Constructor(bool)
        var attr2 = new IndexAttribute(true);
        await Assert.That(attr2.Unique).IsTrue();
        
        // Constructor(string)
        var attr3 = new IndexAttribute("idx_test");
        await Assert.That(attr3.Name).IsEqualTo("idx_test");
        
        // Constructor(string, bool)
        var attr4 = new IndexAttribute("idx_test", true);
        await Assert.That(attr4.Name).IsEqualTo("idx_test");
        await Assert.That(attr4.Unique).IsTrue();
        
        // Constructor(bool, IndexSortDirection)
        var attr5 = new IndexAttribute(true, IndexSortDirection.Descending);
        await Assert.That(attr5.Unique).IsTrue();
        await Assert.That(attr5.SortDirection).IsEqualTo(IndexSortDirection.Descending);
        
        // Constructor(string, bool, IndexSortDirection)
        var attr6 = new IndexAttribute("idx_test", true, IndexSortDirection.Descending);
        await Assert.That(attr6.Name).IsEqualTo("idx_test");
        await Assert.That(attr6.SortDirection).IsEqualTo(IndexSortDirection.Descending);
        
        // Priority
        attr1.Priority = 10;
        await Assert.That(attr1.Priority).IsEqualTo(10);
    }

    [Test]
    public async Task IndexAttribute_ToString()
    {
        var attr1 = new IndexAttribute("test", true) { Priority = 5, SortDirection = IndexSortDirection.Descending };
        var str1 = attr1.ToString();
        await Assert.That(str1).Contains("Name=test");
        await Assert.That(str1).Contains("Unique=True");
        await Assert.That(str1).Contains("Sort=Descending");
        await Assert.That(str1).Contains("Priority=5");
        
        var attr2 = new IndexAttribute();
        var str2 = attr2.ToString();
        await Assert.That(str2).Contains("Unique=False");
        // Name is null, shouldn't be present
        await Assert.That(str2).DoesNotContain("Name=");
        // Priority 0, shouldn't be present
        await Assert.That(str2).DoesNotContain("Priority=");
    }
    
    [Test]
    public async Task CompositeIndexAttribute_Constructors_And_Properties()
    {
        var fields = new[] { "field1", "field2" };
        
        // Constructor(params string[])
        var attr1 = new CompositeIndexAttribute(fields);
        await Assert.That(attr1.Fields).IsEqualTo(fields);
        await Assert.That(attr1.Unique).IsFalse();
        await Assert.That(attr1.Name).IsEqualTo("idx_field1_field2"); // check default naming logic
        
        // Constructor(string, params string[])
        var attr2 = new CompositeIndexAttribute("idx_custom", fields);
        await Assert.That(attr2.Name).IsEqualTo("idx_custom");
        await Assert.That(attr2.Fields).IsEqualTo(fields);
        await Assert.That(attr2.Unique).IsFalse();

        // Constructor(string, bool, params string[])
        var attr3 = new CompositeIndexAttribute("idx_comp_unique", true, fields);
        await Assert.That(attr3.Name).IsEqualTo("idx_comp_unique");
        await Assert.That(attr3.Fields).IsEqualTo(fields);
        await Assert.That(attr3.Unique).IsTrue();
    }
    
    [Test]
    public async Task CompositeIndexAttribute_ToString()
    {
        var fields = new[] { "f1", "f2" };
        var attr = new CompositeIndexAttribute("test", true, fields);
        var str = attr.ToString();
        
        await Assert.That(str).Contains("Name=test");
        await Assert.That(str).Contains("Unique=True");
        await Assert.That(str).Contains("f1, f2");
    }

    [Test]
    public async Task EntityAttribute_Constructors_And_Properties()
    {
        // Default
        var attr1 = new EntityAttribute();
        await Assert.That(attr1.CollectionName).IsNull();
        await Assert.That(attr1.IdProperty).IsNull();
        
        // CollectionName
        var attr2 = new EntityAttribute("users");
        await Assert.That(attr2.CollectionName).IsEqualTo("users");
        await Assert.That(attr2.IdProperty).IsNull();
        
        // CollectionName and IdProperty
        var attr3 = new EntityAttribute("users", "UserId");
        await Assert.That(attr3.CollectionName).IsEqualTo("users");
        await Assert.That(attr3.IdProperty).IsEqualTo("UserId");
        
        // Properties setter
        attr1.CollectionName = "products";
        attr1.IdProperty = "ProductId";
        await Assert.That(attr1.CollectionName).IsEqualTo("products");
        await Assert.That(attr1.IdProperty).IsEqualTo("ProductId");
    }

    [Test]
    public async Task MarkerAttributes_Coverage()
    {
        var idAttr = new IdAttribute();
        await Assert.That(idAttr).IsNotNull();
        
        var ignoreAttr = new BsonIgnoreAttribute();
        await Assert.That(ignoreAttr).IsNotNull();
    }
}
