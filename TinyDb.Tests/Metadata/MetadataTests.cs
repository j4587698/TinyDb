using System;
using System.IO;
using System.Linq;
using TinyDb.Core;
using TinyDb.Metadata;
using TinyDb.Tests.TestEntities;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Metadata;

[NotInParallel]
public class MetadataTests
{
    private string _testFile = null!;
    private TinyDbEngine _engine = null!;
    private MetadataManager _manager = null!;

    [Before(Test)]
    public void Setup()
    {
        _testFile = Path.Combine(Path.GetTempPath(), $"metadata_test_{Guid.NewGuid():N}.db");
        _engine = new TinyDbEngine(_testFile);
        _manager = new MetadataManager(_engine);
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
    public async Task MetadataManager_Should_SaveAndLoadMetadata()
    {
        // Act
        _manager.SaveEntityMetadata(typeof(UserEntity));
        var metadata = _manager.GetEntityMetadata(typeof(UserEntity));

        // Assert
        await Assert.That(metadata).IsNotNull();
        await Assert.That(metadata!.DisplayName).IsEqualTo("用户信息");
        await Assert.That(metadata.Properties).Count().IsEqualTo(6);

        var usernameProp = metadata.Properties.FirstOrDefault(p => p.PropertyName == "Username");
        await Assert.That(usernameProp).IsNotNull();
        await Assert.That(usernameProp!.DisplayName).IsEqualTo("用户名");
        await Assert.That(usernameProp.Required).IsTrue();
        await Assert.That(usernameProp.Order).IsEqualTo(2);
    }

    [Test]
    public async Task MetadataManager_GetRegisteredEntityTypes_ShouldWork()
    {
        // Act
        _manager.SaveEntityMetadata(typeof(UserEntity));
        _manager.SaveEntityMetadata(typeof(ProductEntity));
        var types = _manager.GetRegisteredEntityTypes();

        // Assert
        await Assert.That(types).Count().IsEqualTo(2);
        await Assert.That(types).Contains(typeof(UserEntity).FullName!);
        await Assert.That(types).Contains(typeof(ProductEntity).FullName!);
    }

    [Test]
    public async Task MetadataApi_Should_ProvideEasyAccess()
    {
        // Arrange
        _manager.SaveEntityMetadata(typeof(UserEntity));

        // Act & Assert
        await Assert.That(_manager.GetEntityDisplayName(typeof(UserEntity))).IsEqualTo("用户信息");
        await Assert.That(_manager.GetPropertyDisplayName(typeof(UserEntity), "Username")).IsEqualTo("用户名");
        await Assert.That(_manager.GetPropertyType(typeof(UserEntity), "Age")).IsEqualTo("System.Int32");
        await Assert.That(_manager.IsPropertyRequired(typeof(UserEntity), "Email")).IsTrue();
        await Assert.That(_manager.GetPropertyOrder(typeof(UserEntity), "Email")).IsEqualTo(3);
        
        var props = _manager.GetEntityProperties(typeof(UserEntity));
        await Assert.That(props).Count().IsEqualTo(6);
        await Assert.That(props.Any(p => p.PropertyName == "Username" && p.PropertyType == "System.String")).IsTrue();
    }

    [Test]
    public async Task MetadataManager_Update_ShouldWork()
    {
        // Arrange
        _manager.SaveEntityMetadata(typeof(UserEntity));
        var firstLoad = _manager.GetEntityMetadata(typeof(UserEntity));
        await Task.Delay(10); // Ensure timestamp changes if granularity is low

        // Act
        _manager.SaveEntityMetadata(typeof(UserEntity));
        var secondLoad = _manager.GetEntityMetadata(typeof(UserEntity));

        // Assert
        await Assert.That(secondLoad).IsNotNull();
        // Since it's the same type, content is same but internally it should have been an update
        await Assert.That(secondLoad!.TypeName).IsEqualTo(firstLoad!.TypeName);
    }

    [Test]
    public async Task MetadataDTOs_Coverage()
    {
        var pm = new PasswordMetadata
        {
            IsPassword = true,
            RequiredStrength = PasswordStrength.Strong,
            MinLength = 10,
            MaxLength = 100,
            RequireSpecialChar = true,
            RequireNumber = true,
            RequireUppercase = true,
            RequireLowercase = true,
            Hint = "Hint",
            ShowStrengthIndicator = true,
            AllowToggle = true
        };
        
        await Assert.That(pm.IsPassword).IsTrue();
        await Assert.That(pm.RequiredStrength).IsEqualTo(PasswordStrength.Strong);
        await Assert.That(pm.MinLength).IsEqualTo(10);
        await Assert.That(pm.MaxLength).IsEqualTo(100);
        await Assert.That(pm.RequireSpecialChar).IsTrue();
        await Assert.That(pm.RequireNumber).IsTrue();
        await Assert.That(pm.RequireUppercase).IsTrue();
        await Assert.That(pm.RequireLowercase).IsTrue();
        await Assert.That(pm.Hint).IsEqualTo("Hint");
        await Assert.That(pm.ShowStrengthIndicator).IsTrue();
        await Assert.That(pm.AllowToggle).IsTrue();
    }
}
