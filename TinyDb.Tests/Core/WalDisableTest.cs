using System;
using System.IO;
using TinyDb.Core;
using TinyDb.Tests.TestEntities;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Core;

[NotInParallel]
public class WalDisableTest
{
    private string _testDbPath = null!;

    /// <summary>
    /// 生成WAL文件路径，使用默认格式
    /// </summary>
    private static string GenerateDefaultWalPath(string dbPath)
    {
        var directory = Path.GetDirectoryName(dbPath) ?? string.Empty;
        var fileNameWithoutExt = Path.GetFileNameWithoutExtension(dbPath);
        var extension = Path.GetExtension(dbPath).TrimStart('.');
        return Path.Combine(directory, $"{fileNameWithoutExt}-wal.{extension}");
    }

    [Before(Test)]
    public void Setup()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"wal_disable_test_{Guid.NewGuid():N}.db");
        if (File.Exists(_testDbPath))
            File.Delete(_testDbPath);

        // 也删除可能的WAL文件（支持新旧格式）
        var oldWalPath = _testDbPath + ".wal";
        var newWalPath = GenerateDefaultWalPath(_testDbPath);
        if (File.Exists(oldWalPath))
            File.Delete(oldWalPath);
        if (File.Exists(newWalPath))
            File.Delete(newWalPath);
    }

    [After(Test)]
    public void Cleanup()
    {
        if (File.Exists(_testDbPath))
            File.Delete(_testDbPath);

        // 支持新旧WAL格式清理
        var oldWalPath = _testDbPath + ".wal";
        var newWalPath = GenerateDefaultWalPath(_testDbPath);
        if (File.Exists(oldWalPath))
            File.Delete(oldWalPath);
        if (File.Exists(newWalPath))
            File.Delete(newWalPath);
    }

    [Test]
    public async Task Database_Should_Not_Create_WAL_File_When_Journaling_Disabled()
    {
        // 使用与UI相同的禁用WAL配置
        var options = new TinyDbOptions
        {
            Password = null,
            EnableJournaling = false,  // 禁用WAL
            ReadOnly = false
        };

        using (var engine = new TinyDbEngine(_testDbPath, options))
        {
            var collection = engine.GetCollection<User>("test_users");

            // 插入一些数据
            for (int i = 0; i < 10; i++)
            {
                collection.Insert(new User
                {
                    Name = $"Test User {i}",
                    Age = 20 + i,
                    Email = $"test{i}@example.com"
                });
            }

            engine.Flush();
        }

        // 验证数据库文件存在
        await Assert.That(File.Exists(_testDbPath)).IsTrue();

        // 验证WAL文件不存在（使用新格式）
        string walPath = GenerateDefaultWalPath(_testDbPath);
        await Assert.That(File.Exists(walPath)).IsFalse();

        // 重新打开数据库验证数据持久化
        using (var reopenedEngine = new TinyDbEngine(_testDbPath, options))
        {
            var collection = reopenedEngine.GetCollection<User>("test_users");
            var count = collection.Count();
            await Assert.That(count).IsEqualTo(10);
        }

        // 再次验证WAL文件仍然不存在
        await Assert.That(File.Exists(walPath)).IsFalse();
    }

    [Test]
    public async Task Database_Should_Work_Correctly_Without_WAL()
    {
        // 测试多次操作都不会产生WAL文件
        var options = new TinyDbOptions
        {
            Password = null,
            EnableJournaling = false,
            ReadOnly = false
        };

        // 第一次操作
        using (var engine = new TinyDbEngine(_testDbPath, options))
        {
            var collection = engine.GetCollection<User>("users");
            collection.Insert(new User { Name = "User1", Age = 25, Email = "user1@example.com" });
            engine.Flush();
        }

        await Assert.That(File.Exists(GenerateDefaultWalPath(_testDbPath))).IsFalse();

        // 第二次操作 - 重新打开
        using (var engine = new TinyDbEngine(_testDbPath, options))
        {
            var collection = engine.GetCollection<User>("users");
            collection.Insert(new User { Name = "User2", Age = 30, Email = "user2@example.com" });
            engine.Flush();
        }

        await Assert.That(File.Exists(GenerateDefaultWalPath(_testDbPath))).IsFalse();

        // 验证数据
        using (var engine = new TinyDbEngine(_testDbPath, options))
        {
            var collection = engine.GetCollection<User>("users");
            var count = collection.Count();
            await Assert.That(count).IsEqualTo(2);
        }
    }
}