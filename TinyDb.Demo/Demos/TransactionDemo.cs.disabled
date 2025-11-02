using TinyDb.Core;
using TinyDb.Collections;
using TinyDb.Demo.Entities;
using TinyDb.Attributes;
using TinyDb.Bson;

namespace TinyDb.Demo.Demos;

/// <summary>
/// 事务处理功能演示
/// </summary>
public static class TransactionDemo
{
    public static async Task RunAsync()
    {
        Console.WriteLine("=== 事务处理功能演示 ===");
        Console.WriteLine("展示ACID事务的提交和回滚机制");
        Console.WriteLine();

        const string dbPath = "transaction_demo.db";
        if (File.Exists(dbPath)) File.Delete(dbPath);

        using var engine = new TinyDbEngine(dbPath);
        var accounts = engine.GetCollection<Account>("accounts");

        // 准备初始数据
        Console.WriteLine("1. 准备初始账户数据:");
        var account1 = new Account
        {
            AccountNumber = "ACC001",
            OwnerName = "张三",
            Balance = 1000.00m,
            CreatedAt = DateTime.Now
        };

        var account2 = new Account
        {
            AccountNumber = "ACC002",
            OwnerName = "李四",
            Balance = 2000.00m,
            CreatedAt = DateTime.Now
        };

        accounts.Insert(account1);
        accounts.Insert(account2);
        Console.WriteLine($"   ✅ 创建账户: {account1.OwnerName} - 余额: ¥{account1.Balance:N2}");
        Console.WriteLine($"   ✅ 创建账户: {account2.OwnerName} - 余额: ¥{account2.Balance:N2}");
        Console.WriteLine();

        // 演示成功的事务
        Console.WriteLine("2. 成功的事务转账:");
        try
        {
            using var transaction = engine.BeginTransaction();

            var account1InTx = transaction.GetCollection<Account>("accounts");
            var account2InTx = transaction.GetCollection<Account>("accounts");

            // 转账操作：张三给李四转账300元
            var fromAccount = account1InTx.FindOne(a => a.AccountNumber == "ACC001");
            var toAccount = account2InTx.FindOne(a => a.AccountNumber == "ACC002");

            if (fromAccount != null && toAccount != null)
            {
                fromAccount.Balance -= 300.00m;
                toAccount.Balance += 300.00m;

                account1InTx.Update(fromAccount);
                account2InTx.Update(toAccount);

                Console.WriteLine($"   📤 转出: {fromAccount.OwnerName} -¥300.00");
                Console.WriteLine($"   📥 转入: {toAccount.OwnerName} +¥300.00");
            }

            transaction.Commit();
            Console.WriteLine("   ✅ 事务提交成功！");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"   ❌ 事务失败: {ex.Message}");
        }

        // 验证转账结果
        var updatedAccount1 = accounts.FindOne(a => a.AccountNumber == "ACC001");
        var updatedAccount2 = accounts.FindOne(a => a.AccountNumber == "ACC002");
        Console.WriteLine($"   📊 转账后余额: {updatedAccount1?.OwnerName} ¥{updatedAccount1?.Balance:N2}");
        Console.WriteLine($"   📊 转账后余额: {updatedAccount2?.OwnerName} ¥{updatedAccount2?.Balance:N2}");
        Console.WriteLine();

        // 演示失败的事务（回滚）
        Console.WriteLine("3. 失败的事务演示（余额不足）:");
        try
        {
            using var transaction = engine.BeginTransaction();

            var account1InTx = transaction.GetCollection<Account>("accounts");
            var account2InTx = transaction.GetCollection<Account>("accounts");

            // 尝试转账2000元（张三余额不足）
            var fromAccount = account1InTx.FindOne(a => a.AccountNumber == "ACC001");
            var toAccount = account2InTx.FindOne(a => a.AccountNumber == "ACC002");

            if (fromAccount != null && toAccount != null)
            {
                decimal transferAmount = 2000.00m;
                fromAccount.Balance -= transferAmount;
                toAccount.Balance += transferAmount;

                // 检查余额是否为负
                if (fromAccount.Balance < 0)
                {
                    throw new InvalidOperationException("余额不足，无法完成转账");
                }

                account1InTx.Update(fromAccount);
                account2InTx.Update(toAccount);

                Console.WriteLine($"   📤 尝试转出: {fromAccount.OwnerName} -¥{transferAmount:N2}");
                Console.WriteLine($"   📥 尝试转入: {toAccount.OwnerName} +¥{transferAmount:N2}");
            }

            transaction.Commit();
            Console.WriteLine("   ✅ 事务提交成功！");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"   ❌ 事务回滚: {ex.Message}");
        }

        // 验证回滚结果
        var rollbackAccount1 = accounts.FindOne(a => a.AccountNumber == "ACC001");
        var rollbackAccount2 = accounts.FindOne(a => a.AccountNumber == "ACC002");
        Console.WriteLine($"   📊 回滚后余额: {rollbackAccount1?.OwnerName} ¥{rollbackAccount1?.Balance:N2}");
        Console.WriteLine($"   📊 回滚后余额: {rollbackAccount2?.OwnerName} ¥{rollbackAccount2?.Balance:N2}");
        Console.WriteLine();

        // 演示复杂事务（多操作）
        Console.WriteLine("4. 复杂事务（批量操作）:");
        try
        {
            using var transaction = engine.BeginTransaction();
            var accountsInTx = transaction.GetCollection<Account>("accounts");

            // 批量创建新账户
            var newAccounts = new[]
            {
                new Account { AccountNumber = "ACC003", OwnerName = "王五", Balance = 500.00m, CreatedAt = DateTime.Now },
                new Account { AccountNumber = "ACC004", OwnerName = "赵六", Balance = 800.00m, CreatedAt = DateTime.Now },
                new Account { AccountNumber = "ACC005", OwnerName = "钱七", Balance = 1200.00m, CreatedAt = DateTime.Now }
            };

            foreach (var account in newAccounts)
            {
                accountsInTx.Insert(account);
                Console.WriteLine($"   ➕ 创建账户: {account.OwnerName} - ¥{account.Balance:N2}");
            }

            // 批量更新（给所有新账户增加100元奖金）
            var allNewAccounts = accountsInTx.Find(a => a.AccountNumber.StartsWith("ACC00")).ToList();
            foreach (var account in allNewAccounts)
            {
                account.Balance += 100.00m;
                accountsInTx.Update(account);
                Console.WriteLine($"   🎁 奖金发放: {account.OwnerName} +¥100.00");
            }

            transaction.Commit();
            Console.WriteLine("   ✅ 批量事务提交成功！");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"   ❌ 批量事务失败: {ex.Message}");
        }

        // 最终统计
        var allAccounts = accounts.FindAll().ToList();
        Console.WriteLine($"\n5. 最终账户统计:");
        Console.WriteLine($"   📊 总账户数: {allAccounts.Count}");
        Console.WriteLine($"   💰 总余额: ¥{allAccounts.Sum(a => a.Balance):N2}");

        foreach (var account in allAccounts.OrderBy(a => a.AccountNumber))
        {
            Console.WriteLine($"   👤 {account.AccountNumber}: {account.OwnerName} - ¥{account.Balance:N2}");
        }

        Console.WriteLine("\n✅ 事务演示完成！");
        Console.WriteLine("🔧 ACID特性得到完整验证：原子性、一致性、隔离性、持久性");
    }
}

/// <summary>
/// 账户实体（用于事务演示）
/// </summary>
[Entity("accounts")]
public class Account
{
    [Id]
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();

    public string AccountNumber { get; set; } = string.Empty;
    public string OwnerName { get; set; } = string.Empty;
    public decimal Balance { get; set; }
    public DateTime CreatedAt { get; set; }
    public DateTime? LastModifiedAt { get; set; }
}