using System;
using System.Linq;
using TinyDb.Core;
using TinyDb.Collections;
using TinyDb.Bson;
using TinyDb.Attributes;

namespace TinyDb.Demo;

/// <summary>
/// 事务功能演示
/// </summary>
public static class TransactionDemo
{
    /// <summary>
    /// 运行事务演示
    /// </summary>
    public static async Task RunAsync()
    {
        Console.WriteLine("=== SimpleDb 事务功能演示 ===");
        Console.WriteLine();

        // 创建临时数据库
        var testDbFile = "transaction_demo.db";
        if (System.IO.File.Exists(testDbFile))
        {
            System.IO.File.Delete(testDbFile);
        }

        var options = new TinyDbOptions
        {
            DatabaseName = "TransactionDemoDb",
            PageSize = 4096,
            CacheSize = 100,
            MaxTransactions = 10,
            TransactionTimeout = TimeSpan.FromMinutes(5)
        };

        using var engine = new TinyDbEngine(testDbFile, options);
        var accounts = engine.GetCollection<Account>();

        Console.WriteLine("✅ 数据库引擎创建成功！");

        // 演示基本事务操作
        await BasicTransactionDemo(accounts);

        // 演示事务回滚
        await TransactionRollbackDemo(accounts);

        // 演示保存点
        await SavepointDemo(accounts);

        // 演示并发事务
        await ConcurrentTransactionDemo(engine);

        // 演示事务统计
        TransactionStatisticsDemo(engine);

        Console.WriteLine("\n=== 事务演示完成！ ===");
        Console.WriteLine($"数据库统计: {engine.GetStatistics()}");
        Console.WriteLine($"事务统计: {engine.GetTransactionStatistics()}");

        // 清理
        if (System.IO.File.Exists(testDbFile))
        {
            System.IO.File.Delete(testDbFile);
        }
    }

    /// <summary>
    /// 基本事务操作演示
    /// </summary>
    private static Task BasicTransactionDemo(ILiteCollection<Account> accounts)
    {
        Console.WriteLine("--- 基本事务操作演示 ---");

        // 准备测试数据
        var account1 = new Account { Name = "张三", Balance = 1000 };
        var account2 = new Account { Name = "李四", Balance = 500 };

        accounts.Insert(account1);
        accounts.Insert(account2);

        Console.WriteLine($"初始状态: {account1.Name} 余额 {account1.Balance}, {account2.Name} 余额 {account2.Balance}");

        // 执行转账事务
        using (var transaction = accounts.Database.BeginTransaction())
        {
            try
            {
                Console.WriteLine("开始转账事务: 张三向李四转账 200");

                // 转出
                account1.Balance -= 200;
                accounts.Update(account1);
                Console.WriteLine($"✅ {account1.Name} 转出 200, 余额: {account1.Balance}");

                // 转入
                account2.Balance += 200;
                accounts.Update(account2);
                Console.WriteLine($"✅ {account2.Name} 转入 200, 余额: {account2.Balance}");

                // 提交事务
                transaction.Commit();
                Console.WriteLine("✅ 转账事务提交成功！");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ 转账失败: {ex.Message}");
                transaction.Rollback();
            }
        }

        // 验证最终状态
        var finalAccount1 = accounts.FindById(account1.Id);
        var finalAccount2 = accounts.FindById(account2.Id);
        Console.WriteLine($"最终状态: {finalAccount1?.Name} 余额 {finalAccount1?.Balance}, {finalAccount2?.Name} 余额 {finalAccount2?.Balance}");

        // 清理
        accounts.Delete(account1.Id);
        accounts.Delete(account2.Id);

        Console.WriteLine();
        return Task.CompletedTask;
    }

    /// <summary>
    /// 事务回滚演示
    /// </summary>
    private static Task TransactionRollbackDemo(ILiteCollection<Account> accounts)
    {
        Console.WriteLine("--- 事务回滚演示 ---");

        var account = new Account { Name = "测试用户", Balance = 1000 };
        accounts.Insert(account);

        Console.WriteLine($"创建测试账户: {account.Name}, 余额: {account.Balance}");

        using (var transaction = accounts.Database.BeginTransaction())
        {
            try
            {
                Console.WriteLine("开始事务，执行一些操作...");

                // 修改余额
                account.Balance = 1500;
                accounts.Update(account);
                Console.WriteLine($"✅ 修改余额为: {account.Balance}");

                // 模拟某种错误条件
                Console.WriteLine("❌ 模拟错误条件，事务将回滚");
                throw new InvalidOperationException("模拟的业务错误");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"捕获异常: {ex.Message}");
                Console.WriteLine("事务将自动回滚...");
                // 不调用 Commit，让 using 语句自动回滚
            }
        }

        // 验证回滚结果
        var rollbackAccount = accounts.FindById(account.Id);
        Console.WriteLine($"回滚后状态: {rollbackAccount?.Name}, 余额: {rollbackAccount?.Balance}");

        // 清理
        accounts.Delete(account.Id);

        Console.WriteLine();
        return Task.CompletedTask;
    }

    /// <summary>
    /// 保存点演示
    /// </summary>
    private static Task SavepointDemo(ILiteCollection<Account> accounts)
    {
        Console.WriteLine("--- 保存点演示 ---");

        var account = new Account { Name = "保存点测试", Balance = 1000 };
        accounts.Insert(account);

        using (var transaction = accounts.Database.BeginTransaction())
        {
            Console.WriteLine("开始事务...");

            // 第一次修改
            account.Balance = 1200;
            accounts.Update(account);
            Console.WriteLine($"✅ 第一次修改: 余额 = {account.Balance}");

            // 创建保存点
            var savepointId = transaction.CreateSavepoint("first_modification");
            Console.WriteLine($"✅ 创建保存点: {savepointId:N}");

            // 第二次修改
            account.Balance = 800;
            accounts.Update(account);
            Console.WriteLine($"✅ 第二次修改: 余额 = {account.Balance}");

            // 第三次修改
            account.Balance = 600;
            accounts.Update(account);
            Console.WriteLine($"✅ 第三次修改: 余额 = {account.Balance}");

            // 回滚到保存点
            Console.WriteLine("🔄 回滚到保存点...");
            transaction.RollbackToSavepoint(savepointId);

            // 验证回滚结果
            var checkAccount = accounts.FindById(account.Id);
            Console.WriteLine($"✅ 回滚到保存点后: 余额 = {checkAccount?.Balance}");

            // 继续操作并提交
            account.Balance = 1100;
            accounts.Update(account);
            Console.WriteLine($"✅ 最终修改: 余额 = {account.Balance}");

            transaction.Commit();
            Console.WriteLine("✅ 事务提交成功！");
        }

        // 验证最终结果
        var finalAccount = accounts.FindById(account.Id);
        Console.WriteLine($"最终状态: 余额 = {finalAccount?.Balance}");

        // 清理
        accounts.Delete(account.Id);

        Console.WriteLine();
        return Task.CompletedTask;
    }

    /// <summary>
    /// 并发事务演示
    /// </summary>
    private static async Task ConcurrentTransactionDemo(TinyDbEngine engine)
    {
        Console.WriteLine("--- 并发事务演示 ---");

        var accounts = engine.GetCollection<Account>();

        // 创建测试账户
        var sharedAccount = new Account { Name = "共享账户", Balance = 1000 };
        accounts.Insert(sharedAccount);

        var tasks = new Task[3];

        // 任务1: 存款
        tasks[0] = Task.Run(async () =>
        {
            using var transaction = engine.BeginTransaction();
            try
            {
                var account = accounts.FindById(sharedAccount.Id);
                if (account != null)
                {
                    account.Balance += 300;
                    accounts.Update(account);
                    Console.WriteLine($"📥 任务1: 存款 300, 余额 = {account.Balance}");
                    await Task.Delay(100); // 模拟处理时间
                    transaction.Commit();
                    Console.WriteLine("✅ 任务1: 存款事务提交");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ 任务1 失败: {ex.Message}");
            }
        });

        // 任务2: 取款
        tasks[1] = Task.Run(async () =>
        {
            using var transaction = engine.BeginTransaction();
            try
            {
                var account = accounts.FindById(sharedAccount.Id);
                if (account != null && account.Balance >= 200)
                {
                    account.Balance -= 200;
                    accounts.Update(account);
                    Console.WriteLine($"📤 任务2: 取款 200, 余额 = {account.Balance}");
                    await Task.Delay(50); // 模拟处理时间
                    transaction.Commit();
                    Console.WriteLine("✅ 任务2: 取款事务提交");
                }
                else
                {
                    Console.WriteLine("❌ 任务2: 余额不足");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ 任务2 失败: {ex.Message}");
            }
        });

        // 任务3: 查询余额
        tasks[2] = Task.Run(async () =>
        {
            using var transaction = engine.BeginTransaction();
            try
            {
                var account = accounts.FindById(sharedAccount.Id);
                if (account != null)
                {
                    Console.WriteLine($"🔍 任务3: 查询余额 = {account.Balance}");
                    await Task.Delay(75); // 模拟处理时间
                    transaction.Commit();
                    Console.WriteLine("✅ 任务3: 查询事务提交");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ 任务3 失败: {ex.Message}");
            }
        });

        // 等待所有任务完成
        await Task.WhenAll(tasks);

        // 验证最终余额
        var finalAccount = accounts.FindById(sharedAccount.Id);
        Console.WriteLine($"🏁 最终余额: {finalAccount?.Balance}");

        // 清理
        accounts.Delete(sharedAccount.Id);

        Console.WriteLine();
    }

    /// <summary>
    /// 事务统计演示
    /// </summary>
    private static void TransactionStatisticsDemo(TinyDbEngine engine)
    {
        Console.WriteLine("--- 事务统计演示 ---");

        // 显示初始统计
        var stats = engine.GetTransactionStatistics();
        Console.WriteLine($"初始统计: {stats}");

        // 创建一些事务
        var transactions = new List<ITransaction>();
        for (int i = 0; i < 3; i++)
        {
            transactions.Add(engine.BeginTransaction());
        }

        // 显示活动事务统计
        stats = engine.GetTransactionStatistics();
        Console.WriteLine($"活动事务统计: {stats}");

        // 提交一些事务
        for (int i = 0; i < 2; i++)
        {
            transactions[i].Commit();
        }

        // 显示剩余事务统计
        stats = engine.GetTransactionStatistics();
        Console.WriteLine($"剩余事务统计: {stats}");

        // 清理剩余事务
        transactions[2].Dispose();

        // 显示最终统计
        stats = engine.GetTransactionStatistics();
        Console.WriteLine($"最终统计: {stats}");

        Console.WriteLine();
    }
}

/// <summary>
/// 账户实体
/// </summary>
[Entity("accounts")]
public class Account
{
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();
    public string Name { get; set; } = "";
    public decimal Balance { get; set; }
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    public DateTime UpdatedAt { get; set; } = DateTime.UtcNow;
}
