using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using TinyDb.Core;
using TinyDb.Collections;
using TinyDb.Bson;
using TinyDb.Attributes;

namespace TinyDb.Demo.Demos;

/// <summary>
/// 异步操作演示
/// </summary>
public static class AsyncOperationsDemo
{
    public static async Task RunAsync()
    {
        Console.WriteLine("=== TinyDb 异步操作演示 ===");
        Console.WriteLine();

        var dbPath = "async_demo.db";
        if (File.Exists(dbPath)) File.Delete(dbPath);

        using var engine = new TinyDbEngine(dbPath);
        var tasks = engine.GetCollection<TodoTask>();

        // 异步插入演示
        Console.WriteLine("1. 异步插入操作");
        Console.WriteLine(new string('-', 50));
        await AsyncInsertDemo(tasks);
        Console.WriteLine();

        // 异步更新演示
        Console.WriteLine("2. 异步更新操作");
        Console.WriteLine(new string('-', 50));
        await AsyncUpdateDemo(tasks);
        Console.WriteLine();

        // 异步删除演示
        Console.WriteLine("3. 异步删除操作");
        Console.WriteLine(new string('-', 50));
        await AsyncDeleteDemo(tasks);
        Console.WriteLine();

        // 异步批量操作演示
        Console.WriteLine("4. 异步批量操作");
        Console.WriteLine(new string('-', 50));
        await AsyncBatchDemo(tasks);
        Console.WriteLine();

        // 异步Upsert操作演示
        Console.WriteLine("5. 异步Upsert操作");
        Console.WriteLine(new string('-', 50));
        await AsyncUpsertDemo(tasks);
        Console.WriteLine();

        // 并发异步操作演示
        Console.WriteLine("6. 并发异步操作");
        Console.WriteLine(new string('-', 50));
        await ConcurrentAsyncDemo(engine);
        Console.WriteLine();

        // 带取消令牌的异步操作
        Console.WriteLine("7. 带取消令牌的异步操作");
        Console.WriteLine(new string('-', 50));
        await CancellationTokenDemo(tasks);
        Console.WriteLine();

        // 清理
        if (File.Exists(dbPath)) File.Delete(dbPath);

        Console.WriteLine("✅ 异步操作演示完成！");
    }

    /// <summary>
    /// 异步插入演示
    /// </summary>
    private static async Task AsyncInsertDemo(ITinyCollection<TodoTask> tasks)
    {
        // 单个异步插入
        var task1 = new TodoTask
        {
            Title = "完成项目文档",
            Description = "编写用户手册和API文档",
            Priority = TaskPriority.High,
            DueDate = DateTime.Now.AddDays(7)
        };

        var sw = Stopwatch.StartNew();
        var insertedId = await tasks.InsertAsync(task1);
        sw.Stop();

        Console.WriteLine($"✅ 异步插入任务: {task1.Title}");
        Console.WriteLine($"   ID: {insertedId}");
        Console.WriteLine($"   耗时: {sw.ElapsedMilliseconds}ms");

        // 多个异步插入
        var newTasks = new[]
        {
            new TodoTask { Title = "代码审查", Priority = TaskPriority.Medium, DueDate = DateTime.Now.AddDays(3) },
            new TodoTask { Title = "单元测试", Priority = TaskPriority.High, DueDate = DateTime.Now.AddDays(2) },
            new TodoTask { Title = "部署准备", Priority = TaskPriority.Low, DueDate = DateTime.Now.AddDays(10) }
        };

        sw.Restart();
        var insertedCount = await tasks.InsertAsync(newTasks);
        sw.Stop();

        Console.WriteLine($"✅ 异步批量插入: {insertedCount} 条记录");
        Console.WriteLine($"   耗时: {sw.ElapsedMilliseconds}ms");
    }

    /// <summary>
    /// 异步更新演示
    /// </summary>
    private static async Task AsyncUpdateDemo(ITinyCollection<TodoTask> tasks)
    {
        // 查找要更新的任务
        var taskToUpdate = await tasks.FindOneAsync(t => t.Title == "代码审查");
        if (taskToUpdate != null)
        {
            Console.WriteLine($"📝 更新前: {taskToUpdate.Title}, 状态: {taskToUpdate.Status}, 优先级: {taskToUpdate.Priority}");

            taskToUpdate.Status = TodoStatus.InProgress;
            taskToUpdate.Priority = TaskPriority.High;
            taskToUpdate.UpdatedAt = DateTime.Now;

            var sw = Stopwatch.StartNew();
            var updateCount = await tasks.UpdateAsync(taskToUpdate);
            sw.Stop();

            Console.WriteLine($"✅ 异步更新: {updateCount} 条记录");
            Console.WriteLine($"   更新后: 状态={taskToUpdate.Status}, 优先级={taskToUpdate.Priority}");
            Console.WriteLine($"   耗时: {sw.ElapsedMilliseconds}ms");
        }

        // 批量异步更新
        var tasksToUpdate = tasks.Find(t => t.Priority == TaskPriority.Low).ToList();
        foreach (var task in tasksToUpdate)
        {
            task.Priority = TaskPriority.Medium;
            task.UpdatedAt = DateTime.Now;
        }

        if (tasksToUpdate.Any())
        {
            var sw = Stopwatch.StartNew();
            var updateCount = await tasks.UpdateAsync(tasksToUpdate);
            sw.Stop();
            Console.WriteLine($"✅ 异步批量更新: {updateCount} 条记录的优先级已调整");
            Console.WriteLine($"   耗时: {sw.ElapsedMilliseconds}ms");
        }
    }

    /// <summary>
    /// 异步删除演示
    /// </summary>
    private static async Task AsyncDeleteDemo(ITinyCollection<TodoTask> tasks)
    {
        // 插入一些测试数据用于删除演示
        var tempTask = new TodoTask { Title = "临时任务", Priority = TaskPriority.Low };
        await tasks.InsertAsync(tempTask);

        // 单个异步删除
        var sw = Stopwatch.StartNew();
        var deleteCount = await tasks.DeleteAsync(tempTask.Id);
        sw.Stop();

        Console.WriteLine($"✅ 异步删除单条记录: {deleteCount} 条");
        Console.WriteLine($"   耗时: {sw.ElapsedMilliseconds}ms");

        // 条件删除
        await tasks.InsertAsync(new TodoTask { Title = "待删除1", Priority = TaskPriority.Low, Status = TodoStatus.Completed });
        await tasks.InsertAsync(new TodoTask { Title = "待删除2", Priority = TaskPriority.Low, Status = TodoStatus.Completed });

        sw.Restart();
        var deletedMany = await tasks.DeleteManyAsync(t => t.Status == TodoStatus.Completed);
        sw.Stop();

        Console.WriteLine($"✅ 异步条件删除: {deletedMany} 条已完成的任务");
        Console.WriteLine($"   耗时: {sw.ElapsedMilliseconds}ms");
    }

    /// <summary>
    /// 异步批量操作演示
    /// </summary>
    private static async Task AsyncBatchDemo(ITinyCollection<TodoTask> tasks)
    {
        // 清空现有数据
        await tasks.DeleteAllAsync();

        // 批量插入大量数据
        var batchSize = 100;
        var batch = Enumerable.Range(1, batchSize)
            .Select(i => new TodoTask
            {
                Title = $"批量任务 {i}",
                Description = $"这是第 {i} 个批量创建的任务",
                Priority = (TaskPriority)(i % 3),
                DueDate = DateTime.Now.AddDays(i % 30)
            });

        var sw = Stopwatch.StartNew();
        var insertedCount = await tasks.InsertAsync(batch);
        sw.Stop();

        Console.WriteLine($"✅ 异步批量插入 {insertedCount} 条记录");
        Console.WriteLine($"   平均每条耗时: {(double)sw.ElapsedMilliseconds / insertedCount:F2}ms");
        Console.WriteLine($"   总耗时: {sw.ElapsedMilliseconds}ms");

        // 验证数据
        var totalCount = tasks.Count();
        Console.WriteLine($"📊 当前总记录数: {totalCount}");
    }

    /// <summary>
    /// 异步Upsert操作演示
    /// </summary>
    private static async Task AsyncUpsertDemo(ITinyCollection<TodoTask> tasks)
    {
        // 创建新任务
        var newTask = new TodoTask
        {
            Title = "Upsert测试任务",
            Priority = TaskPriority.High
        };

        // 第一次Upsert (应该是Insert)
        var sw = Stopwatch.StartNew();
        var (updateType1, count1) = await tasks.UpsertAsync(newTask);
        sw.Stop();

        Console.WriteLine($"✅ 第一次Upsert: {updateType1}, 影响 {count1} 条记录");
        Console.WriteLine($"   任务ID: {newTask.Id}");
        Console.WriteLine($"   耗时: {sw.ElapsedMilliseconds}ms");

        // 修改任务
        newTask.Title = "Upsert测试任务 (已更新)";
        newTask.Status = TodoStatus.InProgress;

        // 第二次Upsert (应该是Update)
        sw.Restart();
        var (updateType2, count2) = await tasks.UpsertAsync(newTask);
        sw.Stop();

        Console.WriteLine($"✅ 第二次Upsert: {updateType2}, 影响 {count2} 条记录");
        Console.WriteLine($"   耗时: {sw.ElapsedMilliseconds}ms");

        // 验证更新
        var verifyTask = await tasks.FindByIdAsync(newTask.Id);
        Console.WriteLine($"📝 验证: {verifyTask?.Title}, 状态: {verifyTask?.Status}");
    }

    /// <summary>
    /// 并发异步操作演示
    /// </summary>
    private static async Task ConcurrentAsyncDemo(TinyDbEngine engine)
    {
        var tasks = engine.GetCollection<TodoTask>();

        // 并发插入任务
        var concurrentTasks = new List<Task>();
        var sw = Stopwatch.StartNew();

        for (int i = 0; i < 10; i++)
        {
            var index = i;
            concurrentTasks.Add(Task.Run(async () =>
            {
                var task = new TodoTask
                {
                    Title = $"并发任务 {index}",
                    Priority = (TaskPriority)(index % 3)
                };
                await tasks.InsertAsync(task);
                Console.WriteLine($"   📝 任务 {index} 已插入 (线程: {Environment.CurrentManagedThreadId})");
            }));
        }

        await Task.WhenAll(concurrentTasks);
        sw.Stop();

        Console.WriteLine($"✅ 并发插入 10 条记录完成");
        Console.WriteLine($"   总耗时: {sw.ElapsedMilliseconds}ms");

        // 验证
        var count = tasks.Count(t => t.Title.StartsWith("并发任务"));
        Console.WriteLine($"📊 并发插入的记录数: {count}");
    }

    /// <summary>
    /// 带取消令牌的异步操作演示
    /// </summary>
    private static async Task CancellationTokenDemo(ITinyCollection<TodoTask> tasks)
    {
        // 创建取消令牌
        using var cts = new CancellationTokenSource();

        // 正常操作（不取消）
        var task = new TodoTask { Title = "取消令牌测试" };
        var insertedId = await tasks.InsertAsync(task, cts.Token);
        Console.WriteLine($"✅ 正常插入成功: {task.Title}");

        // 演示取消操作
        var cts2 = new CancellationTokenSource();
        cts2.Cancel(); // 立即取消

        try
        {
            var task2 = new TodoTask { Title = "应该被取消的任务" };
            await tasks.InsertAsync(task2, cts2.Token);
            Console.WriteLine("❌ 操作应该被取消但没有");
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("✅ 操作被正确取消");
        }

        // 带超时的取消令牌
        using var timeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var task3 = new TodoTask { Title = "带超时的任务" };
        await tasks.InsertAsync(task3, timeoutCts.Token);
        Console.WriteLine($"✅ 超时前完成插入: {task3.Title}");
    }
}

/// <summary>
/// 待办任务实体
/// </summary>
[Entity("todo_tasks")]
public class TodoTask
{
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();
    public string Title { get; set; } = string.Empty;
    public string Description { get; set; } = string.Empty;
    public TaskPriority Priority { get; set; } = TaskPriority.Medium;
    public TodoStatus Status { get; set; } = TodoStatus.Pending;
    public DateTime? DueDate { get; set; }
    public DateTime CreatedAt { get; set; } = DateTime.Now;
    public DateTime UpdatedAt { get; set; } = DateTime.Now;
}

/// <summary>
/// 任务优先级
/// </summary>
public enum TaskPriority
{
    Low,
    Medium,
    High
}

/// <summary>
/// 任务状态
/// </summary>
public enum TodoStatus
{
    Pending,
    InProgress,
    Completed,
    Cancelled
}
