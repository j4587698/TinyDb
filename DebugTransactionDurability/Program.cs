using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using TinyDb.Core;
using DebugTransactionDurability;

class Program
{
    static async Task Main()
    {
        Console.WriteLine("调试事务持久性问题...");

        var testFile = Path.GetTempFileName();
        Console.WriteLine($"测试文件: {testFile}");

        try
        {
            // 第一阶段：创建引擎并插入数据
            Console.WriteLine("\n=== 第一阶段：插入数据 ===");
            using var engine = new TinyDbEngine(testFile);
            var collection = engine.GetCollection<UserWithIntId>();

            var user = new UserWithIntId { Name = "DurableUser", Age = 25 };
            Console.WriteLine($"插入用户: {user.Name}, Age: {user.Age}");

            // 在事务中插入数据并提交
            using var transaction = engine.BeginTransaction();
            var userId = collection.Insert(user);
            Console.WriteLine($"插入的用户ID: {userId}");
            transaction.Commit();

            Console.WriteLine("事务已提交");

            // 验证数据在当前引擎中存在
            var currentUser = collection.FindById(userId);
            Console.WriteLine($"当前引擎中查找用户: {(currentUser != null ? $"找到 - {currentUser.Name}" : "未找到")}");

            // 第二阶段：重新创建引擎实例（模拟重启）
            Console.WriteLine("\n=== 第二阶段：模拟重启 ===");
            engine.Dispose();

            using var newEngine = new TinyDbEngine(testFile);
            var newCollection = newEngine.GetCollection<UserWithIntId>();

            Console.WriteLine("新引擎实例已创建");

            // 验证数据持久性
            var persistedUser = newCollection.FindById(userId);
            Console.WriteLine($"重启后查找用户: {(persistedUser != null ? $"找到 - {persistedUser.Name}, Age: {persistedUser.Age}" : "未找到")}");

            // 验证所有数据都持久化了
            var allUsers = newCollection.FindAll().ToList();
            Console.WriteLine($"总用户数: {allUsers.Count}");
            if (allUsers.Count > 0)
            {
                Console.WriteLine($"第一个用户: {allUsers[0].Name}, Age: {allUsers[0].Age}, Id: {allUsers[0].Id}");
            }

            // 判断测试结果
            if (persistedUser != null &&
                persistedUser.Name == "DurableUser" &&
                persistedUser.Age == 25 &&
                allUsers.Count == 1 &&
                allUsers[0].Id == userId)
            {
                Console.WriteLine("\n✅ 持久性测试通过！");
            }
            else
            {
                Console.WriteLine("\n❌ 持久性测试失败！");
                Console.WriteLine("期望: 找到用户 DurableUser, Age 25, 总数1");
                Console.WriteLine($"实际: {(persistedUser != null ? $"找到用户 {persistedUser.Name}, Age {persistedUser.Age}" : "未找到用户")}, 总数{allUsers.Count}");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"\n❌ 发生异常: {ex.Message}");
            Console.WriteLine($"堆栈跟踪: {ex.StackTrace}");
        }
        finally
        {
            // 清理
            if (File.Exists(testFile))
            {
                File.Delete(testFile);
                Console.WriteLine($"\n清理测试文件: {testFile}");
            }
        }

        Console.WriteLine("\n调试完成。");
    }
}