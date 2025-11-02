using TinyDb.Core;
using TinyDb.Collections;
using TinyDb.Demo.Entities;
using TinyDb.Attributes;
using TinyDb.Bson;
using System.Linq.Expressions;

namespace TinyDb.Demo.Demos;

/// <summary>
/// LINQ查询功能演示
/// </summary>
public static class LinqQueryDemo
{
    public static async Task RunAsync()
    {
        Console.WriteLine("=== LINQ查询功能演示 ===");
        Console.WriteLine("展示丰富的查询表达式和筛选功能");
        Console.WriteLine();

        const string dbPath = "linq_demo.db";
        if (File.Exists(dbPath)) File.Delete(dbPath);

        using var engine = new TinyDbEngine(dbPath);
        var employees = engine.GetCollection<Employee>("employees");

        // 准备测试数据
        Console.WriteLine("1. 准备员工数据:");
        await PrepareEmployeeData(employees);
        Console.WriteLine();

        // 基础查询演示
        Console.WriteLine("2. 基础查询演示:");

        // 查询所有员工
        var allEmployees = employees.FindAll().ToList();
        Console.WriteLine($"   📊 总员工数: {allEmployees.Count}");

        // 按条件查询
        var itEmployees = employees.Find(e => e.Department == "IT").ToList();
        Console.WriteLine($"   💻 IT部门员工: {itEmployees.Count}");

        var highSalaryEmployees = employees.Find(e => e.Salary > 8000).ToList();
        Console.WriteLine($"   💰 高薪员工(>8000): {highSalaryEmployees.Count}");
        Console.WriteLine();

        // 复杂条件查询
        Console.WriteLine("3. 复杂条件查询:");

        // 多条件组合
        var seniorItEmployees = employees.Find(e =>
            e.Department == "IT" &&
            e.YearsOfExperience >= 5 &&
            e.Salary >= 10000
        ).ToList();

        Console.WriteLine($"   🔧 资深IT工程师(5年以上&月薪≥10K): {seniorItEmployees.Count}");
        foreach (var emp in seniorItEmployees)
        {
            Console.WriteLine($"      👤 {emp.Name} - {emp.Position} (经验: {emp.YearsOfExperience}年, 薪资: ¥{emp.Salary:N0})");
        }

        // 使用字符串操作
        var managerEmployees = employees.Find(e =>
            e.Position.Contains("经理") || e.Position.Contains("主管")
        ).ToList();

        Console.WriteLine($"   👔 管理层员工: {managerEmployees.Count}");
        foreach (var emp in managerEmployees)
        {
            Console.WriteLine($"      👤 {emp.Name} - {emp.Position} ({emp.Department})");
        }
        Console.WriteLine();

        // 排序和分页
        Console.WriteLine("4. 排序和分页查询:");

        // 按薪资排序（降序）
        var topEarners = employees.FindAll()
            .OrderByDescending(e => e.Salary)
            .Take(5)
            .ToList();

        Console.WriteLine("   💰 薪资排行榜TOP5:");
        for (int i = 0; i < topEarners.Count; i++)
        {
            var emp = topEarners[i];
            Console.WriteLine($"      {i + 1}. {emp.Name} - {emp.Position} (¥{emp.Salary:N0})");
        }

        // 按入职时间排序
        var recentHires = employees.FindAll()
            .OrderByDescending(e => e.HireDate)
            .Take(3)
            .ToList();

        Console.WriteLine("\n   🆕 最新入职员工:");
        foreach (var emp in recentHires)
        {
            Console.WriteLine($"      👤 {emp.Name} - {emp.Department} (入职: {emp.HireDate:yyyy-MM-dd})");
        }
        Console.WriteLine();

        // 聚合查询
        Console.WriteLine("5. 聚合统计查询:");

        // 部门统计
        var departmentStats = employees.FindAll()
            .GroupBy(e => e.Department)
            .Select(g => new
            {
                Department = g.Key,
                Count = g.Count(),
                AvgSalary = g.Average(e => e.Salary),
                MaxSalary = g.Max(e => e.Salary),
                MinSalary = g.Min(e => e.Salary)
            })
            .OrderByDescending(d => d.AvgSalary)
            .ToList();

        Console.WriteLine("   📈 部门薪资统计:");
        foreach (var stat in departmentStats)
        {
            Console.WriteLine($"      🏢 {stat.Department}:");
            Console.WriteLine($"         👥 人数: {stat.Count}");
            Console.WriteLine($"         💰 平均薪资: ¥{stat.AvgSalary:N0}");
            Console.WriteLine($"         📊 薪资范围: ¥{stat.MinSalary:N0} - ¥{stat.MaxSalary:N0}");
        }

        // 年龄分布统计
        var ageGroups = employees.FindAll()
            .GroupBy(e => e.Age / 10 * 10) // 按十年分组
            .Select(g => new
            {
                AgeGroup = $"{g.Key}-{g.Key + 9}岁",
                Count = g.Count(),
                AvgSalary = g.Average(e => e.Salary)
            })
            .OrderBy(g => g.AgeGroup)
            .ToList();

        Console.WriteLine("\n   🎂 年龄分布统计:");
        foreach (var group in ageGroups)
        {
            Console.WriteLine($"      {group.AgeGroup}: {group.Count}人, 平均薪资 ¥{group.AvgSalary:N0}");
        }
        Console.WriteLine();

        // 高级查询表达式
        Console.WriteLine("6. 高级查询表达式:");

        // 复杂的业务逻辑查询
        var candidatesForPromotion = employees.Find(e =>
            e.YearsOfExperience >= 3 &&
            e.Salary < 12000 &&
            e.Department != "HR" &&
            !e.Position.Contains("经理") &&
            !e.Position.Contains("主管")
        ).ToList();

        Console.WriteLine($"   🚀 晋升候选人(经验≥3年&薪资<12K&非管理层): {candidatesForPromotion.Count}");
        foreach (var emp in candidatesForPromotion.Take(5))
        {
            Console.WriteLine($"      👤 {emp.Name} - {emp.Position} ({emp.Department})");
            Console.WriteLine($"         经验: {emp.YearsOfExperience}年, 当前薪资: ¥{emp.Salary:N0}");
        }

        // 使用日期范围查询
        var recentJoiners = employees.Find(e =>
            e.HireDate >= DateTime.Now.AddMonths(-6) &&
            e.HireDate <= DateTime.Now
        ).ToList();

        Console.WriteLine($"\n   📅 近6个月入职员工: {recentJoiners.Count}");
        foreach (var emp in recentJoiners)
        {
            var monthsAgo = (DateTime.Now - emp.HireDate).Days / 30;
            Console.WriteLine($"      👤 {emp.Name} - 入职{monthsAgo}个月前 ({emp.Department})");
        }
        Console.WriteLine();

        // 查询性能展示
        Console.WriteLine("7. 查询性能测试:");

        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        // 执行1000次简单查询
        for (int i = 0; i < 1000; i++)
        {
            var result = employees.Find(e => e.Department == "IT").FirstOrDefault();
        }

        stopwatch.Stop();
        Console.WriteLine($"   ⚡ 1000次简单查询耗时: {stopwatch.ElapsedMilliseconds}ms");
        Console.WriteLine($"   📊 平均每次查询: {stopwatch.ElapsedMilliseconds / 1000.0:F2}ms");

        stopwatch.Restart();

        // 执行100次复杂查询
        for (int i = 0; i < 100; i++)
        {
            var result = employees.Find(e =>
                e.Salary > 5000 &&
                e.YearsOfExperience > 2 &&
                e.Age >= 25 &&
                e.Age <= 45
            ).ToList();
        }

        stopwatch.Stop();
        Console.WriteLine($"   ⚡ 100次复杂查询耗时: {stopwatch.ElapsedMilliseconds}ms");
        Console.WriteLine($"   📊 平均每次查询: {stopwatch.ElapsedMilliseconds / 100.0:F2}ms");

        Console.WriteLine("\n✅ LINQ查询演示完成！");
        Console.WriteLine("🔧 支持丰富的查询表达式：筛选、排序、分组、聚合等");
    }

    private static async Task PrepareEmployeeData(ILiteCollection<Employee> employees)
    {
        var employeeData = new[]
        {
            new Employee { Name = "张伟", Department = "IT", Position = "高级工程师", Age = 32, Salary = 12000, YearsOfExperience = 8, HireDate = DateTime.Now.AddYears(-6) },
            new Employee { Name = "李娜", Department = "IT", Position = "架构师", Age = 35, Salary = 15000, YearsOfExperience = 10, HireDate = DateTime.Now.AddYears(-8) },
            new Employee { Name = "王强", Department = "IT", Position = "工程师", Age = 28, Salary = 8000, YearsOfExperience = 4, HireDate = DateTime.Now.AddYears(-3) },
            new Employee { Name = "刘洋", Department = "IT", Position = "初级工程师", Age = 24, Salary = 6000, YearsOfExperience = 2, HireDate = DateTime.Now.AddYears(-1) },
            new Employee { Name = "陈静", Department = "HR", Position = "HR经理", Age = 30, Salary = 10000, YearsOfExperience = 6, HireDate = DateTime.Now.AddYears(-5) },
            new Employee { Name = "赵敏", Department = "HR", Position = "HR专员", Age = 26, Salary = 7000, YearsOfExperience = 3, HireDate = DateTime.Now.AddYears(-2) },
            new Employee { Name = "孙超", Department = "销售", Position = "销售经理", Age = 34, Salary = 11000, YearsOfExperience = 7, HireDate = DateTime.Now.AddYears(-6) },
            new Employee { Name = "周婷", Department = "销售", Position = "销售代表", Age = 25, Salary = 7500, YearsOfExperience = 3, HireDate = DateTime.Now.AddYears(-2) },
            new Employee { Name = "吴鹏", Department = "销售", Position = "高级销售代表", Age = 29, Salary = 8500, YearsOfExperience = 5, HireDate = DateTime.Now.AddYears(-4) },
            new Employee { Name = "郑雪", Department = "财务", Position = "财务经理", Age = 33, Salary = 10500, YearsOfExperience = 7, HireDate = DateTime.Now.AddYears(-6) },
            new Employee { Name = "王磊", Department = "财务", Position = "会计", Age = 27, Salary = 7200, YearsOfExperience = 4, HireDate = DateTime.Now.AddYears(-3) },
            new Employee { Name = "李芳", Department = "财务", Position = "出纳", Age = 23, Salary = 5500, YearsOfExperience = 1, HireDate = DateTime.Now.AddMonths(-8) },
            new Employee { Name = "张明", Department = "市场", Position = "市场经理", Age = 31, Salary = 9800, YearsOfExperience = 6, HireDate = DateTime.Now.AddYears(-5) },
            new Employee { Name = "刘晓", Department = "市场", Position = "市场专员", Age = 26, Salary = 6800, YearsOfExperience = 3, HireDate = DateTime.Now.AddYears(-2) },
            new Employee { Name = "陈辉", Department = "运营", Position = "运营主管", Age = 30, Salary = 9000, YearsOfExperience = 5, HireDate = DateTime.Now.AddYears(-4) },
            new Employee { Name = "赵琳", Department = "运营", Position = "运营专员", Age = 24, Salary = 6200, YearsOfExperience = 2, HireDate = DateTime.Now.AddMonths(-10) }
        };

        foreach (var emp in employeeData)
        {
            employees.Insert(emp);
            Console.WriteLine($"   ✅ 添加员工: {emp.Name} - {emp.Position} ({emp.Department})");
        }
    }
}

/// <summary>
/// 员工实体（用于LINQ查询演示）
/// </summary>
[Entity("employees")]
public class Employee
{
    [Id]
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();

    public string Name { get; set; } = string.Empty;
    public string Department { get; set; } = string.Empty;
    public string Position { get; set; } = string.Empty;
    public int Age { get; set; }
    public decimal Salary { get; set; }
    public int YearsOfExperience { get; set; }
    public DateTime HireDate { get; set; }
    public string Email { get; set; } = string.Empty;
    public string Phone { get; set; } = string.Empty;
}